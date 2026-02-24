#!/usr/bin/env python3
"""
高管增持数据监控脚本 v2.0
从巨潮资讯网获取高管增持数据，筛选出多位高管集中增持的公司，发送邮件通知。

主要功能：
1. 过滤大股东增持，只保留普通高管/董监高增持
2. 增加金额门槛（增持金额 vs 市值、年薪对比）
3. 新增公司标记（🆕）
4. 排除ST/退市风险股
5. 只在交易日运行
6. 补充股价数据和技术分析
7. 扩大查询时间窗口（3个月）
8. 优化邮件报告
"""

import json
import os
import logging
import smtplib
import sys
import re
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import akshare as ak
import numpy as np
import pandas as pd
import requests

from config import (
    EMAIL_PASSWORD,
    EMAIL_RECEIVER,
    EMAIL_SENDER,
    EXCLUDE_KEYWORDS,
    EXEC_SALARY_MULTIPLIER,
    HISTORY_FILE,
    MIN_EXECUTIVES,
    MIN_MARKET_CAP_RATIO,
    QUERY_MONTHS,
    QUERY_SYMBOL,
    SMTP_PORT,
    SMTP_SERVER,
    TRADE_METHODS,
)

# 日志配置
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "stock-monitor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def get_index_volume_price_data() -> List[Dict]:
    """获取指数ETF量价数据（陈老师量价法）"""
    ETF_LIST = [
        {"name": "沪深300ETF", "code": "510300"},
        {"name": "中证500ETF", "code": "512500"},
        {"name": "创业板ETF", "code": "159957"},
        {"name": "科创50ETF", "code": "588000"},
        {"name": "恒生科技ETF", "code": "513130"},
    ]
    
    results = []
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")  # 1年+buffer
    
    for etf in ETF_LIST:
        try:
            log.info(f"获取ETF数据: {etf['name']} ({etf['code']})")
            df = ak.fund_etf_hist_em(symbol=etf['code'], period="daily",
                                      start_date=start_date, end_date=end_date, adjust="qfq")
            if df is None or df.empty:
                log.warning(f"  {etf['code']} 无数据")
                continue
            
            df = df.sort_values('日期').reset_index(drop=True)
            closes = df['收盘'].values
            volumes = df['成交量'].values
            
            if len(closes) < 60:
                log.warning(f"  {etf['code']} 数据不足60日")
                continue
            
            current_price = closes[-1]
            prev_price = closes[-2] if len(closes) >= 2 else current_price
            change_pct = (current_price - prev_price) / prev_price * 100
            
            ma20 = closes[-20:].mean()
            ma60 = closes[-60:].mean()
            
            # 趋势判断
            trend = "上行" if current_price > ma60 else "下行"
            
            # 成交量均值
            vol_20 = volumes[-20:].mean()
            vol_60 = volumes[-60:].mean()
            
            # 成交量分位：20日均量在过去1年日均量中的百分位
            # 用滚动20日均量序列
            if len(volumes) >= 40:
                rolling_20_vols = []
                for i in range(20, len(volumes) + 1):
                    rolling_20_vols.append(volumes[i-20:i].mean())
                rolling_20_vols = np.array(rolling_20_vols)
                vol_percentile = (rolling_20_vols < vol_20).sum() / len(rolling_20_vols) * 100
            else:
                vol_percentile = 50
            
            # 量价信号
            if vol_percentile < 20 and current_price < ma60:
                signal = "🟢 地量低位（左侧买点）"
            elif vol_percentile < 20 and current_price >= ma60:
                signal = "🟡 缩量上行（观察）"
            elif vol_percentile > 80 and current_price > ma60:
                signal = "🔴 天量高位（注意风险）"
            elif vol_percentile > 80 and current_price <= ma60:
                signal = "🟡 放量下跌（恐慌）"
            else:
                signal = "⏳ 正常"
            
            results.append({
                "name": etf['name'],
                "code": etf['code'],
                "current_price": current_price,
                "change_pct": change_pct,
                "ma20": ma20,
                "ma60": ma60,
                "trend": trend,
                "vol_20": vol_20,
                "vol_60": vol_60,
                "vol_percentile": vol_percentile,
                "signal": signal,
            })
            log.info(f"  {etf['name']}: 价格={current_price:.3f}, 涨跌={change_pct:.2f}%, 趋势={trend}, 量分位={vol_percentile:.0f}%, 信号={signal}")
        except Exception as e:
            log.warning(f"获取 {etf['name']} ({etf['code']}) 失败: {e}")
    
    return results


def is_trading_day() -> bool:
    """检查今天是否为交易日"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        cal = ak.tool_trade_date_hist_sina()
        cal['trade_date'] = cal['trade_date'].astype(str)
        return today in cal['trade_date'].values
    except Exception as e:
        log.warning(f"无法获取交易日历，默认为交易日: {e}")
        return True


def fetch_data() -> pd.DataFrame:
    """从巨潮资讯网获取高管增持明细数据"""
    log.info("正在从巨潮资讯网获取数据...")
    df = ak.stock_hold_management_detail_cninfo(symbol=QUERY_SYMBOL)
    log.info(f"获取到 {len(df)} 条记录，日期范围: {df['截止日期'].min()} ~ {df['截止日期'].max()}")
    return df


def filter_major_shareholders(df: pd.DataFrame) -> pd.DataFrame:
    """过滤大股东增持，只保留普通高管增持"""
    before_count = len(df)
    
    # 过滤包含大股东关键词的记录
    for keyword in EXCLUDE_KEYWORDS:
        df = df[~df["董监高职务"].str.contains(keyword, na=False)]
    
    log.info(f"过滤大股东增持后：{before_count} -> {len(df)} 条记录")
    return df


def filter_st_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """排除ST/退市风险股"""
    before_count = len(df)
    
    # 排除证券简称包含ST、*ST、退的股票
    st_pattern = r'(\*?ST|退)'
    df = df[~df["证券简称"].str.contains(st_pattern, na=False, regex=True)]
    
    log.info(f"排除ST/退市风险股后：{before_count} -> {len(df)} 条记录")
    return df


def get_market_cap(stock_code: str) -> Optional[float]:
    """获取公司市值（亿元）"""
    try:
        info = ak.stock_individual_info_em(symbol=stock_code)
        for _, row in info.iterrows():
            if row['item'] == '总市值':
                val = row['value']
                if isinstance(val, (int, float)):
                    return val / 1e8
                else:
                    return float(str(val).replace(',', '')) / 1e8
        return None
    except Exception as e:
        log.warning(f"获取 {stock_code} 市值失败: {e}")
        return None


def get_executive_salaries(stock_code: str) -> Dict[str, float]:
    """从东方财富获取高管薪酬数据，返回{高管姓名: 年薪万元}"""
    try:
        url = 'https://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/PageAjax'
        params = {
            'code': f'SZ{stock_code}' if stock_code.startswith('3') or stock_code.startswith('0') else f'SH{stock_code}'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://emweb.securities.eastmoney.com/'
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if 'gglb' in data:
            salaries = {}
            for exec_info in data['gglb']:
                name = exec_info.get('PERSON_NAME', '').strip()
                salary = exec_info.get('SALARY', 0)
                if name and salary and salary > 0:
                    # SALARY 单位是元，转为万元
                    salaries[name] = salary / 10000
            
            log.info(f"获取 {stock_code} 高管薪酬: {len(salaries)} 位高管")
            return salaries
        else:
            log.warning(f"{stock_code} 未获取到高管薪酬数据")
            return {}
    except Exception as e:
        log.warning(f"获取 {stock_code} 高管薪酬失败: {e}")
        return {}


def get_stock_price_data(stock_code: str, earliest_date: str = None) -> Dict:
    """获取股票价格和技术指标数据"""
    try:
        # 获取最近3个月的日K线数据（确保至少90个交易日）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                               start_date=start_date, end_date=end_date, adjust="qfq")
        
        if df.empty:
            return {}
        
        # 计算技术指标
        closes = df['收盘'].values
        
        # 均线（10/20/30/60）
        ma10 = closes[-10:].mean() if len(closes) >= 10 else None
        ma20 = closes[-20:].mean() if len(closes) >= 20 else None
        ma30 = closes[-30:].mean() if len(closes) >= 30 else None
        ma60 = closes[-60:].mean() if len(closes) >= 60 else None
        
        current_price = closes[-1]
        prev_price = closes[-2] if len(closes) >= 2 else current_price
        
        # BIAS偏离率（20/30/60）
        bias20 = ((current_price - ma20) / ma20 * 100) if ma20 else None
        bias30 = ((current_price - ma30) / ma30 * 100) if ma30 else None
        bias60 = ((current_price - ma60) / ma60 * 100) if ma60 else None
        
        # 判断均线排列（用10/20/30/60，精细分级）
        ma_status = "未知"
        timing_signal = "观望"
        if all(x is not None for x in [ma10, ma20, ma30, ma60]):
            if current_price > ma10 > ma20 > ma30 > ma60:
                ma_status = "完美多头"
                timing_signal = "🟢 可买入"
            elif current_price > ma10 and ma10 > ma20:
                ma_status = "准多头"
                timing_signal = "🟡 关注"
            elif ma10 > ma20 > ma30 > ma60:
                ma_status = "多头排列"
                timing_signal = "🟢 可买入"
            elif ma10 < ma20 < ma30 < ma60:
                ma_status = "空头排列"
                timing_signal = "🔴 回避"
            elif ma10 < ma20 < ma30:
                ma_status = "偏空"
                timing_signal = "🔴 回避"
            else:
                ma_status = "均线纠缠"
                # 纠缠中看是否站上MA20
                if current_price > ma20:
                    timing_signal = "🟡 关注"
                else:
                    timing_signal = "⏳ 等待站上MA20"
        
        # 如果提供了最早增持日期，计算增持公告日涨跌幅
        announcement_return = None
        if earliest_date:
            try:
                # 确保日期格式正确
                if isinstance(earliest_date, str):
                    announcement_date = datetime.strptime(earliest_date, "%Y-%m-%d")
                else:
                    announcement_date = earliest_date
                
                # 在历史数据中找到对应日期
                df['日期'] = pd.to_datetime(df['日期'])
                df = df.sort_values('日期')
                
                # 找到增持公告日或之后第一个交易日的收盘价
                announcement_price = None
                for _, row in df.iterrows():
                    if row['日期'].date() >= announcement_date.date():
                        announcement_price = row['收盘']
                        break
                
                if announcement_price:
                    announcement_return = (current_price - announcement_price) / announcement_price * 100
                    
            except Exception as e:
                log.warning(f"计算 {stock_code} 增持公告日涨跌幅失败: {e}")
        
        # 60日最高价（用于回撤止损计算）
        high_60d = float(df['最高'].tail(60).max()) if len(df) >= 60 else float(df['最高'].max())
        
        return {
            "current_price": current_price,
            "prev_price": prev_price,
            "price_change_pct": ((current_price - prev_price) / prev_price * 100),
            "ma10": ma10,
            "ma20": ma20,
            "ma30": ma30,
            "ma60": ma60,
            "high_60d": high_60d,
            "bias20": bias20,
            "bias30": bias30,
            "bias60": bias60,
            "ma_status": ma_status,
            "timing_signal": timing_signal,
            "announcement_return": announcement_return,
        }
    except Exception as e:
        log.warning(f"获取 {stock_code} 股价数据失败: {e}")
        return {}


def get_fundamental_data(stock_code: str) -> Dict:
    """获取基本面数据（同花顺财务摘要）"""
    result = {"revenue": None, "net_profit": None, "revenue_growth": None,
              "profit_growth": None, "roe": None, "pe_ratio": None, "pb_ratio": None,
              "prev_net_profit": None, "profit_trend": None, "industry": None}
    try:
        # 从同花顺获取财务摘要（最可靠的接口）
        fin = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按年度")
        if fin is not None and not fin.empty:
            latest = fin.iloc[-1]
            # 解析带"亿"的字符串
            def parse_amount(val):
                if pd.isna(val) or val is None or val == False:
                    return None
                s = str(val).replace(',', '')
                if '亿' in s:
                    return float(s.replace('亿', ''))
                elif '万' in s:
                    return float(s.replace('万', '')) / 10000
                try:
                    return float(s) / 1e8  # 假设是元
                except:
                    return None

            def parse_pct(val):
                if pd.isna(val) or val is None or val == False:
                    return None
                s = str(val).replace('%', '').replace(',', '')
                try:
                    return float(s)
                except:
                    return None

            result["revenue"] = parse_amount(latest.get("营业总收入"))
            result["net_profit"] = parse_amount(latest.get("净利润"))
            result["revenue_growth"] = parse_pct(latest.get("营业总收入同比增长率"))
            result["profit_growth"] = parse_pct(latest.get("净利润同比增长率"))
            result["roe"] = parse_pct(latest.get("净资产收益率"))

            # 获取前一年净利润用于判断利润趋势
            if len(fin) >= 2:
                prev_row = fin.iloc[-2]
                result["prev_net_profit"] = parse_amount(prev_row.get("净利润"))
            
            # 判断利润趋势
            if result["net_profit"] is not None and result["prev_net_profit"] is not None:
                diff_ratio = (result["net_profit"] - result["prev_net_profit"]) / abs(result["prev_net_profit"]) if result["prev_net_profit"] != 0 else 0
                if diff_ratio > 0.1:
                    result["profit_trend"] = "上升"
                elif diff_ratio < -0.1:
                    result["profit_trend"] = "下降"
                else:
                    result["profit_trend"] = "持平"

        # 获取行业信息
        try:
            info_industry = ak.stock_individual_info_em(symbol=stock_code)
            for _, row in info_industry.iterrows():
                if row['item'] == '行业':
                    result["industry"] = str(row['value'])
                    break
        except Exception:
            pass

        # PE/PB：从市值和财务数据计算
        info = ak.stock_individual_info_em(symbol=stock_code)
        total_market_cap = None
        for _, row in info.iterrows():
            if row['item'] == '总市值':
                val = row['value']
                total_market_cap = float(val) if isinstance(val, (int, float)) else None
        
        # PE-TTM = 总市值 / 最近4个季度扣非净利润（陈老师要求：排除一次性损益）
        # 优先用扣非净利润，失败则回退到净利润，再失败用年报
        ttm_profit = None
        ttm_type = None
        try:
            fin_q = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
            if fin_q is not None and not fin_q.empty:
                latest_q = fin_q.iloc[-1]
                latest_period = str(latest_q.get("报告期", ""))
                
                # 优先扣非净利润，回退到净利润
                for profit_col, label in [("扣非净利润", "扣非TTM"), ("净利润", "TTM")]:
                    if ttm_profit is not None:
                        break
                    latest_q_profit = parse_amount(latest_q.get(profit_col))
                    if latest_q_profit is None:
                        continue
                    
                    if latest_period.endswith("12-31"):
                        ttm_profit = latest_q_profit
                        ttm_type = label
                    else:
                        year = int(latest_period[:4])
                        prev_year_annual = None
                        prev_year_same_q = None
                        for _, r in fin_q.iterrows():
                            p = str(r.get("报告期", ""))
                            if p == f"{year-1}-12-31":
                                prev_year_annual = parse_amount(r.get(profit_col))
                            if p == f"{year-1}-{latest_period[5:]}":
                                prev_year_same_q = parse_amount(r.get(profit_col))
                        if prev_year_annual is not None and prev_year_same_q is not None:
                            ttm_profit = latest_q_profit + prev_year_annual - prev_year_same_q
                            ttm_type = label
        except Exception as e:
            log.debug(f"TTM计算失败: {e}")
        
        if total_market_cap and ttm_profit and ttm_profit > 0:
            result["pe_ratio"] = round(total_market_cap / 1e8 / ttm_profit, 2)
            result["pe_type"] = ttm_type
        elif total_market_cap and result["net_profit"] and result["net_profit"] > 0:
            result["pe_ratio"] = round(total_market_cap / 1e8 / result["net_profit"], 2)
            result["pe_type"] = "静态"
        
        # PB: 用每股净资产计算
        try:
            bvps = float(str(fin.iloc[-1].get("每股净资产", "0")).replace(',', ''))
        except:
            bvps = None
        eps_val = bvps
        if eps_val and eps_val > 0:
            current_price_val = None
            for _, row in info.iterrows():
                if row['item'] == '最新':
                    current_price_val = float(row['value']) if isinstance(row['value'], (int, float)) else None
            if current_price_val:
                result["pb_ratio"] = round(current_price_val / eps_val, 2)

        log.info(f"  {stock_code} 基本面: 营收={result['revenue']}, 净利={result['net_profit']}, ROE={result['roe']}, PE={result['pe_ratio']}")
    except Exception as e:
        log.warning(f"获取 {stock_code} 基本面数据失败: {e}")
    return result


def get_latest_announcements(stock_code: str) -> Dict:
    """获取最新公告并按关键词分类
    
    返回: {
        "announcements": [{"date": str, "title": str, "category": str}],
        "signals": {"has_buyback": bool, "has_insider_sell": bool, "has_lawsuit": bool, "has_earnings_forecast": bool},
        "summary": "一句话摘要"
    }
    """
    try:
        # 计算查询时间范围（最近3个月）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        
        # 获取公告数据
        df = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=stock_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if df is None or df.empty:
            return {
                "announcements": [],
                "signals": {"has_buyback": False, "has_insider_sell": False, "has_lawsuit": False, "has_earnings_forecast": False},
                "summary": "暂无公告"
            }
        
        # 关键词分类映射
        category_keywords = {
            "业绩": ["业绩快报", "业绩预告", "年报", "中报", "季报", "盈利预告"],
            "回购": ["回购", "股份回购", "自然人回购"],
            "增持": ["增持", "股东增持", "高管增持"],
            "减持": ["减持", "股东减持", "高管减持"],
            "风险": ["处罚", "诉讼", "仲裁", "调查", "立案", "违规", "风险"],
            "重组": ["重大资产重组", "并购", "收购", "兼并", "注入", "置换"]
        }
        
        announcements = []
        signals = {"has_buyback": False, "has_insider_sell": False, "has_lawsuit": False, "has_earnings_forecast": False}
        
        for _, row in df.iterrows():
            title = str(row.get('公告标题', ''))
            # 去掉HTML标签
            title = re.sub(r'<[^>]+>', '', title)
            date = str(row.get('公告时间', ''))
            
            # 分类标记
            category = "其他"
            for cat, keywords in category_keywords.items():
                if any(keyword in title for keyword in keywords):
                    category = cat
                    break
            
            # 更新信号
            if category == "回购" or any(kw in title for kw in ["回购", "股份回购"]):
                signals["has_buyback"] = True
            elif category == "减持" or any(kw in title for kw in ["减持"]):
                signals["has_insider_sell"] = True
            elif category == "风险" or any(kw in title for kw in ["诉讼", "仲裁", "调查", "处罚"]):
                signals["has_lawsuit"] = True
            elif category == "业绩" or any(kw in title for kw in ["业绩预告", "业绩快报"]):
                signals["has_earnings_forecast"] = True
            
            announcements.append({
                "date": date,
                "title": title,
                "category": category
            })
        
        # 按日期排序，最新的在前
        announcements.sort(key=lambda x: x["date"], reverse=True)
        
        # 生成一句话摘要
        summary_parts = []
        if signals["has_buyback"]:
            summary_parts.append("有回购")
        if signals["has_insider_sell"]:
            summary_parts.append("有减持")
        if signals["has_lawsuit"]:
            summary_parts.append("有风险")
        if signals["has_earnings_forecast"]:
            summary_parts.append("有业绩预告")
        
        if not summary_parts:
            summary = f"近3个月共{len(announcements)}条公告，无重要信号"
        else:
            summary = f"近3个月{len(announcements)}条公告：{'/'.join(summary_parts)}"
        
        log.info(f"获取 {stock_code} 公告监控: {len(announcements)}条，信号={summary_parts}")
        return {
            "announcements": announcements[:10],  # 只保留最新10条
            "signals": signals,
            "summary": summary
        }
        
    except Exception as e:
        log.warning(f"获取 {stock_code} 公告监控失败: {e}")
        return {
            "announcements": [],
            "signals": {"has_buyback": False, "has_insider_sell": False, "has_lawsuit": False, "has_earnings_forecast": False},
            "summary": "获取失败"
        }


def get_holding_announcements(stock_code: str) -> List[Dict]:
    """获取股票的增持公告"""
    try:
        # 计算查询时间范围（最近3个月）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=QUERY_MONTHS * 30)).strftime("%Y%m%d")
        
        # 使用akshare获取增持公告
        df = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=stock_code, 
            keyword='增持',
            start_date=start_date,
            end_date=end_date
        )
        
        if df is None or df.empty:
            return []
        
        # 检查必要的列是否存在
        required_cols = ['公告标题', '公告时间']
        if not all(col in df.columns for col in required_cols):
            log.warning(f"获取 {stock_code} 增持公告: 数据格式异常，列名: {df.columns.tolist()}")
            return []
        
        # 提取公告信息
        announcements = []
        for _, row in df.iterrows():
            title = str(row.get('公告标题', ''))
            # 去掉HTML标签
            title = re.sub(r'<[^>]+>', '', title)
            
            announcement = {
                'title': title,
                'date': str(row.get('公告时间', '')),
                'link': str(row.get('公告链接', ''))
            }
            announcements.append(announcement)
        
        log.info(f"获取 {stock_code} 增持公告: {len(announcements)} 条")
        return announcements
    except Exception as e:
        log.warning(f"获取 {stock_code} 增持公告失败: {e}")
        return []


def generate_sell_signals(price_data: Dict, fundamental_data: Dict, announcement_data: Dict,
                          valuation_pass: bool = False, ma_status: str = "") -> List[Dict]:
    """生成卖出/持有信号
    
    逻辑分两个阶段：
    - 建仓期（三重过滤通过，刚买入）：止损看MA60，给MA20/30空间整理
    - 持有期（已站上MA20后）：跌破MA20减仓，跌破MA30清仓
    
    参数:
        price_data: 价格技术数据
        fundamental_data: 基本面数据
        announcement_data: 公告数据
        valuation_pass: 估值是否通过三重过滤
        ma_status: 均线状态描述
    返回: [{"signal": str, "level": "danger"|"warning"|"info", "action": str}]
    """
    signals = []
    
    try:
        current_price = price_data.get('current_price')
        ma10 = price_data.get('ma10')
        ma20 = price_data.get('ma20')
        ma30 = price_data.get('ma30')
        ma60 = price_data.get('ma60')
        high_60d = price_data.get('high_60d')  # 60日最高价
        profit_trend = fundamental_data.get('profit_trend')
        net_profit = fundamental_data.get('net_profit')
        ann_signals = announcement_data.get('signals', {})
        has_insider_sell = ann_signals.get('has_insider_sell', False)
        has_lawsuit = ann_signals.get('has_lawsuit', False)
        has_buyback = ann_signals.get('has_buyback', False)
        
        # 判断是否处于多头排列（持有期）还是整理期（建仓期）
        is_bullish = False
        if current_price and ma10 and ma20 and ma30:
            is_bullish = current_price > ma10 > ma20 > ma30
        
        is_above_ma20 = current_price > ma20 if (current_price and ma20) else False
        
        # === 亏损股直接标记 ===
        if net_profit is not None and net_profit < 0:
            signals.append({
                "signal": "公司亏损",
                "level": "danger",
                "action": "亏损股建议清仓"
            })
        
        # === 均线信号（区分阶段） ===
        if current_price and ma60 and current_price < ma60:
            # 跌破MA60 — 无论什么阶段都是强烈卖出信号
            signals.append({
                "signal": "价格跌破MA60",
                "level": "danger",
                "action": "清仓（趋势破坏）"
            })
        elif is_above_ma20 and is_bullish:
            # 多头排列中 — 持有期
            signals.append({
                "signal": "多头排列中",
                "level": "info",
                "action": "持有，跌破MA20时减仓"
            })
        elif current_price and ma20 and current_price < ma20 and current_price and ma30 and current_price >= ma30:
            # 在MA20和MA30之间 — 整理区
            if valuation_pass:
                # 三重过滤通过的股票，这里是正常建仓/持有区间
                signals.append({
                    "signal": "MA20-MA30整理区",
                    "level": "info",
                    "action": "建仓区间，MA60为止损线"
                })
            else:
                signals.append({
                    "signal": "价格跌破MA20",
                    "level": "warning",
                    "action": "观望，不宜新建仓"
                })
        elif current_price and ma30 and current_price < ma30 and current_price and ma60 and current_price >= ma60:
            # 跌破MA30但还在MA60上方
            if valuation_pass:
                signals.append({
                    "signal": "跌破MA30",
                    "level": "warning",
                    "action": "减仓至半仓，MA60为底线"
                })
            else:
                signals.append({
                    "signal": "跌破MA30",
                    "level": "danger",
                    "action": "清仓"
                })
        
        # === 止损线：从60日最高点回撤>15% ===
        if high_60d and current_price and high_60d > 0:
            drawdown = (high_60d - current_price) / high_60d
            if drawdown > 0.15:
                signals.append({
                    "signal": f"从高点回撤{drawdown:.0%}",
                    "level": "danger",
                    "action": "触发止损（回撤>15%）"
                })
        
        # === 基本面信号 ===
        if profit_trend == "下降":
            signals.append({
                "signal": "利润趋势下降",
                "level": "warning",
                "action": "注意基本面恶化"
            })
        
        # === 公告信号 ===
        if has_insider_sell:
            signals.append({
                "signal": "高管/股东减持",
                "level": "danger",
                "action": "内部人在卖，警惕"
            })
        
        if has_lawsuit:
            signals.append({
                "signal": "涉及诉讼/处罚",
                "level": "warning",
                "action": "关注诉讼进展"
            })
        
        if has_buyback:
            signals.append({
                "signal": "公司回购股份",
                "level": "info",
                "action": "利好，公司认为低估"
            })
        
        # 无信号时
        if not signals:
            signals.append({
                "signal": "无异常信号",
                "level": "info",
                "action": "正常持有"
            })
        
        log.info(f"生成卖出信号: {len(signals)}个信号")
        return signals
        
    except Exception as e:
        log.warning(f"生成卖出信号失败: {e}")
        return [{"signal": "信号生成失败", "level": "warning", "action": "无法判断"}]


def calculate_avg_holding_price(company_data: pd.DataFrame) -> float:
    """计算高管持仓加权平均价"""
    try:
        # 筛选有效的增持记录（排除减持）
        buy_records = company_data[company_data["变动数量"] > 0].copy()
        
        if buy_records.empty:
            return None
        
        # 计算加权平均价：sum(成交均价 * 变动数量) / sum(变动数量)
        total_value = 0
        total_shares = 0
        
        for _, row in buy_records.iterrows():
            price = row.get("成交均价", 0)
            shares = row.get("变动数量", 0)
            
            if pd.notna(price) and pd.notna(shares) and price > 0 and shares > 0:
                total_value += price * shares
                total_shares += shares
        
        if total_shares > 0:
            return total_value / total_shares
        else:
            return None
            
    except Exception as e:
        log.warning(f"计算加权平均价失败: {e}")
        return None


# 周期行业关键词列表
CYCLICAL_INDUSTRY_KEYWORDS = [
    "化工", "建材", "水泥", "煤炭", "钢铁", "有色", "券商", "证券",
    "养殖", "猪", "石油", "天然气", "航运", "船舶", "电力", "发电"
]


def classify_stock_type(fundamental_data: Dict) -> str:
    """自动分类股票类型：亏损/成长股/周期股/价值股/一般"""
    net_profit = fundamental_data.get("net_profit")
    revenue_growth = fundamental_data.get("revenue_growth") or 0
    profit_growth = fundamental_data.get("profit_growth") or 0
    roe = fundamental_data.get("roe") or 0
    industry = fundamental_data.get("industry") or ""
    prev_net_profit = fundamental_data.get("prev_net_profit")

    # 亏损股
    if net_profit is not None and net_profit < 0:
        return "亏损"

    # 周期股：行业匹配 OR 利润波动大
    is_cyclical_industry = any(kw in industry for kw in CYCLICAL_INDUSTRY_KEYWORDS)
    profit_volatile = False
    if net_profit is not None and prev_net_profit is not None and prev_net_profit != 0:
        change_ratio = abs((net_profit - prev_net_profit) / abs(prev_net_profit))
        if change_ratio > 0.5:  # 利润波动超过50%
            profit_volatile = True
    if is_cyclical_industry or profit_volatile:
        return "周期股"

    # 成长股
    if revenue_growth > 15 and profit_growth > 15:
        return "成长股"

    # 价值股
    if roe > 8 and revenue_growth < 15 and profit_growth < 15:
        return "价值股"

    return "一般"


def evaluate_by_type(stock_type: str, fundamental_data: Dict) -> Tuple[bool, str]:
    """按股票分类判断估值是否合理，返回 (是否通过, 评估描述)"""
    pe = fundamental_data.get("pe_ratio") or 0
    pb = fundamental_data.get("pb_ratio") or 0
    profit_growth = fundamental_data.get("profit_growth") or 0
    profit_trend = fundamental_data.get("profit_trend") or "持平"

    if stock_type == "亏损":
        return False, "❌基本面不合格(亏损)"

    if stock_type == "成长股":
        if profit_growth > 0:
            peg = pe / profit_growth if profit_growth != 0 else 999
            if peg < 1.5:
                return True, f"✅成长股PEG={peg:.2f}合理"
            elif peg <= 2:
                return True, f"⚠️成长股PEG={peg:.2f}偏高"
            else:
                return False, f"❌成长股PEG={peg:.2f}高估"
        else:
            return False, "❌成长股利润负增长"

    if stock_type == "周期股":
        trend_str = f"利润{profit_trend}"
        if pb < 1.5:
            return True, f"✅周期股PB={pb:.2f}低估({trend_str})"
        elif pb <= 2.5:
            if profit_trend == "上升":
                return True, f"✅周期股PB={pb:.2f}合理+{trend_str}"
            else:
                return False, f"⚠️周期股PB={pb:.2f}合理但{trend_str}"
        else:
            return False, f"❌周期股PB={pb:.2f}偏高({trend_str})"

    if stock_type == "价值股":
        if pe > 0 and pe < 15:
            return True, f"✅价值股PE={pe:.1f}合理"
        elif pe >= 15 and pe <= 20:
            return True, f"⚠️价值股PE={pe:.1f}偏高"
        elif pe > 20:
            return False, f"❌价值股PE={pe:.1f}高估"
        else:
            return True, f"✅价值股PE数据异常，默认通过"

    # 一般类型：简单看PE
    if pe > 0 and pe < 30:
        return True, f"✅PE={pe:.1f}尚可"
    elif pe >= 30:
        return False, f"⚠️PE={pe:.1f}偏高"
    else:
        return True, "PE数据不足，默认通过"


def generate_investment_opinion(stock_name: str, fundamental_data: Dict, price_data: Dict, holding_data: Dict, freshness: str = "", chase_risk: str = "", hist_stats: Dict = None, stock_type: str = "一般", valuation_pass: bool = True, valuation_desc: str = "") -> Tuple[str, str]:
    """生成有态度的投资决策分析"""
    
    # 基本数据提取
    net_profit = fundamental_data.get('net_profit', 0)
    roe = fundamental_data.get('roe', 0) or 0
    pe_ratio = fundamental_data.get('pe_ratio', 0) or 0
    ma_status = price_data.get('ma_status', '未知')
    bias20 = price_data.get('bias20', 0) or 0
    salary_ratio = holding_data.get('salary_ratio', 0) or 0
    
    # 判断是否亏损公司
    is_loss_company = net_profit is not None and net_profit < 0
    
    # 判断均线状态
    is_bullish_ma = ma_status == '多头排列'
    is_bearish_ma = ma_status == '空头排列'
    
    # 判断BIAS是否合理
    is_bias_reasonable = abs(bias20) <= 10
    
    # 判断估值和盈利能力
    is_high_valuation = pe_ratio > 50
    is_weak_profitability = roe < 5
    
    # 判断增持信号强度
    is_strong_signal = salary_ratio > 2
    is_weak_signal = salary_ratio < 0.1 and salary_ratio > 0
    
    # 生成观点
    if is_loss_company:
        recommendation = "🔴"
        analysis = f"公司持续亏损，高管增持可能是政治任务/配合维稳，信号强度大打折扣。"
        if is_strong_signal:
            analysis += f"尽管高管用{salary_ratio:.1f}倍年薪增持显示信心，但业绩亏损是硬伤。"
    elif not is_loss_company and is_bullish_ma and is_bias_reasonable:
        recommendation = "🟢"
        analysis = f"盈利稳健+均线多头排列+技术位置健康，基本逻辑完整。"
        if is_strong_signal:
            analysis += f"高管真金白银用{salary_ratio:.1f}倍年薪重仓买入，信号极强，值得持有。"
        elif is_weak_signal:
            analysis += f"增持金额相对薪资偏小（{salary_ratio:.1f}倍），信号一般。"
        else:
            analysis += "高管增持配合技术面向好，可持有待涨。"
    elif not is_loss_company and is_bearish_ma:
        recommendation = "🟡"
        analysis = f"基本面尚可但技术面承压（{ma_status}），建议等待企稳信号。"
        if is_strong_signal:
            analysis += f"好在高管用{salary_ratio:.1f}倍年薪增持，说明对公司前景非常笃定。"
        else:
            analysis += "波段操作为宜，不建议重仓。"
    else:
        recommendation = "🟡"
        analysis = f"公司基本面和技术面都处于中性状态。"
        if is_strong_signal:
            analysis += f"高管{salary_ratio:.1f}倍年薪增持是亮点，可适度关注。"
        else:
            analysis += "增持信号一般，建议观望。"
    
    # 信号新鲜度调整
    if freshness == "🔥 新鲜":
        if recommendation == "🟡":
            analysis += " 信号新鲜（7天内），关注度提升。"
    elif freshness == "💤 过期":
        if recommendation == "🟢":
            recommendation = "🟡"
            analysis += " 但增持信号已过期（>15天），信号衰减。"
    
    # 追高风险调整
    if chase_risk == "⚠️追高风险":
        if recommendation == "🟢":
            recommendation = "🟡"
        analysis += " 公告后涨幅>30%，追高风险大，建议等待回调。"
    elif chase_risk == "⚡注意涨幅":
        analysis += " 公告后涨幅>20%，注意追高风险。"
    
    # 额外风险提示
    warnings = []
    if is_high_valuation:
        warnings.append("估值偏高")
    if is_weak_profitability:
        warnings.append("盈利能力偏弱")
    
    if warnings:
        analysis += f" ⚠️{'/'.join(warnings)}，需注意风险。"
    
    # 历史持续增持加分
    if hist_stats:
        hist_waves = hist_stats.get('历史增持波次', 0)
        hist_duration = hist_stats.get('增持持续月数', 0)
        if hist_waves >= 3 and hist_duration >= 3:
            analysis += f" 🔄高管持续加仓{hist_waves}个月（跨度{hist_duration}个月），长期看好信号极强。"
            if recommendation == "🟡":
                recommendation = "🟢"
        elif hist_waves >= 2:
            analysis += f" 🔄历史有{hist_waves}个月增持记录，非一次性行为。"
    
    # 择时信号（均线排列）
    timing = price_data.get('timing_signal', '观望')
    if '可买入' in timing:
        analysis += " 📊均线多头排列，择时信号良好。"
    elif '回避' in timing:
        analysis += " 📊均线空头/偏空，择时不佳。"
        if recommendation == "🟢":
            recommendation = "🟡"  # 基本面好但技术面差，降级
    elif '等待' in timing:
        analysis += f" 📊均线纠缠中，等待站上MA20再入场。"
    
    # ====== 陈老师三重过滤 ======
    # 第一重：高管增持（已满足，能进入此函数说明增持≥5人）
    filter1_pass = True
    # 第二重：基本面分类+估值
    filter2_pass = valuation_pass
    # 第三重：均线择时
    filter3_pass = '可买入' in timing
    filter3_neutral = '关注' in timing or '等待' in timing
    
    filter_icons = f"{'✅' if filter1_pass else '❌'}{'✅' if filter2_pass else '❌'}{'✅' if filter3_pass else '❌'}"
    
    # 三重过滤综合判断（覆盖之前的recommendation）
    if filter1_pass and filter2_pass and filter3_pass:
        recommendation = "🟢"
        triple_result = "🟢 三重过滤通过 — 建仓30%"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{ma_status} → 建仓30%。" + analysis
    elif filter1_pass and filter2_pass and filter3_neutral:
        recommendation = "🟡"
        triple_result = "🟡 等待均线确认"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{ma_status} → 等待均线确认。" + analysis
    elif filter1_pass and filter2_pass and not filter3_pass:
        recommendation = "🟡"
        triple_result = "🟡 等待均线确认"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{ma_status} → 等待均线走好。" + analysis
    elif filter1_pass and not filter2_pass and filter3_pass:
        recommendation = "🟡"
        triple_result = "⚠️ 技术面好但基本面存疑"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{ma_status} → 基本面存疑，观望。" + analysis
    else:
        recommendation = "🔴"
        triple_result = "🔴 不满足买入条件"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{ma_status} → 不满足买入条件。" + analysis
    
    # 综合操作建议
    if recommendation == "🟢" and chase_risk in ("✅低位机会", "🟡正常"):
        analysis += " 💰操作建议：三重过滤通过，建仓30%！"
    elif recommendation == "🟢":
        analysis += " 💰操作建议：趋势向好，持有待涨。"
    elif recommendation == "🟡" and '回避' in timing:
        analysis += " 💰操作建议：放入自选观察，等均线走好再买。"
    elif recommendation == "🟡":
        analysis += " 💰操作建议：持有观望，等待信号完善。"
    elif recommendation == "🔴":
        analysis += " 💰操作建议：回避。"
    
    return recommendation, analysis


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    """筛选数据"""
    # 筛选日期范围
    cutoff_date = (datetime.now() - timedelta(days=QUERY_MONTHS * 30)).date()
    df["截止日期"] = pd.to_datetime(df["截止日期"]).dt.date
    df = df[df["截止日期"] >= cutoff_date].copy()
    log.info(f"筛选最近 {QUERY_MONTHS} 个月（>= {cutoff_date}）后剩余 {len(df)} 条")

    # 筛选交易方式
    filtered = df[df["持股变动原因"].isin(TRADE_METHODS)].copy()
    log.info(f"筛选交易方式 {TRADE_METHODS} 后剩余 {len(filtered)} 条")

    if filtered.empty:
        return pd.DataFrame()

    # 过滤大股东增持
    filtered = filter_major_shareholders(filtered)
    
    # 排除ST/退市风险股
    filtered = filter_st_stocks(filtered)

    # 按公司分组，统计不同高管人数
    company_exec_count = (
        filtered.groupby(["证券代码", "证券简称"])["高管姓名"]
        .nunique()
        .reset_index()
        .rename(columns={"高管姓名": "增持高管人数"})
    )

    # 筛选达标公司
    qualified = company_exec_count[company_exec_count["增持高管人数"] >= MIN_EXECUTIVES]
    log.info(f"满足 >= {MIN_EXECUTIVES} 位高管增持的公司: {len(qualified)} 家")

    if qualified.empty:
        return pd.DataFrame()

    # 合并详情
    result = filtered.merge(qualified[["证券代码", "证券简称", "增持高管人数"]], 
                           on=["证券代码", "证券简称"])

    return result


def get_historical_holding_stats(stock_code: str, df_all: pd.DataFrame = None) -> Dict:
    """获取某只股票的全量历史增持统计（不受QUERY_MONTHS限制）
    
    返回：历史增持波次数、历史累计金额、最早增持日期、增持持续月数
    """
    try:
        if df_all is None:
            df_all = ak.stock_hold_management_detail_cninfo(symbol=QUERY_SYMBOL)
        
        # 筛选该公司（兼容改名，如华新水泥→华新建材）
        company_data = df_all[df_all['证券代码'] == stock_code].copy()
        
        if company_data.empty:
            return {"历史增持波次": 0, "历史累计金额": 0, "历史首次增持": None, "增持持续月数": 0}
        
        # 只看竞价交易/二级市场买卖
        company_data = company_data[company_data["持股变动原因"].isin(TRADE_METHODS)]
        
        # 过滤大股东
        for keyword in EXCLUDE_KEYWORDS:
            company_data = company_data[~company_data["董监高职务"].str.contains(keyword, na=False)]
        
        if company_data.empty:
            return {"历史增持波次": 0, "历史累计金额": 0, "历史首次增持": None, "增持持续月数": 0}
        
        # 计算累计金额
        total_amount = 0
        for _, row in company_data.iterrows():
            qty = row.get("变动数量", 0)
            price = row.get("成交均价", 0)
            if pd.notna(qty) and pd.notna(price) and qty > 0 and price > 0:
                total_amount += qty * price
        
        # 按月份分组计算波次（有增持记录的月份数）
        company_data["截止日期"] = pd.to_datetime(company_data["截止日期"])
        months = company_data["截止日期"].dt.to_period("M").nunique()
        
        # 最早和最新日期
        earliest = company_data["截止日期"].min()
        latest = company_data["截止日期"].max()
        duration_months = 0
        if pd.notna(earliest) and pd.notna(latest):
            duration_months = max(1, (latest.year - earliest.year) * 12 + latest.month - earliest.month)
        
        log.info(f"  {stock_code} 历史增持: {months}个月有增持, 累计{total_amount/10000:.0f}万, 跨度{duration_months}个月")
        
        return {
            "历史增持波次": months,
            "历史累计金额": total_amount,
            "历史首次增持": earliest.strftime("%Y-%m-%d") if pd.notna(earliest) else None,
            "增持持续月数": duration_months,
        }
    except Exception as e:
        log.warning(f"获取 {stock_code} 历史增持统计失败: {e}")
        return {"历史增持波次": 0, "历史累计金额": 0, "历史首次增持": None, "增持持续月数": 0}


def enrich_data_with_market_info(result: pd.DataFrame) -> pd.DataFrame:
    """补充市值、股价等市场信息"""
    if result.empty:
        return result
    
    log.info("正在补充市场信息...")
    
    # 预加载全量增持数据（用于历史累计统计，避免重复请求）
    try:
        df_all_holding = ak.stock_hold_management_detail_cninfo(symbol=QUERY_SYMBOL)
        log.info(f"预加载全量增持数据: {len(df_all_holding)} 条")
    except Exception as e:
        log.warning(f"预加载全量增持数据失败: {e}")
        df_all_holding = None
    
    # 按公司汇总数据
    company_summary = []
    
    companies = result[["证券代码", "证券简称"]].drop_duplicates()
    
    for _, company in companies.iterrows():
        stock_code = company["证券代码"]
        stock_name = company["证券简称"]
        
        log.info(f"处理 {stock_code} {stock_name}")
        
        # 获取该公司的增持明细
        company_data = result[result["证券代码"] == stock_code]
        
        # 计算增持总金额和总股数
        total_shares = company_data["变动数量"].sum()
        avg_price = company_data["成交均价"].mean()
        total_amount = total_shares * avg_price if pd.notna(avg_price) else 0
        
        # 获取最早增持日期和最新增持日期
        earliest_date = company_data["截止日期"].min()
        latest_date = company_data["截止日期"].max()
        earliest_date_str = earliest_date.strftime("%Y-%m-%d") if pd.notna(earliest_date) else None
        
        # 获取市值
        market_cap = get_market_cap(stock_code)
        
        # 计算增持占市值比例
        holding_ratio = (total_amount / (market_cap * 100000000)) if market_cap else 0
        
        # 计算高管持仓加权平均价
        avg_holding_price = calculate_avg_holding_price(company_data)
        
        # 获取股价数据（包含增持公告日涨跌幅）
        price_data = get_stock_price_data(stock_code, earliest_date_str)
        
        # 获取基本面数据
        fundamental_data = get_fundamental_data(stock_code)
        
        # 获取高管薪酬数据
        exec_salaries = get_executive_salaries(stock_code)
        
        # 计算增持金额/年薪比例
        salary_ratios = []
        for _, row in company_data.iterrows():
            exec_name = row.get("高管姓名", "").strip()
            qty = row.get("变动数量", 0)
            price = row.get("成交均价", 0)
            
            if exec_name in exec_salaries and pd.notna(qty) and pd.notna(price) and qty > 0 and price > 0:
                amount_wan = qty * price / 10000  # 转万元
                exec_salary = exec_salaries[exec_name]  # 已经是万元
                if exec_salary > 0:
                    ratio = amount_wan / exec_salary
                    salary_ratios.append(ratio)
        
        # 计算平均比值
        salary_ratio = sum(salary_ratios) / len(salary_ratios) if salary_ratios else None
        
        # 获取历史增持累计（全量数据，不受QUERY_MONTHS限制）
        hist_stats = get_historical_holding_stats(stock_code, df_all=df_all_holding)
        
        # 获取增持公告
        announcements = get_holding_announcements(stock_code)
        
        # 获取公告监控数据
        announcement_data = get_latest_announcements(stock_code)
        
        # 陈老师三重过滤：第二重 — 基本面分类+估值（提前计算，供卖出信号使用）
        stock_type = classify_stock_type(fundamental_data)
        valuation_pass, valuation_desc = evaluate_by_type(stock_type, fundamental_data)
        log.info(f"  {stock_code} 三重过滤: 类型={stock_type}, 估值={valuation_desc}, 通过={valuation_pass}")
        
        # 生成卖出信号
        sell_signals = generate_sell_signals(price_data, fundamental_data, announcement_data,
                                              valuation_pass=valuation_pass, ma_status=price_data.get('ma_status', ''))
        
        # 信号新鲜度分级
        if pd.notna(latest_date):
            from datetime import date as date_type
            if isinstance(latest_date, datetime):
                latest_dt = latest_date.date()
            elif isinstance(latest_date, date_type):
                latest_dt = latest_date
            else:
                latest_dt = datetime.strptime(str(latest_date), "%Y-%m-%d").date()
            days_diff = (datetime.now().date() - latest_dt).days
            if days_diff <= 7:
                freshness = "🔥 新鲜"
                freshness_score = 3
            elif days_diff <= 15:
                freshness = "⚡ 活跃"
                freshness_score = 2
            else:
                freshness = "💤 过期"
                freshness_score = 1
        else:
            freshness = "💤 过期"
            freshness_score = 1
        
        # 追高风险标记
        ann_return = price_data.get('announcement_return')
        if ann_return is not None and pd.notna(ann_return):
            if ann_return > 30:
                chase_risk = "⚠️追高风险"
            elif ann_return > 20:
                chase_risk = "⚡注意涨幅"
            elif ann_return <= 0:
                chase_risk = "✅低位机会"
            else:
                chase_risk = "🟡正常"
        else:
            chase_risk = "🟡正常"
        
        # 三重过滤已在前面计算（stock_type, valuation_pass, valuation_desc）
        
        # 生成投资观点
        holding_data = {'salary_ratio': salary_ratio}
        recommendation, analysis_text = generate_investment_opinion(
            stock_name, fundamental_data, price_data, holding_data,
            freshness=freshness, chase_risk=chase_risk, hist_stats=hist_stats,
            stock_type=stock_type, valuation_pass=valuation_pass, valuation_desc=valuation_desc
        )
        
        company_info = {
            "证券代码": stock_code,
            "证券简称": stock_name,
            "增持高管人数": company_data["增持高管人数"].iloc[0],
            "最早增持日期": earliest_date,
            "增持总股数": total_shares,
            "增持总金额": total_amount,
            "公司市值": market_cap,
            "增持占市值比例": holding_ratio,
            "增持年薪比": salary_ratio,
            "高管持仓均价": avg_holding_price,
            "最新增持日期": latest_date,
            "信号新鲜度": freshness,
            "freshness_score": freshness_score,
            "追高风险": chase_risk,
            "投资建议": recommendation,
            "分析观点": analysis_text,
            "股票类型": stock_type,
            "估值判断": valuation_desc,
            "估值通过": valuation_pass,
            "增持公告": announcements,
            "公告动态": announcement_data,
            "卖出信号": sell_signals,
            **hist_stats,
            **price_data,
            **fundamental_data
        }
        
        company_summary.append(company_info)
    
    summary_df = pd.DataFrame(company_summary)
    
    # 排序：信号新鲜度降序 → 增持高管人数降序 → 增持总金额降序
    if not summary_df.empty:
        summary_df = summary_df.sort_values(
            ["freshness_score", "增持高管人数", "增持总金额"],
            ascending=[False, False, False]
        ).reset_index(drop=True)
    
    return summary_df


def load_history() -> List[str]:
    """加载历史结果"""
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning(f"加载历史记录失败: {e}")
        return []


def save_history(companies: List[str]):
    """保存当前结果到历史记录"""
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"保存历史记录失败: {e}")


def mark_new_companies(summary_df: pd.DataFrame) -> pd.DataFrame:
    """标记新增公司"""
    if summary_df.empty:
        return summary_df
    
    history = load_history()
    current_companies = summary_df["证券代码"].tolist()
    
    # 标记新增公司
    summary_df["is_new"] = summary_df["证券代码"].apply(lambda x: x not in history)
    
    # 保存当前结果
    save_history(current_companies)
    
    return summary_df


def build_html_report(result: pd.DataFrame, summary_df: pd.DataFrame, index_data: List[Dict] = None) -> str:
    """生成 HTML 邮件报告（精简版：4个表）"""
    today = datetime.now().strftime("%Y-%m-%d")

    if result.empty and not index_data:
        return f"""
        <html><body style="font-family:Arial,sans-serif;padding:20px;">
        <h2>高管增持监控报告 - {today}</h2>
        <p>今日未发现满足条件的股票（≥{MIN_EXECUTIVES}位普通高管/董监高通过竞价交易增持）。</p>
        <p style="color:#999;font-size:12px;">
            筛选条件：排除大股东/实际控制人增持，排除ST股，交易方式={', '.join(TRADE_METHODS)}
        </p>
        <p style="color:#999;font-size:12px;">免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。</p>
        </body></html>
        """

    # 标记新增公司
    if not summary_df.empty:
        summary_df = mark_new_companies(summary_df)
    
    # 统一的表格样式
    table_style = "border-collapse:collapse;width:100%;margin-bottom:24px;"
    header_style = "background:#f5f5f5;color:#333;padding:10px 14px;text-align:center;font-weight:bold;font-size:13px;border-bottom:2px solid #ddd;"
    cell_style = "padding:12px 16px;border-bottom:1px solid #e0e0e0;text-align:left;font-size:14px;"
    cell_right_style = "padding:12px 16px;border-bottom:1px solid #e0e0e0;text-align:right;font-size:14px;"
    cell_center_style = "padding:12px 16px;border-bottom:1px solid #e0e0e0;text-align:center;font-size:14px;"
    
    # ========== 表1：指数量价监控 ==========
    index_html = ""
    if index_data:
        index_rows = ""
        for idx, item in enumerate(index_data):
            bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
            change_color = "#FF0000" if item['change_pct'] > 0 else "#00AA00" if item['change_pct'] < 0 else "black"
            trend_color = "#FF0000" if item['trend'] == "上行" else "#00AA00"
            
            # 量能分位颜色
            vp = item['vol_percentile']
            if vp < 20:
                vol_pct_color = "#00AA00"
                vol_pct_str = f"{vp:.0f}%（地量）"
            elif vp > 80:
                vol_pct_color = "#FF0000"
                vol_pct_str = f"{vp:.0f}%（天量）"
            else:
                vol_pct_color = "black"
                vol_pct_str = f"{vp:.0f}%"
            
            index_rows += f"""
            <tr style="background:{bg_color};">
                <td style="{cell_style}">{item['name']}</td>
                <td style="{cell_right_style}">{item['current_price']:.3f}</td>
                <td style="{cell_right_style};color:{change_color};">{item['change_pct']:+.2f}%</td>
                <td style="{cell_right_style}">{item['ma20']:.3f}</td>
                <td style="{cell_right_style}">{item['ma60']:.3f}</td>
                <td style="{cell_center_style};color:{trend_color};">{item['trend']}</td>
                <td style="{cell_right_style}">{item['vol_20']/10000:.2f}</td>
                <td style="{cell_right_style}">{item['vol_60']/10000:.2f}</td>
                <td style="{cell_center_style};color:{vol_pct_color};">{vol_pct_str}</td>
                <td style="{cell_center_style}">{item['signal']}</td>
            </tr>"""
        
        index_html = f"""
        <h3 style="color:#34495e;">📊 指数量价监控（陈老师量价法：地量=地价，天量=天价）</h3>
        <table style="{table_style}">
            <tr>
                <th style="{header_style}">指数</th>
                <th style="{header_style}">当前价</th>
                <th style="{header_style}">涨跌幅</th>
                <th style="{header_style}">MA20</th>
                <th style="{header_style}">MA60</th>
                <th style="{header_style}">趋势</th>
                <th style="{header_style}">20日均量(万手)</th>
                <th style="{header_style}">60日均量(万手)</th>
                <th style="{header_style}">量能分位</th>
                <th style="{header_style}">量价信号</th>
            </tr>
            {index_rows}
        </table>
        """

    if result.empty:
        return f"""
        <html><body style="font-family:Arial,sans-serif;padding:20px;line-height:1.6;font-size:14px;color:#333;">
        <h2 style="color:#2c3e50;">高管增持监控报告 - {today}</h2>
        {index_html}
        <p>今日未发现满足条件的股票（≥{MIN_EXECUTIVES}位普通高管/董监高通过竞价交易增持）。</p>
        <p style="color:#999;font-size:12px;">免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。</p>
        </body></html>
        """

    # ========== 表2：高管增持筛选（合并汇总表+基本面） ==========
    screening_rows = ""
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        new_mark = "🆕 " if row.get("is_new", False) else ""
        market_cap_str = f"{row['公司市值']:.0f}亿" if pd.notna(row["公司市值"]) else "-"
        amount_str = f"{row['增持总金额']:.0f}万" if pd.notna(row["增持总金额"]) else "-"
        avg_hold_price = f"{row['高管持仓均价']:.2f}" if pd.notna(row.get('高管持仓均价')) else "-"
        freshness = row.get('信号新鲜度', '-')
        
        # 基本面列
        s_type = row.get('股票类型', '-')
        pe_val = row.get('pe_ratio', 0)
        pe_type_label = row.get('pe_type', '')
        pe_suffix = f"<small>({pe_type_label})</small>" if pe_type_label else ""
        pe_ratio = f"{pe_val:.1f}{pe_suffix}" if pd.notna(pe_val) and pe_val else "-"
        pb_ratio = f"{row.get('pb_ratio', 0):.2f}" if pd.notna(row.get('pb_ratio')) else "-"
        roe = f"{row.get('roe', 0):.1f}%" if pd.notna(row.get('roe')) else "-"
        p_trend = row.get('profit_trend', None)
        trend_icon = "↑上升" if p_trend == "上升" else "↓下降" if p_trend == "下降" else "→持平" if p_trend == "持平" else "-"
        trend_color = "#FF0000" if p_trend == "上升" else "#00AA00" if p_trend == "下降" else "black"
        v_desc = row.get('估值判断', '-')
        
        screening_rows += f"""
        <tr style="background:{bg_color};">
            <td style="{cell_style}">{new_mark}{row['证券代码']}</td>
            <td style="{cell_style}">{new_mark}{row['证券简称']}</td>
            <td style="{cell_center_style}">{freshness}</td>
            <td style="{cell_center_style};color:#e74c3c;font-weight:bold;">{row['增持高管人数']}</td>
            <td style="{cell_right_style}">{amount_str}</td>
            <td style="{cell_right_style}">{market_cap_str}</td>
            <td style="{cell_right_style}">{avg_hold_price}</td>
            <td style="{cell_center_style}">{s_type}</td>
            <td style="{cell_right_style}">{pe_ratio}</td>
            <td style="{cell_right_style}">{pb_ratio}</td>
            <td style="{cell_right_style}">{roe}</td>
            <td style="{cell_center_style};color:{trend_color};">{trend_icon}</td>
            <td style="{cell_center_style}">{v_desc}</td>
        </tr>"""

    # ========== 表3：技术面+投资建议（合并技术分析+投资决策） ==========
    tech_advice_rows = ""
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        new_mark = "🆕 " if row.get("is_new", False) else ""
        
        price = f"{row['current_price']:.2f}" if pd.notna(row.get('current_price')) else "-"
        change_pct = row.get('price_change_pct', 0) or 0
        change = f"{change_pct:+.2f}%"
        change_color = "#FF0000" if change_pct > 0 else "#00AA00" if change_pct < 0 else "black"
        
        ma10 = f"{row['ma10']:.2f}" if pd.notna(row.get('ma10')) else "-"
        ma20 = f"{row['ma20']:.2f}" if pd.notna(row.get('ma20')) else "-"
        ma60 = f"{row['ma60']:.2f}" if pd.notna(row.get('ma60')) else "-"
        
        ma_status = row.get('ma_status', '-')
        timing = row.get('timing_signal', '观望')
        if '可买入' in str(timing):
            timing_color = "#FF0000; font-weight:bold"
        elif '回避' in str(timing):
            timing_color = "#00AA00; font-weight:bold"
        else:
            timing_color = "#FF8C00"
        
        # 三重过滤结果
        recommendation = row.get('投资建议', '-')
        analysis = row.get('分析观点', '')
        # 提取三重过滤图标
        triple_match = re.search(r'【三重([✅❌]+)】', str(analysis))
        triple_icons = triple_match.group(1) if triple_match else "---"
        
        # 精简投资建议到一行关键信息
        advice_short = str(analysis)
        # 去掉前缀【三重...】
        advice_short = re.sub(r'【三重[✅❌]+】[^。]+。', '', advice_short)
        # 只取第一个💰操作建议
        op_match = re.search(r'💰操作建议：(.+?)$', advice_short)
        if op_match:
            advice_short = op_match.group(1).strip()
        else:
            # 取最后一句
            advice_short = advice_short.strip()
            if len(advice_short) > 60:
                advice_short = advice_short[-60:]
        
        tech_advice_rows += f"""
        <tr style="background:{bg_color};">
            <td style="{cell_style}">{new_mark}{row['证券代码']}</td>
            <td style="{cell_style}">{new_mark}{row['证券简称']}</td>
            <td style="{cell_right_style}">{price}</td>
            <td style="{cell_right_style};color:{change_color};">{change}</td>
            <td style="{cell_right_style}">{ma10}</td>
            <td style="{cell_right_style}">{ma20}</td>
            <td style="{cell_right_style}">{ma60}</td>
            <td style="{cell_center_style}">{ma_status}</td>
            <td style="{cell_center_style};color:{timing_color};">{timing}</td>
            <td style="{cell_center_style}">{triple_icons}</td>
            <td style="{cell_style};font-size:13px;">{recommendation} {advice_short}</td>
        </tr>"""

    # ========== 生成公告动态表格内容 ==========
    announcement_rows = ""
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        stock_code = row['证券代码']
        stock_name = row['证券简称']
        announcement_data = row.get('公告动态', {})
        announcements = announcement_data.get('announcements', [])
        
        if not announcements:
            # 如果没有公告，显示一行"暂无公告"
            announcement_rows += f"""
            <tr style="background:{bg_color};">
                <td style="{cell_style}">{stock_code}</td>
                <td style="{cell_style}">{stock_name}</td>
                <td style="{cell_center_style}">-</td>
                <td style="{cell_center_style}">-</td>
                <td style="{cell_style}">暂无公告</td>
            </tr>"""
        else:
            # 显示最新3条公告
            for j, ann in enumerate(announcements[:3]):
                date_str = ann.get('date', '')[:10] if ann.get('date') else '-'
                category = ann.get('category', '其他')
                title = ann.get('title', '')[:40] + ('...' if len(ann.get('title', '')) > 40 else '')
                
                # 类别颜色
                if category == "回购" or category == "增持":
                    cat_color = "#FF0000"  # 红色=利好
                elif category == "减持" or category == "风险":
                    cat_color = "#00AA00"  # 绿色=利空
                elif category == "业绩":
                    cat_color = "#FF8C00"  # 橙色=业绩
                else:
                    cat_color = "black"
                
                # 第一行显示股票信息，后续行留空
                code_cell = stock_code if j == 0 else ""
                name_cell = stock_name if j == 0 else ""
                
                announcement_rows += f"""
                <tr style="background:{bg_color};">
                    <td style="{cell_style}">{code_cell}</td>
                    <td style="{cell_style}">{name_cell}</td>
                    <td style="{cell_center_style}">{date_str}</td>
                    <td style="{cell_center_style};color:{cat_color};">{category}</td>
                    <td style="{cell_style};font-size:12px;">{title}</td>
                </tr>"""

    # ========== 生成卖出信号表格内容 ==========
    sell_signal_rows = ""
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        stock_code = row['证券代码']
        stock_name = row['证券简称']
        sell_signals = row.get('卖出信号', [])
        
        if not sell_signals:
            # 如果没有信号，显示一行"无信号"
            sell_signal_rows += f"""
            <tr style="background:{bg_color};">
                <td style="{cell_style}">{stock_code}</td>
                <td style="{cell_style}">{stock_name}</td>
                <td style="{cell_center_style}">无异常信号</td>
                <td style="{cell_center_style}">info</td>
                <td style="{cell_center_style}">正常持有</td>
            </tr>"""
        else:
            # 显示所有信号
            for j, signal in enumerate(sell_signals):
                signal_text = signal.get('signal', '')
                level = signal.get('level', 'info')
                action = signal.get('action', '')
                
                # 级别颜色
                if level == "danger":
                    level_color = "#FF0000"  # 红色=危险
                elif level == "warning":
                    level_color = "#FF8C00"  # 橙色=警告
                else:
                    level_color = "#00AA00"  # 绿色=信息
                
                # 第一行显示股票信息，后续行留空
                code_cell = stock_code if j == 0 else ""
                name_cell = stock_name if j == 0 else ""
                
                sell_signal_rows += f"""
                <tr style="background:{bg_color};">
                    <td style="{cell_style}">{code_cell}</td>
                    <td style="{cell_style}">{name_cell}</td>
                    <td style="{cell_style}">{signal_text}</td>
                    <td style="{cell_center_style};color:{level_color};">{level}</td>
                    <td style="{cell_center_style}">{action}</td>
                </tr>"""

    # ========== 表4：增持明细 ==========
    detail_cols = ["证券代码", "证券简称", "高管姓名", "董监高职务", "变动数量", "成交均价", "截止日期", "持股变动原因"]
    detail_df = result[detail_cols].sort_values(["证券代码", "高管姓名"])
    detail_rows = ""
    for idx, (i, row) in enumerate(detail_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        price = f"{row['成交均价']:.2f}" if pd.notna(row["成交均价"]) else "-"
        detail_rows += f"""
        <tr style="background:{bg_color};">
            <td style="{cell_style}">{row['证券代码']}</td>
            <td style="{cell_style}">{row['证券简称']}</td>
            <td style="{cell_style}">{row['高管姓名']}</td>
            <td style="{cell_style}">{row['董监高职务']}</td>
            <td style="{cell_right_style}">{row['变动数量']:,.0f}</td>
            <td style="{cell_right_style}">{price}</td>
            <td style="{cell_center_style}">{row['截止日期']}</td>
            <td style="{cell_style}">{row['持股变动原因']}</td>
        </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;padding:20px;line-height:1.6;font-size:14px;color:#333;">
    <h2 style="color:#2c3e50;">高管增持监控报告 - {today}</h2>
    <p>筛选条件：排除大股东/实际控制人增持，排除ST股，同一公司 ≥{MIN_EXECUTIVES} 位普通高管增持，查询时间窗口：{QUERY_MONTHS}个月</p>

    {index_html}

    <h3 style="color:#34495e;">📋 高管增持筛选（共 {len(summary_df)} 家公司）</h3>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">信号新鲜度</th>
            <th style="{header_style}">增持高管数</th>
            <th style="{header_style}">增持总金额</th>
            <th style="{header_style}">公司市值</th>
            <th style="{header_style}">高管持仓均价</th>
            <th style="{header_style}">股票类型</th>
            <th style="{header_style}">PE</th>
            <th style="{header_style}">PB</th>
            <th style="{header_style}">ROE</th>
            <th style="{header_style}">利润趋势</th>
            <th style="{header_style}">估值判断</th>
        </tr>
        {screening_rows}
    </table>

    <h3 style="color:#34495e;">📈 技术面 + 投资建议</h3>
    <div style="background:#f8f9fa;border-left:4px solid #3498db;padding:10px 15px;margin-bottom:15px;font-size:13px;color:#555;">
        <b>🔍 三重过滤体系（陈老师框架）</b><br>
        ✅/❌ 第一重：<b>高管增持</b> — ≥5位高管竞价买入（本报告所有股票已通过）<br>
        ✅/❌ 第二重：<b>基本面估值</b> — 按股票类型分别评估（价值股看扣非PE-TTM，成长股看PEG，周期股看PB+利润趋势，亏损股直接淘汰）<br>
        ✅/❌ 第三重：<b>均线择时</b> — MA10/20/30/60多头排列=买入，空头排列=回避
    </div>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">当前价</th>
            <th style="{header_style}">涨跌幅</th>
            <th style="{header_style}">MA10</th>
            <th style="{header_style}">MA20</th>
            <th style="{header_style}">MA60</th>
            <th style="{header_style}">均线状态</th>
            <th style="{header_style}">操作信号</th>
            <th style="{header_style}">三重过滤</th>
            <th style="{header_style}">投资建议</th>
        </tr>
        {tech_advice_rows}
    </table>

    <h3 style="color:#34495e;">📢 最新公告动态</h3>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">公告日期</th>
            <th style="{header_style}">公告类别</th>
            <th style="{header_style}">公告标题</th>
        </tr>
        {announcement_rows}
    </table>

    <h3 style="color:#34495e;">⚠️ 卖出信号监控</h3>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">信号</th>
            <th style="{header_style}">级别</th>
            <th style="{header_style}">建议操作</th>
        </tr>
        {sell_signal_rows}
    </table>

    <h3 style="color:#34495e;">📝 增持明细</h3>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">高管姓名</th>
            <th style="{header_style}">职务</th>
            <th style="{header_style}">变动数量(股)</th>
            <th style="{header_style}">成交均价</th>
            <th style="{header_style}">截止日期</th>
            <th style="{header_style}">交易方式</th>
        </tr>
        {detail_rows}
    </table>

    <p style="color:#999;font-size:12px;margin-top:30px;">
        数据来源：巨潮资讯网、akshare | 🆕 表示新增公司 | 红涨绿跌（A股习惯） | 自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M')}
    </p>
    <p style="color:red;font-size:12px;font-weight:bold;">
        ⚠️ 免责声明：本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。高管增持不等于股价上涨，请结合其他因素综合判断。
    </p>
    </body></html>
    """
    return html


def load_subscribers() -> list:
    """从 subscribers.json 加载订阅者列表"""
    subscribers_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscribers.json")
    try:
        with open(subscribers_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return [s for s in data.get("subscribers", []) if s.get("active", True)]
    except FileNotFoundError:
        log.warning("subscribers.json 不存在，使用默认收件人")
        return [{"email": EMAIL_RECEIVER, "name": "默认", "active": True}]
    except Exception as e:
        log.warning(f"加载订阅者失败: {e}，使用默认收件人")
        return [{"email": EMAIL_RECEIVER, "name": "默认", "active": True}]


def unsubscribe_email(email: str) -> bool:
    """从订阅列表中移除邮箱"""
    subscribers_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscribers.json")
    try:
        with open(subscribers_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for s in data.get("subscribers", []):
            if s["email"].lower() == email.lower():
                s["active"] = False
                s["unsubscribed"] = datetime.now().strftime("%Y-%m-%d")
        with open(subscribers_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log.error(f"退订失败: {e}")
        return False


def add_unsubscribe_footer(html_content: str, email: str) -> str:
    """在报告底部添加退订链接"""
    unsubscribe_url = f"mailto:{EMAIL_SENDER}?subject=退订高管增持报告&body=请将以下邮箱从每日推送中移除：{email}"
    footer = f"""
    <div style="margin-top:30px;padding-top:15px;border-top:1px solid #eee;text-align:center;font-size:12px;color:#999;">
        <p>本报告由「高管增持三重过滤系统」自动生成，每个交易日早8点推送</p>
        <p>数据来源：巨潮资讯网 | 同花顺 | 东方财富</p>
        <p>如不想继续接收，<a href="{unsubscribe_url}" style="color:#999;">点击退订</a></p>
    </div>
    """
    # 在 </body> 或末尾插入
    if '</body>' in html_content:
        html_content = html_content.replace('</body>', footer + '</body>')
    else:
        html_content += footer
    return html_content


def send_email(html_content: str):
    """通过 QQ 邮箱 SMTP 发送 HTML 邮件给所有订阅者"""
    if not EMAIL_PASSWORD:
        log.error("未配置 SMTP 授权码，请在 config.py 中填写 EMAIL_PASSWORD")
        sys.exit(1)

    subscribers = load_subscribers()
    if not subscribers:
        log.warning("无活跃订阅者，跳过发送")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            
            for sub in subscribers:
                receiver = sub["email"]
                name = sub.get("name", "")
                
                # 为每个收件人添加退订链接
                personalized_html = add_unsubscribe_footer(html_content, receiver)
                
                msg = MIMEMultipart("alternative")
                msg["Subject"] = f"高管增持监控报告 - {today}"
                msg["From"] = EMAIL_SENDER
                msg["To"] = receiver
                msg.attach(MIMEText(personalized_html, "html", "utf-8"))
                
                server.sendmail(EMAIL_SENDER, receiver, msg.as_string())
                log.info(f"邮件发送成功: {name}<{receiver}>")
                
    except Exception as e:
        log.error(f"邮件发送失败: {e}")
        raise


def main():
    log.info("=== 高管增持监控开始 ===")
    
    # 检查是否为交易日
    if not is_trading_day():
        log.info("今日非交易日，跳过运行")
        return
    
    try:
        # 获取指数量价数据
        index_data = get_index_volume_price_data()
        
        # 获取数据并筛选
        df = fetch_data()
        result = filter_data(df)
        
        # 补充市场信息
        summary_df = enrich_data_with_market_info(result)
        
        # 生成报告并发送邮件
        html = build_html_report(result, summary_df, index_data=index_data)
        send_email(html)
        
    except Exception:
        log.exception("运行出错")
        sys.exit(1)
    
    log.info("=== 高管增持监控完成 ===")


if __name__ == "__main__":
    main()