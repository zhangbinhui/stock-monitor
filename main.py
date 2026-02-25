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


def convert_etf_code_to_ak_format(code: str) -> str:
    """将ETF代码转换为akshare stock_zh_a_daily可用的格式

    规则: 1开头=sh, 0/3开头=sz, 5开头=sh
    但159957是深交所创业板ETF，需要特殊处理：159开头=sz
    """
    if code.startswith('159'):
        return f'sz{code}'
    elif code.startswith(('0', '3')):
        return f'sz{code}'
    elif code.startswith(('1', '5', '6')):
        return f'sh{code}'
    else:
        # 默认按首字母判断
        if code[0] in '039':
            return f'sz{code}'
        else:
            return f'sh{code}'


def get_realtime_prices(codes: List[str]) -> Dict[str, Dict]:
    """通过腾讯行情接口获取实时价格（不依赖push2）
    
    Args:
        codes: 股票/ETF代码列表，如 ['600733', '510300']
    Returns:
        {代码: {'price': 最新价, 'prev_close': 昨收, 'change_pct': 涨跌幅}}
    """
    try:
        # 转换代码格式
        qq_codes = []
        for code in codes:
            if code.startswith(('5', '6')):
                qq_codes.append(f'sh{code}')
            else:
                qq_codes.append(f'sz{code}')
        
        url = f"http://qt.gtimg.cn/q={','.join(qq_codes)}"
        r = requests.get(url, timeout=5, proxies={'http': '', 'https': ''})
        
        result = {}
        for line in r.text.strip().split(';'):
            line = line.strip()
            if not line or '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 33:
                continue
            code = parts[2]  # 纯数字代码
            try:
                price = float(parts[3]) if parts[3] else None
                prev_close = float(parts[4]) if parts[4] else None
                change_pct = float(parts[32]) if parts[32] else None
                if price and price > 0:
                    result[code] = {
                        'price': price,
                        'prev_close': prev_close,
                        'change_pct': change_pct
                    }
            except (ValueError, IndexError):
                continue
        
        log.info(f"获取实时行情: {len(result)}/{len(codes)} 只")
        return result
    except Exception as e:
        log.warning(f"获取实时行情失败: {e}")
        return {}


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
            df = None

            # ETF代码加前缀：5/6开头=sh，0/1/3开头=sz（159xxx是深交所ETF）
            etf_code = etf['code']
            if etf_code.startswith(('5', '6')):
                etf_symbol = f'sh{etf_code}'
            else:
                etf_symbol = f'sz{etf_code}'

            # 优先使用 fund_etf_hist_sina（新浪源，ETF专用，最稳定）
            try:
                log.info(f"  尝试使用新浪源: {etf_symbol}")
                df = ak.fund_etf_hist_sina(symbol=etf_symbol)
                if df is not None and not df.empty:
                    # 新浪源列名：date, open, high, low, close, volume, amount
                    df.rename(columns={
                        'date': '日期',
                        'close': '收盘',
                        'volume': '成交量'
                    }, inplace=True)
                    log.info(f"  新浪源获取成功: {len(df)} 条数据")
            except Exception as e:
                log.warning(f"  新浪源失败: {e}")
                df = None

            # 备选：东财源
            if df is None or df.empty:
                try:
                    log.info(f"  尝试使用东财源: {etf_code}")
                    df = ak.fund_etf_hist_em(symbol=etf_code, period="daily",
                                          start_date=start_date, end_date=end_date, adjust="qfq")
                    if df is not None and not df.empty:
                        log.info(f"  东财源获取成功: {len(df)} 条数据")
                except Exception as e:
                    log.warning(f"  东财源失败: {e}")
                    df = None

            if df is None or df.empty:
                log.warning(f"  {etf['code']} 所有数据源都无数据")
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

            ma10 = closes[-10:].mean()
            ma20 = closes[-20:].mean()
            ma30 = closes[-30:].mean() if len(closes) >= 30 else None
            ma60 = closes[-60:].mean()

            # 均线排列判断
            if ma30 and current_price > ma10 > ma20 > ma30 > ma60:
                ma_arrangement = "多头排列"
                ma_signal = "🟢 可买入"
            elif ma30 and current_price < ma10 < ma20 < ma30 < ma60:
                ma_arrangement = "空头排列"
                ma_signal = "🔴 空仓回避"
            elif current_price > ma20 > ma60:
                ma_arrangement = "偏多"
                ma_signal = "🟢 可持有"
            elif current_price < ma20 < ma60:
                ma_arrangement = "偏空"
                ma_signal = "🔴 回避"
            elif current_price > ma20 and current_price < ma60:
                ma_arrangement = "反弹中"
                ma_signal = "🟡 观察确认"
            elif current_price < ma20 and current_price > ma60:
                ma_arrangement = "回调中"
                ma_signal = "🟡 等待企稳"
            else:
                ma_arrangement = "纠缠"
                ma_signal = "🟡 观望"

            # 价格相对均线位置
            bias20 = (current_price - ma20) / ma20 * 100
            bias60 = (current_price - ma60) / ma60 * 100

            # 趋势判断
            trend = "上行" if current_price > ma60 else "下行"

            # 成交量均值
            vol_20 = volumes[-20:].mean()
            vol_60 = volumes[-60:].mean()

            # 成交量分位：20日均量在最近1年（250日）滚动20日均量中的百分位
            recent_volumes = volumes[-250:] if len(volumes) > 250 else volumes
            if len(recent_volumes) >= 40:
                rolling_20_vols = []
                for i in range(20, len(recent_volumes) + 1):
                    rolling_20_vols.append(recent_volumes[i-20:i].mean())
                rolling_20_vols = np.array(rolling_20_vols)
                vol_percentile = (rolling_20_vols < vol_20).sum() / len(rolling_20_vols) * 100
            else:
                vol_percentile = 50

            # 量价信号（结合量能分位 + 价格相对MA60位置 + 偏离度）
            ma60_bias = (current_price - ma60) / ma60 * 100  # 偏离MA60百分比
            if vol_percentile < 20 and ma60_bias < -3:
                signal = "🟢 地量低位（左侧买点）"
            elif vol_percentile < 20 and ma60_bias >= -3:
                signal = "🟡 缩量整理（观察）"
            elif vol_percentile > 80 and ma60_bias > 5:
                signal = "🔴 天量高位（注意风险）"
            elif vol_percentile > 80 and ma60_bias < -5:
                signal = "🟡 放量下跌（恐慌）"
            elif vol_percentile > 80:
                signal = "🟡 放量震荡（关注方向）"
            else:
                signal = "⏳ 正常"

            results.append({
                "name": etf['name'],
                "code": etf['code'],
                "current_price": current_price,
                "change_pct": change_pct,
                "ma10": ma10,
                "ma20": ma20,
                "ma30": ma30,
                "ma60": ma60,
                "trend": trend,
                "ma_arrangement": ma_arrangement,
                "ma_signal": ma_signal,
                "bias20": bias20,
                "bias60": bias60,
                "vol_20": vol_20,
                "vol_60": vol_60,
                "vol_percentile": vol_percentile,
                "signal": signal,
            })
            log.info(f"  {etf['name']}: 价格={current_price:.3f}, 涨跌={change_pct:.2f}%, {ma_arrangement}, 偏离MA20={bias20:+.1f}%, 量分位={vol_percentile:.0f}%, 量价={signal}, 均线={ma_signal}")
        except Exception as e:
            log.warning(f"获取 {etf['name']} ({etf['code']}) 失败: {e}")

    # 用实时行情覆盖ETF当前价
    if results:
        etf_codes = [r['code'] for r in results]
        rt_prices = get_realtime_prices(etf_codes)
        for r in results:
            if r['code'] in rt_prices:
                rt = rt_prices[r['code']]
                r['current_price'] = rt['price']
                r['change_pct'] = rt['change_pct']
                r['is_realtime'] = True
                log.info(f"  {r['name']} 实时价格: {rt['price']}")

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
        # 优先尝试原方案 (可能用的是不同域名，还能用)
        info = ak.stock_individual_info_em(symbol=stock_code)
        for _, row in info.iterrows():
            if row['item'] == '总市值':
                val = row['value']
                if isinstance(val, (int, float)):
                    return val / 1e8
                else:
                    return float(str(val).replace(',', '')) / 1e8
        log.warning(f"{stock_code} 原方案未找到总市值字段")
        return None
    except Exception as e:
        log.warning(f"获取 {stock_code} 市值失败（原方案）: {e}")

        # Fallback: 用日K线数据估算市值 = 最新收盘价 * 总股本
        try:
            log.info(f"  尝试用日K线数据估算 {stock_code} 市值")
            # 转换股票代码格式
            if stock_code.startswith(('0', '3')):
                symbol = f'sz{stock_code}'
            else:
                symbol = f'sh{stock_code}'

            # 获取最新日K数据
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date)

            if df is not None and not df.empty:
                latest_close = df.iloc[-1]['close']  # 最新收盘价
                outstanding_share = df.iloc[-1].get('outstanding_share', None)  # 总股本(股)

                if outstanding_share and outstanding_share > 0:
                    market_cap_yuan = latest_close * outstanding_share
                    market_cap_yi = market_cap_yuan / 1e8
                    log.info(f"  {stock_code} 估算市值: {market_cap_yi:.2f}亿元")
                    return market_cap_yi

            log.warning(f"  {stock_code} fallback方案也无法获取市值")
            return None

        except Exception as fallback_e:
            log.warning(f"获取 {stock_code} 市值失败（fallback）: {fallback_e}")
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

        # 转换股票代码格式为 akshare stock_zh_a_daily 可用格式
        if stock_code.startswith(('0', '3')):  # 深交所
            symbol = f'sz{stock_code}'
        else:  # 上交所
            symbol = f'sh{stock_code}'

        df = None
        try:
            log.info(f"获取 {stock_code} 股价数据，使用腾讯源: {symbol}")
            df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date)

            if df is not None and not df.empty:
                # stock_zh_a_daily 返回列: date, open, high, low, close, volume, amount, outstanding_share, turnover
                # 需要映射为中文列名以保持兼容性
                df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                }, inplace=True)
                log.info(f"  腾讯源获取成功: {len(df)} 条数据")
            else:
                log.warning(f"  腾讯源无数据")

        except Exception as e:
            log.warning(f"获取 {stock_code} 腾讯源股价数据失败: {e}")

        # Fallback: 尝试原方案
        if df is None or df.empty:
            try:
                log.info(f"  尝试原方案获取 {stock_code} 数据")
                df = ak.stock_zh_a_hist(symbol=stock_code, period="daily",
                                       start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    log.info(f"  原方案获取成功: {len(df)} 条数据")
            except Exception as e:
                log.warning(f"  原方案也失败: {e}")

        if df is None or df.empty:
            log.warning(f"获取 {stock_code} 股价数据失败：所有数据源都无数据")
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


def get_fundamental_data(stock_code: str, market_cap_yi: float = None) -> Dict:
    """获取基本面数据（同花顺财务摘要）"""
    result = {"revenue": None, "net_profit": None, "revenue_growth": None,
              "profit_growth": None, "roe": None, "pe_ratio": None, "pb_ratio": None,
              "prev_net_profit": None, "profit_trend": None, "industry": None,
              "gross_margin": None, "gross_margin_prev": None, "revenue_growth_recent": None,
              "ps_ratio": None, "market_cap_yi_val": market_cap_yi}
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

            # 毛利率（最新+上期）
            try:
                gm_latest = parse_pct(latest.get("销售毛利率"))
                if gm_latest is not None:
                    result["gross_margin"] = gm_latest
                if len(fin) >= 2:
                    gm_prev = parse_pct(fin.iloc[-2].get("销售毛利率"))
                    if gm_prev is not None:
                        result["gross_margin_prev"] = gm_prev
            except:
                pass

            # 利润趋势：用最近4个季度同比数据判断
            # 每个季度的净利润 vs 去年同期，看趋势方向
            try:
                fin_q = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
                if fin_q is not None and not fin_q.empty:
                    # 构建 {报告期: 净利润} 映射
                    period_profit = {}
                    for _, qrow in fin_q.iterrows():
                        period = str(qrow.get("报告期", ""))
                        profit = parse_amount(qrow.get("扣非净利润")) or parse_amount(qrow.get("净利润"))
                        if period and profit is not None:
                            period_profit[period] = profit
                    
                    # 取最近4个季度的同比增长率
                    sorted_periods = sorted(period_profit.keys(), reverse=True)
                    yoy_changes = []
                    trend_details = []
                    for p in sorted_periods[:4]:
                        year = int(p[:4])
                        prev_period = f"{year-1}{p[4:]}"
                        if prev_period in period_profit and period_profit[prev_period] != 0:
                            yoy = (period_profit[p] - period_profit[prev_period]) / abs(period_profit[prev_period])
                            yoy_changes.append(yoy)
                            trend_details.append(f"{p}:{yoy:+.0%}")
                    
                    if yoy_changes:
                        # 判断趋势：看多数季度的方向
                        up_count = sum(1 for y in yoy_changes if y > 0.1)
                        down_count = sum(1 for y in yoy_changes if y < -0.1)
                        avg_yoy = sum(yoy_changes) / len(yoy_changes)
                        
                        if up_count >= len(yoy_changes) * 0.75:
                            result["profit_trend"] = "上升"
                        elif down_count >= len(yoy_changes) * 0.75:
                            result["profit_trend"] = "下降"
                        elif avg_yoy > 0.1:
                            result["profit_trend"] = "上升"
                        elif avg_yoy < -0.1:
                            result["profit_trend"] = "下降"
                        else:
                            result["profit_trend"] = "持平"
                        
                        result["profit_trend_detail"] = " | ".join(trend_details)
                        log.info(f"  {stock_code} 利润趋势({len(yoy_changes)}Q同比): {result['profit_trend']} [{result['profit_trend_detail']}]")
                    # === 困境反转/成长放缓：季度营收增速 ===
                    try:
                        period_rev = {}
                        period_gm = {}
                        for _, qrow in fin_q.iterrows():
                            period = str(qrow.get("报告期", ""))
                            rev = parse_amount(qrow.get("营业总收入"))
                            gm = parse_pct(qrow.get("销售毛利率"))
                            if period and rev is not None:
                                period_rev[period] = rev
                            if period and gm is not None:
                                period_gm[period] = gm

                        # 最近季度营收同比
                        rev_yoy_list = []
                        for p in sorted_periods[:4]:
                            year = int(p[:4])
                            prev_p = f"{year-1}{p[4:]}"
                            if prev_p in period_rev and period_rev[prev_p] != 0:
                                rev_yoy = (period_rev[p] - period_rev[prev_p]) / abs(period_rev[prev_p]) * 100
                                rev_yoy_list.append((p, rev_yoy))

                        if rev_yoy_list:
                            result["revenue_growth_recent"] = rev_yoy_list[0][1]  # 最新季度营收增速
                            result["revenue_growth_trend"] = rev_yoy_list  # 全部季度营收增速列表

                        # 毛利率季度趋势（用于困境反转判断）
                        gm_sorted = sorted(period_gm.items(), key=lambda x: x[0], reverse=True)
                        if len(gm_sorted) >= 2:
                            result["gross_margin_q_latest"] = gm_sorted[0][1]
                            result["gross_margin_q_prev"] = gm_sorted[1][1]
                            result["gross_margin_q_trend"] = gm_sorted[:4]
                    except Exception as rev_e:
                        log.debug(f"季度营收增速计算失败: {rev_e}")

            except Exception as trend_e:
                log.debug(f"季度利润趋势计算失败: {trend_e}")
            
            # Fallback: 如果季度数据没算出来，用年报对比
            if result.get("profit_trend") is None:
                if len(fin) >= 2:
                    prev_row = fin.iloc[-2]
                    prev_profit = parse_amount(prev_row.get("净利润"))
                    if result["net_profit"] is not None and prev_profit is not None and prev_profit != 0:
                        diff_ratio = (result["net_profit"] - prev_profit) / abs(prev_profit)
                        if diff_ratio > 0.1:
                            result["profit_trend"] = "上升"
                        elif diff_ratio < -0.1:
                            result["profit_trend"] = "下降"
                        else:
                            result["profit_trend"] = "持平"
                        result["profit_trend_detail"] = "年报对比"

        # 获取行业信息
        try:
            info_industry = ak.stock_individual_info_em(symbol=stock_code)
            for _, row in info_industry.iterrows():
                if row['item'] == '行业':
                    result["industry"] = str(row['value'])
                    break
        except Exception as e:
            log.warning(f"获取 {stock_code} 行业信息失败: {e}")
            # 暂无良好的fallback方案获取行业信息
            result["industry"] = None

        # PE/PB：从市值和财务数据计算
        total_market_cap = None
        current_price_val = None

        # 优先尝试从 stock_individual_info_em 获取
        try:
            info = ak.stock_individual_info_em(symbol=stock_code)
            for _, row in info.iterrows():
                if row['item'] == '总市值':
                    val = row['value']
                    total_market_cap = float(val) if isinstance(val, (int, float)) else None
                elif row['item'] == '最新':
                    current_price_val = float(row['value']) if isinstance(row['value'], (int, float)) else None
        except Exception as e:
            log.warning(f"获取 {stock_code} 基本信息失败: {e}")
        
        # Fallback: 用 get_market_cap 的日K线估算结果
        if total_market_cap is None and market_cap_yi is not None:
            total_market_cap = market_cap_yi * 1e8  # 亿元 → 元
            log.info(f"  使用 get_market_cap fallback 市值: {market_cap_yi:.2f}亿元")

            # Fallback: 尝试使用 stock_a_indicator_lg 获取PE/PB指标
            try:
                log.info(f"  尝试使用 stock_a_indicator_lg 获取 {stock_code} 估值指标")
                indicator_df = ak.stock_a_indicator_lg(symbol=stock_code)
                if indicator_df is not None and not indicator_df.empty:
                    latest_indicator = indicator_df.iloc[-1]
                    # stock_a_indicator_lg 可能直接提供PE/PB
                    pe_from_lg = latest_indicator.get('pe', None) or latest_indicator.get('PE', None) or latest_indicator.get('市盈率', None)
                    pb_from_lg = latest_indicator.get('pb', None) or latest_indicator.get('PB', None) or latest_indicator.get('市净率', None)

                    if pe_from_lg and pd.notna(pe_from_lg) and pe_from_lg > 0:
                        result["pe_ratio"] = round(float(pe_from_lg), 2)
                        result["pe_type"] = "LG源"
                        log.info(f"    从LG源获取PE: {result['pe_ratio']}")

                    if pb_from_lg and pd.notna(pb_from_lg) and pb_from_lg > 0:
                        result["pb_ratio"] = round(float(pb_from_lg), 2)
                        log.info(f"    从LG源获取PB: {result['pb_ratio']}")

            except Exception as lg_e:
                log.warning(f"  stock_a_indicator_lg 也失败: {lg_e}")

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

        # PS(市销率) = 市值 / TTM营收（用于困境反转估值）
        if total_market_cap and result.get("revenue") and result["revenue"] > 0:
            result["ps_ratio"] = round(total_market_cap / 1e8 / result["revenue"], 2)
            result["market_cap_yi_val"] = round(total_market_cap / 1e8, 2)

        # PB: 用每股净资产计算 (如果LG源没有提供PB的话)
        if result.get("pb_ratio") is None:
            try:
                bvps = float(str(fin.iloc[-1].get("每股净资产", "0")).replace(',', ''))
            except:
                bvps = None

            if bvps and bvps > 0 and current_price_val:
                result["pb_ratio"] = round(current_price_val / bvps, 2)
            elif bvps and bvps > 0:
                # 如果没有current_price_val，尝试从日K数据获取
                try:
                    if stock_code.startswith(('0', '3')):
                        symbol = f'sz{stock_code}'
                    else:
                        symbol = f'sh{stock_code}'
                    end_date = datetime.now().strftime("%Y%m%d")
                    start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
                    price_df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date)
                    if price_df is not None and not price_df.empty:
                        latest_price = price_df.iloc[-1]['close']
                        result["pb_ratio"] = round(latest_price / bvps, 2)
                        log.info(f"    通过日K数据计算PB: {result['pb_ratio']}")
                except Exception as price_e:
                    log.warning(f"  获取最新价格计算PB失败: {price_e}")

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
                          valuation_pass: bool = False, ma_status: str = "", avg_holding_price: float = None) -> List[Dict]:
    """生成卖出/持有信号

    逻辑：以高管增持均价为锚点，结合基本面和固定止损
    - 跌破高管增持均价 → 警告（内部人都套了）
    - 回撤>15% → 止损（风控底线）
    - 基本面恶化 → 卖出
    - 高管减持 → 强烈卖出

    参数:
        price_data: 价格技术数据
        fundamental_data: 基本面数据
        announcement_data: 公告数据
        valuation_pass: 估值是否通过三重过滤
        ma_status: 均线状态描述（仅作参考显示）
        avg_holding_price: 高管增持加权均价
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

        # === 亏损股直接标记（困境反转除外） ===
        stock_type = fundamental_data.get("_stock_type", "")
        if net_profit is not None and net_profit < 0 and stock_type != "困境反转":
            signals.append({
                "signal": "公司亏损",
                "level": "danger",
                "action": "亏损股建议清仓"
            })

        # === 高管增持均价锚点 ===
        if avg_holding_price and avg_holding_price > 0 and current_price:
            insider_premium = (current_price - avg_holding_price) / avg_holding_price
            if insider_premium < -0.15:
                signals.append({
                    "signal": f"较高管增持均价跌{abs(insider_premium):.0%}",
                    "level": "danger",
                    "action": "深度破发，基本面可能有问题，考虑止损"
                })
            elif insider_premium < 0:
                signals.append({
                    "signal": f"较高管增持均价跌{abs(insider_premium):.0%}",
                    "level": "warning",
                    "action": "已破增持均价，关注基本面是否恶化"
                })
            else:
                signals.append({
                    "signal": f"较高管增持均价涨{insider_premium:.0%}",
                    "level": "info",
                    "action": "仍在增持均价上方，正常持有"
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
                "level": "danger",
                "action": "基本面恶化，考虑减仓或清仓"
            })

        # === 公告信号 ===
        if has_insider_sell:
            signals.append({
                "signal": "高管/股东减持",
                "level": "danger",
                "action": "内部人在卖，强烈建议清仓"
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

    # 亏损股 → 分流为"困境反转"或"亏损"
    if net_profit is not None and net_profit < 0:
        # 困境反转条件：营收高增长(>30%) + 毛利率在改善
        rev_growth_recent = fundamental_data.get("revenue_growth_recent")  # 最新季度营收同比
        gm_latest = fundamental_data.get("gross_margin_q_latest")
        gm_prev = fundamental_data.get("gross_margin_q_prev")

        is_rev_growing = (rev_growth_recent is not None and rev_growth_recent > 30) or \
                         (revenue_growth > 30)
        is_gm_improving = (gm_latest is not None and gm_prev is not None and gm_latest > gm_prev)

        if is_rev_growing and is_gm_improving:
            return "困境反转"
        elif is_rev_growing:
            return "困境反转"  # 营收高增长也给机会观察
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

    if stock_type == "困境反转":
        # 困境反转分级建仓：
        # 观察期 → 不买
        # 试探期（毛利率连续2季度改善）→ 10%仓位
        # 确认期（毛利率转正 或 单季度盈利）→ 20%仓位
        # 成熟期（连续2季度盈利）→ 升级为正常股票评估
        ps = fundamental_data.get("ps_ratio") or 0
        rev_g = fundamental_data.get("revenue_growth_recent") or fundamental_data.get("revenue_growth") or 0
        gm = fundamental_data.get("gross_margin_q_latest") or fundamental_data.get("gross_margin") or 0
        gm_prev = fundamental_data.get("gross_margin_q_prev") or fundamental_data.get("gross_margin_prev")
        gm_trend = fundamental_data.get("gross_margin_q_trend") or []  # [(period, gm%), ...]
        gm_improving = gm_prev is not None and gm > gm_prev

        # 判断毛利率连续改善季度数
        gm_improving_quarters = 0
        if len(gm_trend) >= 2:
            for i in range(len(gm_trend) - 1):
                if gm_trend[i][1] > gm_trend[i + 1][1]:
                    gm_improving_quarters += 1
                else:
                    break

        # 判断最近季度是否盈利
        profit_trend_detail = fundamental_data.get("profit_trend_detail", "")
        latest_profit = fundamental_data.get("net_profit")  # 年报净利润
        # 检查最新季度扣非是否转正（从季度趋势数据推断）
        latest_q_profitable = False
        if profit_trend_detail and ":" in profit_trend_detail:
            try:
                first_q = profit_trend_detail.split("|")[0].strip()
                pct_str = first_q.split(":")[1].strip().replace("%", "").replace("+", "")
                # 如果最新季度同比大幅改善且基数为正，可能已盈利
                # 这里简化：如果毛利率已转正，大概率接近盈利
            except:
                pass

        desc_parts = [f"营收+{rev_g:.0f}%"]
        if gm_improving_quarters >= 2:
            desc_parts.append(f"毛利率连续{gm_improving_quarters}Q改善")
        elif gm_improving:
            desc_parts.append(f"毛利率{gm_prev:.1f}%→{gm:.1f}%↑")
        elif gm_prev is not None:
            desc_parts.append(f"毛利率{gm:.1f}%(未改善)")
        if ps > 0:
            desc_parts.append(f"PS={ps:.1f}")
        desc = "，".join(desc_parts)

        # === 分级判断 ===
        if gm > 0 and rev_g > 20:
            # 确认期：毛利率已转正 → 允许20%仓位
            return True, f"🟢困境反转确认期({desc})——毛利率已转正，建仓20%"
        elif gm_improving_quarters >= 2 and rev_g > 30 and ps < 5:
            # 试探期：毛利率连续2季度改善 → 允许10%仓位
            return True, f"🟡困境反转试探期({desc})——高管增持+毛利率连续改善，试探建仓10%"
        elif rev_g > 30 and gm_improving:
            # 观察期：营收高增长+毛利率有改善但不够连续
            return False, f"👀困境反转观察({desc})——等毛利率连续改善再进场"
        elif rev_g > 30:
            return False, f"👀困境反转观察({desc})——营收在增长但毛利率未改善"
        else:
            return False, f"❌困境反转条件不足({desc})"

    if stock_type == "成长股":
        # PEG优先用最新季度TTM利润增速（避免年报增速过时）
        # revenue_growth_trend = [(period, yoy%), ...]，取最新季度的利润增速
        profit_growth_recent = None
        rev_growth_trend = fundamental_data.get("revenue_growth_trend")
        if rev_growth_trend and len(rev_growth_trend) >= 1:
            # 最新季度营收增速作为参考
            profit_growth_recent = rev_growth_trend[0][1]

        # 利润增速：优先用最新季度同比，回退到年报
        growth_for_peg = profit_growth  # 年报增速
        growth_source = "年报"

        # 如果有季度利润趋势数据，用最新季度同比
        profit_trend_detail = fundamental_data.get("profit_trend_detail", "")
        if profit_trend_detail and profit_trend_detail != "年报对比":
            # 解析 "2025-09-30:+1% | 2025-06-30:+1% | ..."
            try:
                first_q = profit_trend_detail.split("|")[0].strip()
                pct_str = first_q.split(":")[1].strip().replace("%", "").replace("+", "")
                growth_for_peg = float(pct_str)
                growth_source = "最新季度"
            except:
                pass

        rev_recent = fundamental_data.get("revenue_growth_recent")
        rev_annual = fundamental_data.get("revenue_growth") or 0
        decel_warn = ""
        if rev_recent is not None and rev_annual > 0 and rev_recent < rev_annual * 0.5:
            decel_warn = f"⚠️增速放缓({rev_annual:.0f}%→{rev_recent:.0f}%)"
        elif rev_recent is not None and rev_recent < 10:
            decel_warn = f"⚠️增速接近停滞({rev_recent:.0f}%)"

        if growth_for_peg > 0:
            peg = pe / growth_for_peg if growth_for_peg != 0 else 999
            peg_note = f"PEG={peg:.2f}({growth_source}增速{growth_for_peg:.0f}%)"

            if peg < 1.5:
                base = f"✅成长股{peg_note}"
                if decel_warn:
                    return True, f"{base} {decel_warn}"
                return True, base
            elif peg <= 2:
                base = f"⚠️成长股{peg_note}偏高"
                if decel_warn:
                    return False, f"{base} {decel_warn}"
                return True, base
            else:
                return False, f"❌成长股{peg_note}高估 {decel_warn}"
        else:
            return False, f"❌成长股利润负增长({growth_source}增速{growth_for_peg:.0f}%)"

    if stock_type == "周期股":
        trend_str = f"利润{profit_trend}"
        # 周期拐点信号
        inflection = ""
        if profit_trend == "上升":
            inflection = "🟢利润拐点向上"
        elif profit_trend == "下降":
            inflection = "🔴利润仍在下行"

        # 陈老师PE陷阱警告：周期股PE越低越危险（利润高峰），PE越高/亏损反而是买点（利润谷底）
        pe_trap_warn = ""
        if pe > 0 and pe < 8 and profit_trend != "下降":
            pe_trap_warn = " ⚠️PE陷阱：PE极低可能在利润高峰，警惕周期见顶"
        elif pe > 0 and pe < 12 and profit_trend == "上升":
            pe_trap_warn = " ℹ️注意：周期股PE低≠便宜，关注利润能否持续"

        if pb < 1.5:
            desc = f"✅周期股PB={pb:.2f}低估({trend_str})"
            if inflection:
                desc += f" {inflection}"
            desc += pe_trap_warn
            return True, desc
        elif pb <= 2.5:
            if profit_trend == "上升":
                return True, f"✅周期股PB={pb:.2f}合理+{trend_str} {inflection}{pe_trap_warn}"
            else:
                return False, f"⚠️周期股PB={pb:.2f}合理但{trend_str} {inflection}"
        else:
            return False, f"❌周期股PB={pb:.2f}偏高({trend_str}) {inflection}"

    if stock_type == "价值股":
        # 价值股业绩下滑预警
        decline_warn = ""
        if profit_trend == "下降":
            decline_warn = " ⚠️业绩下滑中"
        if pe > 0 and pe < 15:
            return True, f"✅价值股PE={pe:.1f}合理{decline_warn}"
        elif pe >= 15 and pe <= 20:
            return True, f"⚠️价值股PE={pe:.1f}偏高{decline_warn}"
        elif pe > 20:
            return False, f"❌价值股PE={pe:.1f}高估{decline_warn}"
        else:
            return True, f"✅价值股PE数据异常，默认通过"

    # 一般类型：收紧标准
    if pe > 0 and pe < 15:
        return True, f"✅PE={pe:.1f}尚可"
    elif pe >= 15 and pe < 20:
        return True, f"⚠️PE={pe:.1f}，估值一般"
    elif pe >= 20:
        return False, f"❌PE={pe:.1f}偏高"
    else:
        return False, "PE数据不足，无法判断"


def calc_position_and_target(stock_type: str, fundamental_data: Dict, valuation_desc: str = "", premium_rate: float = None) -> Dict:
    """根据股票分类和基本面，计算仓位建议分级和目标涨幅估算
    
    仓位分级：
    - 重仓30%：三重全过 + 周期拐点/成长PEG<1
    - 中仓15%：三重全过 + 价值股有催化剂
    - 轻仓5-10%：困境反转试探 / 估值一般
    
    目标涨幅：基于类型给预期区间
    """
    pe = fundamental_data.get("pe_ratio") or 0
    pb = fundamental_data.get("pb_ratio") or 0
    profit_trend = fundamental_data.get("profit_trend") or "持平"
    
    position_tier = "观望"
    position_pct = "0%"
    target_return = ""
    target_logic = ""
    
    if stock_type == "亏损":
        position_tier = "回避"
        position_pct = "0%"
        target_return = "-"
        target_logic = "亏损股不参与"
    
    elif stock_type == "困境反转":
        if "确认期" in valuation_desc:
            position_tier = "中仓"
            position_pct = "15-20%"
            target_return = "50-100%"
            target_logic = "扭亏后PE从无穷大→正常估值，弹性极大"
        elif "试探期" in valuation_desc:
            position_tier = "轻仓"
            position_pct = "5-10%"
            target_return = "30-80%"
            target_logic = "毛利率改善→盈利预期→戴维斯双击"
        else:
            position_tier = "观望"
            position_pct = "0%"
            target_return = "-"
            target_logic = "等确认信号"
    
    elif stock_type == "成长股":
        peg = 0
        # 优先用最新季度利润增速，回退年报增速（与evaluate_by_type一致）
        growth = fundamental_data.get("profit_growth") or 0
        profit_trend_detail = fundamental_data.get("profit_trend_detail", "")
        if profit_trend_detail and profit_trend_detail != "年报对比" and ":" in profit_trend_detail:
            try:
                first_q = profit_trend_detail.split("|")[0].strip()
                pct_str = first_q.split(":")[1].strip().replace("%", "").replace("+", "")
                _latest_growth = float(pct_str)
                if _latest_growth > 0:
                    growth = _latest_growth
            except:
                pass
        if growth > 0 and pe > 0:
            peg = pe / growth
        
        if peg > 0 and peg < 1:
            position_tier = "重仓"
            position_pct = "25-30%"
            target_return = f"{growth:.0f}-{growth*1.5:.0f}%"
            target_logic = f"PEG={peg:.1f}<1，利润增速{growth:.0f}%，股价应至少跟上利润增速"
        elif peg >= 1 and peg < 1.5:
            position_tier = "中仓"
            position_pct = "15-20%"
            target_return = f"{growth*0.5:.0f}-{growth:.0f}%"
            target_logic = f"PEG={peg:.1f}合理，赚业绩增长的钱"
        elif peg >= 1.5 and peg <= 2:
            position_tier = "轻仓"
            position_pct = "5-10%"
            target_return = "10-20%"
            target_logic = f"PEG={peg:.1f}偏高，上涨空间有限"
        else:
            position_tier = "观望"
            position_pct = "0%"
            target_return = "-"
            target_logic = "增速不足或PEG过高"
    
    elif stock_type == "周期股":
        if profit_trend == "上升" and pb < 2:
            position_tier = "重仓"
            position_pct = "25-30%"
            # 周期股利润拐点，PB从低位修复
            target_pb = max(pb * 1.5, 2.0)
            target_pct = (target_pb / pb - 1) * 100 if pb > 0 else 50
            target_return = f"{target_pct:.0f}-{target_pct*1.5:.0f}%"
            target_logic = f"利润拐点+PB={pb:.1f}低估，PB修复至{target_pb:.1f}即{target_pct:.0f}%+"
        elif profit_trend == "上升":
            position_tier = "中仓"
            position_pct = "15-20%"
            target_return = "20-40%"
            target_logic = f"利润上行但PB={pb:.1f}不算低，赚业绩弹性"
        elif pb < 1.5:
            position_tier = "轻仓"
            position_pct = "5-10%"
            target_return = "10-30%"
            target_logic = f"PB={pb:.1f}低估但利润未拐点，等左侧机会"
        else:
            position_tier = "观望"
            position_pct = "0%"
            target_return = "-"
            target_logic = "利润下行+估值不低"
    
    elif stock_type == "价值股":
        # 价值股目标：PE修复到合理水平
        if pe > 0 and pe < 8:
            # 极低PE，修复空间大
            target_pe = min(pe * 1.5, 12)
            target_pct = (target_pe / pe - 1) * 100
            dividend_yield = round(100 / pe * 0.3, 1)  # 假设30%分红率
            if profit_trend == "上升":
                position_tier = "重仓"
                position_pct = "25-30%"
                target_return = f"{target_pct:.0f}-{target_pct*1.3:.0f}%"
                target_logic = f"PE={pe:.1f}极低+业绩上行→PE修复至{target_pe:.0f}即{target_pct:.0f}%+，股息率约{dividend_yield}%"
            else:
                position_tier = "中仓"
                position_pct = "15-20%"
                target_return = f"{target_pct*0.6:.0f}-{target_pct:.0f}%"
                target_logic = f"PE={pe:.1f}极低→PE修复至{target_pe:.0f}即{target_pct:.0f}%，股息率约{dividend_yield}%"
        elif pe >= 8 and pe < 12:
            target_pe = min(pe * 1.3, 15)
            target_pct = (target_pe / pe - 1) * 100
            if profit_trend == "上升":
                position_tier = "中仓"
                position_pct = "15-20%"
                target_return = f"{target_pct:.0f}-{target_pct*1.3:.0f}%"
                target_logic = f"PE={pe:.1f}合理+业绩上行→赚估值+业绩双升"
            else:
                position_tier = "轻仓"
                position_pct = "10-15%"
                target_return = f"10-{target_pct:.0f}%"
                target_logic = f"PE={pe:.1f}合理，赚估值修复的钱"
        elif pe >= 12 and pe <= 15:
            position_tier = "轻仓"
            position_pct = "5-10%"
            target_return = "5-15%"
            target_logic = f"PE={pe:.1f}偏高，上涨空间有限"
        else:
            position_tier = "观望"
            position_pct = "0%"
            target_return = "-"
            target_logic = "PE偏高"
    
    else:  # 一般
        if pe > 0 and pe < 10:
            position_tier = "中仓"
            position_pct = "15-20%"
            target_pe = pe * 1.4
            target_pct = (target_pe / pe - 1) * 100
            target_return = f"{target_pct*0.5:.0f}-{target_pct:.0f}%"
            target_logic = f"PE={pe:.1f}偏低，有修复空间"
        elif pe >= 10 and pe < 20:
            position_tier = "轻仓"
            position_pct = "5-10%"
            target_return = "10-20%"
            target_logic = f"PE={pe:.1f}一般"
        else:
            position_tier = "观望"
            position_pct = "0%"
            target_return = "-"
            target_logic = f"PE={pe:.1f}偏高"
    
    # 溢价率调整：高溢价降级
    if premium_rate is not None:
        if premium_rate > 0.30:
            position_tier = "观望"
            position_pct = "0%"
            target_logic += f"（当前溢价{premium_rate:.0%}过高，不追）"
        elif premium_rate > 0.10:
            # 降一级
            tier_map = {"重仓": "中仓", "中仓": "轻仓", "轻仓": "轻仓", "观望": "观望", "回避": "回避"}
            pct_map = {"重仓": "15-20%", "中仓": "10-15%", "轻仓": "5-10%", "观望": "0%", "回避": "0%"}
            old_tier = position_tier
            position_tier = tier_map.get(position_tier, position_tier)
            position_pct = pct_map.get(old_tier, position_pct)
            target_logic += f"（溢价{premium_rate:.0%}，仓位降级）"
    
    return {
        "position_tier": position_tier,  # 重仓/中仓/轻仓/观望/回避
        "position_pct": position_pct,    # "25-30%" 
        "target_return": target_return,  # "30-50%"
        "target_logic": target_logic,    # 涨幅逻辑说明
    }


def generate_investment_opinion(stock_name: str, fundamental_data: Dict, price_data: Dict, holding_data: Dict, freshness: str = "", chase_risk: str = "", hist_stats: Dict = None, stock_type: str = "一般", valuation_pass: bool = True, valuation_desc: str = "", avg_holding_price: float = None) -> Tuple[str, str]:
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
    if stock_type == "困境反转":
        # 根据估值结果判断处于哪个阶段
        if "确认期" in valuation_desc:
            recommendation = "🟢"
            analysis = f"困境反转确认期：毛利率已转正，反转逻辑成立。"
            if is_strong_signal:
                analysis += f"高管用{salary_ratio:.1f}倍年薪增持，信心极强。"
            analysis += " 💰建仓20%（非三重过滤标准仓位30%，控制风险）。"
        elif "试探期" in valuation_desc:
            recommendation = "🟡"
            analysis = f"困境反转试探期：毛利率连续改善，高管提前看到反转。"
            if is_strong_signal:
                analysis += f"高管用{salary_ratio:.1f}倍年薪增持，值得跟进。"
            analysis += " 💰试探建仓10%（轻仓试水，等确认再加）。"
        else:
            recommendation = "🟡"
            analysis = f"困境反转观察期：亏损但营收在增长，高管增持可能是对反转有信心。"
            if is_strong_signal:
                analysis += f"高管用{salary_ratio:.1f}倍年薪增持，对反转信心强。"
            analysis += " ⚠️暂不建仓，等毛利率连续2季度改善再试探。"
    elif is_loss_company:
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

    # 均线状态（仅作参考，不影响买卖决策）
    timing = price_data.get('timing_signal', '观望')
    ma_ref = price_data.get('ma_status', '')
    if ma_ref:
        analysis += f" 📊均线参考：{ma_ref}。"

    # ====== 陈老师三重过滤 ======
    # 第一重：高管增持（已满足，能进入此函数说明增持≥5人）
    filter1_pass = True
    # 第二重：基本面分类+估值
    filter2_pass = valuation_pass
    # 第三重：增持溢价率（当前价 vs 高管增持均价）
    current_price = price_data.get('current_price')
    premium_rate = None
    premium_desc = ""
    if avg_holding_price and avg_holding_price > 0 and current_price and current_price > 0:
        premium_rate = (current_price - avg_holding_price) / avg_holding_price
        if premium_rate < 0:
            filter3_pass = True
            premium_desc = f"折价{abs(premium_rate):.0%}（比高管买得还便宜）"
        elif premium_rate <= 0.10:
            filter3_pass = True
            premium_desc = f"溢价{premium_rate:.0%}（低溢价，买入区间）"
        elif premium_rate <= 0.30:
            filter3_pass = False
            filter3_neutral = True
            premium_desc = f"溢价{premium_rate:.0%}（中等溢价，仓位减半）"
        else:
            filter3_pass = False
            filter3_neutral = False
            premium_desc = f"溢价{premium_rate:.0%}（高溢价，不追）"
    else:
        filter3_pass = False
        filter3_neutral = True
        premium_desc = "无增持均价数据"

    filter_icons = f"{'✅' if filter1_pass else '❌'}{'✅' if filter2_pass else '❌'}{'✅' if filter3_pass else '❌'}"

    # 困境反转标记
    is_turnaround = stock_type == "困境反转"

    # ====== 仓位分级 + 目标涨幅 ======
    pt = calc_position_and_target(stock_type, fundamental_data, valuation_desc, premium_rate)
    position_tier = pt["position_tier"]
    position_pct = pt["position_pct"]
    target_return = pt["target_return"]
    target_logic = pt["target_logic"]
    
    # 用 calc_position_and_target 的结果替代硬编码仓位
    std_position = position_pct

    # 三重过滤综合判断（覆盖之前的recommendation）
    if filter1_pass and filter2_pass and filter3_pass:
        recommendation = "🟢"
        if premium_rate is not None and premium_rate < 0:
            triple_result = f"🟢 三重过滤通过 - 折价买入，{position_tier}{std_position}"
        else:
            triple_result = f"🟢 三重过滤通过 - {position_tier}{std_position}"
        target_hint = f"，目标涨幅{target_return}" if target_return and target_return != "-" else ""
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → {position_tier}{std_position}{target_hint}。" + analysis
    elif filter1_pass and filter2_pass and filter3_neutral:
        recommendation = "🟡"
        # 溢价时仓位已在calc_position_and_target中降级
        if premium_rate is not None and premium_rate > 0.10:
            triple_result = f"🟡 溢价偏高，{position_tier}{std_position}或等回调"
        else:
            triple_result = "🟡 等待确认"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → 谨慎建仓或等回调。" + analysis
    elif filter1_pass and filter2_pass and not filter3_pass:
        recommendation = "🔴"
        triple_result = "🔴 溢价过高，不追"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → 涨太多了不追。" + analysis
    elif filter1_pass and not filter2_pass and filter3_pass:
        # 困境反转观察期：基本面不通过但价格合适
        if is_turnaround and "观察" in valuation_desc:
            recommendation = "🟡"
            triple_result = "👀 困境反转观察期，暂不建仓"
            analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → 观察中，等毛利率连续改善。" + analysis
        else:
            recommendation = "🟡"
            triple_result = "⚠️ 价格合适但基本面存疑"
            analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → 基本面存疑，观望。" + analysis
    else:
        recommendation = "🔴"
        triple_result = "🔴 不满足买入条件"
        analysis = f"【三重{filter_icons}】高管增持+{valuation_desc}+{premium_desc} → 不满足买入条件。" + analysis

    # 综合操作建议（含目标涨幅）
    target_hint = f"目标涨幅{target_return}（{target_logic}）" if target_return and target_return != "-" else ""
    if recommendation == "🟢" and premium_rate is not None and premium_rate < 0:
        analysis += f" 💰操作建议：三重过滤通过，折价买入，{position_tier}{std_position}！{target_hint}"
    elif recommendation == "🟢":
        analysis += f" 💰操作建议：三重过滤通过，{position_tier}{std_position}！{target_hint}"
    elif recommendation == "🟡" and premium_rate is not None and premium_rate > 0.10:
        analysis += f" 💰操作建议：溢价偏高，可{position_tier}{std_position}或等回调到增持均价附近。{target_hint}"
    elif recommendation == "🟡":
        analysis += f" 💰操作建议：持有观望，等待信号完善。{target_hint}"
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

    # 获取所有股票的实时行情（腾讯接口）
    all_codes = result["证券代码"].unique().tolist()
    realtime_prices = get_realtime_prices(all_codes)

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

        # 用实时行情覆盖日K线收盘价
        if stock_code in realtime_prices:
            rt = realtime_prices[stock_code]
            old_price = price_data.get('current_price')
            price_data['current_price'] = rt['price']
            price_data['price_change_pct'] = rt['change_pct']
            price_data['is_realtime'] = True
            log.info(f"  {stock_code} 实时价格: {rt['price']}（日K收盘: {old_price}）")

        # 获取基本面数据
        fundamental_data = get_fundamental_data(stock_code, market_cap_yi=market_cap)

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

        # 计算中位数比值（比平均数更抗极端值干扰）
        salary_ratio = sorted(salary_ratios)[len(salary_ratios) // 2] if salary_ratios else None
        salary_ratio_avg = sum(salary_ratios) / len(salary_ratios) if salary_ratios else None

        # 获取历史增持累计（全量数据，不受QUERY_MONTHS限制）
        hist_stats = get_historical_holding_stats(stock_code, df_all=df_all_holding)

        # 获取增持公告
        announcements = get_holding_announcements(stock_code)

        # 获取公告监控数据
        announcement_data = get_latest_announcements(stock_code)

        # 陈老师三重过滤：第二重 - 基本面分类+估值（提前计算，供卖出信号使用）
        stock_type = classify_stock_type(fundamental_data)
        valuation_pass, valuation_desc = evaluate_by_type(stock_type, fundamental_data)
        log.info(f"  {stock_code} 三重过滤: 类型={stock_type}, 估值={valuation_desc}, 通过={valuation_pass}")

        # 传递stock_type给卖出信号（困境反转不标"亏损清仓"）
        fundamental_data["_stock_type"] = stock_type
        # 生成卖出信号
        sell_signals = generate_sell_signals(price_data, fundamental_data, announcement_data,
                                              valuation_pass=valuation_pass, ma_status=price_data.get('ma_status', ''),
                                              avg_holding_price=avg_holding_price)

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

        # 计算溢价率（用于仓位分级）
        _cur_price = price_data.get('current_price')
        _premium_rate = None
        if avg_holding_price and avg_holding_price > 0 and _cur_price and _cur_price > 0:
            _premium_rate = (_cur_price - avg_holding_price) / avg_holding_price

        # 计算仓位分级和目标涨幅
        pos_target = calc_position_and_target(stock_type, fundamental_data, valuation_desc, _premium_rate)

        # 生成投资观点
        holding_data = {'salary_ratio': salary_ratio}
        recommendation, analysis_text = generate_investment_opinion(
            stock_name, fundamental_data, price_data, holding_data,
            freshness=freshness, chase_risk=chase_risk, hist_stats=hist_stats,
            stock_type=stock_type, valuation_pass=valuation_pass, valuation_desc=valuation_desc,
            avg_holding_price=avg_holding_price
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
            "仓位分级": pos_target["position_tier"],
            "建议仓位": pos_target["position_pct"],
            "目标涨幅": pos_target["target_return"],
            "涨幅逻辑": pos_target["target_logic"],
            "增持公告": announcements,
            "公告动态": announcement_data,
            "卖出信号": sell_signals,
            **hist_stats,
            **price_data,
            **fundamental_data
        }

        company_summary.append(company_info)

    summary_df = pd.DataFrame(company_summary)

    # 排序：通过三重过滤优先 → 信号新鲜度降序 → 增持高管人数降序 → 增持总金额降序
    if not summary_df.empty:
        # 投资建议优先级：🟢=3, 🟡=2, 🔴=1
        def _advice_score(x):
            if x == "🟢": return 3
            if x == "🟡": return 2
            return 1
        summary_df["_advice_score"] = summary_df["投资建议"].apply(_advice_score)
        summary_df = summary_df.sort_values(
            ["_advice_score", "freshness_score", "增持高管人数", "增持总金额"],
            ascending=[False, False, False, False]
        ).reset_index(drop=True)
        summary_df.drop(columns=["_advice_score"], inplace=True)

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

            # 均线排列颜色
            ma_arr = item.get('ma_arrangement', '')
            ma_sig = item.get('ma_signal', '')
            if '多头' in ma_arr or '偏多' in ma_arr:
                ma_arr_color = "#FF0000; font-weight:bold"
            elif '空头' in ma_arr or '偏空' in ma_arr:
                ma_arr_color = "#00AA00; font-weight:bold"
            else:
                ma_arr_color = "#FF8C00"

            bias20_val = item.get('bias20', 0)
            bias60_val = item.get('bias60', 0)

            index_rows += f"""
            <tr style="background:{bg_color};">
                <td style="{cell_style}">{item['name']}</td>
                <td style="{cell_right_style}">{item['current_price']:.3f}</td>
                <td style="{cell_right_style};color:{change_color};">{item['change_pct']:+.2f}%</td>
                <td style="{cell_right_style}">{item['ma20']:.3f}</td>
                <td style="{cell_right_style}">{item['ma60']:.3f}</td>
                <td style="{cell_center_style};color:{ma_arr_color};">{ma_arr}</td>
                <td style="{cell_center_style};color:{ma_arr_color};">{ma_sig}</td>
                <td style="{cell_right_style}">{bias20_val:+.1f}%</td>
                <td style="{cell_right_style}">{bias60_val:+.1f}%</td>
                <td style="{cell_right_style}">{item['vol_20']/10000:.2f}</td>
                <td style="{cell_right_style}">{item['vol_60']/10000:.2f}</td>
                <td style="{cell_center_style};color:{vol_pct_color};">{vol_pct_str}</td>
                <td style="{cell_center_style}">{item['signal']}</td>
            </tr>"""

        index_html = f"""
        <h3 style="color:#34495e;">📊 指数量价监控（陈老师量价法：地量=地价，天量=天价）</h3>
        <p style="color:#999;font-size:11px;margin:0 0 8px 0;">💡 当前价为盘中实时价格（非交易时段为最近收盘价） | 均线/量能基于日K线计算 | 量能分位 = 最近20日均量在过去1年的百分位排名</p>
        <table style="{table_style}">
            <tr>
                <th style="{header_style}">指数</th>
                <th style="{header_style}">当前价</th>
                <th style="{header_style}">涨跌幅</th>
                <th style="{header_style}">MA20</th>
                <th style="{header_style}">MA60</th>
                <th style="{header_style}">均线排列</th>
                <th style="{header_style}">均线信号</th>
                <th style="{header_style}">偏离MA20</th>
                <th style="{header_style}">偏离MA60</th>
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

    # 东财行情链接
    def eastmoney_url(code):
        prefix = "sh" if str(code).startswith(('5', '6')) else "sz"
        return f"https://quote.eastmoney.com/{prefix}{code}.html"

    # ========== 表2：高管增持筛选（合并汇总表+基本面） ==========
    screening_rows = ""
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        new_mark = "🆕 " if row.get("is_new", False) else ""
        market_cap_str = f"{row['公司市值']:.0f}亿" if pd.notna(row["公司市值"]) else "-"
        
        # 最近增持日期（替代信号新鲜度）
        latest_dt = row.get('最新增持日期')
        if pd.notna(latest_dt):
            if isinstance(latest_dt, datetime):
                latest_dt_str = latest_dt.strftime("%Y-%m-%d")
            else:
                latest_dt_str = str(latest_dt)[:10]
        else:
            latest_dt_str = "-"
        
        # 增持/年薪比（中位数）
        sr = row.get('增持年薪比')
        if sr and pd.notna(sr) and sr > 0:
            salary_ratio_str = f"{sr:.1f}倍"
            if sr >= 2:
                sr_color = "#FF0000; font-weight:bold"  # 强信号
            elif sr >= 0.5:
                sr_color = "#FF8C00"  # 一般
            else:
                sr_color = "#999"  # 弱信号
        else:
            salary_ratio_str = "-"
            sr_color = "#999"

        # 基本面列
        s_type = row.get('股票类型', '-')
        pe_val = row.get('pe_ratio', 0)
        pe_type_label = row.get('pe_type', '')
        pe_suffix = f"<small>({pe_type_label})</small>" if pe_type_label else ""
        pe_ratio = f"{pe_val:.1f}{pe_suffix}" if pd.notna(pe_val) and pe_val else "-"
        pb_ratio = f"{row.get('pb_ratio', 0):.2f}" if pd.notna(row.get('pb_ratio')) else "-"
        roe = f"{row.get('roe', 0):.1f}%" if pd.notna(row.get('roe')) else "-"
        p_trend = row.get('profit_trend', None)
        p_trend_detail = row.get('profit_trend_detail', '')
        trend_icon = "↑上升" if p_trend == "上升" else "↓下降" if p_trend == "下降" else "→持平" if p_trend == "持平" else "-"
        trend_color = "#FF0000" if p_trend == "上升" else "#00AA00" if p_trend == "下降" else "black"
        # 鼠标悬停显示每个季度同比详情
        trend_title = f' title="{p_trend_detail}"' if p_trend_detail else ""
        v_desc = row.get('估值判断', '-')

        # 当前价和涨跌幅
        _price = row.get('current_price')
        _chg = row.get('price_change_pct', 0) or 0
        price_str = f"{_price:.2f}" if pd.notna(_price) and _price else "-"
        chg_str = f"{_chg:+.2f}%"
        chg_color = "#FF0000" if _chg > 0 else "#00AA00" if _chg < 0 else "black"

        screening_rows += f"""
        <tr style="background:{bg_color};">
            <td style="{cell_style}">{new_mark}<a href="{eastmoney_url(row['证券代码'])}" target="_blank" style="color:#3498db;text-decoration:none;">{row['证券代码']}</a></td>
            <td style="{cell_style}">{new_mark}{row['证券简称']}</td>
            <td style="{cell_right_style}">{price_str}</td>
            <td style="{cell_right_style};color:{chg_color};">{chg_str}</td>
            <td style="{cell_center_style}">{latest_dt_str}</td>
            <td style="{cell_center_style};color:#e74c3c;font-weight:bold;">{row['增持高管人数']}</td>
            <td style="{cell_center_style};color:{sr_color};">{salary_ratio_str}</td>
            <td style="{cell_right_style}">{market_cap_str}</td>
            <td style="{cell_center_style}">{s_type}</td>
            <td style="{cell_right_style}">{pe_ratio}</td>
            <td style="{cell_right_style}">{pb_ratio}</td>
            <td style="{cell_right_style}">{roe}</td>
            <td style="{cell_center_style};color:{trend_color};cursor:help;"{trend_title}>{trend_icon}</td>
            <td style="{cell_right_style}">{f"{row.get('高管持仓均价', 0):.2f}" if pd.notna(row.get('高管持仓均价')) and row.get('高管持仓均价') else "-"}</td>
            <td style="{cell_center_style};color:{sr_color if 'sr_color' in dir() else '#666'};">{f"{((row.get('current_price',0) - row.get('高管持仓均价',0)) / row.get('高管持仓均价',1)):+.0%}" if pd.notna(row.get('高管持仓均价')) and row.get('高管持仓均价') and pd.notna(row.get('current_price')) and row.get('current_price') else "-"}</td>
            <td style="{cell_center_style}">{v_desc}</td>
        </tr>"""

    # ========== 增持明细 ==========
    detail_cols = ["证券代码", "证券简称", "高管姓名", "董监高职务", "变动数量", "成交均价", "截止日期", "持股变动原因"]
    detail_df = result[detail_cols].sort_values(["证券代码", "高管姓名"])
    detail_rows = ""
    for idx, (i, row) in enumerate(detail_df.iterrows()):
        bg_color = "#f0f4f8" if idx % 2 == 0 else "white"
        price = f"{row['成交均价']:.2f}" if pd.notna(row["成交均价"]) else "-"
        detail_rows += f"""
        <tr style="background:{bg_color};">
            <td style="{cell_style}"><a href="{eastmoney_url(row['证券代码'])}" target="_blank" style="color:#3498db;text-decoration:none;">{row['证券代码']}</a></td>
            <td style="{cell_style}">{row['证券简称']}</td>
            <td style="{cell_style}">{row['高管姓名']}</td>
            <td style="{cell_style}">{row['董监高职务']}</td>
            <td style="{cell_right_style}">{row['变动数量']:,.0f}</td>
            <td style="{cell_right_style}">{price}</td>
            <td style="{cell_center_style}">{row['截止日期']}</td>
            <td style="{cell_style}">{row['持股变动原因']}</td>
        </tr>"""

    # ========== 生成买入信号卡片 ==========
    signal_cards = ""
    signal_card_count = 0
    for idx, (i, row) in enumerate(summary_df.iterrows()):
        recommendation = row.get('投资建议', '-')
        # 只展示可操作的信号（🟢买入 / 🟡观望），🔴不通过的只在筛选明细表里
        if recommendation == "🔴":
            continue
        signal_card_count += 1
        analysis = str(row.get('分析观点', ''))
        stock_code = row['证券代码']
        stock_name = row['证券简称']
        
        # 提取三重过滤图标
        triple_match = re.search(r'【三重([✅❌]+)】', analysis)
        triple_icons = triple_match.group(1) if triple_match else "---"
        
        # 提取操作建议
        op_match = re.search(r'💰操作建议：(.+?)$', analysis)
        advice = op_match.group(1).strip() if op_match else ""
        
        # 卡片背景色
        if recommendation == "🟢":
            card_border = "#27ae60"
            card_bg = "#f0fff4"
        elif recommendation == "🟡":
            card_border = "#f39c12"
            card_bg = "#fffbf0"
        else:
            card_border = "#e74c3c"
            card_bg = "#fff5f5"
        
        # 关键指标
        price = f"{row['current_price']:.2f}" if pd.notna(row.get('current_price')) else "-"
        change_pct = row.get('price_change_pct', 0) or 0
        change_color = "#FF0000" if change_pct > 0 else "#00AA00" if change_pct < 0 else "black"
        s_type = row.get('股票类型', '-')
        v_desc = row.get('估值判断', '-')
        
        _avg_hp = row.get('高管持仓均价')
        _cur_p = row.get('current_price')
        if _avg_hp and _avg_hp > 0 and _cur_p and _cur_p > 0:
            _premium = (_cur_p - _avg_hp) / _avg_hp
            premium_str = f"{_premium:+.0%}"
        else:
            premium_str = "-"
        
        sr = row.get('增持年薪比')
        sr_str = f"{sr:.1f}倍" if sr and pd.notna(sr) and sr > 0 else "-"
        
        p_trend = row.get('profit_trend', '')
        p_detail = row.get('profit_trend_detail', '')
        
        pe_val = row.get('pe_ratio', 0)
        pe_type_label = row.get('pe_type', '')
        pe_str = f"{pe_val:.1f}({pe_type_label})" if pd.notna(pe_val) and pe_val else "-"
        
        # 仓位分级和目标涨幅
        pos_tier = row.get('仓位分级', '-')
        pos_pct = row.get('建议仓位', '')
        tgt_return = row.get('目标涨幅', '-')
        tgt_logic = row.get('涨幅逻辑', '')
        
        # 仓位颜色
        pos_color = "#e74c3c" if pos_tier == "重仓" else "#f39c12" if pos_tier == "中仓" else "#3498db" if pos_tier == "轻仓" else "#999"
        
        # 目标涨幅行（仅有效时显示）
        target_line = ""
        if tgt_return and tgt_return != "-":
            target_line = f"<br>🎯 <b style='color:#e74c3c;'>目标涨幅 {tgt_return}</b>（{tgt_logic}）"
        
        signal_cards += f"""
        <div style="border-left:4px solid {card_border};background:{card_bg};padding:12px 16px;margin-bottom:12px;border-radius:4px;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <span style="font-size:16px;font-weight:bold;">{recommendation} <a href="{eastmoney_url(stock_code)}" target="_blank" style="color:inherit;text-decoration:underline;">{stock_code}</a> {stock_name}</span>
                <span style="font-size:14px;color:{change_color};">{price} ({change_pct:+.2f}%)</span>
            </div>
            <div style="font-size:13px;color:#555;line-height:1.8;">
                三重过滤 {triple_icons} | {s_type} | PE {pe_str} | 增持/年薪 {sr_str} | 溢价率 {premium_str} | 利润{p_trend}<br>
                <span style="color:#333;font-weight:bold;">{advice}</span><br>
                💼 <b style="color:{pos_color};">{pos_tier} {pos_pct}</b>{target_line}
                {"<br><span style='color:#888;font-size:11px;'>季度同比: " + p_detail + "</span>" if p_detail else ""}
            </div>
        </div>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;padding:20px;line-height:1.6;font-size:14px;color:#333;">
    <h2 style="color:#2c3e50;">📡 高管增持选股信号 - {today}</h2>
    <p style="color:#666;font-size:13px;">筛选条件：≥{MIN_EXECUTIVES}位普通高管竞价增持，排除大股东/ST，时间窗口{QUERY_MONTHS}个月 | 三重过滤：①高管增持 ②基本面估值 ③增持溢价率</p>

    {index_html}

    <h3 style="color:#34495e;">🎯 选股信号（{signal_card_count} 家可操作 / 共 {len(summary_df)} 家监控）</h3>
    {signal_cards}

    <h3 style="color:#34495e;">📋 筛选明细</h3>
    <table style="{table_style}">
        <tr>
            <th style="{header_style}">证券代码</th>
            <th style="{header_style}">证券简称</th>
            <th style="{header_style}">当前价</th>
            <th style="{header_style}">涨跌幅</th>
            <th style="{header_style}">最近增持日</th>
            <th style="{header_style}">增持高管数</th>
            <th style="{header_style}">增持/年薪</th>
            <th style="{header_style}">公司市值</th>
            <th style="{header_style}">股票类型</th>
            <th style="{header_style}">PE</th>
            <th style="{header_style}">PB</th>
            <th style="{header_style}">ROE</th>
            <th style="{header_style}">利润趋势</th>
            <th style="{header_style}">增持均价</th>
            <th style="{header_style}">溢价率</th>
            <th style="{header_style}">估值判断</th>
        </tr>
        {screening_rows}
    </table>

    <details style="margin-top:15px;">
        <summary style="cursor:pointer;color:#3498db;font-weight:bold;">📝 增持明细（点击展开）</summary>
        <table style="{table_style};margin-top:8px;">
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
    </details>

    <p style="color:#666;font-size:11px;margin-top:20px;line-height:1.8;">
        📖 <b>指标说明：</b>
        <b>增持/年薪</b> = 每位高管增持金额÷其年薪的中位数，≥2倍为强信号 |
        <b>PE</b> = 优先取扣非PE-TTM |
        <b>利润趋势</b> = 最近4个季度扣非净利润同比 |
        <b>溢价率</b> = (当前价-增持均价)/增持均价
    </p>
    <p style="color:#999;font-size:11px;">
        数据来源：巨潮资讯网、akshare | 🆕 新增公司 | 自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M')}
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


def send_email(html_content: str, test_mode: bool = False):
    """通过 QQ 邮箱 SMTP 发送 HTML 邮件给所有订阅者

    test_mode: 只发送给测试邮箱 1225106113@qq.com
    """
    if not EMAIL_PASSWORD:
        log.error("未配置 SMTP 授权码，请在 config.py 中填写 EMAIL_PASSWORD")
        sys.exit(1)

    if test_mode:
        subscribers = [{"email": "1225106113@qq.com", "name": "老板(测试)", "active": True}]
        log.info("🧪 测试模式：只发送给 1225106113@qq.com")
    else:
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-email", action="store_true", help="不发送邮件")
    parser.add_argument("--test", action="store_true", help="测试模式：只发给 1225106113@qq.com")
    args = parser.parse_args()
    
    log.info("=== 高管增持监控开始 ===")
    if args.test:
        log.info("🧪 测试模式")
    
    # 检查是否为交易日（测试模式跳过检查）
    if not args.test and not args.no_email and not is_trading_day():
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
        if args.no_email:
            log.info("--no-email 模式，跳过发送")
        else:
            send_email(html, test_mode=args.test)
        
    except Exception:
        log.exception("运行出错")
        sys.exit(1)
    
    log.info("=== 高管增持监控完成 ===")


if __name__ == "__main__":
    main()