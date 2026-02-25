#!/usr/bin/env python3
"""
系统B：持仓管理监控
- 读取 portfolio.json
- 获取实时行情 + 公告扫描
- 对ETF用均线判断，对个股用基本面判断
- 推送渠道：Telegram / 邮件
- 模式：daily（日报）/ alert（盘中止损监控）
"""

import json
import os
import sys
import logging
import requests
import smtplib
import re
import time
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    import akshare as ak
except ImportError:
    print("请安装 akshare: pip install akshare")
    sys.exit(1)

from config import EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT

# 引入 System A 的三重过滤链，用于持仓复验
try:
    from main import get_fundamental_data, classify_stock_type, evaluate_by_type, get_stock_price_data, get_market_cap
    SYSTEM_A_AVAILABLE = True
except ImportError:
    SYSTEM_A_AVAILABLE = False
    logging.warning("无法引入 System A 三重过滤模块，持仓复验将跳过")

log = logging.getLogger("portfolio")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PORTFOLIO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio.json")
ALERT_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_state.json")
SIGNAL_TRACK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal_track.json")

# Telegram 配置（从环境变量读取）
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")


def load_portfolio() -> Dict:
    with open(PORTFOLIO_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_alert_state() -> Dict:
    """加载止损报警状态（避免重复推送）"""
    if os.path.exists(ALERT_STATE_FILE):
        with open(ALERT_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_alert_state(state: Dict):
    with open(ALERT_STATE_FILE, 'w') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_signal_track() -> Dict:
    """加载信号追踪状态（记录danger信号首次出现日期和累计天数）"""
    if os.path.exists(SIGNAL_TRACK_FILE):
        with open(SIGNAL_TRACK_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_signal_track(track: Dict):
    with open(SIGNAL_TRACK_FILE, 'w') as f:
        json.dump(track, f, ensure_ascii=False, indent=2)


def update_signal_track(code: str, signal_key: str, is_active: bool, track: Dict) -> int:
    """
    更新信号追踪，返回信号持续天数
    signal_key: 如 "空头排列", "触及止损" 等
    """
    key = f"{code}_{signal_key}"
    today = datetime.now().strftime('%Y-%m-%d')

    if is_active:
        if key not in track:
            track[key] = {"first_date": today, "last_date": today, "days": 1}
        else:
            first = track[key]["first_date"]
            days = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(first, '%Y-%m-%d')).days + 1
            # 只算交易日（粗略：总天数 * 5/7）
            trading_days = max(1, int(days * 5 / 7))
            track[key]["last_date"] = today
            track[key]["days"] = trading_days
        return track[key]["days"]
    else:
        # 信号消失，清除追踪
        if key in track:
            del track[key]
        return 0


def get_realtime_prices(codes: List[str]) -> Dict[str, Dict]:
    """腾讯实时行情"""
    try:
        qq_codes = []
        for code in codes:
            prefix = "sh" if code.startswith(('5', '6')) else "sz"
            qq_codes.append(f'{prefix}{code}')

        url = f"http://qt.gtimg.cn/q={','.join(qq_codes)}"
        r = requests.get(url, timeout=5, proxies={'http': '', 'https': ''})

        result = {}
        for line in r.text.strip().split(';'):
            line = line.strip()
            if not line or '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 45:
                continue
            code = parts[2]
            try:
                result[code] = {
                    'price': float(parts[3]),
                    'prev_close': float(parts[4]),
                    'open': float(parts[5]),
                    'high': float(parts[33]),
                    'low': float(parts[34]),
                    'change_pct': float(parts[32]),
                    'volume': float(parts[36]) if parts[36] else 0,
                    'name': parts[1],
                }
            except (ValueError, IndexError):
                continue
        return result
    except Exception as e:
        log.warning(f"获取实时行情失败: {e}")
        return {}


def get_etf_ma_data(code: str) -> Dict:
    """获取ETF均线数据（新浪源）"""
    prefix = "sh" if code.startswith(('5', '6')) else "sz"
    symbol = f"{prefix}{code}"
    try:
        df = ak.fund_etf_hist_sina(symbol=symbol)
        if df is None or df.empty:
            return {}
        df = df.sort_values('date').reset_index(drop=True)
        closes = df['close'].values
        if len(closes) < 60:
            return {}

        ma5 = closes[-5:].mean()
        ma10 = closes[-10:].mean()
        ma20 = closes[-20:].mean()
        ma30 = closes[-30:].mean() if len(closes) >= 30 else None
        ma60 = closes[-60:].mean()
        current = closes[-1]

        if ma30 and current > ma10 > ma20 > ma30 > ma60:
            arrangement = "多头排列"
            signal = "🟢 持有"
        elif ma30 and current < ma10 < ma20 < ma30 < ma60:
            arrangement = "空头排列"
            signal = "🔴 空仓回避"
        elif current > ma20 > ma60:
            arrangement = "偏多"
            signal = "🟢 持有"
        elif current < ma20 < ma60:
            arrangement = "偏空"
            signal = "🔴 考虑减仓"
        elif current > ma20 and current < ma60:
            arrangement = "反弹中"
            signal = "🟡 观察"
        elif current < ma20 and current > ma60:
            arrangement = "回调中"
            signal = "🟡 关注MA60支撑"
        else:
            arrangement = "纠缠"
            signal = "🟡 观望"

        return {
            'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma30': ma30, 'ma60': ma60,
            'arrangement': arrangement, 'signal': signal,
            'bias_ma20': (current - ma20) / ma20 * 100,
            'bias_ma60': (current - ma60) / ma60 * 100,
        }
    except Exception as e:
        log.warning(f"获取ETF {code} 均线数据失败: {e}")
        return {}


# ============================================================
# 公告扫描（高管减持 / 业绩 / 监管处罚等重大公告）
# ============================================================

def scan_announcements(codes: List[str], names: Dict[str, str]) -> List[Dict]:
    """
    扫描持仓股票的重大公告
    返回: [{"code", "name", "type", "title", "date", "level", "action"}]
    """
    alerts = []

    # 1. 高管减持（巨潮）
    alerts.extend(_scan_insider_selling(codes, names))

    # 2. 重大公告关键词（巨潮搜索）
    alerts.extend(_scan_key_announcements(codes, names))

    return alerts


def _scan_insider_selling(codes: List[str], names: Dict[str, str]) -> List[Dict]:
    """检查持仓股高管减持"""
    alerts = []
    try:
        df = ak.stock_hold_management_detail_cninfo(symbol="减持")
        if df is None or df.empty:
            return alerts
        cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        for code in codes:
            matches = df[df['证券代码'] == code]
            if matches.empty:
                continue
            # 只看最近30天
            recent = matches[matches['截止日期'].astype(str) >= cutoff]
            if recent.empty:
                continue
            latest_date = str(recent['截止日期'].max())[:10]
            total_rows = len(recent)
            alerts.append({
                "code": code,
                "name": names.get(code, code),
                "type": "高管减持",
                "title": f"近30天有{total_rows}笔高管减持",
                "date": latest_date,
                "level": "danger",
                "action": "内部人在卖，强烈建议清仓"
            })
    except Exception as e:
        log.warning(f"检查高管减持失败: {e}")
    return alerts


def _scan_key_announcements(codes: List[str], names: Dict[str, str]) -> List[Dict]:
    """
    通过巨潮/同花顺检查重大公告关键词
    关键词：立案调查、行政处罚、业绩预亏、业绩大幅下降、退市风险、ST
    """
    alerts = []
    danger_keywords = ["立案调查", "行政处罚", "监管措施", "退市风险警示", "暂停上市",
                       "业绩预亏", "业绩大幅下降", "重大亏损"]
    warning_keywords = ["业绩预减", "业绩修正", "股东减持", "质押"]

    for code in codes:
        try:
            # 用同花顺个股公告
            df = ak.stock_notice_report(symbol=code)
            if df is None or df.empty:
                continue
            # 只看最近7天的公告
            cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            for _, row in df.iterrows():
                pub_date = str(row.get('公告日期', ''))[:10]
                if pub_date < cutoff:
                    continue
                title = str(row.get('公告标题', ''))

                for kw in danger_keywords:
                    if kw in title:
                        alerts.append({
                            "code": code,
                            "name": names.get(code, code),
                            "type": "重大公告",
                            "title": title[:50],
                            "date": pub_date,
                            "level": "danger",
                            "action": f"⚠️ 检测到「{kw}」，立即评估是否清仓"
                        })
                        break

                for kw in warning_keywords:
                    if kw in title:
                        alerts.append({
                            "code": code,
                            "name": names.get(code, code),
                            "type": "关注公告",
                            "title": title[:50],
                            "date": pub_date,
                            "level": "warning",
                            "action": f"关注「{kw}」"
                        })
                        break
        except Exception as e:
            log.debug(f"检查 {code} 公告失败: {e}")

    return alerts


def revalidate_with_system_a(code: str, name: str, manual_class: str = None) -> List[Dict]:
    """
    用 System A 的三重过滤链对持仓进行复验：
    1. 获取基本面数据（get_fundamental_data）
    2. 自动分类（classify_stock_type）
    3. 估值判断（evaluate_by_type）
    
    如果"买入理由已失效"（分类变了、估值不通过），返回对应的danger/warning信号。
    """
    if not SYSTEM_A_AVAILABLE:
        return []

    signals = []
    try:
        # 获取市值（用于PS计算等）
        market_cap = None
        try:
            market_cap = get_market_cap(code)
        except:
            pass

        # 获取基本面数据
        fund_data = get_fundamental_data(code, market_cap_yi=market_cap, stock_name=name)
        if not fund_data or fund_data.get("net_profit") is None:
            return []

        # 自动分类
        auto_type = classify_stock_type(fund_data)

        # 估值判断
        valuation_pass, valuation_desc = evaluate_by_type(auto_type, fund_data)

        # 如果手动指定了困境反转，用困境反转重新评估
        if manual_class == "困境反转" and auto_type == "亏损":
            auto_type = "困境反转"
            valuation_pass, valuation_desc = evaluate_by_type("困境反转", fund_data)

        # === 核心逻辑：检查买入理由是否还成立 ===

        # 1. 分类漂移检查
        if manual_class and auto_type != manual_class and manual_class != "困境反转":
            signals.append({
                "signal": f"⚠️ 分类漂移：买入时={manual_class}，现在={auto_type}",
                "level": "warning",
                "action": f"股票性质可能已变，重新评估"
            })

        # 2. 三重过滤不通过 = 买入理由失效
        if not valuation_pass:
            # 根据分类给出具体的"理由失效"描述
            effective_class = manual_class or auto_type

            if effective_class == "成长股":
                reason = "成长股利润增速转负/放缓 → 林奇原则：增速下滑立即清仓"
                level = "danger"
            elif effective_class == "周期股":
                reason = "周期股估值不再便宜或利润拐头向下"
                level = "warning"
            elif effective_class == "价值股":
                reason = "价值股估值偏高或业绩下滑 → PE可能是陷阱"
                level = "warning"
            elif effective_class == "困境反转":
                reason = "困境反转条件不再满足"
                level = "warning"
            else:
                reason = "估值不通过"
                level = "warning"

            signals.append({
                "signal": f"🔴 三重过滤复验不通过({auto_type}): {valuation_desc}",
                "level": level,
                "action": reason
            })
        else:
            # 通过了，给个正面确认
            signals.append({
                "signal": f"✅ 三重过滤复验通过({auto_type}): {valuation_desc}",
                "level": "info",
                "action": "买入逻辑仍然成立"
            })

        log.info(f"  {code} {name} 三重过滤复验: 类型={auto_type}, 通过={valuation_pass}, {valuation_desc}")

    except Exception as e:
        log.warning(f"  {code} 三重过滤复验失败: {e}")

    return signals


def get_stock_fundamental_signals(code: str, stock_type_hint: str = None) -> Tuple[List[Dict], str]:
    """
    获取个股基本面信号（利润趋势 + 分类相关预警）
    返回: (signals, stock_type)
    """
    signals = []
    stock_type = stock_type_hint or "未知"

    try:
        fin_q = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
        if fin_q is None or fin_q.empty:
            return signals, stock_type

        def parse_amount(val):
            if val is None or str(val).strip() in ('', '-', 'None', 'nan'):
                return None
            s = str(val).replace(',', '')
            if '亿' in s:
                return float(s.replace('亿', ''))
            elif '万' in s:
                return float(s.replace('万', '')) / 10000
            try:
                return float(s)
            except:
                return None

        def parse_pct(val):
            if val is None or str(val).strip() in ('', '-', 'None', 'nan', 'False'):
                return None
            s = str(val).replace('%', '').replace(',', '')
            try:
                return float(s)
            except:
                return None

        period_profit = {}
        period_rev = {}
        period_gm = {}
        for _, row in fin_q.iterrows():
            period = str(row.get("报告期", ""))
            profit = parse_amount(row.get("扣非净利润")) or parse_amount(row.get("净利润"))
            rev = parse_amount(row.get("营业总收入"))
            gm = parse_pct(row.get("销售毛利率"))
            if period:
                if profit is not None:
                    period_profit[period] = profit
                if rev is not None:
                    period_rev[period] = rev
                if gm is not None:
                    period_gm[period] = gm

        sorted_periods = sorted(period_profit.keys(), reverse=True)

        # === 利润趋势 ===
        yoy_changes = []
        for p in sorted_periods[:4]:
            year = int(p[:4])
            prev_p = f"{year-1}{p[4:]}"
            if prev_p in period_profit and period_profit[prev_p] != 0:
                yoy = (period_profit[p] - period_profit[prev_p]) / abs(period_profit[prev_p])
                yoy_changes.append((p, yoy))

        if yoy_changes:
            down_count = sum(1 for _, y in yoy_changes if y < -0.1)
            up_count = sum(1 for _, y in yoy_changes if y > 0.1)

            if down_count >= len(yoy_changes) * 0.75:
                signals.append({
                    "signal": "利润趋势下降",
                    "level": "danger",
                    "detail": " | ".join(f"{p}:{y:+.0%}" for p, y in yoy_changes),
                    "action": "基本面恶化，考虑减仓或清仓"
                })
            elif down_count >= len(yoy_changes) * 0.5:
                signals.append({
                    "signal": "利润增速放缓",
                    "level": "warning",
                    "detail": " | ".join(f"{p}:{y:+.0%}" for p, y in yoy_changes),
                    "action": "关注后续季度表现"
                })

            # 周期股利润拐点
            if up_count >= len(yoy_changes) * 0.75:
                signals.append({
                    "signal": "🟢 利润拐点向上",
                    "level": "info",
                    "detail": " | ".join(f"{p}:{y:+.0%}" for p, y in yoy_changes),
                    "action": "周期反转信号，继续持有"
                })

        # === 营收增速趋势（成长股放缓预警） ===
        rev_yoy_list = []
        for p in sorted_periods[:4]:
            year = int(p[:4])
            prev_p = f"{year-1}{p[4:]}"
            if prev_p in period_rev and period_rev[prev_p] != 0:
                rev_yoy = (period_rev[p] - period_rev[prev_p]) / abs(period_rev[prev_p]) * 100
                rev_yoy_list.append((p, rev_yoy))

        if rev_yoy_list and len(rev_yoy_list) >= 2:
            latest_rev_g = rev_yoy_list[0][1]
            prev_rev_g = rev_yoy_list[1][1]
            if prev_rev_g > 20 and latest_rev_g < prev_rev_g * 0.5:
                signals.append({
                    "signal": f"营收增速放缓({prev_rev_g:.0f}%→{latest_rev_g:.0f}%)",
                    "level": "warning",
                    "detail": " | ".join(f"{p}:+{y:.0f}%" for p, y in rev_yoy_list),
                    "action": "成长放缓，关注PEG变化"
                })

        # === 毛利率趋势 ===
        # 如果利润在上升（周期反转），毛利率下滑降为info（量增价升但成本也涨的正常现象）
        profit_is_rising = any(s.get('signal', '').startswith('🟢') for s in signals)
        gm_sorted = sorted(period_gm.items(), key=lambda x: x[0], reverse=True)
        if len(gm_sorted) >= 3:
            gm_vals = [v for _, v in gm_sorted[:3]]
            if all(gm_vals[i] < gm_vals[i+1] for i in range(len(gm_vals)-1)):
                gm_level = "info" if profit_is_rising else "warning"
                gm_action = "利润在涨，毛利率下滑影响有限" if profit_is_rising else "盈利能力下降"
                signals.append({
                    "signal": f"毛利率下滑({gm_vals[-1]:.1f}%→{gm_vals[0]:.1f}%)",
                    "level": gm_level,
                    "detail": " | ".join(f"{p}:{v:.1f}%" for p, v in gm_sorted[:3]),
                    "action": gm_action
                })

        # === 自动分类（提前，供后续判断使用） ===
        latest_profit = period_profit.get(sorted_periods[0]) if sorted_periods else None
        latest_rev_g_val = rev_yoy_list[0][1] if rev_yoy_list else 0
        latest_profit_g = yoy_changes[0][1] * 100 if yoy_changes else 0

        if latest_profit is not None and latest_profit < 0:
            if latest_rev_g_val > 30:
                stock_type = "困境反转"
            else:
                stock_type = "亏损"
        elif latest_rev_g_val > 15 and latest_profit_g > 15:
            stock_type = "成长股"
        elif latest_profit_g > 50 or latest_profit_g < -50:
            stock_type = "周期股"
        else:
            stock_type = "价值股"

        # 外部手动指定的分类优先（stock_type_hint）
        if stock_type_hint:
            stock_type = stock_type_hint

        # === 最新季度亏损（困境反转除外） ===
        if sorted_periods and period_profit.get(sorted_periods[0], 0) < 0:
            if stock_type == "困境反转":
                signals.append({
                    "signal": "最新季度仍亏损（困境反转中）",
                    "level": "info",
                    "detail": f"{sorted_periods[0]}: {period_profit[sorted_periods[0]]:.2f}亿",
                    "action": "困境反转预期中的亏损，关注营收和毛利率趋势"
                })
            else:
                signals.append({
                    "signal": "最新季度亏损",
                    "level": "danger",
                    "detail": f"{sorted_periods[0]}: {period_profit[sorted_periods[0]]:.2f}亿",
                    "action": "亏损股建议清仓"
                })

    except Exception as e:
        log.warning(f"获取 {code} 基本面信号失败: {e}")

    return signals, stock_type


# ============================================================
# 大盘仓位指引
# ============================================================

MARKET_INDICES = {
    'sh000001': '上证指数',
    'sh000300': '沪深300',
    'sz399006': '创业板指',
    'sz399673': '创业板50',
    'sh000688': '科创50',
}

# 仓位指引表：(多头数, 偏多数) → 建议仓位区间
POSITION_GUIDE = {
    # 多头数>=3 → 激进
    'bullish': (70, 80),
    # 偏多为主 → 积极
    'positive': (50, 70),
    # 纠缠为主 → 中性
    'neutral': (30, 50),
    # 偏空为主 → 防守
    'bearish': (15, 30),
}


def get_index_ma_status() -> List[Dict]:
    """获取主要指数均线状态"""
    results = []
    for code, name in MARKET_INDICES.items():
        try:
            df = ak.stock_zh_index_daily(symbol=code)
            if df is None or df.empty:
                continue
            df = df.sort_values('date').reset_index(drop=True)
            closes = df['close'].values
            if len(closes) < 60:
                continue

            current = closes[-1]
            ma10 = closes[-10:].mean()
            ma20 = closes[-20:].mean()
            ma30 = closes[-30:].mean()
            ma60 = closes[-60:].mean()
            ma250 = closes[-250:].mean() if len(closes) >= 250 else None

            # 均线排列
            if current > ma10 > ma20 > ma30 > ma60:
                arrangement = "多头排列"
                score = 2
                icon = "🟢"
            elif current < ma10 < ma20 < ma30 < ma60:
                arrangement = "空头排列"
                score = -2
                icon = "🔴"
            elif current > ma20 > ma60:
                arrangement = "偏多"
                score = 1
                icon = "🟢"
            elif current < ma20 < ma60:
                arrangement = "偏空"
                score = -1
                icon = "🔴"
            elif current > ma20 and current < ma60:
                arrangement = "反弹中"
                score = 0
                icon = "🟡"
            elif current < ma20 and current > ma60:
                arrangement = "回调中"
                score = 0
                icon = "🟡"
            else:
                arrangement = "纠缠"
                score = 0
                icon = "🟡"

            # 用实时价格覆盖（日K不含当天）
            try:
                r = requests.get(f'http://qt.gtimg.cn/q={code}', timeout=5, proxies={'http': '', 'https': ''})
                parts = r.text.split('~')
                if len(parts) > 32:
                    current = float(parts[3])
                    change_pct = float(parts[32])
                else:
                    change_pct = 0
            except:
                change_pct = 0

            above_ma250 = current > ma250 if ma250 else None
            bias20 = (current - ma20) / ma20 * 100

            results.append({
                'code': code, 'name': name, 'price': current,
                'change_pct': change_pct,
                'arrangement': arrangement, 'score': score, 'icon': icon,
                'bias20': bias20, 'above_ma250': above_ma250,
                'ma20': ma20, 'ma60': ma60,
            })
        except Exception as e:
            log.warning(f"获取 {name} 均线数据失败: {e}")

    return results


def calc_position_guide(index_data: List[Dict], current_position_pct: float, total_assets: float) -> Dict:
    """
    根据大盘指数状态计算仓位建议
    返回: {level, target_low, target_high, suggestion, details}
    """
    if not index_data:
        return {"level": "neutral", "target_low": 40, "target_high": 60, "suggestion": "数据不足，维持半仓"}

    total_score = sum(d['score'] for d in index_data)
    bullish_count = sum(1 for d in index_data if d['score'] >= 1)
    bearish_count = sum(1 for d in index_data if d['score'] <= -1)
    above250_count = sum(1 for d in index_data if d.get('above_ma250'))
    n = len(index_data)

    # 判断大盘整体状态
    if bullish_count >= n * 0.6 and above250_count >= n * 0.6:
        level = "bullish"
        target_low, target_high = 70, 80
        market_status = "多数偏多/多头+站上年线"
    elif bullish_count >= n * 0.4:
        level = "positive"
        target_low, target_high = 50, 70
        market_status = "偏多格局"
    elif bearish_count >= n * 0.6:
        level = "bearish"
        target_low, target_high = 15, 30
        market_status = "偏空/空头格局"
    elif bearish_count >= n * 0.4:
        level = "bearish"
        target_low, target_high = 20, 40
        market_status = "偏弱格局"
    else:
        level = "neutral"
        target_low, target_high = 30, 50
        market_status = "纠缠震荡"

    # 检查持仓中有无待清仓标的（空头ETF等）
    # 这个在外部传入
    has_sell_signals = False  # 默认，外部覆盖

    # 建议
    target_mid = (target_low + target_high) / 2
    if current_position_pct < target_low:
        diff_yuan = (target_mid - current_position_pct) / 100 * total_assets
        suggestion = f"仓位偏低，建议仓位{target_low}-{target_high}%，可加仓约{diff_yuan/10000:.1f}万"
        suggestion_icon = "📈"
        # 但如果没有选股标的，提示不要盲目加仓
        suggestion += "\n   ⚠️ 加仓前提：有System A三重过滤通过的标的，不要为了加仓而买"
    elif current_position_pct > target_high:
        diff_yuan = (current_position_pct - target_mid) / 100 * total_assets
        suggestion = f"仓位偏高，考虑减仓约{diff_yuan/10000:.1f}万（到{target_mid:.0f}%）"
        suggestion_icon = "📉"
    else:
        suggestion = "仓位在合理区间"
        suggestion_icon = "✅"

    return {
        "level": level,
        "market_status": market_status,
        "target_low": target_low,
        "target_high": target_high,
        "suggestion": suggestion,
        "suggestion_icon": suggestion_icon,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "above250_count": above250_count,
        "total": n,
        "index_data": index_data,
    }


# ============================================================
# 核心分析
# ============================================================

def analyze_portfolio(include_announcements=True) -> Tuple[str, List[Dict]]:
    """
    分析持仓，返回 (report_text, announcement_alerts)
    """
    portfolio = load_portfolio()
    account = portfolio['accounts'][0]
    rules = portfolio.get('rules', {})

    holdings = account['holdings']
    all_codes = [h['code'] for h in holdings]
    code_names = {h['code']: h['name'] for h in holdings}

    # 获取实时行情
    rt = get_realtime_prices(all_codes)
    log.info(f"获取实时行情: {len(rt)}/{len(all_codes)}")

    total_assets = account['total_assets']
    available = account['available_cash']

    results = []
    total_market_value = 0
    total_pnl = 0

    for h in holdings:
        code = h['code']
        name = h['name']
        shares = h['shares']
        cost = h['cost']
        h_type = h.get('type', 'stock')

        if code in rt:
            price = rt[code]['price']
            change_pct = rt[code]['change_pct']
        else:
            price = cost
            change_pct = 0

        market_value = price * shares
        pnl = (price - cost) * shares
        pnl_pct = (price - cost) / cost * 100
        position_pct = market_value / total_assets * 100

        total_market_value += market_value
        total_pnl += pnl

        signals = []
        advice = "持有"
        advice_icon = "🟢"
        detected_type = "指数ETF" if h_type == 'etf' else "未知"

        stop_method = h.get('stop_method', 'price')

        if h_type == 'etf' and stop_method == 'ma':
            ma_data = get_etf_ma_data(code)
            if ma_data:
                arr = ma_data['arrangement']
                if "空头" in arr:
                    signals.append({"signal": f"均线{arr}", "level": "danger", "action": "🔴 空头排列，建议清仓"})
                    advice = "均线空头，清仓"
                    advice_icon = "🔴"
                elif "偏空" in arr:
                    signals.append({"signal": f"均线{arr}", "level": "warning", "action": "偏空，考虑减仓"})
                    advice = "均线偏空，减仓"
                    advice_icon = "🟡"
                elif "回调" in arr:
                    signals.append({"signal": f"均线{arr}，关注MA60支撑", "level": "warning", "action": "跌破MA60则清仓"})
                    if advice_icon == "🟢":
                        advice_icon = "🟡"
                        advice = "回调中，关注MA60"
                else:
                    signals.append({"signal": f"均线{arr}", "level": "info", "action": ma_data['signal']})

                signals.append({
                    "signal": f"偏离MA20 {ma_data['bias_ma20']:+.1f}% | MA60 {ma_data['bias_ma60']:+.1f}%",
                    "level": "info", "action": ""
                })
        else:
            stop_price = h.get('stop_price')
            high_price = h.get('high_price') or cost
            trailing_stop_pct = rules.get('trailing_stop_pct', 15)

            # 更新历史最高价
            if price > high_price:
                high_price = price
                # 写回portfolio（运行时更新）
                h['high_price'] = high_price

            # 移动止损：从最高价回撤trailing_stop_pct%
            trailing_stop = high_price * (1 - trailing_stop_pct / 100)
            # 取移动止损和固定止损中较高的那个（更严格）
            effective_stop = max(trailing_stop, stop_price) if stop_price else trailing_stop

            if effective_stop and price <= effective_stop:
                if stop_price and price <= stop_price:
                    signals.append({"signal": f"‼️ 跌破固定止损价{stop_price}", "level": "danger", "action": "按计划止损清仓"})
                else:
                    signals.append({"signal": f"‼️ 触发移动止损（高点{high_price:.3f}回撤{trailing_stop_pct}%→{trailing_stop:.3f}）", "level": "danger", "action": "移动止损触发，清仓"})
                advice = "触及止损，清仓"
                advice_icon = "🔴"
            elif effective_stop and price <= effective_stop * 1.05:
                signals.append({"signal": f"接近止损（固定{stop_price}/移动{trailing_stop:.3f}，仅差{(price/effective_stop-1)*100:.1f}%）", "level": "warning", "action": "密切关注"})

            # === 分批止盈 ===
            gain_pct = (price - cost) / cost * 100
            take_profit_rules = rules.get('take_profit_rules', [])
            for tp in take_profit_rules:
                tp_gain = tp.get('gain_pct', -1)
                tp_sell = tp.get('sell_pct', 0)
                if tp_gain > 0 and gain_pct >= tp_gain and tp_sell > 0:
                    signals.append({
                        "signal": f"📈 盈利{gain_pct:.0f}%，达到{tp_gain}%减仓线",
                        "level": "info",
                        "action": f"{tp.get('note', f'减仓{tp_sell}%')}"
                    })
                    if advice_icon != "🔴":
                        advice = f"盈利{gain_pct:.0f}%，可减仓{tp_sell}%锁定利润"
                        advice_icon = "🟡"
                    break  # 只触发最高档位

            # 基本面（传入手动分类，避免困境反转被误杀）
            manual_class = h.get('stock_class')
            fund_signals, detected_type = get_stock_fundamental_signals(code, stock_type_hint=manual_class)
            # portfolio.json中手动指定的分类优先
            if manual_class:
                detected_type = manual_class
            signals.extend(fund_signals)
            for s in fund_signals:
                if s['level'] == 'danger' and advice_icon != "🔴":
                    advice = s['action']
                    advice_icon = "🔴"
                elif s['level'] == 'warning' and advice_icon == "🟢":
                    advice = s['action']
                    advice_icon = "🟡"

            insider_price = h.get('insider_avg_price')
            if insider_price:
                insider_pct = (price - insider_price) / insider_price * 100
                if insider_pct < -15:
                    signals.append({"signal": f"深度跌破增持均价({insider_pct:.0f}%)", "level": "danger", "action": "基本面可能有问题"})
                elif insider_pct < 0:
                    signals.append({"signal": f"跌破增持均价({insider_pct:.0f}%)", "level": "warning", "action": "关注基本面"})
                else:
                    signals.append({"signal": f"高于增持均价{insider_pct:.0f}%", "level": "info", "action": ""})

        # === P0: System A 三重过滤复验（买入理由是否还成立） ===
        if h_type != 'etf':
            reval_signals = revalidate_with_system_a(code, name, manual_class=h.get('stock_class'))
            signals.extend(reval_signals)
            for s in reval_signals:
                if s['level'] == 'danger' and advice_icon != "🔴":
                    advice = s['action']
                    advice_icon = "🔴"
                elif s['level'] == 'warning' and advice_icon == "🟢":
                    advice = s['action']
                    advice_icon = "🟡"

        # === P1: 分类专属卖出/持有逻辑（林奇：卖出理由=买入逻辑失效） ===
        if h_type != 'etf' and detected_type:
            _class = detected_type
            # 从fund_signals中提取已检测到的信号
            _has_profit_down = any('利润趋势下降' in s.get('signal', '') for s in fund_signals)
            _has_profit_up = any('利润拐点向上' in s.get('signal', '') for s in fund_signals)
            _has_rev_decel = any('营收增速放缓' in s.get('signal', '') for s in fund_signals)
            _has_gm_decline = any('毛利率下滑' in s.get('signal', '') for s in fund_signals)
            _has_q_loss = any('最新季度亏损' in s.get('signal', '') and s.get('level') == 'danger' for s in fund_signals)

            if _class == "成长股":
                # 成长股卖出逻辑：增速连续放缓 → 立即卖出（林奇：成长停滞就卖）
                if _has_rev_decel and _has_profit_down:
                    signals.append({"signal": "⚠️ 成长股核心逻辑失效：营收+利润双放缓", "level": "danger",
                                    "action": "成长停滞=卖出，不等反弹"})
                    if advice_icon != "🔴":
                        advice = "成长逻辑失效，建议清仓"
                        advice_icon = "🔴"
                elif _has_rev_decel:
                    signals.append({"signal": "成长股预警：营收增速放缓", "level": "warning",
                                    "action": "密切关注下季度，连续放缓则清仓"})

            elif _class == "周期股":
                # 周期股卖出逻辑：利润重新转负 → 周期见顶信号
                if _has_profit_down and not _has_profit_up:
                    signals.append({"signal": "⚠️ 周期股预警：利润拐头向下", "level": "warning",
                                    "action": "周期可能见顶，考虑减仓"})
                if _has_q_loss:
                    signals.append({"signal": "⚠️ 周期股：最新季度亏损", "level": "danger",
                                    "action": "周期下行确认，建议清仓"})
                    if advice_icon != "🔴":
                        advice = "周期下行，建议清仓"
                        advice_icon = "🔴"

            elif _class == "价值股":
                # 价值股卖出逻辑：业绩持续下滑 → PE"假便宜"
                if _has_profit_down:
                    signals.append({"signal": "⚠️ 价值股预警：业绩下滑，PE可能是陷阱", "level": "warning",
                                    "action": "PE低但利润在降，检查是否假便宜"})

            elif _class == "困境反转":
                # 困境反转卖出逻辑：毛利率停止改善 → 反转失败
                if _has_gm_decline:
                    signals.append({"signal": "⚠️ 困境反转预警：毛利率停止改善", "level": "warning",
                                    "action": "反转逻辑动摇，考虑减仓或清仓"})
                elif _has_profit_up:
                    signals.append({"signal": "🟢 困境反转进展：利润趋势改善", "level": "info",
                                    "action": "反转逻辑验证中，继续持有"})

        # 股票分类标签
        s_type = detected_type if h_type != 'etf' else "指数ETF"

        # 计算移动止损价（用于显示）
        if h_type != 'etf':
            _high = h.get('high_price') or cost
            if price > _high:
                _high = price
            _trailing = _high * (1 - rules.get('trailing_stop_pct', 15) / 100)
        else:
            _trailing = None

        results.append({
            'code': code, 'name': name, 'type': h_type,
            'shares': shares, 'cost': cost, 'price': price,
            'change_pct': change_pct, 'market_value': market_value,
            'pnl': pnl, 'pnl_pct': pnl_pct, 'position_pct': position_pct,
            'signals': signals, 'advice': advice, 'advice_icon': advice_icon,
            'stop_price': h.get('stop_price'), 'trailing_stop': _trailing,
            'stock_type': s_type,
        })

    # === 信号不执行追踪 ===
    signal_track = load_signal_track()
    for r in results:
        code = r['code']
        is_danger = r['advice_icon'] == "🔴"
        signal_key = r['advice'] if is_danger else ""

        if is_danger and signal_key:
            days = update_signal_track(code, signal_key, True, signal_track)
            if days >= 3:
                # 计算持有期间的额外亏损
                daily_loss = r['market_value'] * abs(r['change_pct']) / 100
                r['unexecuted_days'] = days
                # 升级提醒语气
                if days >= 10:
                    r['signals'].append({
                        "signal": f"‼️ 此信号已持续{days}个交易日未执行！",
                        "level": "danger",
                        "action": f"拖延不是策略。信号出了就要执行，否则系统等于摆设。"
                    })
                elif days >= 5:
                    r['signals'].append({
                        "signal": f"⚠️ 此信号已连续{days}个交易日",
                        "level": "danger",
                        "action": f"每天不执行都在承担额外风险，请尽快决策"
                    })
                elif days >= 3:
                    r['signals'].append({
                        "signal": f"📅 此信号已持续{days}个交易日",
                        "level": "warning",
                        "action": "建议尽快执行或明确调整策略"
                    })
        else:
            # 清除该股票的danger追踪
            keys_to_remove = [k for k in signal_track if k.startswith(f"{code}_")]
            for k in keys_to_remove:
                del signal_track[k]

    save_signal_track(signal_track)

    # 公告扫描
    ann_alerts = []
    if include_announcements:
        log.info("扫描公告...")
        stock_codes = [h['code'] for h in holdings if h.get('type') != 'etf']
        if stock_codes:
            ann_alerts = scan_announcements(stock_codes, code_names)
            log.info(f"公告扫描完成: {len(ann_alerts)} 条提醒")

    cash_pct = available / total_assets * 100
    position_pct = total_market_value / total_assets * 100
    today_pnl = sum(r['price'] * r['shares'] * r['change_pct'] / 100 for r in results)

    # 大盘仓位指引
    log.info("检测大盘指数...")
    index_data = get_index_ma_status()
    pos_guide = calc_position_guide(index_data, position_pct, total_assets)
    log.info(f"大盘状态: {pos_guide['market_status']}，建议仓位{pos_guide['target_low']}-{pos_guide['target_high']}%")

    report = format_report(account, results, total_market_value, total_pnl, today_pnl, cash_pct, ann_alerts, pos_guide)
    return report, ann_alerts


# ============================================================
# 盘中止损监控（轻量模式）
# ============================================================

def check_stop_loss_alerts() -> Optional[str]:
    """
    轻量止损检查：只查实时价格，触及止损才返回消息
    返回 None 表示无报警
    """
    portfolio = load_portfolio()
    account = portfolio['accounts'][0]
    rules = portfolio.get('rules', {})
    holdings = account['holdings']
    all_codes = [h['code'] for h in holdings]
    trailing_stop_pct = rules.get('trailing_stop_pct', 15)

    rt = get_realtime_prices(all_codes)
    if not rt:
        return None

    # 加载已报警状态（每天重置）
    state = load_alert_state()
    today = datetime.now().strftime('%Y-%m-%d')
    if state.get('date') != today:
        state = {'date': today, 'alerted': {}}

    alerts = []
    need_save_portfolio = False

    for h in holdings:
        code = h['code']
        price = rt.get(code, {}).get('price')
        if not price:
            continue

        stop_method = h.get('stop_method', 'price')

        if stop_method == 'price':
            stop_price = h.get('stop_price')
            high_price = h.get('high_price') or h['cost']

            # 更新最高价
            if price > high_price:
                h['high_price'] = price
                high_price = price
                need_save_portfolio = True

            # 移动止损
            trailing_stop = high_price * (1 - trailing_stop_pct / 100)
            effective_stop = max(trailing_stop, stop_price) if stop_price else trailing_stop

            if effective_stop and price <= effective_stop:
                alert_key = f"{code}_stop"
                if alert_key not in state['alerted']:
                    if stop_price and price <= stop_price:
                        alerts.append(f"🚨 <b>{h['name']}({code})</b> 跌破固定止损!\n   现价 {price} ≤ 止损 {stop_price}\n   ➡️ 按计划清仓")
                    else:
                        alerts.append(f"🚨 <b>{h['name']}({code})</b> 触发移动止损!\n   现价 {price}，高点 {high_price} 回撤{trailing_stop_pct}%→{trailing_stop:.3f}\n   ➡️ 移动止损清仓")
                    state['alerted'][alert_key] = datetime.now().isoformat()
            elif effective_stop and price <= effective_stop * 1.03:
                alert_key = f"{code}_near_stop"
                if alert_key not in state['alerted']:
                    gap = (price / effective_stop - 1) * 100
                    alerts.append(f"⚠️ <b>{h['name']}({code})</b> 接近止损!\n   现价 {price}，止损线 {effective_stop:.3f}（仅差{gap:.1f}%）\n   ➡️ 密切关注")
                    state['alerted'][alert_key] = datetime.now().isoformat()

        # ETF 硬止损
        if h.get('type') == 'etf':
            cost = h['cost']
            pnl_pct = (price - cost) / cost * 100
            if pnl_pct <= -25:
                alert_key = f"{code}_etf_hard_stop"
                if alert_key not in state['alerted']:
                    alerts.append(f"🚨 <b>{h['name']}({code})</b> 亏损{pnl_pct:.1f}%!\n   现价 {price}，成本 {cost}\n   ➡️ 严重亏损，建议止损")
                    state['alerted'][alert_key] = datetime.now().isoformat()

    # 保存更新后的最高价
    if need_save_portfolio:
        try:
            with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
                json.dump(portfolio, f, ensure_ascii=False, indent=2)
        except:
            pass

    if alerts:
        save_alert_state(state)
        header = f"🔔 <b>盘中止损预警</b> {datetime.now().strftime('%H:%M')}\n"
        return header + "\n\n".join(alerts)

    save_alert_state(state)
    return None


# ============================================================
# 报告格式化
# ============================================================

def format_report(account, results, total_mv, total_pnl, today_pnl, cash_pct, ann_alerts=None, pos_guide=None) -> str:
    """生成持仓日报（HTML格式，兼容Telegram和邮件）"""
    now = datetime.now()
    total_assets = account['total_assets']
    position_pct = total_mv / total_assets * 100

    lines = []
    lines.append(f"📊 <b>持仓日报</b> {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append(f"💰 <b>{account['name']}</b> 总资产 {total_assets/10000:.2f}万")
    lines.append(f"   持仓 {total_mv/10000:.2f}万({position_pct:.0f}%) | 现金 {account['available_cash']/10000:.2f}万({cash_pct:.0f}%)")

    today_icon = "📈" if today_pnl >= 0 else "📉"
    pnl_dir = "+" if total_pnl >= 0 else ""
    today_dir = "+" if today_pnl >= 0 else ""
    lines.append(f"   总盈亏 {pnl_dir}{total_pnl:,.0f}元 | {today_icon} 今日 {today_dir}{today_pnl:,.0f}元")
    lines.append("")

    # 大盘仓位指引
    if pos_guide:
        lines.append("━━━ 🏛️ 大盘仓位指引 ━━━")
        idx_summary = []
        for d in pos_guide.get('index_data', []):
            idx_summary.append(f"{d['icon']}{d['name']} {d['arrangement']}")
        lines.append("   " + " | ".join(idx_summary[:3]))
        if len(idx_summary) > 3:
            lines.append("   " + " | ".join(idx_summary[3:]))

        lines.append(f"   大盘: <b>{pos_guide['market_status']}</b>")
        lines.append(f"   建议仓位: {pos_guide['target_low']}-{pos_guide['target_high']}% | 当前: {position_pct:.0f}%")
        lines.append(f"   {pos_guide['suggestion_icon']} {pos_guide['suggestion']}")
        above = pos_guide.get('above250_count', 0)
        total = pos_guide.get('total', 0)
        if total:
            lines.append(f"   年线: {above}/{total}指数站上年线")
        lines.append("")

    # 公告提醒（置顶）
    if ann_alerts:
        lines.append("━━━ 📢 公告提醒 ━━━")
        for a in ann_alerts:
            icon = "🔴" if a['level'] == 'danger' else "🟡"
            lines.append(f"{icon} <b>{a['name']}</b> [{a['type']}] {a['date']}")
            lines.append(f"   {a['title']}")
            lines.append(f"   ➡️ {a['action']}")
        lines.append("")

    # 持仓明细
    lines.append("━━━ 💼 持仓明细 ━━━")
    for r in results:
        type_tag = f" [{r.get('stock_type', '')}]" if r.get('stock_type') else ""
        lines.append(f"\n{r['advice_icon']} <b>{r['name']}</b> ({r['code']}){type_tag}")
        pnl_pct_str = f"{r['pnl_pct']:+.2f}%"
        pnl_str = f"{r['pnl']:+,.0f}元"
        chg_str = f"{r['change_pct']:+.2f}%"
        lines.append(f"   现价 {r['price']:.3f} | 成本 {r['cost']:.3f} | {pnl_pct_str} ({pnl_str})")
        lines.append(f"   仓位 {r['position_pct']:.1f}% | 市值 {r['market_value']/10000:.2f}万 | 今日 {chg_str}")

        stop_price = r.get('stop_price')
        trailing_stop = r.get('trailing_stop')
        if stop_price or trailing_stop:
            parts = []
            if stop_price:
                gap = (r['price'] / stop_price - 1) * 100
                parts.append(f"固定{stop_price}({gap:.1f}%)")
            if trailing_stop and trailing_stop > (stop_price or 0):
                gap_t = (r['price'] / trailing_stop - 1) * 100
                parts.append(f"移动{trailing_stop:.3f}({gap_t:.1f}%)")
            lines.append(f"   止损: {' | '.join(parts)}")

        for s in r['signals']:
            level_icon = "🔴" if s['level'] == 'danger' else "🟡" if s['level'] == 'warning' else "ℹ️"
            action_str = f" → {s['action']}" if s.get('action') else ""
            # 去掉信号文本开头与level_icon重复的emoji
            sig_text = s['signal']
            if sig_text.startswith(level_icon):
                sig_text = sig_text[len(level_icon):].lstrip()
            lines.append(f"   {level_icon} {sig_text}{action_str}")
            if s.get('detail'):
                lines.append(f"      <i>{s['detail']}</i>")

    # 操作建议汇总
    lines.append("")
    actions = [r for r in results if r['advice_icon'] != "🟢"]
    has_actions = actions or ann_alerts or (pos_guide and pos_guide.get('suggestion_icon') != "✅")

    if has_actions:
        lines.append("━━━ 📋 操作建议 ━━━")
        if pos_guide and pos_guide.get('suggestion_icon') != "✅":
            lines.append(f"   {pos_guide['suggestion_icon']} 仓位: {pos_guide['suggestion']}")
        for r in actions:
            lines.append(f"   {r['advice_icon']} {r['name']}: {r['advice']}")
        if ann_alerts:
            for a in ann_alerts:
                icon = "🔴" if a['level'] == 'danger' else "🟡"
                lines.append(f"   {icon} {a['name']}: {a['action']}")
    else:
        lines.append("━━━ 📋 操作建议 ━━━")
        lines.append("   ✅ 无异常，仓位合理，正常持有")

    return "\n".join(lines)


# ============================================================
# Telegram 推送
# ============================================================

def send_telegram(text: str, token: str = None, chat_id: str = None) -> bool:
    """通过 Telegram Bot API 发送消息"""
    token = token or TG_BOT_TOKEN
    chat_id = chat_id or TG_CHAT_ID
    if not token or not chat_id:
        log.error("未配置 TG_BOT_TOKEN 或 TG_CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Telegram 消息最长4096字符，超长截断
    if len(text) > 4000:
        text = text[:4000] + "\n\n... (内容过长已截断)"

    try:
        r = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        if r.status_code == 200 and r.json().get('ok'):
            log.info(f"Telegram 推送成功")
            return True
        else:
            log.error(f"Telegram 推送失败: {r.text}")
            return False
    except Exception as e:
        log.error(f"Telegram 推送异常: {e}")
        return False


# ============================================================
# 邮件推送
# ============================================================

def format_email_report(text_report: str) -> str:
    html = text_report.replace("\n", "<br>")
    return f"""
    <html><body style="font-family:monospace;font-size:14px;line-height:1.8;padding:20px;color:#333;">
    {html}
    <br><br>
    <p style="color:#999;font-size:11px;">此报告仅发送给账户持有人，请勿转发。</p>
    </body></html>
    """


def send_email_report(html: str, to_email: str = "1225106113@qq.com"):
    if not EMAIL_PASSWORD:
        log.error("未配置SMTP授权码")
        return
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"持仓日报 - {today}"
            msg["From"] = EMAIL_SENDER
            msg["To"] = to_email
            msg.attach(MIMEText(html, "html", "utf-8"))
            server.sendmail(EMAIL_SENDER, to_email, msg.as_string())
            log.info(f"持仓日报发送成功: {to_email}")
    except Exception as e:
        log.error(f"发送失败: {e}")


# ============================================================
# 主入口
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="持仓管理监控（系统B）")
    parser.add_argument("--mode", choices=["daily", "alert"], default="daily",
                        help="daily=完整日报(含公告扫描), alert=盘中止损监控(轻量)")
    parser.add_argument("--telegram", action="store_true", help="推送到Telegram")
    parser.add_argument("--email", action="store_true", help="发送邮件")
    parser.add_argument("--stdout", action="store_true", help="输出到终端（默认）")
    args = parser.parse_args()

    if args.mode == "alert":
        # === 盘中止损监控 ===
        alert_msg = check_stop_loss_alerts()
        if alert_msg:
            log.info("检测到止损预警!")
            if args.telegram:
                send_telegram(alert_msg)
            else:
                clean = re.sub(r'<[^>]+>', '', alert_msg)
                print(clean)
        else:
            log.info("无止损预警")
    else:
        # === 完整日报 ===
        report, ann_alerts = analyze_portfolio(include_announcements=True)

        if args.telegram:
            send_telegram(report)
        if args.email:
            html = format_email_report(report)
            send_email_report(html)
        if not args.telegram and not args.email or args.stdout:
            clean = re.sub(r'<[^>]+>', '', report)
            print(clean)


if __name__ == "__main__":
    main()
