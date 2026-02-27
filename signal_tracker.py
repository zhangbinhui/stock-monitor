#!/usr/bin/env python3
"""
信号追踪与反馈系统

功能：
1. 记录每次 System A 发出的选股信号快照（推荐日期、价格、分类、估值等）
2. 定期回溯历史信号的实际表现（1周/1月/3月后涨跌幅）
3. 按分类统计胜率和收益，生成反馈报告
4. 输出可用于优化模型参数的洞察

数据文件：signal_history.json
"""

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("signal_tracker")

HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "signal_history.json")


def load_history() -> List[Dict]:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_history(data: List[Dict]):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def record_signal(code: str, name: str, signal_data: Dict):
    """记录一条选股信号快照
    
    signal_data 应包含:
    - recommendation: 🟢/🟡/🔴
    - stock_type: 成长股/周期股/价值股/困境反转/一般
    - valuation_pass: bool
    - valuation_desc: str
    - price_at_signal: float (推荐时价格)
    - premium_rate: float (增持溢价率)
    - pe: float
    - pb: float
    - profit_trend: str
    - insider_count: int (增持高管人数)
    - insider_amount: float (增持总金额)
    - triple_filter_pass: bool (三重过滤是否全通过)
    - position_tier: str (仓位建议)
    """
    history = load_history()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 检查是否今天已记录过该股票（避免重复）
    for item in history:
        if item['code'] == code and item['signal_date'] == today:
            log.debug(f"  {code} 今日已记录，跳过")
            return
    
    record = {
        'code': code,
        'name': name,
        'signal_date': today,
        'timestamp': datetime.now().isoformat(),
        **signal_data,
        # 后续回溯填入
        'performance': {}  # {7d: +x%, 30d: +x%, 90d: +x%}
    }
    
    history.append(record)
    save_history(history)
    log.info(f"  📝 记录信号: {code} {name} {signal_data.get('recommendation', '?')} {signal_data.get('stock_type', '?')}")


def backfill_performance(get_price_func):
    """回溯历史信号的表现
    
    get_price_func: callable(code) -> float, 获取当前/历史价格的函数
    
    对每条信号，检查 1周/1月/3月后的涨跌幅
    """
    history = load_history()
    today = datetime.now().date()
    updated = False
    
    for record in history:
        signal_date = datetime.strptime(record['signal_date'], '%Y-%m-%d').date()
        price_at_signal = record.get('price_at_signal')
        if not price_at_signal or price_at_signal <= 0:
            continue
        
        perf = record.get('performance', {})
        
        # 检查各时间窗口
        for label, days in [('7d', 7), ('30d', 30), ('90d', 90)]:
            if label in perf:
                continue  # 已有数据，跳过
            
            target_date = signal_date + timedelta(days=days)
            if today < target_date:
                continue  # 还没到时间
            
            # 获取目标日期的价格
            try:
                target_price = get_price_func(record['code'], target_date.strftime('%Y-%m-%d'))
                if target_price and target_price > 0:
                    change_pct = (target_price - price_at_signal) / price_at_signal * 100
                    perf[label] = {
                        'price': target_price,
                        'change_pct': round(change_pct, 2),
                        'date': target_date.strftime('%Y-%m-%d')
                    }
                    updated = True
                    log.info(f"  📊 回溯 {record['code']} {record['name']} {label}: {change_pct:+.1f}%")
            except Exception as e:
                log.debug(f"  回溯 {record['code']} {label} 失败: {e}")
        
        record['performance'] = perf
    
    if updated:
        save_history(history)


def generate_feedback_report() -> str:
    """生成反馈统计报告"""
    history = load_history()
    if not history:
        return "暂无历史信号数据"
    
    lines = []
    lines.append("📊 <b>选股信号反馈报告</b>")
    lines.append(f"   总信号数: {len(history)} | 有回溯数据: {sum(1 for h in history if h.get('performance'))}")
    lines.append("")
    
    # === 按分类统计 ===
    type_stats = {}  # {type: {count, wins_30d, total_return_30d, ...}}
    
    for record in history:
        st = record.get('stock_type', '未知')
        if st not in type_stats:
            type_stats[st] = {
                'count': 0, 'with_perf': 0,
                'wins_7d': 0, 'total_7d': 0, 'returns_7d': [],
                'wins_30d': 0, 'total_30d': 0, 'returns_30d': [],
                'wins_90d': 0, 'total_90d': 0, 'returns_90d': [],
            }
        
        stats = type_stats[st]
        stats['count'] += 1
        
        perf = record.get('performance', {})
        if perf:
            stats['with_perf'] += 1
        
        for label in ['7d', '30d', '90d']:
            if label in perf:
                pct = perf[label]['change_pct']
                stats[f'total_{label}'] += 1
                stats[f'returns_{label}'].append(pct)
                if pct > 0:
                    stats[f'wins_{label}'] += 1
    
    # === 按推荐级别统计 ===
    rec_stats = {}  # {🟢/🟡/🔴: same structure}
    for record in history:
        rec = record.get('recommendation', '?')
        if rec not in rec_stats:
            rec_stats[rec] = {'count': 0, 'returns_30d': []}
        rec_stats[rec]['count'] += 1
        perf = record.get('performance', {})
        if '30d' in perf:
            rec_stats[rec]['returns_30d'].append(perf['30d']['change_pct'])
    
    # === 三重过滤 vs 非通过 ===
    triple_pass = {'count': 0, 'returns_30d': []}
    triple_fail = {'count': 0, 'returns_30d': []}
    for record in history:
        bucket = triple_pass if record.get('triple_filter_pass') else triple_fail
        bucket['count'] += 1
        perf = record.get('performance', {})
        if '30d' in perf:
            bucket['returns_30d'].append(perf['30d']['change_pct'])
    
    # --- 输出 ---
    lines.append("━━━ 按股票分类 ━━━")
    for st, stats in sorted(type_stats.items(), key=lambda x: -x[1]['count']):
        line = f"   <b>{st}</b> ({stats['count']}只)"
        for label in ['7d', '30d', '90d']:
            total = stats[f'total_{label}']
            if total > 0:
                wins = stats[f'wins_{label}']
                avg_ret = sum(stats[f'returns_{label}']) / total
                winrate = wins / total * 100
                line += f" | {label}: 胜率{winrate:.0f}% 均收{avg_ret:+.1f}%"
        lines.append(line)
    
    lines.append("")
    lines.append("━━━ 按推荐级别 ━━━")
    for rec in ['🟢', '🟡', '🔴']:
        if rec in rec_stats:
            s = rec_stats[rec]
            line = f"   {rec} ({s['count']}只)"
            if s['returns_30d']:
                avg = sum(s['returns_30d']) / len(s['returns_30d'])
                wins = sum(1 for r in s['returns_30d'] if r > 0)
                line += f" | 30d胜率{wins/len(s['returns_30d'])*100:.0f}% 均收{avg:+.1f}%"
            lines.append(line)
    
    lines.append("")
    lines.append("━━━ 三重过滤效果 ━━━")
    for label, bucket in [("✅通过", triple_pass), ("❌未通过", triple_fail)]:
        line = f"   {label} ({bucket['count']}只)"
        if bucket['returns_30d']:
            avg = sum(bucket['returns_30d']) / len(bucket['returns_30d'])
            wins = sum(1 for r in bucket['returns_30d'] if r > 0)
            line += f" | 30d胜率{wins/len(bucket['returns_30d'])*100:.0f}% 均收{avg:+.1f}%"
        lines.append(line)
    
    # === 最佳/最差信号 ===
    signals_with_30d = [(r, r['performance']['30d']['change_pct']) 
                        for r in history if '30d' in r.get('performance', {})]
    
    if signals_with_30d:
        signals_with_30d.sort(key=lambda x: -x[1])
        lines.append("")
        lines.append("━━━ 最佳信号 TOP3 ━━━")
        for r, pct in signals_with_30d[:3]:
            lines.append(f"   🏆 {r['name']}({r['code']}) {r.get('stock_type','')} {r['signal_date']} → 30d {pct:+.1f}%")
        
        lines.append("")
        lines.append("━━━ 最差信号 TOP3 ━━━")
        for r, pct in signals_with_30d[-3:]:
            lines.append(f"   💀 {r['name']}({r['code']}) {r.get('stock_type','')} {r['signal_date']} → 30d {pct:+.1f}%")
    
    # === 洞察 ===
    lines.append("")
    lines.append("━━━ 💡 洞察 ━━━")
    
    # 哪个分类最赚钱
    best_type = None
    best_avg = -999
    for st, stats in type_stats.items():
        if stats['returns_30d']:
            avg = sum(stats['returns_30d']) / len(stats['returns_30d'])
            if avg > best_avg:
                best_avg = avg
                best_type = st
    if best_type:
        lines.append(f"   📈 最赚钱分类: {best_type} (30d均收{best_avg:+.1f}%)")
    
    # 三重过滤是否有效
    pass_avg = sum(triple_pass['returns_30d']) / len(triple_pass['returns_30d']) if triple_pass['returns_30d'] else 0
    fail_avg = sum(triple_fail['returns_30d']) / len(triple_fail['returns_30d']) if triple_fail['returns_30d'] else 0
    if triple_pass['returns_30d'] and triple_fail['returns_30d']:
        diff = pass_avg - fail_avg
        if diff > 0:
            lines.append(f"   ✅ 三重过滤有效: 通过比未通过30d多赚{diff:.1f}个百分点")
        else:
            lines.append(f"   ⚠️ 三重过滤待验证: 通过反而比未通过30d少赚{abs(diff):.1f}个百分点")
    
    return "\n".join(lines)


def get_historical_price(code: str, date_str: str) -> Optional[float]:
    """获取历史某天的收盘价（用akshare日K线）"""
    try:
        import akshare as ak
        
        # 转换代码格式
        if code.startswith(('6', '5')):
            ak_code = f'sh{code}'
        else:
            ak_code = f'sz{code}'
        
        # 获取日K线，范围缩小到目标日期前后几天
        target = datetime.strptime(date_str, '%Y-%m-%d')
        start = (target - timedelta(days=5)).strftime('%Y%m%d')
        end = (target + timedelta(days=5)).strftime('%Y%m%d')
        
        df = ak.stock_zh_a_daily(symbol=ak_code, start_date=start, end_date=end, adjust="qfq")
        if df.empty:
            return None
        
        # 找最接近目标日期的交易日
        df['date'] = df['date'].astype(str)
        target_str = date_str
        
        # 精确匹配
        match = df[df['date'] == target_str]
        if not match.empty:
            return float(match.iloc[0]['close'])
        
        # 找最近的（之前的交易日）
        df_before = df[df['date'] <= target_str]
        if not df_before.empty:
            return float(df_before.iloc[-1]['close'])
        
        return None
    except Exception as e:
        log.debug(f"获取 {code} {date_str} 历史价格失败: {e}")
        return None


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    parser = argparse.ArgumentParser(description="信号追踪与反馈系统")
    parser.add_argument("--backfill", action="store_true", help="回溯历史信号表现")
    parser.add_argument("--report", action="store_true", help="生成反馈报告")
    parser.add_argument("--stats", action="store_true", help="简要统计")
    args = parser.parse_args()
    
    if args.backfill:
        log.info("=== 回溯历史信号 ===")
        backfill_performance(get_historical_price)
        log.info("=== 回溯完成 ===")
    
    if args.report:
        import re
        report = generate_feedback_report()
        # 终端输出去掉HTML标签
        clean = re.sub(r'<[^>]+>', '', report)
        print(clean)
    
    if args.stats:
        history = load_history()
        print(f"总信号: {len(history)}")
        with_perf = sum(1 for h in history if h.get('performance'))
        print(f"有回溯: {with_perf}")
        types = {}
        for h in history:
            t = h.get('stock_type', '?')
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c}")
