"""
波动率突破策略 v5 - 聚宽研究环境（纯信号验证版）
=================================

【策略验证三步走】
  第一步（本版本）：纯信号验证 — 每个信号假设都能成交，验证选股逻辑本身有没有edge
  第二步（后续）：加仓位管理 — 资金限制、最大持仓、手续费
  第三步（后续）：压力测试 — 牛熊分段、参数敏感性

【策略逻辑】
  1. 选股：从全A股里找「跌惨了的大票」（市值≥50亿，股价相对1年高点大幅回撤）
  2. 盯盘：用分钟K线（5分/15分/30分）盘中监控
  3. 买入：某根K线涨幅突破历史极值 → 以当根K线收盘价买入
  4. 卖出：次日移动止损（跟踪盘中最高价，回撤N%卖出）+ 保底止损 + 兜底收盘卖
  5. 统计：胜率、盈亏比、平均收益率、最大连亏

【使用方法】
  聚宽(joinquant.com) → 研究环境 → 新建Notebook → 按Cell分段粘贴运行
"""

# ============================================================
# Cell 1：导入库 & 参数配置
# ============================================================

from jqdata import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ======== 股票池参数 ========
MIN_MARKET_CAP = 50e8           # 最低市值50亿
MAX_PRICE_RATIO_DEFAULT = 1/3   # 默认：当前价 < 1年最高价 × 1/3（跌67%）
MARKET_CAP_TIERS = [
    (100e8, 1/2),   # 100亿以上：跌50%入池
    (50e8,  1/3),   # 50亿以上：跌67%入池
]

# ======== 回测参数 ========
END_DATE = '2026-02-25'
BACKTEST_YEARS = 3              # 回测3年
COOLDOWN_DAYS = 2               # 信号冷却天数

# ======== 手续费（固定金额，用于计算净收益率）========
COMMISSION_PER_SIDE = 5         # 买卖各5元
ASSUMED_TRADE_AMOUNT = 10000    # 假设每笔交易金额1万元（仅用于计算手续费率）

# ======== 网格搜索参数空间 ========
FREQ_LIST = ['5m', '15m', '30m']

# 触发类型
MULT_LIST = [1.5, 2.0, 2.5, 3.0]

# 回看周期
LOOKBACK_PERIODS = ['3m', '1y']

# 卖出参数：移动止损回撤比例
TRAILING_STOP_LIST = [0.03, 0.05]
FLOOR_STOP = 0.03               # 保底止损：跌破买入价3%无条件走

# ======== 显示配置 ========
n_trigger = 1 + len(MULT_LIST)
n_trailing = len(TRAILING_STOP_LIST)
n_total = len(FREQ_LIST) * n_trigger * len(LOOKBACK_PERIODS) * n_trailing

# 手续费率（双边）
fee_rate = COMMISSION_PER_SIDE * 2 / ASSUMED_TRADE_AMOUNT * 100

print("=" * 60)
print("✅ Cell 1 配置完成（纯信号验证模式）")
print("=" * 60)
print(f"  📌 股票池: 市值≥{MIN_MARKET_CAP/1e8:.0f}亿，从1年高点大幅回撤")
print(f"  📌 K线周期: {FREQ_LIST}")
print(f"  📌 触发方式: A类(突破最大值) + B类(中位数×{MULT_LIST})")
print(f"  📌 回看周期: {LOOKBACK_PERIODS}")
print(f"  📌 卖出策略: 移动止损(回撤{[f'{x*100:.0f}%' for x in TRAILING_STOP_LIST]}) + 保底止损({FLOOR_STOP*100:.0f}%) + 兜底收盘卖")
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 手续费率: 约{fee_rate:.2f}%（按每笔{ASSUMED_TRADE_AMOUNT/10000:.0f}万估算）")
print(f"  📌 参数组合: {n_total} 种")
print(f"  ⚠️ 纯信号模式：不模拟资金，每个信号假设都能成交")


# ============================================================
# Cell 2：构建滚动股票池
# ============================================================

def build_rolling_pool(end_date, backtest_years=BACKTEST_YEARS,
                       min_cap=MIN_MARKET_CAP, tiers=MARKET_CAP_TIERS):
    """
    构建滚动股票池：每个交易日判断哪些股票「跌惨了」
    
    返回:
        pool_calendar: {股票代码: set(日期)} 每只股票哪些天在池子里
        stock_info:    {股票代码: {'name': 名称, 'market_cap': 市值(亿)}}
    """
    print(f"\n{'='*60}")
    print(f"📊 第一步：构建滚动股票池")
    print(f"{'='*60}")

    bt_start = pd.to_datetime(end_date) - timedelta(days=365 * backtest_years)
    data_start = bt_start - timedelta(days=365)
    bt_start_str = bt_start.strftime('%Y-%m-%d')
    data_start_str = data_start.strftime('%Y-%m-%d')

    all_trade_days = get_trade_days(start_date=bt_start_str, end_date=end_date)
    print(f"  回测区间: {bt_start_str} ~ {end_date} ({len(all_trade_days)}个交易日)")

    # 过滤ST、上市不满2年
    all_stocks = get_all_securities(types=['stock'], date=end_date)
    two_years_ago = (pd.to_datetime(end_date) - timedelta(days=365*2)).date()
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    valid_codes = [c for c in valid.index
                   if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]
    print(f"  非ST且上市>2年: {len(valid_codes)} 只")

    # 市值初筛
    trade_days_list = get_trade_days(end_date=end_date, count=5)
    last_trade = str(trade_days_list[-1])
    q = query(valuation.code, valuation.market_cap).filter(
        valuation.code.in_(valid_codes),
        valuation.market_cap >= min_cap / 1e8 * 0.5
    )
    cap_df = get_fundamentals(q, date=last_trade)
    candidate_codes = list(cap_df['code'])
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    print(f"  市值初筛(≥{min_cap/1e8*0.5:.0f}亿): {len(candidate_codes)} 只")

    # 逐只计算入池日期
    print(f"  拉取日K线并计算...")
    pool_calendar = {}
    stock_info = {}
    total_pool_days = 0

    for i in range(0, len(candidate_codes), 50):
        batch = candidate_codes[i:i+50]
        prices = get_price(batch, start_date=data_start_str, end_date=end_date,
                           frequency='daily', fields=['high', 'close'], panel=True)
        for code in batch:
            try:
                if isinstance(prices['high'], pd.DataFrame):
                    highs = prices['high'][code].dropna()
                    closes = prices['close'][code].dropna()
                else:
                    continue
                if len(highs) < 300:
                    continue

                name = get_security_info(code).display_name
                cap = cap_dict.get(code, 0)
                ratio = MAX_PRICE_RATIO_DEFAULT
                for tier_cap, tier_ratio in tiers:
                    if cap * 1e8 >= tier_cap:
                        ratio = tier_ratio
                        break

                valid_dates = set()
                close_arr = closes.values
                high_arr = highs.values
                dates_arr = closes.index
                for j in range(250, len(close_arr)):
                    year_high = high_arr[j-250:j].max()
                    current = close_arr[j]
                    d = str(dates_arr[j].date())
                    if d < bt_start_str:
                        continue
                    if current < year_high * ratio:
                        valid_dates.add(d)

                if valid_dates:
                    pool_calendar[code] = valid_dates
                    stock_info[code] = {'name': name, 'market_cap': cap}
                    total_pool_days += len(valid_dates)
            except:
                continue

        done = min(i+50, len(candidate_codes))
        print(f"    已处理 {done}/{len(candidate_codes)} ({done/len(candidate_codes)*100:.0f}%)")

    print(f"\n  ✅ 股票池构建完成!")
    print(f"  入池股票: {len(pool_calendar)} 只")
    print(f"  平均入池: {total_pool_days/max(len(pool_calendar),1):.0f} 天/只")

    if pool_calendar:
        sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:20]
        print(f"\n  📋 入池天数TOP20:")
        print(f"  {'股票':>10s}  {'代码':>14s}  {'入池天数':>6s}  {'市值(亿)':>8s}")
        for code, dates in sorted_stocks:
            info = stock_info[code]
            print(f"  {info['name']:>10s}  {code:>14s}  {len(dates):>6d}天  {info['market_cap']:>8.0f}")

    return pool_calendar, stock_info

pool_calendar, stock_info = build_rolling_pool(END_DATE)
print(f"\n✅ Cell 2 完成")


# ============================================================
# Cell 3：核心引擎（纯信号验证）
# ============================================================

def get_lookback_bars(freq, period):
    """计算回看需要多少根K线"""
    bars_per_day = {'5m': 48, '15m': 16, '30m': 8}
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def simulate_trailing_stop_sell(stock_code, buy_date, buy_price, freq, trailing_pct, floor_stop_pct):
    """
    模拟次日的移动止损卖出
    
    返回:
        (卖出价, 卖出方式, 盘中最高价, 持有K线数) 或 None
    """
    next_days = get_trade_days(start_date=buy_date, count=3)
    if len(next_days) < 2:
        return None
    
    buy_date_pd = pd.to_datetime(buy_date).date()
    next_day = None
    for d in next_days:
        if d > buy_date_pd:
            next_day = d
            break
    if next_day is None:
        return None
    
    next_day_str = str(next_day)
    next_day_end = (pd.to_datetime(next_day_str) + timedelta(days=1)).strftime('%Y-%m-%d')
    min_bars = get_price(stock_code, start_date=next_day_str, end_date=next_day_end,
                         frequency=freq, fields=['open', 'close', 'high', 'low'])
    
    if min_bars is None or len(min_bars) == 0:
        return None
    
    min_bars = min_bars[min_bars.index.date == next_day]
    if len(min_bars) == 0:
        return None
    
    floor_price = buy_price * (1 - floor_stop_pct)
    intraday_high = 0
    
    for idx in range(len(min_bars)):
        bar = min_bars.iloc[idx]
        bar_high = bar['high']
        bar_low = bar['low']
        bar_close = bar['close']
        
        if bar_high > intraday_high:
            intraday_high = bar_high
        
        # 保底止损
        if bar_low <= floor_price:
            return (round(floor_price, 3), '保底止损', round(intraday_high, 3), idx + 1)
        
        # 移动止损
        if intraday_high > 0:
            trailing_price = intraday_high * (1 - trailing_pct)
            if bar_low <= trailing_price:
                sell_p = min(trailing_price, bar_close)
                return (round(sell_p, 3), f'移动止损{trailing_pct*100:.0f}%', round(intraday_high, 3), idx + 1)
    
    # 兜底收盘卖
    last_close = min_bars.iloc[-1]['close']
    return (round(last_close, 3), '次日收盘卖', round(intraday_high, 3), len(min_bars))


def backtest_signals(pool_calendar, stock_info, max_stocks,
                     freq, period, signal_type, multiplier,
                     trailing_pct,
                     end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    纯信号回测：收集所有信号，计算每笔收益率
    不涉及任何资金管理逻辑
    
    返回:
        {
            '参数': ...,
            '信号列表': [...],    # 每笔信号的完整信息
            '个股统计': [...],
            '汇总': {...},        # 胜率、盈亏比等
        }
    """
    lookback_days_map = {'3m': 63, '1y': 250}
    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:max_stocks]
    
    all_signals = []
    stock_summaries = []
    
    for si, (code, valid_dates) in enumerate(sorted_stocks):
        name = stock_info[code]['name']
        
        try:
            earliest = min(valid_dates)
            data_start = (pd.to_datetime(earliest) - timedelta(days=lookback_days_map[period] * 2)).strftime('%Y-%m-%d')
            
            # 日K线
            daily = get_price(code, start_date=data_start, end_date=end_date,
                              frequency='daily', fields=['open', 'high', 'low', 'close', 'pre_close'])
            if daily is None or len(daily) < lookback_days_map[period] + 30:
                continue
            
            # 分钟K线
            min_df = get_price(code, start_date=data_start, end_date=end_date,
                               frequency=freq, fields=['open', 'close', 'high', 'low', 'volume'])
            if min_df is None or len(min_df) < get_lookback_bars(freq, period):
                continue
            
            min_df['bar_return'] = (min_df['close'] / min_df['open'] - 1)
            min_df['date'] = min_df.index.date
            lookback_bars = get_lookback_bars(freq, period)
            
            trade_dates = sorted([d for d in min_df['date'].unique() if str(d) in valid_dates])
            
            stock_signals = []
            last_signal_date_idx = -999
            
            for i, date in enumerate(trade_dates):
                date_str = str(date)
                
                if i - last_signal_date_idx <= cooldown:
                    continue
                
                day_bars = min_df[min_df['date'] == date]
                if len(day_bars) < 5:
                    continue
                
                hist = min_df[min_df['date'] < date]
                if len(hist) < lookback_bars:
                    continue
                
                hist_returns = hist['bar_return'].iloc[-lookback_bars:]
                
                # 计算阈值
                if signal_type == 'max_break':
                    threshold = hist_returns.max()
                elif signal_type == 'mult_break':
                    pos_returns = hist_returns[hist_returns > 0]
                    base = pos_returns.median() if len(pos_returns) > 0 else 0
                    if pd.isna(base) or base <= 0:
                        continue
                    threshold = base * multiplier
                else:
                    continue
                
                if pd.isna(threshold) or threshold <= 0:
                    continue
                
                # 扫描当天K线找信号
                triggered = False
                for idx in range(len(day_bars)):
                    bar = day_bars.iloc[idx]
                    bar_ret = bar['bar_return']
                    if not pd.isna(bar_ret) and bar_ret > threshold:
                        triggered = True
                        trigger_bar = day_bars.index[idx]
                        trigger_price = bar['close']
                        trigger_return = bar_ret
                        break
                
                if not triggered:
                    continue
                
                # 模拟次日移动止损卖出
                sell_result = simulate_trailing_stop_sell(
                    code, date_str, trigger_price, freq, trailing_pct, FLOOR_STOP)
                
                if sell_result is None:
                    continue
                
                sell_price, sell_type, intraday_high, hold_bars = sell_result
                
                # 毛收益率（不含手续费）
                raw_ret = (sell_price - trigger_price) / trigger_price * 100
                # 净收益率（扣手续费）
                fee_pct = COMMISSION_PER_SIDE * 2 / ASSUMED_TRADE_AMOUNT * 100
                net_ret = raw_ret - fee_pct
                
                # 次日涨幅（盘中最高相对买入价）
                max_profit_pct = (intraday_high - trigger_price) / trigger_price * 100 if intraday_high > trigger_price else 0
                
                last_signal_date_idx = i
                
                signal = {
                    '日期': date_str,
                    '股票': name,
                    '代码': code,
                    '触发时间': str(trigger_bar),
                    '买入价': trigger_price,
                    'K线涨幅%': round(trigger_return * 100, 2),
                    '阈值%': round(threshold * 100, 2),
                    '卖出价': sell_price,
                    '卖出方式': sell_type,
                    '盘中最高': intraday_high,
                    '最大浮盈%': round(max_profit_pct, 2),
                    '毛收益率%': round(raw_ret, 2),
                    '净收益率%': round(net_ret, 2),
                    '持有K线数': hold_bars,
                }
                stock_signals.append(signal)
                all_signals.append(signal)
            
            # 个股统计
            if stock_signals:
                rets = [s['净收益率%'] for s in stock_signals]
                wins = len([r for r in rets if r > 0])
                total = len(rets)
                avg_win = np.mean([r for r in rets if r > 0]) if wins > 0 else 0
                avg_loss = np.mean([r for r in rets if r <= 0]) if total - wins > 0 else 0
                pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
                
                stock_summaries.append({
                    '股票': name,
                    '代码': code,
                    '入池天数': len(valid_dates),
                    '信号数': total,
                    '胜率%': round(wins/total*100, 1),
                    '平均收益%': round(np.mean(rets), 2),
                    '平均盈利%': round(avg_win, 2),
                    '平均亏损%': round(avg_loss, 2),
                    '盈亏比': round(pl_ratio, 2) if pl_ratio != float('inf') else 999,
                    '最大浮盈%': round(max(s['最大浮盈%'] for s in stock_signals), 2),
                })
        except:
            continue
    
    if not all_signals:
        return None
    
    # ---- 汇总统计 ----
    rets = [s['净收益率%'] for s in all_signals]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    
    # 最大连亏
    max_consec_loss = 0
    cur_consec = 0
    for r in rets:
        if r <= 0:
            cur_consec += 1
            max_consec_loss = max(max_consec_loss, cur_consec)
        else:
            cur_consec = 0
    
    # 期望值 = 胜率 × 平均盈利 + (1-胜率) × 平均亏损
    win_rate = len(wins) / len(rets) if rets else 0
    expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss
    
    # 卖出方式分布
    sell_types = {}
    for s in all_signals:
        st = s['卖出方式']
        sell_types[st] = sell_types.get(st, 0) + 1
    
    summary = {
        '总信号数': len(rets),
        '胜率%': round(win_rate * 100, 1),
        '平均盈利%': round(avg_win, 2),
        '平均亏损%': round(avg_loss, 2),
        '盈亏比': round(pl_ratio, 2) if pl_ratio != float('inf') else 999,
        '平均净收益%': round(np.mean(rets), 2),
        '收益中位数%': round(np.median(rets), 2),
        '最大单笔盈利%': round(max(rets), 2),
        '最大单笔亏损%': round(min(rets), 2),
        '期望值%': round(expectancy, 3),
        '最大连亏次数': max_consec_loss,
        '有信号股票数': len(stock_summaries),
        '卖出方式分布': sell_types,
    }
    
    return {
        '信号列表': all_signals,
        '个股统计': stock_summaries,
        '汇总': summary,
    }


print("✅ Cell 3 引擎定义完成")


# ============================================================
# Cell 4：运行网格搜索
# ============================================================

MAX_STOCKS = 15  # ← 先15只验证，确认后改大

print(f"\n{'='*60}")
print(f"📊 第二步：网格搜索（{MAX_STOCKS}只股票）")
print(f"{'='*60}")

# 构建参数网格
param_grid = []
for freq in FREQ_LIST:
    for period in LOOKBACK_PERIODS:
        for trailing in TRAILING_STOP_LIST:
            trailing_label = f"回撤{trailing*100:.0f}%止损"
            # A类
            param_grid.append({
                'freq': freq, 'signal_type': 'max_break',
                'period': period, 'multiplier': None,
                'trailing_pct': trailing,
                'label': f"{freq}|突破最大值|回看{period}|{trailing_label}"
            })
            # B类
            for mult in MULT_LIST:
                param_grid.append({
                    'freq': freq, 'signal_type': 'mult_break',
                    'period': period, 'multiplier': mult,
                    'trailing_pct': trailing,
                    'label': f"{freq}|中位数×{mult}|回看{period}|{trailing_label}"
                })

print(f"  参数组合: {len(param_grid)} 种")
print(f"  开始搜索...\n")

grid_results = []

for pi, params in enumerate(param_grid):
    label = params['label']
    print(f"  [{pi+1}/{len(param_grid)}] {label}")
    
    result = backtest_signals(
        pool_calendar, stock_info, MAX_STOCKS,
        freq=params['freq'],
        period=params['period'],
        signal_type=params['signal_type'],
        multiplier=params['multiplier'],
        trailing_pct=params['trailing_pct'],
    )
    
    if result:
        s = result['汇总']
        grid_results.append({
            '策略': label,
            'K线周期': params['freq'],
            '触发类型': '突破最大值' if params['signal_type'] == 'max_break' else f"中位数×{params['multiplier']}",
            '回看周期': params['period'],
            '止损回撤': f"{params['trailing_pct']*100:.0f}%",
            '信号数': s['总信号数'],
            '胜率%': s['胜率%'],
            '盈亏比': s['盈亏比'],
            '平均净收益%': s['平均净收益%'],
            '收益中位数%': s['收益中位数%'],
            '期望值%': s['期望值%'],
            '最大连亏': s['最大连亏次数'],
            '平均盈利%': s['平均盈利%'],
            '平均亏损%': s['平均亏损%'],
            '最大单笔盈%': s['最大单笔盈利%'],
            '最大单笔亏%': s['最大单笔亏损%'],
            '有信号股票数': s['有信号股票数'],
            '_result': result,
        })
        print(f"      → 信号{s['总信号数']}个 | 胜率{s['胜率%']}% | 盈亏比{s['盈亏比']} | "
              f"期望值{s['期望值%']:+.3f}% | 连亏max{s['最大连亏次数']}")
    else:
        print(f"      → 无信号")

print(f"\n✅ 网格搜索完成! {len(grid_results)} 组有结果")


# ============================================================
# Cell 5：排行榜
# ============================================================

if not grid_results:
    print("❌ 无结果")
else:
    gdf = pd.DataFrame(grid_results)
    
    # 综合评分：期望值40% + 盈亏比20% + 胜率20% + 信号量20%
    # 期望值是最核心的指标：胜率 × 平均盈 + (1-胜率) × 平均亏
    gdf['信号量得分'] = gdf['信号数'].apply(lambda x: min(x / 50, 1.0) * 100)
    gdf['期望值得分'] = (gdf['期望值%'] * 100).clip(-100, 100)  # 放大100倍作为得分
    gdf['盈亏比得分'] = gdf['盈亏比'].clip(0, 5) * 20           # 盈亏比5封顶=100分
    gdf['综合评分'] = (
        gdf['期望值得分'] * 0.4 +
        gdf['盈亏比得分'] * 0.2 +
        gdf['胜率%'] * 0.2 +
        gdf['信号量得分'] * 0.2
    ).round(1)
    
    gdf = gdf.sort_values('综合评分', ascending=False)
    
    print(f"\n{'='*60}")
    print(f"🏆 策略排行榜 TOP 20（纯信号验证）")
    print(f"{'='*60}")
    print(f"  评分 = 期望值×40% + 盈亏比×20% + 胜率×20% + 信号量×20%")
    print(f"  ⚠️ 这是假设每笔都能成交的理论值，后续需加资金管理验证\n")
    
    show_cols = ['策略', '信号数', '胜率%', '盈亏比', '平均净收益%',
                 '期望值%', '最大连亏', '综合评分']
    print(gdf[show_cols].head(20).to_string(index=False))
    
    # ---- 按维度拆解 ----
    freq_cn = {'5m': '5分钟', '15m': '15分钟', '30m': '30分钟'}
    
    print(f"\n{'='*60}")
    print("📈 按K线周期汇总")
    print(f"{'='*60}")
    for freq in FREQ_LIST:
        sub = gdf[gdf['K线周期'] == freq]
        if sub.empty:
            continue
        print(f"  {freq_cn[freq]}: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"盈亏比{sub['盈亏比'].mean():.2f}")
    
    print(f"\n{'='*60}")
    print("📈 按止损回撤比例汇总")
    print(f"{'='*60}")
    for t in TRAILING_STOP_LIST:
        sub = gdf[gdf['止损回撤'] == f"{t*100:.0f}%"]
        if sub.empty:
            continue
        print(f"  回撤{t*100:.0f}%止损: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"盈亏比{sub['盈亏比'].mean():.2f}")
    
    print(f"\n{'='*60}")
    print("📈 按触发类型汇总")
    print(f"{'='*60}")
    for tt in gdf['触发类型'].unique():
        sub = gdf[gdf['触发类型'] == tt]
        print(f"  {tt}: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"盈亏比{sub['盈亏比'].mean():.2f}")
    
    # ---- 最佳策略详情 ----
    best = gdf.iloc[0]
    print(f"\n{'='*60}")
    print(f"🏆 最佳策略: {best['策略']}")
    print(f"{'='*60}")
    print(f"  信号数:       {best['信号数']:.0f} 个（{best['有信号股票数']:.0f}只股票）")
    print(f"  胜率:         {best['胜率%']:.1f}%")
    print(f"  盈亏比:       {best['盈亏比']:.2f}")
    print(f"  平均净收益:   {best['平均净收益%']:+.2f}%")
    print(f"  收益中位数:   {best['收益中位数%']:+.2f}%")
    print(f"  期望值:       {best['期望值%']:+.3f}%")
    print(f"  最大连亏:     {best['最大连亏']:.0f} 次")
    print(f"  最大单笔盈:   {best['最大单笔盈%']:+.2f}%")
    print(f"  最大单笔亏:   {best['最大单笔亏%']:+.2f}%")
    
    # 解读
    print(f"\n  💡 策略是否有edge?")
    ev = best['期望值%']
    if ev > 0.1:
        print(f"     ✅ 期望值{ev:+.3f}%为正，策略有正期望")
        print(f"     每笔交易平均赚{ev:.3f}%，做{best['信号数']:.0f}笔理论累计{ev * best['信号数']:.1f}%")
    elif ev > 0:
        print(f"     ⚠️ 期望值{ev:+.3f}%微正，扣除滑点后可能归零")
    else:
        print(f"     ❌ 期望值{ev:+.3f}%为负，策略没有edge")
    
    wr = best['胜率%']
    pr = best['盈亏比']
    if wr > 50 and pr > 1:
        print(f"     ✅ 胜率>{50}% + 盈亏比>{1}，攻守兼备")
    elif wr <= 50 and pr > 2:
        print(f"     ⚠️ 胜率偏低但盈亏比{pr:.1f}，靠大赚弥补，心态要求高")
    elif wr > 50 and pr <= 1:
        print(f"     ⚠️ 胜率高但盈亏比{pr:.1f}偏低，小亏大赚不多")


# ============================================================
# Cell 6：最佳策略详情
# ============================================================

if grid_results and len(gdf) > 0:
    best_result = gdf.iloc[0]['_result']
    best_label = gdf.iloc[0]['策略']
    
    # 个股统计
    if best_result['个股统计']:
        sdf = pd.DataFrame(best_result['个股统计']).sort_values('平均收益%', ascending=False)
        print(f"\n{'='*60}")
        print(f"🏆 最佳策略 [{best_label}] 各股票表现:")
        print(f"{'='*60}")
        print(sdf.to_string(index=False))
        
        profitable = len(sdf[sdf['平均收益%'] > 0])
        losing = len(sdf[sdf['平均收益%'] <= 0])
        print(f"\n  {profitable}只平均赚钱，{losing}只平均亏钱")
    
    # 卖出方式分布
    sell_dist = best_result['汇总']['卖出方式分布']
    total_sig = best_result['汇总']['总信号数']
    print(f"\n  卖出方式分布:")
    for st, cnt in sorted(sell_dist.items(), key=lambda x: -x[1]):
        print(f"    {st}: {cnt}笔 ({cnt/total_sig*100:.1f}%)")
    
    # 交易明细
    if best_result['信号列表']:
        tlog = pd.DataFrame(best_result['信号列表'])
        print(f"\n{'='*60}")
        print(f"📋 信号明细（共{len(tlog)}笔）")
        print(f"{'='*60}")
        
        show_cols_detail = ['日期', '股票', '买入价', '卖出价', '卖出方式', '最大浮盈%', '净收益率%']
        
        if len(tlog) <= 30:
            print(f"\n{tlog[show_cols_detail].to_string(index=False)}")
        else:
            print(f"\n  前15笔:")
            print(tlog[show_cols_detail].head(15).to_string(index=False))
            print(f"\n  ...省略 {len(tlog)-25} 笔...")
            print(f"\n  后10笔:")
            print(tlog[show_cols_detail].tail(10).to_string(index=False))
        
        # 收益分布
        print(f"\n{'='*60}")
        print(f"📊 收益率分布")
        print(f"{'='*60}")
        rets = tlog['净收益率%']
        bins = [(-999, -5), (-5, -3), (-3, -1), (-1, 0), (0, 1), (1, 3), (3, 5), (5, 10), (10, 999)]
        labels = ['<-5%', '-5~-3%', '-3~-1%', '-1~0%', '0~1%', '1~3%', '3~5%', '5~10%', '>10%']
        for (lo, hi), label in zip(bins, labels):
            cnt = len(rets[(rets > lo) & (rets <= hi)])
            bar = '█' * int(cnt / len(rets) * 50)
            print(f"  {label:>8s}: {cnt:>3d}笔 ({cnt/len(rets)*100:>5.1f}%) {bar}")
        
        # 按月统计
        tlog['月份'] = pd.to_datetime(tlog['日期']).dt.to_period('M').astype(str)
        monthly = tlog.groupby('月份').agg(
            信号数=('净收益率%', 'count'),
            胜率=('净收益率%', lambda x: f"{(x > 0).sum() / len(x) * 100:.0f}%"),
            平均收益=('净收益率%', lambda x: f"{x.mean():+.2f}%"),
            最大盈=('净收益率%', 'max'),
            最大亏=('净收益率%', 'min'),
        )
        print(f"\n{'='*60}")
        print(f"📅 按月统计")
        print(f"{'='*60}")
        print(monthly.to_string())
