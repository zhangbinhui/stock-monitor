"""
波动率突破策略 v4.1 - 聚宽研究环境
滚动股票池 + 多周期K线 + 多触发策略 + 网格搜索最优参数

股票池：每日滚动筛选（市值≥50亿 & 当前价 < 滚动1年高点 × ratio）
触发策略：
  A类(max_break): 突破过去3月/1年同周期K线涨幅最大值
  B类(mult_break): 突破过去3月/1年同周期K线涨幅均值/中位数的N倍
K线周期：1min / 5min / 15min / 30min
卖出：次日涨停→涨停价卖；否则次日收盘卖

使用：聚宽 → 研究环境 → 新建Notebook → 分Cell粘贴运行
"""

# ============ Cell 1：导入和配置 ============

from jqdata import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ---- 股票池参数 ----
MIN_MARKET_CAP = 50e8           # 最低市值（元），50亿
MAX_PRICE_RATIO_DEFAULT = 1/3   # 默认：当前价 < 1年高点 × 1/3（回撤2/3）
MARKET_CAP_TIERS = [
    (100e8, 1/2),   # 100亿以上，回撤1/2即可
    (50e8,  1/3),   # 50亿以上，回撤2/3
]

# ---- 回测参数 ----
END_DATE = '2026-02-25'
BACKTEST_YEARS = 1              # 回测区间（年）
COOLDOWN_DAYS = 2               # 同一只股票信号冷却

# ---- 交易成本 ----
BUY_FEE = 0.00015
SELL_FEE = 0.00015
STAMP_TAX = 0.001

# ---- 网格搜索参数 ----
FREQ_LIST = ['1m', '5m', '15m', '30m']
SIGNAL_TYPES = ['max_break', 'mult_break']
LOOKBACK_PERIODS = ['3m', '1y']
MULT_LIST = [1.5, 2.0, 2.5, 3.0]
STAT_METHODS = ['mean', 'median']

print("✅ Cell 1 配置完成")
print(f"   股票池: 市值≥{MIN_MARKET_CAP/1e8:.0f}亿, 滚动深跌筛选")
print(f"   K线周期: {FREQ_LIST}")
print(f"   回测区间: {BACKTEST_YEARS}年, 截止{END_DATE}")


# ============ Cell 2：构建滚动股票池 ============

def build_rolling_pool(end_date, backtest_years=BACKTEST_YEARS,
                       min_cap=MIN_MARKET_CAP, tiers=MARKET_CAP_TIERS):
    """
    构建滚动股票池：用日K线计算每只股票在每个交易日是否满足深跌条件
    
    Returns:
        pool_calendar: dict {stock_code: set(date_str)} 每只股票哪些天在池子里
        stock_info: dict {stock_code: {'name': ..., 'market_cap': ...}}
    """
    print(f"\n{'='*60}")
    print(f"构建滚动股票池 (回测{backtest_years}年, 截至{end_date})")
    print(f"{'='*60}")

    # 回测起始日（多留1年给滚动1年高点计算）
    bt_start = pd.to_datetime(end_date) - timedelta(days=365 * backtest_years)
    data_start = bt_start - timedelta(days=365)  # 再往前1年算高点
    bt_start_str = bt_start.strftime('%Y-%m-%d')
    data_start_str = data_start.strftime('%Y-%m-%d')

    # 获取回测区间的交易日
    all_trade_days = get_trade_days(start_date=bt_start_str, end_date=end_date)
    print(f"回测交易日: {len(all_trade_days)} 天 ({bt_start_str} ~ {end_date})")

    # 获取所有A股（用end_date，确保覆盖全部）
    all_stocks = get_all_securities(types=['stock'], date=end_date)

    # 过滤上市不足2年的（需要至少1年高点+1年回测）
    two_years_ago = (pd.to_datetime(end_date) - timedelta(days=365*2)).date()
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    valid_codes = [c for c in valid.index
                   if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]
    print(f"非ST/上市>2年: {len(valid_codes)} 只")

    # 用end_date市值做初筛（粗筛，减少后续计算量）
    # 注意：历史上市值可能不同，但作为初筛已经够用
    trade_days_list = get_trade_days(end_date=end_date, count=5)
    last_trade = str(trade_days_list[-1])

    q = query(
        valuation.code,
        valuation.market_cap
    ).filter(
        valuation.code.in_(valid_codes),
        valuation.market_cap >= min_cap / 1e8 * 0.5  # 放宽50%做初筛，历史市值可能更大
    )
    cap_df = get_fundamentals(q, date=last_trade)
    candidate_codes = list(cap_df['code'])
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    print(f"市值初筛(≥{min_cap/1e8*0.5:.0f}亿): {len(candidate_codes)} 只")

    # 批量获取日K线（数据起始到end_date）
    print(f"批量获取日K线 ({data_start_str} ~ {end_date})...")
    pool_calendar = {}  # {code: set(date_str)}
    stock_info = {}     # {code: {name, market_cap}}
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

                # 确定该市值档位的回撤要求
                ratio = MAX_PRICE_RATIO_DEFAULT
                for tier_cap, tier_ratio in tiers:
                    if cap * 1e8 >= tier_cap:
                        ratio = tier_ratio
                        break

                # 滚动计算：每个交易日，算过去250天(1年)的最高价
                valid_dates = set()
                close_arr = closes.values
                high_arr = highs.values
                dates_arr = closes.index

                for j in range(250, len(close_arr)):
                    # 过去250天最高价
                    year_high = high_arr[j-250:j].max()
                    current = close_arr[j]
                    d = str(dates_arr[j].date())

                    # 只看回测区间内的日期
                    if d < bt_start_str:
                        continue

                    if current < year_high * ratio:
                        valid_dates.add(d)

                if valid_dates:
                    pool_calendar[code] = valid_dates
                    stock_info[code] = {'name': name, 'market_cap': cap}
                    total_pool_days += len(valid_dates)

            except Exception:
                continue

        print(f"  已处理 {min(i+50, len(candidate_codes))}/{len(candidate_codes)}")

    # 统计
    print(f"\n✅ 滚动股票池构建完成:")
    print(f"   入池股票: {len(pool_calendar)} 只")
    print(f"   总池日数: {total_pool_days} (平均每只 {total_pool_days/max(len(pool_calendar),1):.0f} 天)")

    # 显示入池天数最多的前20只
    if pool_calendar:
        sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:20]
        print(f"\n   入池天数TOP20:")
        for code, dates in sorted_stocks:
            info = stock_info[code]
            print(f"     {info['name']:>8s} ({code}): {len(dates):>3d}天, 市值{info['market_cap']:.0f}亿")

    return pool_calendar, stock_info

pool_calendar, stock_info = build_rolling_pool(END_DATE)
print(f"\n✅ Cell 2 完成")


# ============ Cell 3：核心回测引擎（滚动池版） ============

def get_lookback_bars(freq, period):
    """根据K线周期和回看期，计算需要的bar数"""
    bars_per_day = {'1m': 240, '5m': 48, '15m': 16, '30m': 8}
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def backtest_single_rolling(stock_code, stock_name, valid_dates,
                            freq, signal_type, period,
                            multiplier=None, stat_method=None,
                            end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    单只股票回测（滚动池版）：只在 valid_dates 中的日期检查信号

    Args:
        valid_dates: set(date_str) 该股票在池子里的日期集合
        其余同 v4
    Returns:
        (signals_list, summary_dict) or None
    """
    lookback_days_map = {'3m': 63, '1y': 250}

    # 数据起始日：回测最早日期 - 回看期 - buffer
    earliest = min(valid_dates)
    data_start = (pd.to_datetime(earliest) - timedelta(days=lookback_days_map[period] * 2)).strftime('%Y-%m-%d')

    # 获取日K线
    daily = get_price(stock_code, start_date=data_start, end_date=end_date,
                      frequency='daily', fields=['open', 'high', 'low', 'close', 'pre_close'])
    if daily is None or len(daily) < lookback_days_map[period] + 30:
        return None
    daily['date_str'] = daily.index.strftime('%Y-%m-%d')

    # 获取分钟K线
    min_df = get_price(stock_code, start_date=data_start, end_date=end_date,
                       frequency=freq, fields=['open', 'close', 'high', 'low', 'volume'])
    if min_df is None or len(min_df) < get_lookback_bars(freq, period):
        return None

    min_df['bar_return'] = (min_df['close'] / min_df['open'] - 1)
    min_df['date'] = min_df.index.date

    lookback_bars = get_lookback_bars(freq, period)

    # 只扫描 valid_dates 中的日期
    trade_dates = sorted([d for d in min_df['date'].unique() if str(d) in valid_dates])

    signals = []
    last_signal_date_idx = -999

    for i, date in enumerate(trade_dates):
        date_str = str(date)

        # 冷却（按实际交易日计算）
        if i - last_signal_date_idx <= cooldown:
            continue

        # 当天K线
        day_bars = min_df[min_df['date'] == date]
        if len(day_bars) < 5:
            continue

        # 当天之前的历史K线（不含当天）
        hist = min_df[min_df['date'] < date]
        if len(hist) < lookback_bars:
            continue

        # 取最近lookback_bars根的涨幅
        hist_returns = hist['bar_return'].iloc[-lookback_bars:]

        # 计算阈值
        if signal_type == 'max_break':
            threshold = hist_returns.max()
        elif signal_type == 'mult_break':
            if stat_method == 'mean':
                base = hist_returns[hist_returns > 0].mean()
            else:
                pos_returns = hist_returns[hist_returns > 0]
                base = pos_returns.median() if len(pos_returns) > 0 else 0
            if pd.isna(base) or base <= 0:
                continue
            threshold = base * multiplier
        else:
            continue

        if pd.isna(threshold) or threshold <= 0:
            continue

        # 扫描当天每根K线
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

        # 找次日日K线
        daily_mask = daily['date_str'] == date_str
        if not daily_mask.any():
            continue
        daily_idx = daily.index.get_loc(daily.index[daily_mask][0])
        if daily_idx + 1 >= len(daily):
            continue

        today_daily = daily.iloc[daily_idx]
        next_day = daily.iloc[daily_idx + 1]
        today_close = today_daily['close']
        next_close = next_day['close']
        next_high = next_day['high']

        # 涨停价
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_pct = 0.20
        else:
            limit_pct = 0.10
        limit_price = round(today_close * (1 + limit_pct), 2)

        if next_high >= limit_price:
            sell_price = limit_price
            sell_type = 'limit_up'
        else:
            sell_price = next_close
            sell_type = 'next_close'

        cost = trigger_price * BUY_FEE + sell_price * (SELL_FEE + STAMP_TAX)
        ret = (sell_price - trigger_price - cost) / trigger_price * 100

        last_signal_date_idx = i

        signals.append({
            'date': date_str,
            'trigger_time': str(trigger_bar),
            'trigger_price': round(trigger_price, 3),
            'trigger_return_pct': round(trigger_return * 100, 3),
            'threshold_pct': round(threshold * 100, 3),
            'sell_price': round(sell_price, 3),
            'sell_type': sell_type,
            'return_pct': round(ret, 2),
        })

    if not signals:
        return None

    df = pd.DataFrame(signals)
    total = len(df)
    wins = len(df[df['return_pct'] > 0])
    wr = wins / total * 100 if total > 0 else 0
    avg_ret = df['return_pct'].mean()
    avg_win = df[df['return_pct'] > 0]['return_pct'].mean() if wins > 0 else 0
    avg_loss = df[df['return_pct'] <= 0]['return_pct'].mean() if total - wins > 0 else 0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    limit_sells = len(df[df['sell_type'] == 'limit_up'])

    summary = {
        'stock': stock_name,
        'code': stock_code,
        'pool_days': len(valid_dates),
        'signals': total,
        'wins': wins,
        'win_rate': round(wr, 1),
        'avg_return': round(avg_ret, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'pl_ratio': round(pl_ratio, 2),
        'limit_sells': limit_sells,
    }

    return signals, summary


def run_grid_search(pool_calendar, stock_info, max_stocks=30):
    """网格搜索所有参数组合"""

    # 按入池天数排序，优先测入池时间长的（信号更多）
    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:max_stocks]

    print(f"\n{'='*60}")
    print(f"网格搜索 ({len(sorted_stocks)} 只股票)")
    print(f"{'='*60}")

    # 构建参数网格
    param_grid = []
    for freq in FREQ_LIST:
        for period in LOOKBACK_PERIODS:
            param_grid.append({
                'freq': freq, 'signal_type': 'max_break',
                'period': period, 'multiplier': None, 'stat_method': None,
                'label': f"{freq}|max|{period}"
            })
            for stat in STAT_METHODS:
                for mult in MULT_LIST:
                    param_grid.append({
                        'freq': freq, 'signal_type': 'mult_break',
                        'period': period, 'multiplier': mult, 'stat_method': stat,
                        'label': f"{freq}|{stat}x{mult}|{period}"
                    })

    print(f"参数组合: {len(param_grid)} 种")
    print(f"总任务: {len(param_grid)} x {len(sorted_stocks)} = {len(param_grid)*len(sorted_stocks)} 次回测\n")

    results = []

    for si, (code, valid_dates) in enumerate(sorted_stocks):
        info = stock_info[code]
        name = info['name']
        print(f"[{si+1}/{len(sorted_stocks)}] {name} ({code}) 入池{len(valid_dates)}天, 市值{info['market_cap']:.0f}亿")

        for params in param_grid:
            try:
                r = backtest_single_rolling(
                    code, name, valid_dates,
                    freq=params['freq'],
                    signal_type=params['signal_type'],
                    period=params['period'],
                    multiplier=params['multiplier'],
                    stat_method=params['stat_method'],
                )
                if r:
                    sigs, summary = r
                    summary['label'] = params['label']
                    summary['freq'] = params['freq']
                    summary['signal_type'] = params['signal_type']
                    summary['period'] = params['period']
                    summary['multiplier'] = params['multiplier']
                    summary['stat_method'] = params['stat_method']
                    results.append(summary)
            except Exception as e:
                pass

    return results

print("✅ Cell 3 回测引擎定义完成")


# ============ Cell 4：运行网格搜索 ============

# 建议先 max_stocks=10 快速验证，确认后改大
MAX_STOCKS = 15

all_results = run_grid_search(pool_calendar, stock_info, max_stocks=MAX_STOCKS)

print(f"\n✅ 回测完成，共 {len(all_results)} 条结果")


# ============ Cell 5：汇总分析 - 找最优策略 ============

if not all_results:
    print("无结果，请检查股票池或参数")
else:
    rdf = pd.DataFrame(all_results)

    # 按参数组合(label)汇总
    grouped = rdf.groupby('label')
    agg = pd.DataFrame()
    agg['stocks_with_signal'] = grouped['stock'].nunique()
    agg['total_signals'] = grouped['signals'].sum()
    agg['total_wins'] = grouped['wins'].sum()
    agg['avg_return'] = grouped['avg_return'].mean()
    agg['avg_win_rate'] = grouped['win_rate'].mean()
    agg['avg_pl_ratio'] = grouped['pl_ratio'].apply(
        lambda x: x[x < float('inf')].mean() if len(x[x < float('inf')]) > 0 else 0)
    agg['total_limit_sells'] = grouped['limit_sells'].sum()
    agg = agg.reset_index()

    agg['overall_win_rate'] = (agg['total_wins'] / agg['total_signals'] * 100).round(1)
    agg['limit_sell_pct'] = (agg['total_limit_sells'] / agg['total_signals'] * 100).round(1)

    # 综合评分
    agg['sig_score'] = agg['total_signals'].apply(
        lambda x: min(x / 20, 1.0) * 100 if x >= 5 else 0
    )
    agg['composite_score'] = (
        agg['overall_win_rate'] * 0.4 +
        agg['avg_return'].clip(-10, 10) * 5 * 0.3 +
        agg['avg_pl_ratio'].clip(0, 5) * 20 * 0.2 +
        agg['sig_score'] * 0.1
    ).round(1)

    valid_agg = agg[agg['total_signals'] >= 5].sort_values('composite_score', ascending=False)

    print(f"\n{'='*60}")
    print(f"📊 策略排行榜 TOP 20 (至少5个信号)")
    print(f"{'='*60}")

    display_cols = ['label', 'stocks_with_signal', 'total_signals', 'overall_win_rate',
                    'avg_return', 'avg_pl_ratio', 'limit_sell_pct', 'composite_score']
    print(valid_agg[display_cols].head(20).to_string(index=False))

    # 按维度拆解
    print(f"\n{'='*60}")
    print("📈 按K线周期汇总")
    print(f"{'='*60}")
    for freq in FREQ_LIST:
        fdf = rdf[rdf['freq'] == freq]
        if fdf.empty:
            continue
        ts = fdf['signals'].sum()
        tw = fdf['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = fdf['avg_return'].mean()
        print(f"  {freq:>4s}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    print(f"\n{'='*60}")
    print("📈 按触发类型汇总")
    print(f"{'='*60}")
    for st in SIGNAL_TYPES:
        sdf = rdf[rdf['signal_type'] == st]
        if sdf.empty:
            continue
        ts = sdf['signals'].sum()
        tw = sdf['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = sdf['avg_return'].mean()
        label = "突破最大值" if st == 'max_break' else "倍数突破"
        print(f"  {label}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    print(f"\n{'='*60}")
    print("📈 按回看周期汇总")
    print(f"{'='*60}")
    for p in LOOKBACK_PERIODS:
        pdf_sub = rdf[rdf['period'] == p]
        if pdf_sub.empty:
            continue
        ts = pdf_sub['signals'].sum()
        tw = pdf_sub['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = pdf_sub['avg_return'].mean()
        print(f"  {p}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    if 'mult_break' in rdf['signal_type'].values:
        print(f"\n{'='*60}")
        print("📈 B类策略：按倍数x统计方法")
        print(f"{'='*60}")
        bdf = rdf[rdf['signal_type'] == 'mult_break']
        for stat in STAT_METHODS:
            for mult in MULT_LIST:
                sub = bdf[(bdf['stat_method'] == stat) & (bdf['multiplier'] == mult)]
                if sub.empty:
                    continue
                ts = sub['signals'].sum()
                tw = sub['wins'].sum()
                wr = tw / ts * 100 if ts > 0 else 0
                ar = sub['avg_return'].mean()
                print(f"  {stat}x{mult}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    if len(valid_agg) > 0:
        best = valid_agg.iloc[0]
        print(f"\n{'='*60}")
        print(f"🏆 最佳策略: {best['label']}")
        print(f"   信号数: {best['total_signals']:.0f}")
        print(f"   胜率: {best['overall_win_rate']:.1f}%")
        print(f"   均收: {best['avg_return']:.2f}%")
        print(f"   盈亏比: {best['avg_pl_ratio']:.2f}")
        print(f"   涨停卖出: {best['limit_sell_pct']:.1f}%")
        print(f"   综合评分: {best['composite_score']}")
        print(f"{'='*60}")


# ============ Cell 6（可选）：查看最佳策略的个股详情 ============

BEST_LABEL = valid_agg.iloc[0]['label'] if len(valid_agg) > 0 else None

if BEST_LABEL:
    best_results = rdf[rdf['label'] == BEST_LABEL].sort_values('avg_return', ascending=False)
    print(f"\n🏆 最佳策略 [{BEST_LABEL}] 个股表现:")
    print(best_results[['stock', 'code', 'pool_days', 'signals', 'win_rate', 'avg_return',
                         'avg_win', 'avg_loss', 'pl_ratio', 'limit_sells']].to_string(index=False))


# ============ Cell 7：资金曲线模拟 ============
"""
用最佳策略重新跑全部股票，收集每笔交易，按时间排序模拟真实账户资金变化。

参数：
  - 初始资金 5万
  - 单笔买入上限 1万元（买不起就跳过）
  - 手续费：买入5元 + 卖出5元 = 每笔10元（替代之前的费率计算）
  - 同时最多持有5只
  - 风控：日亏上限1000元、连亏10次停、总亏2万停
"""

# ---- 资金模拟参数 ----
INIT_CAPITAL = 50000        # 初始资金
MAX_PER_TRADE = 10000       # 单笔最大买入金额
COMMISSION_PER_SIDE = 5     # 每次交易手续费（买卖各5元）
MAX_POSITIONS = 5           # 同时最多持有
DAILY_LOSS_LIMIT = -1000    # 日亏上限（元）
MAX_CONSECUTIVE_LOSS = 10   # 连亏N次暂停
TOTAL_LOSS_LIMIT = -20000   # 总亏损上限（元）

# ---- 选择要模拟的策略 ----
SIM_LABEL = BEST_LABEL  # 用最佳策略，也可以手动指定如 '15m|max|1y'

print(f"\n{'='*60}")
print(f"💰 资金曲线模拟 — 策略: {SIM_LABEL}")
print(f"{'='*60}")
print(f"   初始资金: {INIT_CAPITAL:,.0f}元")
print(f"   单笔上限: {MAX_PER_TRADE:,.0f}元")
print(f"   手续费: 买卖各{COMMISSION_PER_SIDE}元")
print(f"   最大持仓: {MAX_POSITIONS}只")
print(f"   风控: 日亏{DAILY_LOSS_LIMIT}元 / 连亏{MAX_CONSECUTIVE_LOSS}次 / 总亏{TOTAL_LOSS_LIMIT}元")

# 解析策略参数
parts = SIM_LABEL.split('|')
sim_freq = parts[0]
sim_trigger = parts[1]
sim_period = parts[2]

if sim_trigger == 'max':
    sim_signal_type = 'max_break'
    sim_multiplier = None
    sim_stat_method = None
else:
    sim_signal_type = 'mult_break'
    sim_stat_method = sim_trigger[:sim_trigger.index('x')]
    sim_multiplier = float(sim_trigger[sim_trigger.index('x')+1:])

# 收集所有信号（重新跑全部股票，这次用固定手续费）
print(f"\n收集全部股票的交易信号...")

all_trades = []
sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:MAX_STOCKS]

for si, (code, valid_dates) in enumerate(sorted_stocks):
    info = stock_info[code]
    name = info['name']
    try:
        r = backtest_single_rolling(
            code, name, valid_dates,
            freq=sim_freq,
            signal_type=sim_signal_type,
            period=sim_period,
            multiplier=sim_multiplier,
            stat_method=sim_stat_method,
        )
        if r:
            sigs, summary = r
            for s in sigs:
                s['stock'] = name
                s['code'] = code
                all_trades.append(s)
    except:
        pass
    if (si + 1) % 50 == 0:
        print(f"  已处理 {si+1}/{len(sorted_stocks)}")

print(f"  总信号数: {len(all_trades)}")

if not all_trades:
    print("❌ 无信号，无法模拟")
else:
    # 按触发日期排序
    trades_df = pd.DataFrame(all_trades).sort_values('date').reset_index(drop=True)

    # ---- 模拟交易 ----
    capital = INIT_CAPITAL
    peak_capital = INIT_CAPITAL
    max_drawdown = 0
    max_drawdown_pct = 0
    positions = {}          # {code: {shares, buy_price, buy_date, sell_date, name}}
    equity_curve = []       # [(date, equity)]
    trade_log = []          # 成交记录
    consecutive_losses = 0
    strategy_paused = False
    pause_reason = None
    daily_pnl = {}          # {date: pnl}

    for _, trade in trades_df.iterrows():
        buy_date = trade['date']
        code = trade['code']
        buy_price = trade['trigger_price']
        sell_price = trade['sell_price']

        # 检查风控
        if strategy_paused:
            continue

        # 检查是否已持有该股票
        if code in positions:
            continue

        # 检查持仓数量
        if len(positions) >= MAX_POSITIONS:
            continue

        # 计算可买股数（100的整数倍）
        max_afford = min(MAX_PER_TRADE, capital - COMMISSION_PER_SIDE)  # 留出手续费
        if max_afford < buy_price * 100:  # 买不起1手
            continue
        shares = int(max_afford / buy_price / 100) * 100
        if shares <= 0:
            continue

        buy_cost = shares * buy_price + COMMISSION_PER_SIDE
        sell_revenue = shares * sell_price - COMMISSION_PER_SIDE
        pnl = sell_revenue - buy_cost

        # 扣除买入资金
        capital -= buy_cost
        # 加回卖出资金（T+1，次日卖出）
        capital += sell_revenue

        # 记录
        trade_log.append({
            'buy_date': buy_date,
            'stock': trade['stock'],
            'code': code,
            'shares': shares,
            'buy_price': buy_price,
            'sell_price': sell_price,
            'sell_type': trade['sell_type'],
            'pnl': round(pnl, 2),
            'pnl_pct': round(pnl / buy_cost * 100, 2),
            'capital_after': round(capital, 2),
        })

        # 更新日PnL
        daily_pnl[buy_date] = daily_pnl.get(buy_date, 0) + pnl

        # 更新峰值和回撤
        if capital > peak_capital:
            peak_capital = capital
        dd = capital - peak_capital
        dd_pct = dd / peak_capital * 100 if peak_capital > 0 else 0
        if dd < max_drawdown:
            max_drawdown = dd
            max_drawdown_pct = dd_pct

        equity_curve.append((buy_date, round(capital, 2)))

        # 风控检查
        # 1) 日亏损
        if daily_pnl.get(buy_date, 0) <= DAILY_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"日亏损触限: {daily_pnl[buy_date]:.0f}元 (限{DAILY_LOSS_LIMIT}元)"
            break

        # 2) 连续亏损
        if pnl <= 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
        if consecutive_losses >= MAX_CONSECUTIVE_LOSS:
            strategy_paused = True
            pause_reason = f"连续亏损{consecutive_losses}次"
            break

        # 3) 总亏损
        total_pnl = capital - INIT_CAPITAL
        if total_pnl <= TOTAL_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"总亏损触限: {total_pnl:.0f}元 (限{TOTAL_LOSS_LIMIT}元)"
            break

    # ---- 输出结果 ----
    total_pnl = capital - INIT_CAPITAL
    total_trades = len(trade_log)
    win_trades = len([t for t in trade_log if t['pnl'] > 0])
    lose_trades = len([t for t in trade_log if t['pnl'] <= 0])

    print(f"\n{'='*60}")
    print(f"💰 资金模拟结果")
    print(f"{'='*60}")
    print(f"   初始资金:   {INIT_CAPITAL:>10,.0f} 元")
    print(f"   最终资金:   {capital:>10,.2f} 元")
    print(f"   总盈亏:     {total_pnl:>+10,.2f} 元 ({total_pnl/INIT_CAPITAL*100:+.1f}%)")
    print(f"   峰值资金:   {peak_capital:>10,.2f} 元")
    print(f"   最大回撤:   {max_drawdown:>10,.2f} 元 ({max_drawdown_pct:.1f}%)")
    print(f"   总交易次数: {total_trades}")
    print(f"   盈利次数:   {win_trades} ({win_trades/total_trades*100:.1f}%)" if total_trades > 0 else "")
    print(f"   亏损次数:   {lose_trades} ({lose_trades/total_trades*100:.1f}%)" if total_trades > 0 else "")

    if trade_log:
        pnls = [t['pnl'] for t in trade_log]
        print(f"   单笔最大盈: {max(pnls):>+10,.2f} 元")
        print(f"   单笔最大亏: {min(pnls):>+10,.2f} 元")
        print(f"   平均盈亏:   {sum(pnls)/len(pnls):>+10,.2f} 元")

    if strategy_paused:
        print(f"\n   ⚠️ 策略已暂停: {pause_reason}")

    # 显示交易记录（前20条和后10条）
    if trade_log:
        tlog = pd.DataFrame(trade_log)
        print(f"\n📋 交易记录 (共{len(tlog)}笔):")
        print(f"\n前20笔:")
        print(tlog.head(20).to_string(index=False))
        if len(tlog) > 20:
            print(f"\n...后10笔:")
            print(tlog.tail(10).to_string(index=False))

    # 资金曲线（按日汇总）
    if equity_curve:
        eq_df = pd.DataFrame(equity_curve, columns=['date', 'equity'])
        eq_daily = eq_df.groupby('date')['equity'].last().reset_index()
        print(f"\n📈 资金曲线 (按日):")
        print(eq_daily.to_string(index=False))
