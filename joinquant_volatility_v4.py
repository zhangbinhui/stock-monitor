"""
波动率突破策略 v4 - 聚宽研究环境
大改版：动态股票池 + 多周期K线 + 多触发策略 + 网格搜索最优参数

股票池：市值≥100亿 & 当前价 < 1年高点 × max_price_ratio（深跌股）
触发策略：
  A类(max_break): 突破过去3月/1年的K线涨幅最大值
  B类(mult_break): 突破过去3月/1年的K线涨幅均值/中位数的N倍
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
# 市值越大，回撤要求越低：(市值下限, price_ratio)
MARKET_CAP_TIERS = [
    (100e8, 1/2),   # 100亿以上，回撤1/2即可
    (50e8,  1/3),   # 50亿以上，回撤2/3
]

# ---- 回测参数 ----
END_DATE = '2026-02-25'
LOOKBACK_DAYS = 365             # 分钟K线回看天数（回测区间）
COOLDOWN_DAYS = 2               # 同一只股票信号冷却

# ---- 交易成本 ----
BUY_FEE = 0.00015
SELL_FEE = 0.00015
STAMP_TAX = 0.001

# ---- 网格搜索参数 ----
FREQ_LIST = ['1m', '5m', '15m', '30m']           # K线周期
SIGNAL_TYPES = ['max_break', 'mult_break']         # 触发类型
LOOKBACK_PERIODS = ['3m', '1y']                    # 波动率回看期
MULT_LIST = [1.5, 2.0, 2.5, 3.0]                  # B类倍数
STAT_METHODS = ['mean', 'median']                  # B类统计方法

print("✅ Cell 1 配置完成")
print(f"   股票池: 市值≥{MIN_MARKET_CAP/1e8:.0f}亿, 深跌股")
print(f"   K线周期: {FREQ_LIST}")
print(f"   回测截止: {END_DATE}")


# ============ Cell 2：构建股票池 ============

def build_stock_pool(date, min_cap=MIN_MARKET_CAP, tiers=MARKET_CAP_TIERS):
    """筛选股票池：市值够大 + 从高点深跌"""
    print(f"\n{'='*60}")
    print(f"构建股票池 (截至 {date})")
    print(f"{'='*60}")

    # 获取所有A股
    all_stocks = get_all_securities(types=['stock'], date=date)
    all_codes = list(all_stocks.index)
    print(f"全市场: {len(all_codes)} 只")

    # 过滤ST、停牌、次新股(上市<1年)
    one_year_ago = (pd.to_datetime(date) - timedelta(days=365)).date()
    valid = all_stocks[all_stocks['start_date'] <= one_year_ago]
    valid_codes = [c for c in valid.index if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]

    # 过滤停牌
    trade_days = get_trade_days(end_date=date, count=5)
    last_trade = str(trade_days[-1])
    paused = get_price(valid_codes, end_date=last_trade, count=1, fields=['paused'])
    if paused is not None:
        paused_df = paused['paused']
        if isinstance(paused_df, pd.DataFrame):
            active_codes = [c for c in valid_codes if c in paused_df.columns and paused_df[c].iloc[-1] == 0]
        else:
            active_codes = valid_codes
    else:
        active_codes = valid_codes
    print(f"非ST/非停牌/上市>1年: {len(active_codes)} 只")

    # 获取市值
    q = query(
        valuation.code,
        valuation.market_cap  # 亿元
    ).filter(
        valuation.code.in_(active_codes),
        valuation.market_cap >= min_cap / 1e8  # 转为亿
    )
    cap_df = get_fundamentals(q, date=last_trade)
    print(f"市值≥{min_cap/1e8:.0f}亿: {len(cap_df)} 只")

    if cap_df.empty:
        return []

    # 获取当前价和1年高点
    pool = []
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    codes_to_check = list(cap_dict.keys())

    # 批量获取1年日K线
    print("获取1年日K线计算高点...")
    for i in range(0, len(codes_to_check), 50):
        batch = codes_to_check[i:i+50]
        prices = get_price(batch, end_date=date, count=250, fields=['high', 'close'])

        for code in batch:
            try:
                if isinstance(prices['high'], pd.DataFrame):
                    highs = prices['high'][code].dropna()
                    closes = prices['close'][code].dropna()
                else:
                    continue

                if len(highs) < 60:
                    continue

                year_high = highs.max()
                current_price = closes.iloc[-1]
                cap = cap_dict[code]  # 亿元

                # 根据市值档位确定回撤要求
                ratio = MAX_PRICE_RATIO_DEFAULT
                for tier_cap, tier_ratio in tiers:
                    if cap * 1e8 >= tier_cap:
                        ratio = tier_ratio
                        break

                if current_price < year_high * ratio:
                    drawdown = (1 - current_price / year_high) * 100
                    name = get_security_info(code).display_name
                    pool.append({
                        'code': code,
                        'name': name,
                        'market_cap': round(cap, 1),
                        'current_price': round(current_price, 2),
                        'year_high': round(year_high, 2),
                        'drawdown_pct': round(drawdown, 1),
                        'required_ratio': ratio,
                    })
            except Exception:
                continue

        print(f"  已处理 {min(i+50, len(codes_to_check))}/{len(codes_to_check)}")

    pool.sort(key=lambda x: -x['drawdown_pct'])
    print(f"\n✅ 股票池: {len(pool)} 只 (市值≥{min_cap/1e8:.0f}亿 & 深跌)")

    if pool:
        pdf = pd.DataFrame(pool)
        print(pdf[['name', 'code', 'market_cap', 'current_price', 'year_high',
                    'drawdown_pct']].head(20).to_string(index=False))
        if len(pool) > 20:
            print(f"  ... 共 {len(pool)} 只")

    return pool

stock_pool = build_stock_pool(END_DATE)
print(f"\n✅ Cell 2 完成，股票池 {len(stock_pool)} 只")


# ============ Cell 3：核心回测引擎 ============

def get_lookback_bars(freq, period):
    """根据K线周期和回看期，计算需要的bar数"""
    # 每天交易240分钟
    trading_mins_per_day = 240
    bars_per_day = {
        '1m': 240,
        '5m': 48,
        '15m': 16,
        '30m': 8,
    }
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def calc_bar_returns(df):
    """计算每根K线的涨幅（收盘/开盘 - 1）"""
    return (df['close'] / df['open'] - 1).dropna()


def backtest_single(stock_code, stock_name, freq, signal_type, period,
                    multiplier=None, stat_method=None,
                    end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    单只股票、单参数组合回测

    Args:
        freq: '1m', '5m', '15m', '30m'
        signal_type: 'max_break' 或 'mult_break'
        period: '3m' 或 '1y' — 计算阈值的回看期
        multiplier: B类倍数 (仅mult_break)
        stat_method: 'mean' 或 'median' (仅mult_break)

    Returns:
        (signals_list, summary_dict) or None
    """
    # 需要的历史数据：回看期 + 回测期(1年)
    lookback_days_map = {'3m': 63, '1y': 250}
    total_days = lookback_days_map[period] + LOOKBACK_DAYS
    start_date = (pd.to_datetime(end_date) - timedelta(days=int(total_days * 1.5))).strftime('%Y-%m-%d')

    # 获取日K线（用于次日卖出）
    daily = get_price(stock_code, start_date=start_date, end_date=end_date,
                      frequency='daily', fields=['open', 'high', 'low', 'close', 'pre_close'])
    if daily is None or len(daily) < lookback_days_map[period] + 60:
        return None

    daily['date_str'] = daily.index.strftime('%Y-%m-%d')

    # 获取分钟K线
    min_df = get_price(stock_code, start_date=start_date, end_date=end_date,
                       frequency=freq, fields=['open', 'close', 'high', 'low', 'volume'])
    if min_df is None or len(min_df) < get_lookback_bars(freq, period) + 1000:
        return None

    min_df['bar_return'] = (min_df['close'] / min_df['open'] - 1)
    min_df['date'] = min_df.index.date

    # 回测区间：最近1年的交易日
    backtest_start = (pd.to_datetime(end_date) - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    trade_dates = sorted([d for d in min_df['date'].unique() if str(d) >= backtest_start])

    bars_per_day_map = {'1m': 240, '5m': 48, '15m': 16, '30m': 8}
    lookback_bars = get_lookback_bars(freq, period)

    signals = []
    last_signal_idx = -999

    for i, date in enumerate(trade_dates):
        date_str = str(date)

        # 冷却
        if i - last_signal_idx <= cooldown:
            continue

        # 当天K线
        day_bars = min_df[min_df['date'] == date]
        if len(day_bars) < 5:
            continue

        # 当天之前的历史K线（不含当天）
        hist_mask = min_df['date'] < date
        hist = min_df[hist_mask]
        if len(hist) < lookback_bars:
            continue

        # 取最近lookback_bars根的涨幅
        hist_returns = hist['bar_return'].iloc[-lookback_bars:]

        # 计算阈值
        if signal_type == 'max_break':
            threshold = hist_returns.max()
        elif signal_type == 'mult_break':
            if stat_method == 'mean':
                base = hist_returns[hist_returns > 0].mean()  # 只看正涨幅的均值
            else:  # median
                pos_returns = hist_returns[hist_returns > 0]
                base = pos_returns.median() if len(pos_returns) > 0 else 0
            if pd.isna(base) or base <= 0:
                continue
            threshold = base * multiplier
        else:
            continue

        if pd.isna(threshold) or threshold <= 0:
            continue

        # 扫描当天每根K线是否突破
        triggered = False
        trigger_bar = None

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

        # 卖出逻辑
        if next_high >= limit_price:
            sell_price = limit_price
            sell_type = 'limit_up'
        else:
            sell_price = next_close
            sell_type = 'next_close'

        # 收益
        cost = trigger_price * BUY_FEE + sell_price * (SELL_FEE + STAMP_TAX)
        ret = (sell_price - trigger_price - cost) / trigger_price * 100

        last_signal_idx = i

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


def run_grid_search(pool, max_stocks=30):
    """
    网格搜索所有参数组合，返回汇总结果

    参数组合：
      A类: freq × period (无multiplier)
      B类: freq × period × stat_method × multiplier
    """
    # 限制股票数量避免超时
    stocks = pool[:max_stocks]
    print(f"\n{'='*60}")
    print(f"网格搜索 ({len(stocks)} 只股票)")
    print(f"{'='*60}")

    # 构建参数网格
    param_grid = []
    for freq in FREQ_LIST:
        for period in LOOKBACK_PERIODS:
            # A类：突破历史最大值
            param_grid.append({
                'freq': freq, 'signal_type': 'max_break',
                'period': period, 'multiplier': None, 'stat_method': None,
                'label': f"{freq}|max|{period}"
            })
            # B类：突破均值/中位数的N倍
            for stat in STAT_METHODS:
                for mult in MULT_LIST:
                    param_grid.append({
                        'freq': freq, 'signal_type': 'mult_break',
                        'period': period, 'multiplier': mult, 'stat_method': stat,
                        'label': f"{freq}|{stat}×{mult}|{period}"
                    })

    print(f"参数组合: {len(param_grid)} 种")
    print(f"总任务: {len(param_grid)} × {len(stocks)} = {len(param_grid)*len(stocks)} 次回测\n")

    # 预取分钟K线数据（按freq缓存，避免重复拉取）
    # 聚宽研究环境有内存限制，逐股票处理
    results = []  # [{label, freq, signal_type, period, mult, stat, total_signals, total_wins, ...}]

    # 按 (股票, freq) 分组：同一freq的数据只拉一次
    for si, stock_info in enumerate(stocks):
        code = stock_info['code']
        name = stock_info['name']
        print(f"[{si+1}/{len(stocks)}] {name} ({code})  市值{stock_info['market_cap']}亿  回撤{stock_info['drawdown_pct']}%")

        for params in param_grid:
            try:
                r = backtest_single(
                    code, name,
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
                pass  # 静默跳过

    return results


print("✅ Cell 3 回测引擎定义完成")


# ============ Cell 4：运行网格搜索 ============

# 控制回测股票数量（聚宽分钟数据有额度限制）
# 建议先用 max_stocks=10 快速验证，确认OK后改大
MAX_STOCKS = 15

all_results = run_grid_search(stock_pool, max_stocks=MAX_STOCKS)

print(f"\n✅ 回测完成，共 {len(all_results)} 条结果")


# ============ Cell 5：汇总分析 - 找最优策略 ============

if not all_results:
    print("无结果，请检查股票池或参数")
else:
    rdf = pd.DataFrame(all_results)

    # 按参数组合(label)汇总
    agg = rdf.groupby('label').agg(
        stocks_with_signal=('stock', 'nunique'),
        total_signals=('signals', 'sum'),
        total_wins=('wins', 'sum'),
        avg_return=('avg_return', 'mean'),
        avg_win_rate=('win_rate', 'mean'),
        avg_pl_ratio=('pl_ratio', lambda x: x[x < float('inf')].mean() if len(x[x < float('inf')]) > 0 else 0),
        total_limit_sells=('limit_sells', 'sum'),
    ).reset_index()

    agg['overall_win_rate'] = (agg['total_wins'] / agg['total_signals'] * 100).round(1)
    agg['limit_sell_pct'] = (agg['total_limit_sells'] / agg['total_signals'] * 100).round(1)

    # 综合评分 = 胜率×0.4 + 均收×0.3 + 盈亏比×0.2 + 信号数量分×0.1
    # 信号数量分：信号太少没统计意义，太多噪音大
    max_sig = agg['total_signals'].max() if agg['total_signals'].max() > 0 else 1
    agg['sig_score'] = agg['total_signals'].apply(
        lambda x: min(x / 20, 1.0) * 100 if x >= 5 else 0  # 至少5个信号才算
    )
    agg['composite_score'] = (
        agg['overall_win_rate'] * 0.4 +
        agg['avg_return'].clip(-10, 10) * 5 * 0.3 +  # 均收映射到0-100
        agg['avg_pl_ratio'].clip(0, 5) * 20 * 0.2 +   # 盈亏比映射到0-100
        agg['sig_score'] * 0.1
    ).round(1)

    # 过滤掉信号太少的
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
        pdf = rdf[rdf['period'] == p]
        if pdf.empty:
            continue
        ts = pdf['signals'].sum()
        tw = pdf['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = pdf['avg_return'].mean()
        print(f"  {p}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    if 'mult_break' in rdf['signal_type'].values:
        print(f"\n{'='*60}")
        print("📈 B类策略：按倍数×统计方法")
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
                print(f"  {stat}×{mult}: 信号={ts:>4d}, 胜率={wr:.1f}%, 均收={ar:+.2f}%")

    # 最佳策略
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

# 把最佳策略的label填这里
BEST_LABEL = valid_agg.iloc[0]['label'] if len(valid_agg) > 0 else None

if BEST_LABEL:
    best_results = rdf[rdf['label'] == BEST_LABEL].sort_values('avg_return', ascending=False)
    print(f"\n🏆 最佳策略 [{BEST_LABEL}] 个股表现:")
    print(best_results[['stock', 'code', 'signals', 'win_rate', 'avg_return',
                         'avg_win', 'avg_loss', 'pl_ratio', 'limit_sells']].to_string(index=False))
