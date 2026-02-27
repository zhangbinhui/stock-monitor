"""
波动率突破策略 v4.2 - 聚宽研究环境
=================================

【策略逻辑（白话版）】
1. 选股：从全A股里找「跌惨了的大票」（市值≥50亿，股价相对1年高点大幅回撤）
2. 盯盘：用分钟K线（1分/5分/15分/30分）盘中监控，看有没有「异常大涨」
3. 买入：某根K线涨幅突破历史极值 → 信号触发 → 以当前价买入
4. 卖出：次日如果碰到涨停 → 涨停价卖掉；没涨停 → 收盘价卖掉
5. 网格搜索：测试72种参数组合，找出最优策略
6. 资金模拟：用5万本金模拟真实交易，算出收益、回撤、风控

【触发策略】
  A类「突破最大值」: 某根K线涨幅 > 过去3月/1年同周期K线涨幅的历史最大值
  B类「倍数突破」  : 某根K线涨幅 > 过去3月/1年同周期K线涨幅的 均值/中位数 × N倍

【使用方法】
  聚宽(joinquant.com) → 研究环境 → 新建Notebook → 按Cell分段粘贴运行

【版本更新 v4.2】
  - 回测区间改为3年（覆盖熊牛周期，避免单一市场环境偏差）
  - 全部输出中文化，方便阅读理解
  - 新增资金曲线模拟（Cell 7）
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
MIN_MARKET_CAP = 50e8           # 最低市值50亿元（过滤小盘股，确保流动性）
MAX_PRICE_RATIO_DEFAULT = 1/3   # 默认：当前价 < 1年最高价 × 1/3（即从高点跌了67%以上）
MARKET_CAP_TIERS = [            # 不同市值档位的回撤要求（大票不用跌那么多）
    (100e8, 1/2),   # 100亿以上：跌50%就入池
    (50e8,  1/3),   # 50亿以上：跌67%才入池
]

# ======== 回测参数 ========
END_DATE = '2026-02-25'         # 回测截止日期
BACKTEST_YEARS = 3              # ★ 回测3年（覆盖2023-2026，含熊市和牛市）
COOLDOWN_DAYS = 2               # 同一只股票触发信号后，冷却2个交易日再允许下次触发

# ======== 交易成本（用于网格搜索的胜率/均收计算）========
BUY_FEE = 0.00015              # 买入佣金万1.5
SELL_FEE = 0.00015             # 卖出佣金万1.5
STAMP_TAX = 0.001              # 印花税千1（仅卖出收取）

# ======== 网格搜索的参数空间 ========
FREQ_LIST = ['1m', '5m', '15m', '30m']     # 要测试的K线周期
SIGNAL_TYPES = ['max_break', 'mult_break']  # 触发类型
LOOKBACK_PERIODS = ['3m', '1y']             # 回看周期：过去3个月 / 过去1年
MULT_LIST = [1.5, 2.0, 2.5, 3.0]           # B类策略的倍数
STAT_METHODS = ['mean', 'median']           # B类策略的统计方法：均值 / 中位数

# ======== 显示配置信息 ========
print("=" * 60)
print("✅ Cell 1 配置完成")
print("=" * 60)
print(f"  📌 股票池条件: 市值≥{MIN_MARKET_CAP/1e8:.0f}亿，从1年高点大幅回撤的股票")
print(f"  📌 K线周期: {FREQ_LIST}")
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 交易成本: 买入万{BUY_FEE*10000:.1f} + 卖出万{SELL_FEE*10000:.1f} + 印花税千{STAMP_TAX*1000:.0f}")
print(f"  📌 信号冷却: 同一只股票触发后冷却{COOLDOWN_DAYS}个交易日")
print(f"  📌 参数组合: {len(FREQ_LIST)} 周期 × (1个A类 + {len(STAT_METHODS)}×{len(MULT_LIST)}个B类) × {len(LOOKBACK_PERIODS)} 回看 = {len(FREQ_LIST) * (1 + len(STAT_METHODS)*len(MULT_LIST)) * len(LOOKBACK_PERIODS)} 种")


# ============================================================
# Cell 2：构建滚动股票池
# ============================================================
# 
# 【做什么】遍历全A股，对每只股票的每个交易日判断：
#   "这一天，这只股票的收盘价是否低于过去1年最高价 × 回撤比例？"
#   如果是 → 这只股票在这一天「在池子里」
#
# 【为什么要滚动】因为股票跌不跌是动态的。
#   比如某股票2024年1月还没跌，7月才开始暴跌，那它只有7月以后才该被监控。

def build_rolling_pool(end_date, backtest_years=BACKTEST_YEARS,
                       min_cap=MIN_MARKET_CAP, tiers=MARKET_CAP_TIERS):
    """
    构建滚动股票池
    
    返回:
        pool_calendar: {股票代码: set(日期字符串)} 每只股票在哪些天满足条件
        stock_info:    {股票代码: {'name': 名称, 'market_cap': 市值(亿)}}
    """
    print(f"\n{'='*60}")
    print(f"📊 第一步：构建滚动股票池")
    print(f"{'='*60}")
    print(f"  回测{backtest_years}年，截至{end_date}")
    print(f"  筛选条件：市值≥{min_cap/1e8:.0f}亿 & 股价大幅低于1年高点")

    # 时间范围：回测区间 + 往前多留1年（用来算"过去1年最高价"）
    bt_start = pd.to_datetime(end_date) - timedelta(days=365 * backtest_years)
    data_start = bt_start - timedelta(days=365)
    bt_start_str = bt_start.strftime('%Y-%m-%d')
    data_start_str = data_start.strftime('%Y-%m-%d')

    all_trade_days = get_trade_days(start_date=bt_start_str, end_date=end_date)
    print(f"\n  回测交易日: {len(all_trade_days)} 天 ({bt_start_str} ~ {end_date})")

    # ---- 过滤：非ST、上市满2年 ----
    all_stocks = get_all_securities(types=['stock'], date=end_date)
    two_years_ago = (pd.to_datetime(end_date) - timedelta(days=365*2)).date()
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    valid_codes = [c for c in valid.index
                   if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]
    print(f"  非ST且上市>2年: {len(valid_codes)} 只")

    # ---- 市值初筛（粗筛，减少后续计算量）----
    trade_days_list = get_trade_days(end_date=end_date, count=5)
    last_trade = str(trade_days_list[-1])

    q = query(
        valuation.code,
        valuation.market_cap
    ).filter(
        valuation.code.in_(valid_codes),
        valuation.market_cap >= min_cap / 1e8 * 0.5  # 放宽50%（历史市值可能更大）
    )
    cap_df = get_fundamentals(q, date=last_trade)
    candidate_codes = list(cap_df['code'])
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    print(f"  市值初筛(≥{min_cap/1e8*0.5:.0f}亿): {len(candidate_codes)} 只")

    # ---- 逐只计算：哪些天在池子里 ----
    print(f"\n  开始拉取日K线并计算入池日期 ({data_start_str} ~ {end_date})...")
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

                # 根据市值确定回撤要求
                ratio = MAX_PRICE_RATIO_DEFAULT
                for tier_cap, tier_ratio in tiers:
                    if cap * 1e8 >= tier_cap:
                        ratio = tier_ratio
                        break

                # 滚动计算：每个交易日，看「当前价 < 过去250天最高价 × ratio」是否成立
                valid_dates = set()
                close_arr = closes.values
                high_arr = highs.values
                dates_arr = closes.index

                for j in range(250, len(close_arr)):
                    year_high = high_arr[j-250:j].max()  # 过去250天（约1年）最高价
                    current = close_arr[j]                # 当天收盘价
                    d = str(dates_arr[j].date())

                    if d < bt_start_str:  # 只看回测区间
                        continue

                    if current < year_high * ratio:       # 满足深跌条件 → 入池
                        valid_dates.add(d)

                if valid_dates:
                    pool_calendar[code] = valid_dates
                    stock_info[code] = {'name': name, 'market_cap': cap}
                    total_pool_days += len(valid_dates)

            except Exception:
                continue

        done = min(i+50, len(candidate_codes))
        print(f"    已处理 {done}/{len(candidate_codes)} ({done/len(candidate_codes)*100:.0f}%)")

    # ---- 结果统计 ----
    print(f"\n  {'─'*40}")
    print(f"  ✅ 股票池构建完成!")
    print(f"  入池股票总数: {len(pool_calendar)} 只")
    print(f"  总「池日数」: {total_pool_days} 天（所有股票的入池天数之和）")
    print(f"  平均每只在池子里: {total_pool_days/max(len(pool_calendar),1):.0f} 天")
    print(f"  {'─'*40}")

    if pool_calendar:
        sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:20]
        print(f"\n  📋 入池天数最多的前20只（在池子里时间越长 = 跌得越久）:")
        print(f"  {'股票名':>10s}  {'代码':>14s}  {'入池天数':>6s}  {'市值(亿)':>8s}")
        print(f"  {'─'*48}")
        for code, dates in sorted_stocks:
            info = stock_info[code]
            print(f"  {info['name']:>10s}  {code:>14s}  {len(dates):>6d}天  {info['market_cap']:>8.0f}")

    return pool_calendar, stock_info

pool_calendar, stock_info = build_rolling_pool(END_DATE)
print(f"\n✅ Cell 2 完成")


# ============================================================
# Cell 3：核心回测引擎
# ============================================================
#
# 【做什么】对单只股票，用指定的参数组合进行回测：
#   1. 拉取分钟K线
#   2. 在该股票「在池子里」的日期，逐根K线扫描
#   3. 如果某根K线涨幅突破阈值 → 记录为一次信号（以该K线收盘价买入）
#   4. 计算次日卖出价和收益率
#
# 【阈值怎么算】
#   A类「突破最大值」: 阈值 = 过去N根K线涨幅的最大值
#   B类「倍数突破」  : 阈值 = 过去N根K线涨幅的 均值(或中位数) × 倍数

def get_lookback_bars(freq, period):
    """计算回看需要多少根K线
    
    例如：15分钟K线 + 回看3个月 = 16根/天 × 63天 = 1008根
    """
    bars_per_day = {'1m': 240, '5m': 48, '15m': 16, '30m': 8}
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def backtest_single_rolling(stock_code, stock_name, valid_dates,
                            freq, signal_type, period,
                            multiplier=None, stat_method=None,
                            end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    单只股票回测
    
    参数:
        stock_code:  股票代码，如 '600519.XSHG'
        stock_name:  股票名称，如 '贵州茅台'
        valid_dates: set(日期字符串) 该股票在池子里的日期
        freq:        K线周期 '1m'/'5m'/'15m'/'30m'
        signal_type: 触发类型 'max_break'(突破最大值) / 'mult_break'(倍数突破)
        period:      回看周期 '3m'(3个月) / '1y'(1年)
        multiplier:  B类策略的倍数（A类不需要）
        stat_method:  B类策略的统计方法 'mean'(均值) / 'median'(中位数)
    
    返回:
        (信号列表, 统计摘要) 或 None（无信号时）
    """
    lookback_days_map = {'3m': 63, '1y': 250}

    # 数据起始日：最早入池日期 - 回看期 - 缓冲
    earliest = min(valid_dates)
    data_start = (pd.to_datetime(earliest) - timedelta(days=lookback_days_map[period] * 2)).strftime('%Y-%m-%d')

    # 拉取日K线（用于计算涨停价和次日卖出价）
    daily = get_price(stock_code, start_date=data_start, end_date=end_date,
                      frequency='daily', fields=['open', 'high', 'low', 'close', 'pre_close'])
    if daily is None or len(daily) < lookback_days_map[period] + 30:
        return None
    daily['date_str'] = daily.index.strftime('%Y-%m-%d')

    # 拉取分钟K线
    min_df = get_price(stock_code, start_date=data_start, end_date=end_date,
                       frequency=freq, fields=['open', 'close', 'high', 'low', 'volume'])
    if min_df is None or len(min_df) < get_lookback_bars(freq, period):
        return None

    # 计算每根K线的涨幅（收盘价/开盘价 - 1）
    min_df['bar_return'] = (min_df['close'] / min_df['open'] - 1)
    min_df['date'] = min_df.index.date

    lookback_bars = get_lookback_bars(freq, period)

    # 只扫描「在池子里」的日期
    trade_dates = sorted([d for d in min_df['date'].unique() if str(d) in valid_dates])

    signals = []
    last_signal_date_idx = -999  # 上次触发信号的索引（用于冷却判断）

    for i, date in enumerate(trade_dates):
        date_str = str(date)

        # 冷却期内 → 跳过
        if i - last_signal_date_idx <= cooldown:
            continue

        # 当天的所有K线
        day_bars = min_df[min_df['date'] == date]
        if len(day_bars) < 5:
            continue

        # 当天之前的历史K线（不含当天）
        hist = min_df[min_df['date'] < date]
        if len(hist) < lookback_bars:
            continue

        # 取最近 lookback_bars 根K线的涨幅，用于计算阈值
        hist_returns = hist['bar_return'].iloc[-lookback_bars:]

        # ---- 计算触发阈值 ----
        if signal_type == 'max_break':
            # A类：阈值 = 历史K线涨幅的最大值
            threshold = hist_returns.max()
        elif signal_type == 'mult_break':
            # B类：阈值 = 正涨幅的均值/中位数 × 倍数
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

        # ---- 逐根K线扫描：有没有突破阈值 ----
        triggered = False
        for idx in range(len(day_bars)):
            bar = day_bars.iloc[idx]
            bar_ret = bar['bar_return']
            if not pd.isna(bar_ret) and bar_ret > threshold:
                triggered = True
                trigger_bar = day_bars.index[idx]   # 触发时间
                trigger_price = bar['close']         # 买入价 = 该K线收盘价
                trigger_return = bar_ret             # 该K线涨幅
                break  # 当天只取第一个信号

        if not triggered:
            continue

        # ---- 计算次日卖出价 ----
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

        # 计算涨停价（创业板/科创板20%，主板10%）
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_pct = 0.20
        else:
            limit_pct = 0.10
        limit_price = round(today_close * (1 + limit_pct), 2)

        # 次日最高价碰到涨停 → 以涨停价卖出；否则 → 以收盘价卖出
        if next_high >= limit_price:
            sell_price = limit_price
            sell_type = '涨停卖出'
        else:
            sell_price = next_close
            sell_type = '次日收盘卖'

        # 扣除交易成本后的收益率
        cost = trigger_price * BUY_FEE + sell_price * (SELL_FEE + STAMP_TAX)
        ret = (sell_price - trigger_price - cost) / trigger_price * 100

        last_signal_date_idx = i

        signals.append({
            '日期': date_str,
            '触发时间': str(trigger_bar),
            '买入价': round(trigger_price, 3),
            'K线涨幅%': round(trigger_return * 100, 3),
            '阈值%': round(threshold * 100, 3),
            '卖出价': round(sell_price, 3),
            '卖出方式': sell_type,
            '收益率%': round(ret, 2),
            # 保留英文key用于内部计算
            'date': date_str,
            'trigger_price': round(trigger_price, 3),
            'sell_price': round(sell_price, 3),
            'sell_type': sell_type,
            'return_pct': round(ret, 2),
        })

    if not signals:
        return None

    # ---- 统计摘要 ----
    df = pd.DataFrame(signals)
    total = len(df)
    wins = len(df[df['return_pct'] > 0])
    wr = wins / total * 100 if total > 0 else 0
    avg_ret = df['return_pct'].mean()
    avg_win = df[df['return_pct'] > 0]['return_pct'].mean() if wins > 0 else 0
    avg_loss = df[df['return_pct'] <= 0]['return_pct'].mean() if total - wins > 0 else 0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    limit_sells = len(df[df['sell_type'] == '涨停卖出'])

    summary = {
        '股票': stock_name,
        '代码': stock_code,
        '入池天数': len(valid_dates),
        '信号数': total,
        '盈利次数': wins,
        '胜率%': round(wr, 1),
        '平均收益%': round(avg_ret, 2),
        '平均盈利%': round(avg_win, 2),
        '平均亏损%': round(avg_loss, 2),
        '盈亏比': round(pl_ratio, 2),
        '涨停卖出次数': limit_sells,
        # 保留英文key用于内部
        'stock': stock_name, 'code': stock_code,
        'pool_days': len(valid_dates), 'signals': total, 'wins': wins,
        'win_rate': round(wr, 1), 'avg_return': round(avg_ret, 2),
        'avg_win': round(avg_win, 2), 'avg_loss': round(avg_loss, 2),
        'pl_ratio': round(pl_ratio, 2), 'limit_sells': limit_sells,
    }

    return signals, summary


def run_grid_search(pool_calendar, stock_info, max_stocks=30):
    """
    网格搜索：对每只股票 × 每种参数组合 进行回测，找出最优策略
    """
    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:max_stocks]

    print(f"\n{'='*60}")
    print(f"📊 第二步：网格搜索最优参数")
    print(f"{'='*60}")
    print(f"  测试股票数: {len(sorted_stocks)} 只（按入池天数从多到少）")

    # 构建参数网格
    param_grid = []
    for freq in FREQ_LIST:
        for period in LOOKBACK_PERIODS:
            # A类：突破最大值
            param_grid.append({
                'freq': freq, 'signal_type': 'max_break',
                'period': period, 'multiplier': None, 'stat_method': None,
                'label': f"{freq}|突破最大值|回看{period}"
            })
            # B类：倍数突破
            for stat in STAT_METHODS:
                for mult in MULT_LIST:
                    stat_cn = '均值' if stat == 'mean' else '中位数'
                    param_grid.append({
                        'freq': freq, 'signal_type': 'mult_break',
                        'period': period, 'multiplier': mult, 'stat_method': stat,
                        'label': f"{freq}|{stat_cn}×{mult}|回看{period}"
                    })

    print(f"  参数组合数: {len(param_grid)} 种")
    print(f"  总回测任务: {len(param_grid)} × {len(sorted_stocks)} = {len(param_grid)*len(sorted_stocks)} 次")
    print(f"\n  开始回测...\n")

    results = []

    for si, (code, valid_dates) in enumerate(sorted_stocks):
        info = stock_info[code]
        name = info['name']
        print(f"  [{si+1}/{len(sorted_stocks)}] {name} ({code}) — 在池子里{len(valid_dates)}天, 市值{info['market_cap']:.0f}亿")

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


# ============================================================
# Cell 4：运行网格搜索
# ============================================================
# 
# ★ 修改 MAX_STOCKS 控制测试范围：
#   - 15  → 快速验证（几分钟）
#   - 100 → 中等规模（可能半小时）
#   - 453 → 全量（可能几小时，看聚宽性能）

MAX_STOCKS = 15  # ← 先用15只快速验证，确认没问题后改成 453

all_results = run_grid_search(pool_calendar, stock_info, max_stocks=MAX_STOCKS)

print(f"\n{'='*60}")
print(f"✅ 网格搜索完成! 共 {len(all_results)} 条回测结果")
print(f"{'='*60}")


# ============================================================
# Cell 5：汇总分析 - 策略排行榜
# ============================================================

if not all_results:
    print("❌ 无结果，请检查股票池或参数")
else:
    rdf = pd.DataFrame(all_results)

    # ---- 按参数组合汇总 ----
    grouped = rdf.groupby('label')
    agg = pd.DataFrame()
    agg['有信号的股票数'] = grouped['stock'].nunique()
    agg['总信号数'] = grouped['signals'].sum()
    agg['总盈利次数'] = grouped['wins'].sum()
    agg['平均收益%'] = grouped['avg_return'].mean()
    agg['平均胜率%'] = grouped['win_rate'].mean()
    agg['平均盈亏比'] = grouped['pl_ratio'].apply(
        lambda x: x[x < float('inf')].mean() if len(x[x < float('inf')]) > 0 else 0)
    agg['涨停卖出总次数'] = grouped['limit_sells'].sum()
    agg = agg.reset_index()

    agg['整体胜率%'] = (agg['总盈利次数'] / agg['总信号数'] * 100).round(1)
    agg['涨停卖出占比%'] = (agg['涨停卖出总次数'] / agg['总信号数'] * 100).round(1)

    # 综合评分（加权：胜率40% + 收益30% + 盈亏比20% + 信号量10%）
    agg['信号量得分'] = agg['总信号数'].apply(
        lambda x: min(x / 20, 1.0) * 100 if x >= 5 else 0
    )
    agg['综合评分'] = (
        agg['整体胜率%'] * 0.4 +
        agg['平均收益%'].clip(-10, 10) * 5 * 0.3 +
        agg['平均盈亏比'].clip(0, 5) * 20 * 0.2 +
        agg['信号量得分'] * 0.1
    ).round(1)

    valid_agg = agg[agg['总信号数'] >= 5].sort_values('综合评分', ascending=False)

    # ---- 排行榜 ----
    print(f"\n{'='*60}")
    print(f"📊 策略排行榜 TOP 20（至少5个信号才入榜）")
    print(f"{'='*60}")
    print(f"  评分规则：胜率×40% + 收益×30% + 盈亏比×20% + 信号量×10%")
    print()

    display_cols = ['label', '有信号的股票数', '总信号数', '整体胜率%',
                    '平均收益%', '平均盈亏比', '涨停卖出占比%', '综合评分']
    print(valid_agg[display_cols].head(20).to_string(index=False))

    # ---- 按K线周期汇总 ----
    freq_cn = {'1m': '1分钟', '5m': '5分钟', '15m': '15分钟', '30m': '30分钟'}
    print(f"\n{'='*60}")
    print("📈 按K线周期汇总（哪个周期整体表现好？）")
    print(f"{'='*60}")
    for freq in FREQ_LIST:
        fdf = rdf[rdf['freq'] == freq]
        if fdf.empty:
            continue
        ts = fdf['signals'].sum()
        tw = fdf['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = fdf['avg_return'].mean()
        print(f"  {freq_cn[freq]:>5s}: 信号{ts:>5d}个, 胜率{wr:.1f}%, 平均收益{ar:+.2f}%")

    # ---- 按触发类型汇总 ----
    print(f"\n{'='*60}")
    print("📈 按触发类型汇总（A类突破最大值 vs B类倍数突破）")
    print(f"{'='*60}")
    for st in SIGNAL_TYPES:
        sdf = rdf[rdf['signal_type'] == st]
        if sdf.empty:
            continue
        ts = sdf['signals'].sum()
        tw = sdf['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = sdf['avg_return'].mean()
        label = "A类 突破最大值" if st == 'max_break' else "B类 倍数突破"
        print(f"  {label}: 信号{ts:>5d}个, 胜率{wr:.1f}%, 平均收益{ar:+.2f}%")

    # ---- 按回看周期汇总 ----
    period_cn = {'3m': '过去3个月', '1y': '过去1年'}
    print(f"\n{'='*60}")
    print("📈 按回看周期汇总（看多远的历史来算阈值？）")
    print(f"{'='*60}")
    for p in LOOKBACK_PERIODS:
        pdf_sub = rdf[rdf['period'] == p]
        if pdf_sub.empty:
            continue
        ts = pdf_sub['signals'].sum()
        tw = pdf_sub['wins'].sum()
        wr = tw / ts * 100 if ts > 0 else 0
        ar = pdf_sub['avg_return'].mean()
        print(f"  {period_cn[p]}: 信号{ts:>5d}个, 胜率{wr:.1f}%, 平均收益{ar:+.2f}%")

    # ---- B类策略细分 ----
    if 'mult_break' in rdf['signal_type'].values:
        print(f"\n{'='*60}")
        print("📈 B类策略细分（均值/中位数 × 不同倍数）")
        print(f"{'='*60}")
        bdf = rdf[rdf['signal_type'] == 'mult_break']
        for stat in STAT_METHODS:
            stat_cn = '均值' if stat == 'mean' else '中位数'
            for mult in MULT_LIST:
                sub = bdf[(bdf['stat_method'] == stat) & (bdf['multiplier'] == mult)]
                if sub.empty:
                    continue
                ts = sub['signals'].sum()
                tw = sub['wins'].sum()
                wr = tw / ts * 100 if ts > 0 else 0
                ar = sub['avg_return'].mean()
                print(f"  {stat_cn}×{mult}: 信号{ts:>5d}个, 胜率{wr:.1f}%, 平均收益{ar:+.2f}%")

    # ---- 最佳策略 ----
    if len(valid_agg) > 0:
        best = valid_agg.iloc[0]
        print(f"\n{'='*60}")
        print(f"🏆 最佳策略: {best['label']}")
        print(f"{'='*60}")
        print(f"  总信号数:     {best['总信号数']:.0f} 个")
        print(f"  整体胜率:     {best['整体胜率%']:.1f}%")
        print(f"  平均收益:     {best['平均收益%']:.2f}%")
        print(f"  平均盈亏比:   {best['平均盈亏比']:.2f}")
        print(f"  涨停卖出占比: {best['涨停卖出占比%']:.1f}%")
        print(f"  综合评分:     {best['综合评分']}")
        print()
        print(f"  💡 解读：")
        if best['平均收益%'] > 0:
            print(f"     平均每次交易赚 {best['平均收益%']:.2f}%，策略有正期望")
        else:
            print(f"     平均每次交易亏 {abs(best['平均收益%']):.2f}%，策略暂无正期望")
        if best['整体胜率%'] > 50:
            print(f"     胜率超过50%（{best['整体胜率%']:.1f}%），赢多输少")
        else:
            print(f"     胜率不到50%（{best['整体胜率%']:.1f}%），需要靠大盈利弥补")
        if best['平均盈亏比'] > 1.5:
            print(f"     盈亏比{best['平均盈亏比']:.1f}（赚的时候赚得比亏的多），风险回报合理")
        else:
            print(f"     盈亏比{best['平均盈亏比']:.1f}，赚亏差不多，需要高胜率才能盈利")


# ============================================================
# Cell 6：查看最佳策略的个股详情
# ============================================================

BEST_LABEL = valid_agg.iloc[0]['label'] if len(valid_agg) > 0 else None

if BEST_LABEL:
    best_results = rdf[rdf['label'] == BEST_LABEL].sort_values('avg_return', ascending=False)
    print(f"\n{'='*60}")
    print(f"🏆 最佳策略 [{BEST_LABEL}] 各股票的表现:")
    print(f"{'='*60}")
    print(f"  （按平均收益从高到低排列）\n")
    
    display_df = best_results[['股票', '代码', '入池天数', '信号数', '胜率%', 
                                '平均收益%', '平均盈利%', '平均亏损%', '盈亏比', '涨停卖出次数']]
    print(display_df.to_string(index=False))
    
    # 补充说明
    profitable = best_results[best_results['avg_return'] > 0]
    losing = best_results[best_results['avg_return'] <= 0]
    print(f"\n  📊 统计：{len(profitable)}只股票平均赚钱，{len(losing)}只股票平均亏钱")


# ============================================================
# Cell 7：资金曲线模拟
# ============================================================
"""
【做什么】
用最佳策略，模拟一个真实账户的交易过程：
  - 5万初始资金
  - 信号来了 → 计算能买多少股（不超过1万元，100股整数倍）
  - 手续费固定每次买卖各5元
  - 记录每一笔交易，计算资金变化

【风控规则】
  - 当天亏超1000元 → 当天停止交易
  - 连续亏10笔 → 暂停策略
  - 总共亏2万 → 永久停止

【输出】
  - 最终资金、总收益、最大回撤
  - 每笔交易明细
  - 资金曲线
"""

# ---- 资金模拟参数 ----
INIT_CAPITAL = 50000        # 初始资金5万
MAX_PER_TRADE = 10000       # 单笔最大买入1万
COMMISSION_PER_SIDE = 5     # 手续费：买卖各5元
MAX_POSITIONS = 5           # 同时最多持5只
DAILY_LOSS_LIMIT = -1000    # 当天亏超1000元停止
MAX_CONSECUTIVE_LOSS = 10   # 连亏10次暂停
TOTAL_LOSS_LIMIT = -20000   # 总亏2万停止

# ---- 选择要模拟的策略 ----
SIM_LABEL = BEST_LABEL  # 用排行榜第一的策略（也可以手动指定，如 '15m|突破最大值|回看1y'）

print(f"\n{'='*60}")
print(f"💰 第三步：资金曲线模拟")
print(f"{'='*60}")
print(f"  使用策略: {SIM_LABEL}")
print(f"  初始资金: {INIT_CAPITAL:,.0f}元")
print(f"  单笔上限: {MAX_PER_TRADE:,.0f}元（买不起1手就跳过）")
print(f"  手续费:   买入{COMMISSION_PER_SIDE}元 + 卖出{COMMISSION_PER_SIDE}元 = 每笔{COMMISSION_PER_SIDE*2}元")
print(f"  最大持仓: 同时{MAX_POSITIONS}只")
print(f"  风控:     日亏{abs(DAILY_LOSS_LIMIT)}元停 / 连亏{MAX_CONSECUTIVE_LOSS}次停 / 总亏{abs(TOTAL_LOSS_LIMIT)}元停")

# 解析策略参数（从中文label还原）
# label格式: "5m|突破最大值|回看3m" 或 "15m|均值×2.0|回看1y"
parts = SIM_LABEL.split('|')
sim_freq = parts[0]
trigger_part = parts[1]
period_part = parts[2].replace('回看', '')

if '突破最大值' in trigger_part:
    sim_signal_type = 'max_break'
    sim_multiplier = None
    sim_stat_method = None
else:
    sim_signal_type = 'mult_break'
    if '均值' in trigger_part:
        sim_stat_method = 'mean'
    else:
        sim_stat_method = 'median'
    sim_multiplier = float(trigger_part.split('×')[1])

sim_period = period_part

# ---- 收集所有信号 ----
print(f"\n  收集全部股票的交易信号...")

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
        print(f"    已处理 {si+1}/{len(sorted_stocks)}")

print(f"  总信号数: {len(all_trades)} 个")

if not all_trades:
    print("\n  ❌ 无信号，无法模拟。请检查策略参数或扩大股票范围。")
else:
    # 按日期排序（模拟真实的时间顺序）
    trades_df = pd.DataFrame(all_trades).sort_values('date').reset_index(drop=True)

    # ---- 逐笔模拟 ----
    capital = INIT_CAPITAL
    peak_capital = INIT_CAPITAL
    max_drawdown = 0
    max_drawdown_pct = 0
    equity_curve = []
    trade_log = []
    consecutive_losses = 0
    strategy_paused = False
    pause_reason = None
    daily_pnl = {}

    for _, trade in trades_df.iterrows():
        buy_date = trade['date']
        code = trade['code']
        buy_price = trade['trigger_price']
        sell_price = trade['sell_price']

        # 风控检查
        if strategy_paused:
            continue

        # 计算能买多少股
        max_afford = min(MAX_PER_TRADE, capital - COMMISSION_PER_SIDE)
        if max_afford < buy_price * 100:  # 买不起1手(100股)
            continue
        shares = int(max_afford / buy_price / 100) * 100
        if shares <= 0:
            continue

        # 计算盈亏
        buy_cost = shares * buy_price + COMMISSION_PER_SIDE       # 买入总花费
        sell_revenue = shares * sell_price - COMMISSION_PER_SIDE   # 卖出总收入
        pnl = sell_revenue - buy_cost                              # 本笔盈亏

        # 更新资金
        capital = capital - buy_cost + sell_revenue

        # 记录交易
        trade_log.append({
            '买入日期': buy_date,
            '股票': trade['stock'],
            '代码': code,
            '买入股数': shares,
            '买入价': buy_price,
            '卖出价': sell_price,
            '卖出方式': trade['sell_type'],
            '盈亏(元)': round(pnl, 2),
            '盈亏率%': round(pnl / buy_cost * 100, 2),
            '账户余额': round(capital, 2),
        })

        # 更新当日盈亏
        daily_pnl[buy_date] = daily_pnl.get(buy_date, 0) + pnl

        # 更新峰值和最大回撤
        if capital > peak_capital:
            peak_capital = capital
        dd = capital - peak_capital
        dd_pct = dd / peak_capital * 100 if peak_capital > 0 else 0
        if dd < max_drawdown:
            max_drawdown = dd
            max_drawdown_pct = dd_pct

        equity_curve.append((buy_date, round(capital, 2)))

        # ---- 风控 ----
        # 1) 当天亏太多
        if daily_pnl.get(buy_date, 0) <= DAILY_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"当天亏损{daily_pnl[buy_date]:.0f}元，超过日亏上限{abs(DAILY_LOSS_LIMIT)}元"
            break

        # 2) 连续亏损
        if pnl <= 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
        if consecutive_losses >= MAX_CONSECUTIVE_LOSS:
            strategy_paused = True
            pause_reason = f"连续亏损{consecutive_losses}笔，触发暂停"
            break

        # 3) 总亏损
        total_pnl_check = capital - INIT_CAPITAL
        if total_pnl_check <= TOTAL_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"总亏损{total_pnl_check:.0f}元，超过上限{abs(TOTAL_LOSS_LIMIT)}元"
            break

    # ---- 输出模拟结果 ----
    total_pnl = capital - INIT_CAPITAL
    total_trades = len(trade_log)
    win_trades = len([t for t in trade_log if t['盈亏(元)'] > 0])
    lose_trades = total_trades - win_trades

    print(f"\n{'='*60}")
    print(f"💰 资金模拟结果")
    print(f"{'='*60}")
    print(f"  初始资金:     {INIT_CAPITAL:>10,.0f} 元")
    print(f"  最终资金:     {capital:>10,.2f} 元")
    print(f"  总盈亏:       {total_pnl:>+10,.2f} 元 ({total_pnl/INIT_CAPITAL*100:+.1f}%)")
    print(f"  资金峰值:     {peak_capital:>10,.2f} 元（过程中最高点）")
    print(f"  最大回撤:     {max_drawdown:>10,.2f} 元 ({max_drawdown_pct:.1f}%)（从峰值到谷底的最大跌幅）")
    print(f"  {'─'*40}")
    print(f"  总交易笔数:   {total_trades}")
    if total_trades > 0:
        print(f"  盈利笔数:     {win_trades} ({win_trades/total_trades*100:.1f}%)")
        print(f"  亏损笔数:     {lose_trades} ({lose_trades/total_trades*100:.1f}%)")
        pnls = [t['盈亏(元)'] for t in trade_log]
        print(f"  单笔最大盈利: {max(pnls):>+10,.2f} 元")
        print(f"  单笔最大亏损: {min(pnls):>+10,.2f} 元")
        print(f"  平均每笔盈亏: {sum(pnls)/len(pnls):>+10,.2f} 元")

    if strategy_paused:
        print(f"\n  ⚠️ 风控触发，策略已暂停！")
        print(f"     原因: {pause_reason}")
        print(f"     暂停时已完成 {total_trades} 笔交易")

    # ---- 交易明细 ----
    if trade_log:
        tlog = pd.DataFrame(trade_log)
        print(f"\n{'='*60}")
        print(f"📋 交易明细（共{len(tlog)}笔）")
        print(f"{'='*60}")
        if len(tlog) <= 30:
            print(tlog.to_string(index=False))
        else:
            print(f"\n  前20笔:")
            print(tlog.head(20).to_string(index=False))
            print(f"\n  ...中间省略 {len(tlog)-30} 笔...")
            print(f"\n  后10笔:")
            print(tlog.tail(10).to_string(index=False))

    # ---- 资金曲线（按日）----
    if equity_curve:
        eq_df = pd.DataFrame(equity_curve, columns=['日期', '账户资金'])
        eq_daily = eq_df.groupby('日期')['账户资金'].last().reset_index()
        print(f"\n{'='*60}")
        print(f"📈 资金曲线（每天收盘后的账户余额）")
        print(f"{'='*60}")
        if len(eq_daily) <= 50:
            print(eq_daily.to_string(index=False))
        else:
            print(f"  前10天:")
            print(eq_daily.head(10).to_string(index=False))
            print(f"\n  ...中间省略...")
            print(f"\n  后10天:")
            print(eq_daily.tail(10).to_string(index=False))
        
        print(f"\n  📊 资金曲线摘要:")
        print(f"     起始: {eq_daily['账户资金'].iloc[0]:,.2f}元 ({eq_daily['日期'].iloc[0]})")
        print(f"     结束: {eq_daily['账户资金'].iloc[-1]:,.2f}元 ({eq_daily['日期'].iloc[-1]})")
        print(f"     最高: {eq_daily['账户资金'].max():,.2f}元")
        print(f"     最低: {eq_daily['账户资金'].min():,.2f}元")
