"""
波动率突破策略 v3 - 聚宽研究环境
改进：信号分级（1/2/3级）控制仓位（1份/1.5份/2份）
规则：
  波动率：分别算3年/1年/3月，各自独立判断是否突破
  信号强度：突破1个=普通(1份)，2个=强(1.5份)，3个=最强(2份)
  买入：触发价买入，仓位按信号强度调整
  卖出：次日最高价触涨停→涨停价卖；否则次日收盘价卖
使用：聚宽 → 我的策略 → 研究环境 → 新建Notebook → 粘贴运行
"""

# ============ Cell 1：导入和配置 ============

from jqdata import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

STOCKS = [
    '000898.XSHE',  # 鞍钢股份
    '601390.XSHG',  # 中国中铁
    '688126.XSHG',  # 沪硅产业
    '603690.XSHG',  # 至纯科技
    '603195.XSHG',  # 公牛集团
    '000002.XSHE',  # 万科A
    '603260.XSHG',  # 合盛硅业
    '300014.XSHE',  # 亿纬锂能
]

MULTIPLIER = 2.0       # 波动率倍数阈值
END_DATE = '2026-07-25'

# 交易成本
BUY_FEE = 0.00015
SELL_FEE = 0.00015
STAMP_TAX = 0.001

# 信号去重冷却
COOLDOWN_DAYS = 2

# 信号强度 → 仓位倍数
POSITION_MULTIPLIER = {1: 1.0, 2: 1.5, 3: 2.0}

print("配置完成，运行下一个Cell")


# ============ Cell 2：核心回测函数 ============

def backtest_stock(stock_code, multiplier=2.0, end_date='2026-07-25', cooldown=2):
    """单只股票波动率突破回测（信号分级版）"""
    stock_name = get_security_info(stock_code).display_name

    # 获取4年日K线算波动率
    start_daily = (pd.to_datetime(end_date) - timedelta(days=365*4)).strftime('%Y-%m-%d')
    daily = get_price(stock_code, start_date=start_daily, end_date=end_date,
                      frequency='daily', fields=['close', 'high', 'low', 'open', 'pre_close'])

    if len(daily) < 800:
        print(f"  {stock_name}: 日K数据不足({len(daily)}条)，跳过")
        return None

    daily['daily_return'] = daily['close'].pct_change()
    # 三个周期波动率分开存，不取max
    daily['hv_3y'] = daily['daily_return'].rolling(750).std()
    daily['hv_1y'] = daily['daily_return'].rolling(250).std()
    daily['hv_3m'] = daily['daily_return'].rolling(60).std()

    daily['date_str'] = daily.index.strftime('%Y-%m-%d')

    # 获取最近1年分钟K线
    start_min = (pd.to_datetime(end_date) - timedelta(days=365)).strftime('%Y-%m-%d')
    print(f"  {stock_name}: 获取分钟K线...")
    min_df = get_price(stock_code, start_date=start_min, end_date=end_date,
                       frequency='1m', fields=['close', 'open', 'high', 'low', 'volume'])

    if min_df is None or len(min_df) == 0:
        print(f"  {stock_name}: 无分钟数据")
        return None

    print(f"  {stock_name}: {len(min_df)} 条分钟K线")
    min_df['date'] = min_df.index.date

    # 逐日扫描信号
    signals = []
    dates = sorted(min_df['date'].unique())
    last_signal_daily_idx = -999

    for date in dates:
        date_str = str(date)

        mask = daily['date_str'] == date_str
        if not mask.any():
            continue
        daily_idx = daily.index.get_loc(daily.index[mask][0])
        if daily_idx < 1:
            continue

        # 冷却期
        if daily_idx - last_signal_daily_idx <= cooldown:
            continue

        # 取前一日的三个波动率
        prev = daily.iloc[daily_idx - 1]
        hv_3y = prev['hv_3y']
        hv_1y = prev['hv_1y']
        hv_3m = prev['hv_3m']

        # 三个阈值
        thresholds = {}
        if not pd.isna(hv_3y) and hv_3y > 0:
            thresholds['3y'] = hv_3y * multiplier
        if not pd.isna(hv_1y) and hv_1y > 0:
            thresholds['1y'] = hv_1y * multiplier
        if not pd.isna(hv_3m) and hv_3m > 0:
            thresholds['3m'] = hv_3m * multiplier

        if not thresholds:
            continue

        # 当天分钟数据
        day = min_df[min_df['date'] == date].copy()
        if len(day) < 10:
            continue

        open_price = day.iloc[0]['open']
        day['min_return'] = day['close'].pct_change()
        day['cum_return'] = (day['close'] - open_price) / open_price

        # 对每个周期分别检测是否突破
        # 用累计涨幅（从开盘到当前）和单分钟涨幅，任一超过该周期阈值即算突破
        breached = {}  # {'3y': first_trigger_idx, '1y': ..., '3m': ...}

        for period, thresh in thresholds.items():
            # 单分钟突破
            trig_1m = day[day['min_return'] > thresh]
            # 累计突破
            trig_cum = day[day['cum_return'] > thresh]

            first_time = None
            if len(trig_1m) > 0:
                first_time = trig_1m.index[0]
            if len(trig_cum) > 0:
                t = trig_cum.index[0]
                if first_time is None or t < first_time:
                    first_time = t

            if first_time is not None:
                breached[period] = first_time

        if not breached:
            continue

        # 信号强度 = 突破了几个周期
        signal_level = len(breached)
        pos_mult = POSITION_MULTIPLIER.get(signal_level, 1.0)

        # 触发时间取最早的那个
        first_trigger = min(breached.values())
        trigger_price = day.loc[first_trigger, 'close']

        # 哪些周期被突破
        breached_periods = sorted(breached.keys())

        # 次日数据
        if daily_idx + 1 >= len(daily):
            continue

        next_day = daily.iloc[daily_idx + 1]
        next_close = next_day['close']
        next_high = next_day['high']
        today_close = daily.iloc[daily_idx]['close']

        # 涨停价
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_pct = 0.20
        else:
            limit_pct = 0.10
        limit_price = round(today_close * (1 + limit_pct), 2)

        # 卖出
        if next_high >= limit_price:
            sell_price = limit_price
            sell_type = '涨停卖出'
        else:
            sell_price = next_close
            sell_type = '次日收盘'

        # 收益（含交易成本）
        cost = trigger_price * BUY_FEE + sell_price * (SELL_FEE + STAMP_TAX)
        raw_ret = (sell_price - trigger_price - cost) / trigger_price * 100
        # 加权收益 = 原始收益 × 仓位倍数（反映实际盈亏）
        weighted_ret = raw_ret * pos_mult

        last_signal_daily_idx = daily_idx

        signals.append({
            'date': date_str,
            'trigger_time': str(first_trigger),
            'trigger_price': round(trigger_price, 3),
            'sell_price': round(sell_price, 3),
            'sell_type': sell_type,
            'signal_level': signal_level,
            'breached': '+'.join(breached_periods),
            'position_mult': pos_mult,
            'raw_return_pct': round(raw_ret, 2),
            'weighted_return_pct': round(weighted_ret, 2),
            'limit_price': limit_price,
            'next_high': round(next_high, 3),
            'next_close': round(next_close, 3),
            'hv_3y_pct': round(hv_3y * 100, 3) if not pd.isna(hv_3y) else None,
            'hv_1y_pct': round(hv_1y * 100, 3) if not pd.isna(hv_1y) else None,
            'hv_3m_pct': round(hv_3m * 100, 3) if not pd.isna(hv_3m) else None,
        })

    if not signals:
        print(f"  {stock_name}: 无信号")
        return None

    df = pd.DataFrame(signals)

    # 统计
    total = len(df)
    wins = len(df[df['weighted_return_pct'] > 0])
    wr = wins / total * 100
    avg_raw = df['raw_return_pct'].mean()
    avg_weighted = df['weighted_return_pct'].mean()
    max_win = df['weighted_return_pct'].max()
    max_loss = df['weighted_return_pct'].min()
    avg_win = df[df['weighted_return_pct'] > 0]['weighted_return_pct'].mean() if wins > 0 else 0
    avg_loss = df[df['weighted_return_pct'] <= 0]['weighted_return_pct'].mean() if total - wins > 0 else 0
    pl = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    limit_sells = len(df[df['sell_type'] == '涨停卖出'])

    # 按信号级别统计
    level_stats = {}
    for lv in [1, 2, 3]:
        lv_df = df[df['signal_level'] == lv]
        if len(lv_df) > 0:
            lv_wins = len(lv_df[lv_df['raw_return_pct'] > 0])
            level_stats[lv] = {
                'count': len(lv_df),
                'win_rate': round(lv_wins / len(lv_df) * 100, 1),
                'avg_return': round(lv_df['raw_return_pct'].mean(), 2),
            }

    summary = {
        'stock_name': stock_name,
        'stock_code': stock_code,
        'total_signals': total,
        'win_count': wins,
        'win_rate': round(wr, 1),
        'avg_raw_return': round(avg_raw, 2),
        'avg_weighted_return': round(avg_weighted, 2),
        'max_win': round(max_win, 2),
        'max_loss': round(max_loss, 2),
        'profit_loss_ratio': round(pl, 2),
        'limit_sells': limit_sells,
        'level_stats': level_stats,
    }

    # 打印
    lv_str = ' | '.join([f"L{k}:{v['count']}次/{v['win_rate']}%胜/{v['avg_return']}%均收"
                         for k, v in sorted(level_stats.items())])
    print(f"  {stock_name}: {total}次信号, 胜率{wr:.1f}%, 原始均收{avg_raw:.2f}%, 加权均收{avg_weighted:.2f}%, 盈亏比{pl:.2f}")
    print(f"    信号分级: {lv_str}")

    return summary, df

print("函数定义完成，运行下一个Cell")


# ============ Cell 3：运行回测 ============

print("=" * 60)
print(f"波动率突破策略 v3（{MULTIPLIER}x, 信号分级, 含交易成本）")
print(f"仓位: L1=1份, L2=1.5份, L3=2份")
print(f"分钟K线区间: 最近1年")
print("=" * 60)

all_summaries = []
all_details = {}

for stock in STOCKS:
    print(f"\n处理 {stock}...")
    try:
        result = backtest_stock(stock, MULTIPLIER, END_DATE, COOLDOWN_DAYS)
        if result:
            summary, details = result
            all_summaries.append(summary)
            all_details[stock] = details
    except Exception as e:
        print(f"  错误: {e}")

print("\n" + "=" * 60)
print("回测总结")
print("=" * 60)

if all_summaries:
    sdf = pd.DataFrame(all_summaries)
    print(sdf[['stock_name', 'total_signals', 'win_rate', 'avg_raw_return',
               'avg_weighted_return', 'max_win', 'max_loss', 'profit_loss_ratio',
               'limit_sells']].to_string(index=False))

    total_sig = sdf['total_signals'].sum()
    total_wins = sdf['win_count'].sum()
    total_limit = sdf['limit_sells'].sum()
    print(f"\n汇总:")
    print(f"  总信号: {total_sig}, 总胜率: {total_wins/total_sig*100:.1f}%")
    print(f"  平均原始收益: {sdf['avg_raw_return'].mean():.2f}%")
    print(f"  平均加权收益: {sdf['avg_weighted_return'].mean():.2f}%")
    print(f"  平均盈亏比: {sdf['profit_loss_ratio'].mean():.2f}")
    print(f"  涨停卖出: {total_limit}次 ({total_limit/total_sig*100:.0f}%)")

    # 汇总各级别信号
    print(f"\n信号级别汇总:")
    for lv in [1, 2, 3]:
        counts = []
        rets = []
        wrs = []
        for s in all_summaries:
            if lv in s['level_stats']:
                ls = s['level_stats'][lv]
                counts.append(ls['count'])
                rets.append(ls['avg_return'])
                wrs.append(ls['win_rate'])
        if counts:
            print(f"  L{lv}({'⭐'*lv}): 共{sum(counts)}次, 平均胜率{np.mean(wrs):.1f}%, 平均收益{np.mean(rets):.2f}%")
        else:
            print(f"  L{lv}({'⭐'*lv}): 无信号")
else:
    print("无有效结果")


# ============ Cell 4（可选）：查看信号详情 ============

stock_code = '000898.XSHE'

if stock_code in all_details:
    d = all_details[stock_code]
    name = get_security_info(stock_code).display_name
    print(f"\n{name} 信号详情:")
    print(d[['date', 'signal_level', 'breached', 'position_mult', 'trigger_price',
             'sell_price', 'sell_type', 'raw_return_pct', 'weighted_return_pct']].to_string(index=False))
else:
    print(f"{stock_code} 无信号")


# ============ Cell 5（可选）：参数敏感性 ============

print("\n" + "=" * 60)
print("参数敏感性测试")
print("=" * 60)

for mult in [1.5, 2.0, 2.5, 3.0]:
    batch = []
    for stock in STOCKS:
        try:
            r = backtest_stock(stock, mult, END_DATE, COOLDOWN_DAYS)
            if r:
                batch.append(r[0])
        except:
            pass

    if batch:
        bdf = pd.DataFrame(batch)
        ts = bdf['total_signals'].sum()
        tw = bdf['win_count'].sum()
        tl = bdf['limit_sells'].sum()
        print(f"  {mult}x: 信号={ts}, 胜率={tw/ts*100:.1f}%, 原始均收={bdf['avg_raw_return'].mean():.2f}%, 加权均收={bdf['avg_weighted_return'].mean():.2f}%, 盈亏比={bdf['profit_loss_ratio'].mean():.2f}")
    else:
        print(f"  {mult}x: 无信号")
