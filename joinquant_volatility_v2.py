"""
波动率突破策略 v2 - 聚宽研究环境
规则：
  买入：盘中1分钟涨幅/累计涨幅突破波动率阈值 → 触发价买入
  卖出：次日最高价触涨停 → 涨停价卖；否则次日收盘价卖
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
END_DATE = '2026-07-25'  # 回测截止日期

# 交易成本
BUY_FEE = 0.00015      # 买入佣金万1.5
SELL_FEE = 0.00015     # 卖出佣金万1.5
STAMP_TAX = 0.001      # 卖出印花税千1
# 单次交易总成本约 0.00015 + 0.00015 + 0.001 = 0.13%（买入） + 0.115%（卖出）

# 信号去重：触发信号后N个交易日内不重复触发（因为次日就卖了，冷却2天够了）
COOLDOWN_DAYS = 2

print("配置完成，运行下一个Cell")


# ============ Cell 2：核心回测函数 ============

def backtest_stock(stock_code, multiplier=2.0, end_date='2026-07-25', cooldown=2):
    """单只股票波动率突破回测"""
    stock_name = get_security_info(stock_code).display_name
    
    # 获取4年日K线算波动率
    start_daily = (pd.to_datetime(end_date) - timedelta(days=365*4)).strftime('%Y-%m-%d')
    daily = get_price(stock_code, start_date=start_daily, end_date=end_date,
                      frequency='daily', fields=['close', 'high', 'low', 'open', 'pre_close'])
    
    if len(daily) < 800:
        print(f"  {stock_name}: 日K数据不足({len(daily)}条)，跳过")
        return None
    
    daily['daily_return'] = daily['close'].pct_change()
    daily['hv_3y'] = daily['daily_return'].rolling(750).std()
    daily['hv_1y'] = daily['daily_return'].rolling(250).std()
    daily['hv_3m'] = daily['daily_return'].rolling(60).std()
    daily['hv_max'] = daily[['hv_3y', 'hv_1y', 'hv_3m']].max(axis=1)
    
    # 日期索引转字符串方便查找
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
    last_signal_daily_idx = -999  # 信号去重用
    
    for date in dates:
        date_str = str(date)
        
        # 找前一日波动率
        mask = daily['date_str'] == date_str
        if not mask.any():
            continue
        daily_idx = daily.index.get_loc(daily.index[mask][0])
        if daily_idx < 1:
            continue
        
        # 冷却期检查：距上次信号不足cooldown个交易日则跳过
        if daily_idx - last_signal_daily_idx <= cooldown:
            continue
        prev_hv = daily.iloc[daily_idx - 1]['hv_max']
        if pd.isna(prev_hv) or prev_hv == 0:
            continue
        
        # 阈值（1分钟涨幅直接对比日波动率×倍数，不做√240折算）
        threshold_1m = prev_hv * multiplier
        threshold_cum = prev_hv * multiplier
        
        # 当天分钟数据
        day = min_df[min_df['date'] == date].copy()
        if len(day) < 10:
            continue
        
        open_price = day.iloc[0]['open']
        day['min_return'] = day['close'].pct_change()
        day['cum_return'] = (day['close'] - open_price) / open_price
        
        # 检测突破
        trig_1m = day[day['min_return'] > threshold_1m]
        trig_cum = day[day['cum_return'] > threshold_cum]
        
        triggered = False
        trigger_price = None
        trigger_time = None
        trigger_type = None
        
        if len(trig_1m) > 0:
            triggered = True
            trigger_time = trig_1m.index[0]
            trigger_price = trig_1m.iloc[0]['close']
            trigger_type = '1分钟'
        
        if len(trig_cum) > 0:
            t = trig_cum.index[0]
            if not triggered or t < trigger_time:
                triggered = True
                trigger_time = t
                trigger_price = trig_cum.iloc[0]['close']
                trigger_type = '累计'
        
        if not triggered:
            continue
        
        # 找次日数据
        if daily_idx + 1 >= len(daily):
            continue
        
        next_day = daily.iloc[daily_idx + 1]
        next_close = next_day['close']
        next_high = next_day['high']
        today_close = daily.iloc[daily_idx]['close']
        
        # 涨停价 = 前收盘 × 1.10（主板），科创/创业板 × 1.20
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_pct = 0.20
        else:
            limit_pct = 0.10
        limit_price = round(today_close * (1 + limit_pct), 2)
        
        # 卖出逻辑：次日最高触涨停→涨停价卖，否则收盘价卖
        if next_high >= limit_price:
            sell_price = limit_price
            sell_type = '涨停卖出'
        else:
            sell_price = next_close
            sell_type = '次日收盘'
        
        # 扣除交易成本
        cost = trigger_price * BUY_FEE + sell_price * (SELL_FEE + STAMP_TAX)
        ret = (sell_price - trigger_price - cost) / trigger_price * 100
        
        last_signal_daily_idx = daily_idx  # 更新冷却计时
        
        signals.append({
            'date': date_str,
            'trigger_time': str(trigger_time),
            'trigger_type': trigger_type,
            'trigger_price': round(trigger_price, 3),
            'sell_price': round(sell_price, 3),
            'sell_type': sell_type,
            'return_pct': round(ret, 2),
            'limit_price': limit_price,
            'next_high': round(next_high, 3),
            'next_close': round(next_close, 3),
            'hv_max_pct': round(prev_hv * 100, 3),
            'threshold_1m_pct': round(threshold_1m * 100, 4),
            'threshold_cum_pct': round(threshold_cum * 100, 3),
        })
    
    if not signals:
        print(f"  {stock_name}: 无信号")
        return None
    
    df = pd.DataFrame(signals)
    
    # 统计
    total = len(df)
    wins = len(df[df['return_pct'] > 0])
    wr = wins / total * 100
    avg_ret = df['return_pct'].mean()
    max_win = df['return_pct'].max()
    max_loss = df['return_pct'].min()
    avg_win = df[df['return_pct'] > 0]['return_pct'].mean() if wins > 0 else 0
    avg_loss = df[df['return_pct'] <= 0]['return_pct'].mean() if total - wins > 0 else 0
    pl = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    limit_sells = len(df[df['sell_type'] == '涨停卖出'])
    
    summary = {
        'stock_name': stock_name,
        'stock_code': stock_code,
        'total_signals': total,
        'win_count': wins,
        'win_rate': round(wr, 1),
        'avg_return': round(avg_ret, 2),
        'max_win': round(max_win, 2),
        'max_loss': round(max_loss, 2),
        'profit_loss_ratio': round(pl, 2),
        'limit_sells': limit_sells,
        'limit_sell_pct': round(limit_sells / total * 100, 1),
    }
    
    print(f"  {stock_name}: {total}次信号, 胜率{wr:.1f}%, 均收{avg_ret:.2f}%, 盈亏比{pl:.2f}, 涨停卖出{limit_sells}次({limit_sells/total*100:.0f}%)")
    
    return summary, df

print("函数定义完成，运行下一个Cell")


# ============ Cell 3：运行回测 ============

print("=" * 60)
print(f"波动率突破策略 v2（{MULTIPLIER}x阈值, 触发价买入, 次日涨停/收盘卖出, 含交易成本）")
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
    print(sdf[['stock_name', 'total_signals', 'win_rate', 'avg_return',
               'max_win', 'max_loss', 'profit_loss_ratio', 'limit_sells']].to_string(index=False))
    
    total_sig = sdf['total_signals'].sum()
    total_wins = sdf['win_count'].sum()
    total_limit = sdf['limit_sells'].sum()
    print(f"\n汇总:")
    print(f"  总信号: {total_sig}, 总胜率: {total_wins/total_sig*100:.1f}%")
    print(f"  平均收益: {sdf['avg_return'].mean():.2f}%")
    print(f"  平均盈亏比: {sdf['profit_loss_ratio'].mean():.2f}")
    print(f"  涨停卖出: {total_limit}次 ({total_limit/total_sig*100:.0f}%)")
else:
    print("无有效结果")


# ============ Cell 4（可选）：查看信号详情 ============

stock_code = '000898.XSHE'  # 改这里看不同股票

if stock_code in all_details:
    d = all_details[stock_code]
    name = get_security_info(stock_code).display_name
    print(f"\n{name} 信号详情:")
    print(d[['date', 'trigger_type', 'trigger_price', 'sell_price', 'sell_type',
             'return_pct', 'next_high', 'limit_price']].to_string(index=False))
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
        print(f"  {mult}x: 信号={ts}, 胜率={tw/ts*100:.1f}%, 均收={bdf['avg_return'].mean():.2f}%, 盈亏比={bdf['profit_loss_ratio'].mean():.2f}, 涨停卖出={tl}({tl/ts*100:.0f}%)")
    else:
        print(f"  {mult}x: 无信号")
