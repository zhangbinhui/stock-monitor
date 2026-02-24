import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def backtest_volatility_breakout(stock_code, stock_name, multiplier=2.0, hold_days=5):
    """对单只股票做波动率突破回测"""
    # 获取4年日K线（需要3年历史+1年回测期）
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365*4+30)).strftime("%Y%m%d")
    
    df = ak.stock_zh_a_hist(symbol=stock_code, period="daily",
                            start_date=start_date, end_date=end_date, adjust="qfq")
    
    if df is None or len(df) < 800:
        print(f"{stock_name}({stock_code}): 数据不足，跳过")
        return None
    
    # 计算日收益率
    df['return'] = df['收盘'].pct_change()
    
    # 滚动计算三个周期的波动率
    df['hv_3y'] = df['return'].rolling(750).std()
    df['hv_1y'] = df['return'].rolling(250).std()
    df['hv_3m'] = df['return'].rolling(60).std()
    
    # 取三者最大值作为阈值
    df['hv_max'] = df[['hv_3y', 'hv_1y', 'hv_3m']].max(axis=1)
    
    # 阈值 = max波动率 × multiplier
    df['threshold'] = df['hv_max'] * multiplier
    
    # 买入信号：当日涨幅超过阈值（只看向上突破）
    df['daily_return'] = df['return']
    df['signal'] = df['daily_return'] > df['threshold']
    
    # 只取有完整波动率数据的区间（从第750天开始）
    df_valid = df.iloc[750:].copy()
    
    # 计算每个信号的N日后收益
    signals = df_valid[df_valid['signal'] == True].copy()
    
    results = []
    for idx in signals.index:
        pos = df.index.get_loc(idx)
        if pos + hold_days < len(df):
            buy_price = df.iloc[pos]['收盘']  # 当日收盘买入
            sell_price = df.iloc[pos + hold_days]['收盘']  # N日后收盘卖出
            ret = (sell_price - buy_price) / buy_price * 100
            results.append({
                'date': df.iloc[pos]['日期'],
                'buy_price': buy_price,
                'sell_price': sell_price,
                'return_pct': ret,
                'daily_return_pct': df.iloc[pos]['daily_return'] * 100,
                'threshold_pct': df.iloc[pos]['threshold'] * 100,
            })
    
    if not results:
        print(f"{stock_name}({stock_code}): 无信号触发")
        return None
    
    results_df = pd.DataFrame(results)
    
    # 统计
    total_signals = len(results_df)
    win_count = len(results_df[results_df['return_pct'] > 0])
    win_rate = win_count / total_signals * 100
    avg_return = results_df['return_pct'].mean()
    max_win = results_df['return_pct'].max()
    max_loss = results_df['return_pct'].min()
    avg_win = results_df[results_df['return_pct'] > 0]['return_pct'].mean() if win_count > 0 else 0
    avg_loss = results_df[results_df['return_pct'] <= 0]['return_pct'].mean() if total_signals - win_count > 0 else 0
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    
    summary = {
        'stock_code': stock_code,
        'stock_name': stock_name,
        'total_signals': total_signals,
        'win_count': win_count,
        'win_rate': win_rate,
        'avg_return': avg_return,
        'max_win': max_win,
        'max_loss': max_loss,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_loss_ratio': profit_loss_ratio,
    }
    
    return summary, results_df

# 运行回测
stocks = [
    ('000898', '鞍钢股份'),
    ('601390', '中国中铁'),
    ('688126', '沪硅产业'),
    ('603690', '至纯科技'),
    ('603195', '公牛集团'),
    ('000002', '万科A'),
    ('603260', '合盛硅业'),
    ('300014', '亿纬锂能'),
]

all_summaries = []
for code, name in stocks:
    try:
        result = backtest_volatility_breakout(code, name, multiplier=2.0, hold_days=5)
        if result:
            summary, details = result
            all_summaries.append(summary)
            print(f"\n{name}({code}):")
            print(f"  信号数={summary['total_signals']}, 胜率={summary['win_rate']:.1f}%, 平均收益={summary['avg_return']:.2f}%")
            print(f"  最大赢={summary['max_win']:.2f}%, 最大亏={summary['max_loss']:.2f}%")
            print(f"  盈亏比={summary['profit_loss_ratio']:.2f}")
    except Exception as e:
        print(f"{name}({code}): 错误 {e}")

# 总结
print("\n" + "="*60)
print("波动率突破策略回测总结（2倍标准差，持有5天）")
print("="*60)
summary_df = pd.DataFrame(all_summaries)
print(summary_df[['stock_name','total_signals','win_rate','avg_return','max_win','max_loss','profit_loss_ratio']].to_string(index=False))
print(f"\n全部股票汇总:")
print(f"  总信号数: {summary_df['total_signals'].sum()}")
print(f"  平均胜率: {summary_df['win_rate'].mean():.1f}%")
print(f"  平均收益: {summary_df['avg_return'].mean():.2f}%")
print(f"  平均盈亏比: {summary_df['profit_loss_ratio'].mean():.2f}")

# 也测试不同参数
print("\n\n" + "="*60)
print("参数敏感性测试（鞍钢股份为例）")
print("="*60)
for mult in [1.5, 2.0, 2.5, 3.0]:
    for days in [3, 5, 10]:
        result = backtest_volatility_breakout('000898', '鞍钢股份', multiplier=mult, hold_days=days)
        if result:
            s = result[0]
            print(f"  {mult}倍阈值+持有{days}天: 信号={s['total_signals']}, 胜率={s['win_rate']:.1f}%, 均收={s['avg_return']:.2f}%, 盈亏比={s['profit_loss_ratio']:.2f}")
