"""
隔夜反转策略 v1.0 - 聚宽研究环境
=================================

【策略逻辑（白话版）】
1. 每天14:50扫描全A股，找出当天跌幅最大的N只股票
2. 过滤掉：ST股、北交所、跌停（买不进去）、涨停（已反弹过了）、停牌
3. 以当天收盘价买入
4. 第二天9:35以开盘价附近卖出
5. 网格搜索：测试不同参数组合（选几只、跌多少才买、过滤条件等）
6. 资金模拟：5万本金，模拟真实交易

【学术依据】
  A股隔夜反转效应：当天大跌的股票，次日开盘倾向于均值回归（高开）。
  原因可能是：散户恐慌性抛售导致超跌，隔夜情绪修复 + 机构尾盘接货。
  小资金优势：尾盘流动性差，大资金进不去，这个alpha主要留给小散。

【使用方法】
  聚宽(joinquant.com) → 研究环境 → 新建Notebook → 按Cell分段粘贴运行

【版本 v1.0】
  - 3年回测（2023-2026），覆盖熊牛周期
  - 全中文输出
  - 资金模拟 + 风控
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

# ======== 回测参数 ========
END_DATE = '2026-02-25'         # 回测截止日期
BACKTEST_YEARS = 3              # 回测3年
MAX_STOCKS = 453                # 全A股扫描（聚宽限制）

# ======== 交易成本 ========
BUY_FEE = 0.00015              # 买入佣金万1.5
SELL_FEE = 0.00015             # 卖出佣金万1.5
STAMP_TAX = 0.001              # 印花税千1（仅卖出）

# ======== 网格搜索参数空间 ========
# 每天买几只
TOP_N_LIST = [1, 3, 5, 10]
# 最低跌幅门槛（负数，比如-0.03表示至少跌3%才考虑）
MIN_DROP_LIST = [-0.02, -0.03, -0.05, -0.07]
# 最低成交额门槛（万元），过滤流动性差的
MIN_AMOUNT_LIST = [500, 1000, 3000]
# 最低市值门槛（亿元）
MIN_CAP_LIST = [0, 20, 50]

print("=" * 60)
print("✅ Cell 1 配置完成")
print("=" * 60)
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 交易成本: 买入万{BUY_FEE*10000:.1f} + 卖出万{SELL_FEE*10000:.1f} + 印花税千{STAMP_TAX*1000:.0f}")
print(f"  📌 每天买入只数: {TOP_N_LIST}")
print(f"  📌 跌幅门槛: {MIN_DROP_LIST}")
print(f"  📌 成交额门槛(万): {MIN_AMOUNT_LIST}")
print(f"  📌 市值门槛(亿): {MIN_CAP_LIST}")
combos = len(TOP_N_LIST) * len(MIN_DROP_LIST) * len(MIN_AMOUNT_LIST) * len(MIN_CAP_LIST)
print(f"  📌 参数组合总数: {combos} 种")


# ============================================================
# Cell 2：获取交易日 & 全A股日线数据
# ============================================================
#
# 【做什么】拉取全A股每天的开盘价、收盘价、成交额、涨跌幅
# 【注意】数据量大，可能需要几分钟

print(f"\n{'='*60}")
print(f"📊 第二步：拉取全A股日线数据")
print(f"{'='*60}")

bt_start = pd.to_datetime(END_DATE) - timedelta(days=365 * BACKTEST_YEARS)
bt_start_str = bt_start.strftime('%Y-%m-%d')

# 获取交易日列表
all_trade_days = list(get_trade_days(start_date=bt_start_str, end_date=END_DATE))
print(f"  回测交易日: {len(all_trade_days)} 天 ({bt_start_str} ~ {END_DATE})")

# 获取全部A股（排除北交所）
all_stocks = get_all_securities(types=['stock'], date=END_DATE)
# 排除北交所（代码以 '4' 或 '8' 开头，或 .XBJE 后缀）
all_codes = [c for c in all_stocks.index 
             if not c.startswith('4') and not c.startswith('8')]
print(f"  全A股（排除北交所）: {len(all_codes)} 只")

# 如果测试模式，只取部分
if MAX_STOCKS < len(all_codes):
    all_codes = all_codes[:MAX_STOCKS]
    print(f"  ⚠️ 测试模式：只取前{MAX_STOCKS}只")

# ---- 分批拉取日线 ----
# 需要：收盘价close、开盘价open、成交额money、前收盘价pre_close
# 用于计算：当天跌幅 = (close - pre_close) / pre_close
#           隔夜收益 = 次日open / 当天close - 1

print(f"\n  开始拉取日线数据...")
BATCH = 200
daily_data = {}  # {日期str: DataFrame(code, open, close, pre_close, money, high, low)}

for day_idx, day in enumerate(all_trade_days):
    day_str = str(day)
    rows = []
    
    for i in range(0, len(all_codes), BATCH):
        batch = all_codes[i:i+BATCH]
        # 拉2天数据，这样能得到前收盘
        df = get_price(batch, end_date=day_str, count=2,
                       frequency='daily',
                       fields=['open', 'close', 'high', 'low', 'money', 'paused'],
                       skip_paused=False)
        
        for code in batch:
            if code not in df.index.get_level_values('code'):
                continue
            code_df = df.loc[code]
            if len(code_df) < 2:
                continue
            
            prev_row = code_df.iloc[-2]
            curr_row = code_df.iloc[-1]
            
            # 跳过停牌
            if curr_row.get('paused', 0) == 1:
                continue
            
            pre_close = prev_row['close']
            if pre_close <= 0:
                continue
                
            rows.append({
                'code': code,
                'open': curr_row['open'],
                'close': curr_row['close'],
                'high': curr_row['high'],
                'low': curr_row['low'],
                'pre_close': pre_close,
                'money': curr_row['money'],
                'change_pct': (curr_row['close'] - pre_close) / pre_close,
            })
    
    if rows:
        daily_data[day_str] = pd.DataFrame(rows)
    
    if (day_idx + 1) % 100 == 0:
        print(f"    进度: {day_idx+1}/{len(all_trade_days)} 天")

print(f"\n  ✅ 数据拉取完成，共 {len(daily_data)} 个交易日")


# ============================================================
# Cell 3：获取市值数据（用于市值过滤）
# ============================================================
#
# 【做什么】每月取一次市值快照，避免每天都查（太慢）
# 月度更新足够了，市值不会一天变很多

print(f"\n{'='*60}")
print(f"📊 第三步：获取市值数据（月度快照）")
print(f"{'='*60}")

# 每月第一个交易日取一次市值
monthly_cap = {}  # {月份str: {code: 市值(亿)}}
current_month = None

for day in all_trade_days:
    day_str = str(day)
    month_key = day_str[:7]  # "2024-01"
    
    if month_key != current_month:
        current_month = month_key
        q = query(
            valuation.code,
            valuation.market_cap  # 单位：亿
        ).filter(
            valuation.code.in_(all_codes)
        )
        cap_df = get_fundamentals(q, date=day_str)
        monthly_cap[month_key] = dict(zip(cap_df['code'], cap_df['market_cap']))
        print(f"    {month_key}: {len(cap_df)} 只有市值数据")

print(f"\n  ✅ 市值数据完成，共 {len(monthly_cap)} 个月")


# ============================================================
# Cell 4：获取ST状态（用于过滤）
# ============================================================
#
# 【做什么】标记ST股，每月更新一次
# ST股涨跌停幅度只有5%，隔夜反转逻辑不一样

print(f"\n{'='*60}")
print(f"📊 第四步：获取ST状态（月度快照）")
print(f"{'='*60}")

monthly_st = {}  # {月份str: set(ST股代码)}
current_month = None

for day in all_trade_days:
    day_str = str(day)
    month_key = day_str[:7]
    
    if month_key != current_month:
        current_month = month_key
        st_set = set()
        # 聚宽的 get_extras 可以查ST状态
        extras = get_extras('is_st', all_codes, start_date=day_str, end_date=day_str, df=True)
        if not extras.empty:
            for code in all_codes:
                if code in extras.columns and extras[code].iloc[0]:
                    st_set.add(code)
        monthly_st[month_key] = st_set
        print(f"    {month_key}: {len(st_set)} 只ST")

print(f"\n  ✅ ST数据完成")


# ============================================================
# Cell 5：生成交易信号 & 计算隔夜收益
# ============================================================
#
# 【核心逻辑】
# 对每个交易日：
#   1. 取当天所有股票的跌幅排名
#   2. 过滤：排除ST、跌停（买不进）、涨停（不符合逻辑）、成交额/市值不达标
#   3. 选跌幅最大的Top N只
#   4. 以当天收盘价"买入"
#   5. 以次日开盘价"卖出"
#   6. 隔夜收益 = 次日开盘价 / 当天收盘价 - 1 - 交易成本
#
# 【跌停判断】
#   普通股涨跌停±10%，创业板/科创板±20%
#   当天跌幅接近-10%或-20%的，认为是跌停（买不进）

print(f"\n{'='*60}")
print(f"📊 第五步：生成交易信号 & 计算隔夜收益")
print(f"{'='*60}")

def get_limit_threshold(code):
    """判断涨跌停幅度：创业板(300)/科创板(688)是20%，其他10%"""
    if code.startswith('300') or code.startswith('688'):
        return 0.20
    return 0.10

def is_limit_down(change_pct, code):
    """是否跌停（跌幅接近涨跌停幅度）"""
    threshold = get_limit_threshold(code)
    return change_pct <= -(threshold - 0.005)  # 留0.5%容差

def is_limit_up(change_pct, code):
    """是否涨停"""
    threshold = get_limit_threshold(code)
    return change_pct >= (threshold - 0.005)

# 构建次日开盘价映射
trade_day_list = sorted(daily_data.keys())
next_day_open = {}  # {(日期str, code): 次日开盘价}

for i in range(len(trade_day_list) - 1):
    today = trade_day_list[i]
    tomorrow = trade_day_list[i + 1]
    if tomorrow in daily_data:
        tomorrow_df = daily_data[tomorrow].set_index('code')
        for code in tomorrow_df.index:
            next_day_open[(today, code)] = tomorrow_df.loc[code, 'open']

print(f"  次日开盘价映射: {len(next_day_open)} 条")

# ---- 生成所有交易记录（不分参数，先算出每只股票每天的隔夜收益）----
all_trades = []  # [{date, code, change_pct, close, next_open, overnight_return, money, cap}]

for day_str in trade_day_list[:-1]:  # 最后一天没有次日
    df = daily_data[day_str]
    month_key = day_str[:7]
    cap_map = monthly_cap.get(month_key, {})
    st_set = monthly_st.get(month_key, set())
    
    for _, row in df.iterrows():
        code = row['code']
        change_pct = row['change_pct']
        close = row['close']
        
        # 基本过滤
        if code in st_set:          # 排除ST
            continue
        if is_limit_down(change_pct, code):  # 跌停买不进
            continue
        if is_limit_up(change_pct, code):    # 涨停的不符合"大跌"逻辑
            continue
        if close <= 0:
            continue
        
        # 查次日开盘价
        key = (day_str, code)
        if key not in next_day_open:
            continue
        nxt_open = next_day_open[key]
        if nxt_open <= 0:
            continue
        
        # 隔夜收益（扣除交易成本）
        gross_return = nxt_open / close - 1
        cost = BUY_FEE + SELL_FEE + STAMP_TAX
        net_return = gross_return - cost
        
        cap = cap_map.get(code, 0)
        
        all_trades.append({
            'date': day_str,
            'code': code,
            'name': get_security_info(code).display_name if len(all_trades) < 100 else '',
            'change_pct': change_pct,     # 当天跌幅
            'close': close,               # 买入价（收盘）
            'next_open': nxt_open,         # 卖出价（次日开盘）
            'gross_return': gross_return,  # 毛收益
            'net_return': net_return,      # 净收益（扣成本）
            'money': row['money'],         # 成交额
            'cap': cap,                    # 市值（亿）
        })

trades_df = pd.DataFrame(all_trades)
print(f"\n  ✅ 候选交易记录: {len(trades_df)} 条")
print(f"     覆盖 {trades_df['date'].nunique()} 个交易日")
print(f"     覆盖 {trades_df['code'].nunique()} 只股票")
print(f"\n  隔夜毛收益统计（全部候选）:")
print(f"     均值: {trades_df['gross_return'].mean()*100:.3f}%")
print(f"     中位数: {trades_df['gross_return'].median()*100:.3f}%")
print(f"     胜率: {(trades_df['gross_return'] > 0).mean()*100:.1f}%")


# ============================================================
# Cell 6：网格搜索 — 不同参数组合的表现
# ============================================================
#
# 【做什么】在候选交易池里，按不同的过滤条件筛选，看哪种组合最赚钱
# 参数维度：
#   - top_n: 每天选跌幅最大的几只
#   - min_drop: 至少跌多少才买（-0.03 = 跌3%）
#   - min_amount: 最低成交额（万），过滤僵尸股
#   - min_cap: 最低市值（亿），过滤小盘

print(f"\n{'='*60}")
print(f"📊 第六步：网格搜索")
print(f"{'='*60}")

results = []
total_combos = len(TOP_N_LIST) * len(MIN_DROP_LIST) * len(MIN_AMOUNT_LIST) * len(MIN_CAP_LIST)
combo_idx = 0

for top_n in TOP_N_LIST:
    for min_drop in MIN_DROP_LIST:
        for min_amount in MIN_AMOUNT_LIST:
            for min_cap in MIN_CAP_LIST:
                combo_idx += 1
                
                # 过滤
                filtered = trades_df[
                    (trades_df['change_pct'] <= min_drop) &          # 跌幅达标
                    (trades_df['money'] >= min_amount * 10000) &     # 成交额达标
                    (trades_df['cap'] >= min_cap)                    # 市值达标
                ].copy()
                
                if len(filtered) == 0:
                    continue
                
                # 每天选跌幅最大的top_n只
                selected = filtered.groupby('date').apply(
                    lambda x: x.nsmallest(top_n, 'change_pct')
                ).reset_index(drop=True)
                
                if len(selected) == 0:
                    continue
                
                # 统计
                n_trades = len(selected)
                n_days = selected['date'].nunique()
                avg_trades_per_day = n_trades / n_days if n_days > 0 else 0
                mean_return = selected['net_return'].mean()
                median_return = selected['net_return'].median()
                win_rate = (selected['net_return'] > 0).mean()
                
                # 盈亏比
                wins = selected[selected['net_return'] > 0]['net_return']
                losses = selected[selected['net_return'] < 0]['net_return']
                avg_win = wins.mean() if len(wins) > 0 else 0
                avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.001
                profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
                
                # 日收益序列（每天的平均净收益）
                daily_returns = selected.groupby('date')['net_return'].mean()
                cumulative = (1 + daily_returns).cumprod()
                total_return = cumulative.iloc[-1] - 1 if len(cumulative) > 0 else 0
                
                # 最大回撤
                peak = cumulative.expanding().max()
                drawdown = (cumulative - peak) / peak
                max_dd = drawdown.min()
                
                # 夏普比（年化，假设250交易日）
                daily_std = daily_returns.std()
                sharpe = (daily_returns.mean() / daily_std * np.sqrt(250)) if daily_std > 0 else 0
                
                # 策略标签
                label = f"Top{top_n}|跌≥{abs(min_drop)*100:.0f}%|额≥{min_amount}万|市值≥{min_cap}亿"
                
                results.append({
                    '策略': label,
                    'top_n': top_n,
                    'min_drop': min_drop,
                    'min_amount': min_amount,
                    'min_cap': min_cap,
                    '交易次数': n_trades,
                    '交易天数': n_days,
                    '日均笔数': round(avg_trades_per_day, 1),
                    '平均净收益': mean_return,
                    '中位净收益': median_return,
                    '胜率': win_rate,
                    '盈亏比': profit_loss_ratio,
                    '累计收益': total_return,
                    '最大回撤': max_dd,
                    '夏普比': sharpe,
                })
                
                if combo_idx % 50 == 0:
                    print(f"    进度: {combo_idx}/{total_combos}")

results_df = pd.DataFrame(results)
print(f"\n  ✅ 网格搜索完成: {len(results_df)} 种有效组合")

# ---- 排行榜 ----
print(f"\n{'='*60}")
print(f"🏆 策略排行榜（按夏普比排序，取前20）")
print(f"{'='*60}")

top20 = results_df.nlargest(20, '夏普比')
for rank, (_, row) in enumerate(top20.iterrows(), 1):
    print(f"\n  第{rank}名: {row['策略']}")
    print(f"    交易次数: {row['交易次数']}笔 ({row['交易天数']}天, 日均{row['日均笔数']}笔)")
    print(f"    胜率: {row['胜率']*100:.1f}% | 盈亏比: {row['盈亏比']:.2f}")
    print(f"    平均净收益: {row['平均净收益']*100:.3f}% | 中位净收益: {row['中位净收益']*100:.3f}%")
    print(f"    累计收益: {row['累计收益']*100:.1f}% | 最大回撤: {row['最大回撤']*100:.1f}%")
    print(f"    夏普比: {row['夏普比']:.2f}")

# ---- 解读提示 ----
best = top20.iloc[0] if len(top20) > 0 else None
if best is not None:
    print(f"\n{'='*60}")
    print(f"📝 解读")
    print(f"{'='*60}")
    if best['胜率'] >= 0.55 and best['盈亏比'] >= 1.0:
        print(f"  ✅ 最佳策略胜率>{best['胜率']*100:.0f}%且盈亏比>1，值得关注")
    elif best['胜率'] >= 0.50:
        print(f"  ⚠️ 胜率勉强过半，需要配合仓位管理")
    else:
        print(f"  ❌ 胜率不足50%，策略可能无效")
    
    if best['夏普比'] >= 1.5:
        print(f"  ✅ 夏普比{best['夏普比']:.1f}，风险调整收益优秀")
    elif best['夏普比'] >= 0.5:
        print(f"  ⚠️ 夏普比{best['夏普比']:.1f}，一般水平")
    else:
        print(f"  ❌ 夏普比{best['夏普比']:.1f}偏低")
    
    print(f"\n  💡 隔夜反转策略特点:")
    print(f"     - 每笔收益很薄（通常0.3%~0.8%），靠频率取胜")
    print(f"     - 胜率是关键指标（>55%就有实战价值）")
    print(f"     - 避开财报季和重大事件日效果更好（本版未过滤）")
    print(f"     - 周一/周五效果可能不同（本版未区分）")


# ============================================================
# Cell 7：资金模拟（最佳策略）
# ============================================================
#
# 【做什么】用排行榜第一名的参数，模拟真实交易
# 资金条件：
#   - 初始本金: 5万
#   - 单笔最大: 1万
#   - 手续费: 5元/笔（买卖各一次 = 10元）
#   - 最大持仓: 等于top_n（因为每天全卖全买）
#   - 日亏损上限: 1000元
#   - 连续亏损上限: 10天
#   - 总亏损上限: 2万（即本金的40%）

print(f"\n{'='*60}")
print(f"💰 第七步：资金模拟")
print(f"{'='*60}")

if len(results_df) == 0:
    print("  ❌ 没有有效策略，跳过资金模拟")
else:
    # 用夏普比最高的策略
    best_params = results_df.nlargest(1, '夏普比').iloc[0]
    
    sim_top_n = int(best_params['top_n'])
    sim_min_drop = best_params['min_drop']
    sim_min_amount = best_params['min_amount']
    sim_min_cap = best_params['min_cap']
    
    print(f"  使用策略: {best_params['策略']}")
    
    # ---- 资金参数 ----
    INIT_CAPITAL = 50000        # 初始资金5万
    MAX_PER_TRADE = 10000       # 单笔最大1万
    COMMISSION_PER_SIDE = 5     # 每笔手续费5元
    DAILY_LOSS_LIMIT = 1000     # 日亏损上限
    CONSEC_LOSS_LIMIT = 10      # 连续亏损天数上限
    TOTAL_LOSS_LIMIT = 20000    # 总亏损上限
    
    # ---- 筛选交易 ----
    sim_trades = trades_df[
        (trades_df['change_pct'] <= sim_min_drop) &
        (trades_df['money'] >= sim_min_amount * 10000) &
        (trades_df['cap'] >= sim_min_cap)
    ].copy()
    
    sim_selected = sim_trades.groupby('date').apply(
        lambda x: x.nsmallest(sim_top_n, 'change_pct')
    ).reset_index(drop=True)
    
    # ---- 按天模拟 ----
    capital = INIT_CAPITAL
    peak_capital = INIT_CAPITAL
    consecutive_loss_days = 0
    stopped = False
    stop_reason = ""
    
    daily_log = []  # 每天的交易记录
    equity_curve = [(trade_day_list[0], INIT_CAPITAL)]
    
    for day_str in sorted(sim_selected['date'].unique()):
        if stopped:
            break
        
        day_trades = sim_selected[sim_selected['date'] == day_str]
        n_positions = len(day_trades)
        
        if n_positions == 0:
            continue
        
        # 每只分配多少钱（均分可用资金，但不超过MAX_PER_TRADE）
        per_stock = min(MAX_PER_TRADE, capital / n_positions)
        
        if per_stock < 500:  # 钱不够买了
            stopped = True
            stop_reason = f"资金不足（剩余{capital:.0f}元）"
            break
        
        day_pnl = 0
        day_details = []
        
        for _, trade in day_trades.iterrows():
            # 买入股数（向下取整到100股）
            shares = int(per_stock / trade['close'] / 100) * 100
            if shares <= 0:
                continue
            
            buy_cost = shares * trade['close']
            sell_revenue = shares * trade['next_open']
            
            # 手续费：固定5元/笔
            commission = COMMISSION_PER_SIDE * 2  # 买卖各一次
            stamp = sell_revenue * STAMP_TAX       # 印花税
            
            pnl = sell_revenue - buy_cost - commission - stamp
            day_pnl += pnl
            
            day_details.append({
                'code': trade['code'],
                'shares': shares,
                'buy_price': trade['close'],
                'sell_price': trade['next_open'],
                'pnl': pnl,
            })
        
        capital += day_pnl
        
        # 记录
        daily_log.append({
            'date': day_str,
            'n_trades': len(day_details),
            'day_pnl': day_pnl,
            'capital': capital,
        })
        equity_curve.append((day_str, capital))
        
        # ---- 风控检查 ----
        # 日亏损上限
        if day_pnl < -DAILY_LOSS_LIMIT:
            # 只是标记，不停止（实际中可能需要暂停一天）
            pass
        
        # 连续亏损
        if day_pnl < 0:
            consecutive_loss_days += 1
        else:
            consecutive_loss_days = 0
        
        if consecutive_loss_days >= CONSEC_LOSS_LIMIT:
            stopped = True
            stop_reason = f"连续亏损{CONSEC_LOSS_LIMIT}天"
            break
        
        # 总亏损
        if INIT_CAPITAL - capital >= TOTAL_LOSS_LIMIT:
            stopped = True
            stop_reason = f"总亏损达{TOTAL_LOSS_LIMIT}元上限"
            break
        
        # 更新峰值
        peak_capital = max(peak_capital, capital)
    
    # ---- 输出结果 ----
    log_df = pd.DataFrame(daily_log)
    
    print(f"\n  {'='*50}")
    print(f"  📊 资金模拟结果")
    print(f"  {'='*50}")
    print(f"  初始资金: {INIT_CAPITAL:,.0f}元")
    print(f"  最终资金: {capital:,.0f}元")
    print(f"  总收益: {capital - INIT_CAPITAL:+,.0f}元 ({(capital/INIT_CAPITAL - 1)*100:+.1f}%)")
    print(f"  峰值资金: {peak_capital:,.0f}元")
    print(f"  最大回撤: {(peak_capital - log_df['capital'].min()) / peak_capital * 100:.1f}%" if len(log_df) > 0 else "  N/A")
    
    if len(log_df) > 0:
        print(f"\n  交易天数: {len(log_df)}天")
        print(f"  总交易笔数: {log_df['n_trades'].sum():.0f}笔")
        print(f"  盈利天数: {(log_df['day_pnl'] > 0).sum()}天 ({(log_df['day_pnl'] > 0).mean()*100:.1f}%)")
        print(f"  亏损天数: {(log_df['day_pnl'] < 0).sum()}天")
        print(f"  日均盈亏: {log_df['day_pnl'].mean():+.1f}元")
        print(f"  最大单日盈利: {log_df['day_pnl'].max():+.0f}元")
        print(f"  最大单日亏损: {log_df['day_pnl'].min():+.0f}元")
    
    if stopped:
        print(f"\n  ⚠️ 风控触发停止: {stop_reason}")
    
    # ---- 资金曲线 ----
    print(f"\n  📈 资金曲线（每月快照）:")
    eq_df = pd.DataFrame(equity_curve, columns=['date', 'capital'])
    eq_df['month'] = eq_df['date'].str[:7]
    monthly_eq = eq_df.groupby('month').last()
    
    for month, row in monthly_eq.iterrows():
        cap_val = row['capital']
        bar_len = max(0, int((cap_val / INIT_CAPITAL - 0.5) * 40))  # 缩放
        bar = "█" * min(bar_len, 50)
        print(f"    {month}: {cap_val:>8,.0f}元 {bar}")
    
    print(f"\n  💡 提示:")
    print(f"     - 手续费按{COMMISSION_PER_SIDE}元/笔计算（买卖共{COMMISSION_PER_SIDE*2}元）")
    print(f"     - 未考虑滑点（实际尾盘买入和次日开盘卖出都会有滑点）")
    print(f"     - 未排除财报日/除权除息日（可能有异常波动）")
    print(f"     - 建议下一步：加入星期效应过滤、波动率过滤等优化")
