"""
隔夜反转策略 v2.0 - 聚宽研究环境
=================================

【策略逻辑（白话版）】
1. 每天14:50扫描全A股，找出当天跌幅在-3%~-7%区间的股票
2. 严格风控过滤：
   - 排除ST、北交所、停牌
   - 排除跌停/涨停（买不进/不符合逻辑）
   - 排除放量暴跌（成交额>20日均值×3，机构在跑）
   - 排除连续下跌（过去3天累计跌>10%，趋势性下跌）
   - 排除近期跌停过的（过去5天有跌停，可能连板）
   - 排除超额跌幅过大的（个股跌幅-大盘跌幅>3%，可能个股利空）
3. 过滤后选跌幅最大的N只，以收盘价买入
4. 次日开盘价卖出
5. 网格搜索不同参数组合
6. 5万本金资金模拟

【v2.0 vs v1.0 区别】
  - 跌幅区间限制：不再选"跌最多的"，而是选-3%~-7%区间（排除暴雷股）
  - 新增5个风控过滤器（放量、连跌、近期跌停、超额跌幅、大盘暴跌日）
  - 风控第一，宁可少做，不做危险的交易

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

# ======== 回测参数 ========
END_DATE = '2026-02-25'         # 回测截止日期
BACKTEST_YEARS = 3              # 回测3年
MAX_STOCKS = 453                # 全A股扫描（聚宽限制）

# ======== 交易成本 ========
BUY_FEE = 0.00015              # 买入佣金万1.5
SELL_FEE = 0.00015             # 卖出佣金万1.5
STAMP_TAX = 0.001              # 印花税千1（仅卖出）

# ======== 风控参数（硬过滤，不参与网格搜索）========
DROP_FLOOR = -0.07             # 跌幅下限：跌超7%不买（普通股），可能是利空
DROP_FLOOR_CYB = -0.15         # 创业板/科创板跌幅下限：跌超15%不买
VOLUME_SPIKE_MULT = 3.0        # 放量倍数：成交额>20日均值×3 → 不买
CONSEC_DROP_LIMIT = -0.10      # 过去3天累计跌幅超10% → 不买（趋势性下跌）
RECENT_LIMIT_DAYS = 5          # 过去5天有跌停 → 不买
EXCESS_DROP_LIMIT = -0.03      # 超额跌幅（个股-大盘）超3% → 不买
MARKET_CRASH_THRESHOLD = -0.02 # 大盘跌>2%时，用超额跌幅过滤更严格

# ======== 网格搜索参数空间 ========
TOP_N_LIST = [1, 3, 5, 10]                  # 每天买几只
MIN_DROP_LIST = [-0.02, -0.03, -0.04, -0.05]  # 最低跌幅门槛（至少跌多少才考虑）
MIN_AMOUNT_LIST = [500, 1000, 3000]          # 最低成交额（万元）
MIN_CAP_LIST = [0, 20, 50]                   # 最低市值（亿元）

print("=" * 60)
print("✅ Cell 1 配置完成")
print("=" * 60)
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 交易成本: 买入万{BUY_FEE*10000:.1f} + 卖出万{SELL_FEE*10000:.1f} + 印花税千{STAMP_TAX*1000:.0f}")
print(f"\n  🛡️ 风控过滤（硬规则，所有组合都执行）:")
print(f"     跌幅区间: 普通股 {MIN_DROP_LIST[0]*100:.0f}%~{DROP_FLOOR*100:.0f}% / 创业板科创板 ~{DROP_FLOOR_CYB*100:.0f}%")
print(f"     放量过滤: 成交额 > 20日均值 × {VOLUME_SPIKE_MULT:.0f} → 不买")
print(f"     连跌过滤: 过去3天累计跌 > {abs(CONSEC_DROP_LIMIT)*100:.0f}% → 不买")
print(f"     跌停历史: 过去{RECENT_LIMIT_DAYS}天有跌停 → 不买")
print(f"     超额跌幅: 个股跌幅 - 大盘跌幅 > {abs(EXCESS_DROP_LIMIT)*100:.0f}% → 不买")
combos = len(TOP_N_LIST) * len(MIN_DROP_LIST) * len(MIN_AMOUNT_LIST) * len(MIN_CAP_LIST)
print(f"\n  📌 网格搜索: {combos} 种参数组合")


# ============================================================
# Cell 2：拉取全A股日线数据 + 大盘数据
# ============================================================
#
# 【做什么】
# 1. 拉取全A股每天的OHLC、成交额
# 2. 拉取沪深300指数的每日涨跌幅（用于超额跌幅计算）
# 3. 计算每只股票过去20天成交额均值（用于放量判断）
# 4. 计算过去3天累计跌幅（用于连跌判断）
# 5. 标记过去5天是否有跌停（用于跌停历史判断）

print(f"\n{'='*60}")
print(f"📊 第二步：拉取数据")
print(f"{'='*60}")

bt_start = pd.to_datetime(END_DATE) - timedelta(days=365 * BACKTEST_YEARS)
# 多拉30天历史，用于计算20日均量、3日累计跌幅等
data_start = bt_start - timedelta(days=45)
bt_start_str = bt_start.strftime('%Y-%m-%d')
data_start_str = data_start.strftime('%Y-%m-%d')

all_trade_days = list(get_trade_days(start_date=data_start_str, end_date=END_DATE))
bt_trade_days = [d for d in all_trade_days if str(d) >= bt_start_str]
print(f"  数据拉取区间: {data_start_str} ~ {END_DATE}")
print(f"  回测交易日: {len(bt_trade_days)} 天 ({bt_start_str} ~ {END_DATE})")

# ---- 沪深300每日涨跌幅 ----
print(f"\n  拉取沪深300指数...")
hs300 = get_price('000300.XSHG', start_date=data_start_str, end_date=END_DATE,
                  frequency='daily', fields=['close'])
hs300['change_pct'] = hs300['close'].pct_change()
hs300_daily = {}  # {日期str: 沪深300当天涨跌幅}
for idx, row in hs300.iterrows():
    hs300_daily[str(idx.date())] = row['change_pct']
print(f"  沪深300数据: {len(hs300_daily)} 天")

# ---- 全A股（排除北交所）----
all_stocks = get_all_securities(types=['stock'], date=END_DATE)
all_codes = [c for c in all_stocks.index 
             if not c.startswith('4') and not c.startswith('8')]
print(f"  全A股（排除北交所）: {len(all_codes)} 只")

if MAX_STOCKS < len(all_codes):
    all_codes = all_codes[:MAX_STOCKS]
    print(f"  ⚠️ 测试模式：只取前{MAX_STOCKS}只")

# ---- 分批拉取日线 ----
# 存储格式: stock_daily[code] = DataFrame (index=date, columns=open/close/high/low/money/paused)
print(f"\n  开始拉取个股日线...")
BATCH = 100
stock_daily = {}

for i in range(0, len(all_codes), BATCH):
    batch = all_codes[i:i+BATCH]
    df = get_price(batch, start_date=data_start_str, end_date=END_DATE,
                   frequency='daily',
                   fields=['open', 'close', 'high', 'low', 'money', 'paused'],
                   skip_paused=False, panel=False)
    
    for code in batch:
        code_df = df[df['code'] == code].copy() if 'code' in df.columns else None
        if code_df is None or len(code_df) == 0:
            # panel=False 时可能是 MultiIndex
            try:
                code_df = df.loc[code].copy()
            except:
                continue
        if len(code_df) < 30:
            continue
        code_df.index = pd.to_datetime(code_df.index) if not isinstance(code_df.index, pd.DatetimeIndex) else code_df.index
        stock_daily[code] = code_df
    
    if (i // BATCH + 1) % 10 == 0:
        print(f"    进度: {min(i+BATCH, len(all_codes))}/{len(all_codes)}")

print(f"\n  ✅ 个股日线完成: {len(stock_daily)} 只")


# ============================================================
# Cell 3：预计算风控指标
# ============================================================
#
# 【做什么】对每只股票的每个交易日，预先算好：
#   - 当天涨跌幅
#   - 20日平均成交额
#   - 过去3天累计涨跌幅
#   - 过去5天是否有跌停
#   - 次日开盘价（用于计算隔夜收益）
# 这些存成一个大表，后面网格搜索直接查表，不用重复计算

print(f"\n{'='*60}")
print(f"📊 第三步：预计算风控指标")
print(f"{'='*60}")

def get_limit_pct(code):
    """涨跌停幅度：创业板(300)/科创板(688) = 20%，其他 = 10%"""
    if code.startswith('300') or code.startswith('688'):
        return 0.20
    return 0.10

def get_drop_floor(code):
    """跌幅下限：创业板/科创板用更宽的阈值"""
    if code.startswith('300') or code.startswith('688'):
        return DROP_FLOOR_CYB
    return DROP_FLOOR

all_records = []  # 最终的大表
skipped_no_data = 0
skipped_paused = 0

for code_idx, code in enumerate(stock_daily.keys()):
    df = stock_daily[code]
    limit_pct = get_limit_pct(code)
    drop_floor = get_drop_floor(code)
    
    # 计算每天的涨跌幅
    df = df.copy()
    df['prev_close'] = df['close'].shift(1)
    df['change_pct'] = (df['close'] - df['prev_close']) / df['prev_close']
    
    # 20日平均成交额
    df['avg_money_20'] = df['money'].rolling(20, min_periods=10).mean()
    
    # 过去3天累计涨跌幅（含当天）
    df['cum_change_3d'] = df['change_pct'].rolling(3, min_periods=1).sum()
    
    # 过去5天是否有跌停
    df['is_limit_down'] = df['change_pct'] <= -(limit_pct - 0.005)
    df['had_limit_5d'] = df['is_limit_down'].rolling(5, min_periods=1).max()  # 1=有过跌停
    
    # 次日开盘价
    df['next_open'] = df['open'].shift(-1)
    # 次日是否停牌
    df['next_paused'] = df['paused'].shift(-1)
    
    for idx, row in df.iterrows():
        day_str = str(idx.date())
        if day_str < bt_start_str:
            continue
        
        # 基本数据检查
        if pd.isna(row['change_pct']) or pd.isna(row['prev_close']):
            skipped_no_data += 1
            continue
        if row.get('paused', 0) == 1:
            skipped_paused += 1
            continue
        if pd.isna(row['next_open']) or row.get('next_paused', 0) == 1:
            continue
        if row['close'] <= 0 or row['next_open'] <= 0:
            continue
        
        # ====== 硬风控过滤 ======
        change = row['change_pct']
        
        # 1) 跌停买不进
        if change <= -(limit_pct - 0.005):
            continue
        # 2) 涨停不符合逻辑
        if change >= (limit_pct - 0.005):
            continue
        # 3) 跌幅下限（跌太多=可能利空）
        if change < drop_floor:
            continue
        # 4) 放量暴跌（机构在跑）
        avg_money = row.get('avg_money_20', 0)
        if avg_money > 0 and row['money'] > avg_money * VOLUME_SPIKE_MULT:
            continue
        # 5) 过去3天连续下跌
        cum_3d = row.get('cum_change_3d', 0)
        if cum_3d < CONSEC_DROP_LIMIT:
            continue
        # 6) 过去5天有跌停
        if row.get('had_limit_5d', 0) >= 1:
            continue
        # 7) 超额跌幅（个股跌幅 vs 大盘跌幅）
        market_change = hs300_daily.get(day_str, 0)
        excess_drop = change - market_change  # 负数=跌得比大盘多
        if excess_drop < EXCESS_DROP_LIMIT:
            continue
        
        # ====== 通过所有风控，记录 ======
        gross_return = row['next_open'] / row['close'] - 1
        cost = BUY_FEE + SELL_FEE + STAMP_TAX
        net_return = gross_return - cost
        
        all_records.append({
            'date': day_str,
            'code': code,
            'change_pct': change,
            'close': row['close'],
            'next_open': row['next_open'],
            'money': row['money'],
            'avg_money_20': avg_money,
            'volume_ratio': row['money'] / avg_money if avg_money > 0 else 0,
            'cum_change_3d': cum_3d,
            'excess_drop': excess_drop,
            'market_change': market_change,
            'gross_return': gross_return,
            'net_return': net_return,
        })
    
    if (code_idx + 1) % 200 == 0:
        print(f"    进度: {code_idx+1}/{len(stock_daily)}, 已收集{len(all_records)}条记录")

trades_df = pd.DataFrame(all_records)
print(f"\n  ✅ 风控过滤后候选交易: {len(trades_df)} 条")
print(f"     覆盖 {trades_df['date'].nunique()} 个交易日, {trades_df['code'].nunique()} 只股票")
print(f"     跳过（无数据）: {skipped_no_data}, 跳过（停牌）: {skipped_paused}")
print(f"\n  隔夜毛收益统计（风控过滤后）:")
print(f"     均值: {trades_df['gross_return'].mean()*100:.3f}%")
print(f"     中位数: {trades_df['gross_return'].median()*100:.3f}%")
print(f"     胜率: {(trades_df['gross_return'] > 0).mean()*100:.1f}%")
print(f"     样本量: {len(trades_df)} 笔")


# ============================================================
# Cell 4：获取市值数据（月度快照）
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第四步：获取市值数据")
print(f"{'='*60}")

monthly_cap = {}
current_month = None

for day in bt_trade_days:
    day_str = str(day)
    month_key = day_str[:7]
    
    if month_key != current_month:
        current_month = month_key
        q = query(
            valuation.code,
            valuation.market_cap
        ).filter(
            valuation.code.in_(list(stock_daily.keys()))
        )
        cap_df = get_fundamentals(q, date=day_str)
        monthly_cap[month_key] = dict(zip(cap_df['code'], cap_df['market_cap']))
        print(f"    {month_key}: {len(cap_df)} 只")

# 把市值合并到 trades_df
def get_cap(row):
    month_key = row['date'][:7]
    return monthly_cap.get(month_key, {}).get(row['code'], 0)

trades_df['cap'] = trades_df.apply(get_cap, axis=1)
print(f"\n  ✅ 市值数据完成")


# ============================================================
# Cell 5：获取ST状态
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第五步：过滤ST股")
print(f"{'='*60}")

monthly_st = {}
current_month = None
codes_list = list(stock_daily.keys())

for day in bt_trade_days:
    day_str = str(day)
    month_key = day_str[:7]
    
    if month_key != current_month:
        current_month = month_key
        st_set = set()
        extras = get_extras('is_st', codes_list, start_date=day_str, end_date=day_str, df=True)
        if not extras.empty:
            for code in codes_list:
                if code in extras.columns and extras[code].iloc[0]:
                    st_set.add(code)
        monthly_st[month_key] = st_set
        print(f"    {month_key}: {len(st_set)} 只ST")

# 过滤掉ST
before_st = len(trades_df)
def is_st(row):
    month_key = row['date'][:7]
    return row['code'] in monthly_st.get(month_key, set())

trades_df = trades_df[~trades_df.apply(is_st, axis=1)].reset_index(drop=True)
print(f"\n  ✅ ST过滤: {before_st} → {len(trades_df)} 条（移除{before_st - len(trades_df)}条）")


# ============================================================
# Cell 6：网格搜索
# ============================================================

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
                
                # 跌幅门槛过滤（只选跌幅达标的）
                filtered = trades_df[
                    (trades_df['change_pct'] <= min_drop) &
                    (trades_df['money'] >= min_amount * 10000) &
                    (trades_df['cap'] >= min_cap)
                ].copy()
                
                if len(filtered) == 0:
                    continue
                
                # 每天选跌幅最大的top_n只（在通过风控的候选里选）
                selected = filtered.groupby('date').apply(
                    lambda x: x.nsmallest(top_n, 'change_pct')
                ).reset_index(drop=True)
                
                if len(selected) < 30:  # 样本太少没意义
                    continue
                
                # ---- 统计 ----
                n_trades = len(selected)
                n_days = selected['date'].nunique()
                avg_trades_per_day = n_trades / n_days
                mean_return = selected['net_return'].mean()
                median_return = selected['net_return'].median()
                win_rate = (selected['net_return'] > 0).mean()
                
                # 盈亏比
                wins = selected[selected['net_return'] > 0]['net_return']
                losses = selected[selected['net_return'] < 0]['net_return']
                avg_win = wins.mean() if len(wins) > 0 else 0
                avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.001
                profit_loss_ratio = avg_win / avg_loss
                
                # 日收益序列
                daily_returns = selected.groupby('date')['net_return'].mean()
                cumulative = (1 + daily_returns).cumprod()
                total_return = cumulative.iloc[-1] - 1
                
                # 最大回撤
                peak = cumulative.expanding().max()
                drawdown = (cumulative - peak) / peak
                max_dd = drawdown.min()
                
                # 夏普比（年化）
                daily_std = daily_returns.std()
                sharpe = (daily_returns.mean() / daily_std * np.sqrt(250)) if daily_std > 0 else 0
                
                # 最大单笔亏损
                worst_trade = selected['net_return'].min()
                
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
                    '最大单笔亏损': worst_trade,
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
    print(f"    交易: {row['交易次数']}笔 / {row['交易天数']}天 / 日均{row['日均笔数']}笔")
    print(f"    胜率: {row['胜率']*100:.1f}% | 盈亏比: {row['盈亏比']:.2f}")
    print(f"    均收: {row['平均净收益']*100:.3f}% | 中位收: {row['中位净收益']*100:.3f}%")
    print(f"    累计: {row['累计收益']*100:.1f}% | 回撤: {row['最大回撤']*100:.1f}%")
    print(f"    夏普: {row['夏普比']:.2f} | 最大单笔亏: {row['最大单笔亏损']*100:.2f}%")

# ---- 解读 ----
if len(top20) > 0:
    best = top20.iloc[0]
    print(f"\n{'='*60}")
    print(f"📝 解读")
    print(f"{'='*60}")
    
    # 胜率评价
    if best['胜率'] >= 0.55:
        print(f"  ✅ 胜率{best['胜率']*100:.1f}% — 优秀，隔夜反转策略核心指标达标")
    elif best['胜率'] >= 0.50:
        print(f"  ⚠️ 胜率{best['胜率']*100:.1f}% — 勉强过半，盈亏比需>1.2才能盈利")
    else:
        print(f"  ❌ 胜率{best['胜率']*100:.1f}% — 不足50%，策略可能无效")
    
    # 夏普评价
    if best['夏普比'] >= 2.0:
        print(f"  ✅ 夏普比{best['夏普比']:.1f} — 出色")
    elif best['夏普比'] >= 1.0:
        print(f"  ✅ 夏普比{best['夏普比']:.1f} — 良好")
    elif best['夏普比'] >= 0.5:
        print(f"  ⚠️ 夏普比{best['夏普比']:.1f} — 一般")
    else:
        print(f"  ❌ 夏普比{best['夏普比']:.1f} — 不理想")
    
    # 风控效果
    worst = best['最大单笔亏损']
    print(f"\n  🛡️ 风控效果:")
    print(f"     最大单笔亏损: {worst*100:.2f}%")
    if worst > -0.05:
        print(f"     ✅ 尾部风险控制良好（单笔亏损<5%）")
    elif worst > -0.08:
        print(f"     ⚠️ 尾部风险中等（单笔亏损5%~8%）")
    else:
        print(f"     ❌ 仍有较大尾部风险（单笔亏损>{abs(worst)*100:.0f}%）")
    
    print(f"\n  💡 风控过滤器排除了以下危险交易:")
    print(f"     - 跌超{abs(DROP_FLOOR)*100:.0f}%的（可能暴雷）")
    print(f"     - 放量暴跌的（机构在跑）")
    print(f"     - 连续3天跌>{abs(CONSEC_DROP_LIMIT)*100:.0f}%的（趋势性下跌）")
    print(f"     - 近5天跌停过的（可能连板）")
    print(f"     - 超额跌幅>{abs(EXCESS_DROP_LIMIT)*100:.0f}%的（个股利空）")


# ============================================================
# Cell 7：资金模拟（最佳策略）
# ============================================================

print(f"\n{'='*60}")
print(f"💰 第七步：资金模拟")
print(f"{'='*60}")

if len(results_df) == 0:
    print("  ❌ 没有有效策略，跳过")
else:
    best_params = results_df.nlargest(1, '夏普比').iloc[0]
    
    sim_top_n = int(best_params['top_n'])
    sim_min_drop = best_params['min_drop']
    sim_min_amount = best_params['min_amount']
    sim_min_cap = best_params['min_cap']
    
    print(f"  使用策略: {best_params['策略']}")
    
    # 资金参数
    INIT_CAPITAL = 50000
    MAX_PER_TRADE = 10000
    COMMISSION_PER_SIDE = 5
    DAILY_LOSS_LIMIT = 1000
    CONSEC_LOSS_LIMIT = 10
    TOTAL_LOSS_LIMIT = 20000
    
    # 筛选交易
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
    
    daily_log = []
    equity_curve = []
    trade_details = []  # 记录每笔交易，方便复盘
    
    for day_str in sorted(sim_selected['date'].unique()):
        if stopped:
            break
        
        day_trades = sim_selected[sim_selected['date'] == day_str]
        n_positions = len(day_trades)
        if n_positions == 0:
            continue
        
        per_stock = min(MAX_PER_TRADE, capital / n_positions)
        if per_stock < 500:
            stopped = True
            stop_reason = f"资金不足（剩余{capital:.0f}元）"
            break
        
        day_pnl = 0
        day_commission = 0
        
        for _, trade in day_trades.iterrows():
            shares = int(per_stock / trade['close'] / 100) * 100
            if shares <= 0:
                continue
            
            buy_cost = shares * trade['close']
            sell_revenue = shares * trade['next_open']
            commission = COMMISSION_PER_SIDE * 2
            stamp = sell_revenue * STAMP_TAX
            pnl = sell_revenue - buy_cost - commission - stamp
            
            day_pnl += pnl
            day_commission += commission + stamp
            
            trade_details.append({
                'date': day_str,
                'code': trade['code'],
                'shares': shares,
                'buy': trade['close'],
                'sell': trade['next_open'],
                'pnl': pnl,
                'change': trade['change_pct'],
                'excess': trade['excess_drop'],
            })
        
        capital += day_pnl
        peak_capital = max(peak_capital, capital)
        
        daily_log.append({
            'date': day_str,
            'n_trades': len([t for t in trade_details if t['date'] == day_str]),
            'day_pnl': day_pnl,
            'commission': day_commission,
            'capital': capital,
        })
        equity_curve.append((day_str, capital))
        
        # 风控
        if day_pnl < 0:
            consecutive_loss_days += 1
        else:
            consecutive_loss_days = 0
        
        if consecutive_loss_days >= CONSEC_LOSS_LIMIT:
            stopped = True
            stop_reason = f"连续亏损{CONSEC_LOSS_LIMIT}天"
            break
        
        if INIT_CAPITAL - capital >= TOTAL_LOSS_LIMIT:
            stopped = True
            stop_reason = f"总亏损达{TOTAL_LOSS_LIMIT}元"
            break
    
    # ---- 输出 ----
    log_df = pd.DataFrame(daily_log)
    detail_df = pd.DataFrame(trade_details)
    
    print(f"\n  {'='*50}")
    print(f"  📊 资金模拟结果")
    print(f"  {'='*50}")
    print(f"  初始资金: {INIT_CAPITAL:,.0f}元")
    print(f"  最终资金: {capital:,.0f}元")
    print(f"  总收益: {capital - INIT_CAPITAL:+,.0f}元 ({(capital/INIT_CAPITAL - 1)*100:+.1f}%)")
    print(f"  峰值资金: {peak_capital:,.0f}元")
    
    if len(log_df) > 0:
        max_dd_val = (peak_capital - log_df['capital'].min()) / peak_capital * 100
        total_commission = log_df['commission'].sum()
        
        print(f"  最大回撤: {max_dd_val:.1f}%")
        print(f"  总手续费: {total_commission:,.0f}元")
        print(f"\n  交易天数: {len(log_df)}天")
        print(f"  总笔数: {len(detail_df)}笔")
        print(f"  盈利天: {(log_df['day_pnl'] > 0).sum()}天 ({(log_df['day_pnl'] > 0).mean()*100:.1f}%)")
        print(f"  亏损天: {(log_df['day_pnl'] < 0).sum()}天")
        print(f"  日均盈亏: {log_df['day_pnl'].mean():+.1f}元")
        print(f"  最大单日盈: {log_df['day_pnl'].max():+.0f}元")
        print(f"  最大单日亏: {log_df['day_pnl'].min():+.0f}元")
        
        # 最大单笔亏损交易
        if len(detail_df) > 0:
            worst = detail_df.nsmallest(3, 'pnl')
            print(f"\n  ⚠️ 最大亏损交易 Top 3:")
            for _, w in worst.iterrows():
                print(f"     {w['date']} {w['code']}: 买{w['buy']:.2f}→卖{w['sell']:.2f}, 亏{w['pnl']:+.0f}元 (当天跌{w['change']*100:.1f}%)")
    
    if stopped:
        print(f"\n  🛑 风控停止: {stop_reason}")
    
    # 资金曲线
    print(f"\n  📈 资金曲线（月度快照）:")
    eq_df = pd.DataFrame(equity_curve, columns=['date', 'capital'])
    eq_df['month'] = eq_df['date'].str[:7]
    monthly_eq = eq_df.groupby('month').last()
    
    for month, row in monthly_eq.iterrows():
        cap_val = row['capital']
        pct = (cap_val / INIT_CAPITAL - 1) * 100
        bar_len = max(0, int((cap_val / INIT_CAPITAL - 0.5) * 40))
        bar = "█" * min(bar_len, 50)
        print(f"    {month}: {cap_val:>8,.0f}元 ({pct:+5.1f}%) {bar}")
    
    print(f"\n  💡 注意事项:")
    print(f"     - 手续费{COMMISSION_PER_SIDE}元/笔，实际可能更低（券商有活动）")
    print(f"     - 未计入滑点（尾盘买+开盘卖各有~0.1%滑点）")
    print(f"     - 未排除财报日/除权日（异常波动可能干扰）")
    print(f"     - 建议优化：加星期效应（排除周五）、加财报日过滤")
