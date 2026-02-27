"""
隔夜反转策略 v3.0 - 聚宽研究环境
=================================

【策略逻辑（白话版）】
1. 每天14:50扫描全A股，找当天跌幅-3%~-7%区间的股票
2. 严格风控过滤（放量、连跌、近期跌停、超额跌幅）
3. 过滤后按跌幅排序，选Top N只买入（同行业最多1只）
4. 仓位按信号强度分配（温和下跌多买，大跌少买）
5. 次日开盘卖出
6. 网格搜索 + 星期效应分析 + 资金模拟

【v3.0 新增】
  - ⭐ 星期效应：按周几分组统计，可选排除表现差的星期几
  - ⭐ 仓位管理：按跌幅+市值动态分配权重（不再等权）
  - ⭐ 行业分散：同一申万一级行业最多买1只，防止行业踩坑
  - 保留v2.0所有风控过滤器

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
END_DATE = '2026-02-25'
BACKTEST_YEARS = 3
MAX_STOCKS = 453

# ======== 交易成本 ========
BUY_FEE = 0.00015
SELL_FEE = 0.00015
STAMP_TAX = 0.001

# ======== 风控参数（硬过滤）========
DROP_FLOOR = -0.07
DROP_FLOOR_CYB = -0.12         # 创业板/科创板：跌超12%不买（±20%涨跌停，12%已接近危险区）
VOLUME_SPIKE_MULT = 3.0
CONSEC_DROP_LIMIT = -0.10
RECENT_LIMIT_DAYS = 5
EXCESS_DROP_LIMIT = -0.03

# ======== 星期效应参数 ========
# 排除表现差的星期几（0=周一, 4=周五）
# 先设为空，Cell 6会统计各星期表现，然后可以手动调整这里再跑
EXCLUDE_WEEKDAYS = []  # 例如 [3] 表示排除周四买入（周四买→周五卖）

# ======== 行业分散 ========
MAX_SAME_INDUSTRY = 1  # 同一行业最多买几只

# ======== 网格搜索参数 ========
TOP_N_LIST = [1, 3, 5, 10]
MIN_DROP_LIST = [-0.02, -0.03, -0.04, -0.05]
MIN_AMOUNT_LIST = [500, 1000, 3000]
MIN_CAP_LIST = [0, 20, 50]

# ======== 仓位权重模式 ========
# 'equal'=等权, 'signal'=按信号强度, 'cap'=按市值
WEIGHT_MODE_LIST = ['equal', 'signal']

print("=" * 60)
print("✅ Cell 1 配置完成")
print("=" * 60)
print(f"  📌 回测: {BACKTEST_YEARS}年 截止{END_DATE}")
print(f"  📌 成本: 买万{BUY_FEE*10000:.1f}+卖万{SELL_FEE*10000:.1f}+印花千{STAMP_TAX*1000:.0f}")
print(f"\n  🛡️ 风控:")
print(f"     跌幅区间: -{abs(DROP_FLOOR)*100:.0f}%~跌幅门槛 / 创业板-{abs(DROP_FLOOR_CYB)*100:.0f}%")
print(f"     放量: >{VOLUME_SPIKE_MULT:.0f}倍20日均量→不买")
print(f"     连跌: 3天>{abs(CONSEC_DROP_LIMIT)*100:.0f}%→不买")
print(f"     跌停史: {RECENT_LIMIT_DAYS}天内有→不买")
print(f"     超额跌: >{abs(EXCESS_DROP_LIMIT)*100:.0f}%→不买")
print(f"\n  🆕 v3.0新增:")
print(f"     星期排除: {EXCLUDE_WEEKDAYS if EXCLUDE_WEEKDAYS else '无（先看统计再决定）'}")
print(f"     行业分散: 同行业最多{MAX_SAME_INDUSTRY}只")
print(f"     仓位模式: {WEIGHT_MODE_LIST}")
combos = len(TOP_N_LIST) * len(MIN_DROP_LIST) * len(MIN_AMOUNT_LIST) * len(MIN_CAP_LIST) * len(WEIGHT_MODE_LIST)
print(f"     参数组合: {combos} 种")


# ============================================================
# Cell 2：拉取数据（日线 + 大盘 + 行业）
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第二步：拉取数据")
print(f"{'='*60}")

bt_start = pd.to_datetime(END_DATE) - timedelta(days=365 * BACKTEST_YEARS)
data_start = bt_start - timedelta(days=45)
bt_start_str = bt_start.strftime('%Y-%m-%d')
data_start_str = data_start.strftime('%Y-%m-%d')

all_trade_days = list(get_trade_days(start_date=data_start_str, end_date=END_DATE))
bt_trade_days = [d for d in all_trade_days if str(d) >= bt_start_str]
print(f"  回测交易日: {len(bt_trade_days)} 天")

# ---- 沪深300 ----
print(f"  拉取沪深300...")
hs300 = get_price('000300.XSHG', start_date=data_start_str, end_date=END_DATE,
                  frequency='daily', fields=['close'])
hs300['change_pct'] = hs300['close'].pct_change()
hs300_daily = {str(idx.date()): row['change_pct'] for idx, row in hs300.iterrows()}

# ---- 全A股（排除北交所）----
all_stocks = get_all_securities(types=['stock'], date=END_DATE)
all_codes = [c for c in all_stocks.index
             if not c.startswith('4') and not c.startswith('8')]
print(f"  全A股（排除北交所）: {len(all_codes)} 只")

if MAX_STOCKS < len(all_codes):
    all_codes = all_codes[:MAX_STOCKS]
    print(f"  ⚠️ 测试模式：前{MAX_STOCKS}只")

# ---- 行业映射（申万一级）----
print(f"  获取行业分类...")
industry_map = {}  # {code: 行业名}
for code in all_codes:
    try:
        ind = get_industry(code, date=END_DATE)
        if code in ind and 'sw_l1' in ind[code]:
            industry_map[code] = ind[code]['sw_l1']['industry_name']
    except:
        pass
print(f"  行业数据: {len(industry_map)} 只有分类, {len(set(industry_map.values()))} 个行业")

# ---- 个股日线 ----
print(f"  拉取个股日线...")
BATCH = 100
stock_daily = {}

for i in range(0, len(all_codes), BATCH):
    batch = all_codes[i:i+BATCH]
    # panel=True 返回 Panel 结构，方便按 code 取数据
    # 聚宽较新版本 panel=True 返回 dict of DataFrame
    df = get_price(batch, start_date=data_start_str, end_date=END_DATE,
                   frequency='daily',
                   fields=['open', 'close', 'high', 'low', 'money', 'paused'],
                   skip_paused=False, panel=False)
    
    # panel=False 返回的是带 code 列的长表（MultiIndex 或 code 列）
    if isinstance(df.index, pd.MultiIndex):
        # MultiIndex: (code, date) → 按第一级分组
        for code in batch:
            try:
                code_df = df.xs(code, level=0).copy()
            except KeyError:
                continue
            if len(code_df) < 30:
                continue
            stock_daily[code] = code_df
    elif 'code' in df.columns:
        # 扁平表，有 code 列
        for code in batch:
            code_df = df[df['code'] == code].copy()
            if len(code_df) < 30:
                continue
            code_df = code_df.set_index(code_df.index)  # 保留日期索引
            stock_daily[code] = code_df
    else:
        # 兜底：尝试 loc
        for code in batch:
            try:
                code_df = df.loc[code].copy()
                if len(code_df) < 30:
                    continue
                stock_daily[code] = code_df
            except:
                continue

    if (i // BATCH + 1) % 10 == 0:
        print(f"    {min(i+BATCH, len(all_codes))}/{len(all_codes)}")

print(f"\n  ✅ 个股日线: {len(stock_daily)} 只")


# ============================================================
# Cell 3：预计算风控指标 + 行业标注
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第三步：预计算指标")
print(f"{'='*60}")

def get_limit_pct(code):
    if code.startswith('300') or code.startswith('688'):
        return 0.20
    return 0.10

def get_drop_floor(code):
    if code.startswith('300') or code.startswith('688'):
        return DROP_FLOOR_CYB
    return DROP_FLOOR

all_records = []

for code_idx, code in enumerate(stock_daily.keys()):
    df = stock_daily[code].copy()
    limit_pct = get_limit_pct(code)
    drop_floor = get_drop_floor(code)
    industry = industry_map.get(code, '未知')

    df['prev_close'] = df['close'].shift(1)
    df['change_pct'] = (df['close'] - df['prev_close']) / df['prev_close']
    df['avg_money_20'] = df['money'].rolling(20, min_periods=10).mean()
    df['cum_change_3d'] = df['change_pct'].rolling(3, min_periods=1).sum()
    df['is_limit_down'] = (df['change_pct'] <= -(limit_pct - 0.005)).astype(int)
    df['had_limit_5d'] = df['is_limit_down'].rolling(RECENT_LIMIT_DAYS, min_periods=1).max()
    df['next_open'] = df['open'].shift(-1)
    df['next_paused'] = df['paused'].shift(-1)

    for idx, row in df.iterrows():
        day_str = str(idx.date())
        if day_str < bt_start_str:
            continue

        # 基本检查
        if pd.isna(row['change_pct']) or pd.isna(row['prev_close']):
            continue
        if row.get('paused', 0) == 1:
            continue
        if pd.isna(row['next_open']) or row.get('next_paused', 0) == 1:
            continue
        if row['close'] <= 0 or row['next_open'] <= 0:
            continue

        change = row['change_pct']

        # ====== 硬风控 ======
        if change <= -(limit_pct - 0.005):    # 跌停
            continue
        if change >= (limit_pct - 0.005):     # 涨停
            continue
        if change < drop_floor:               # 跌太多
            continue
        avg_money = row.get('avg_money_20', 0)
        if avg_money > 0 and row['money'] > avg_money * VOLUME_SPIKE_MULT:
            continue                          # 放量
        cum_3d = row.get('cum_change_3d', 0)
        if cum_3d < CONSEC_DROP_LIMIT:
            continue                          # 连跌
        if row.get('had_limit_5d', 0) >= 1:
            continue                          # 近期跌停
        market_change = hs300_daily.get(day_str, 0)
        excess_drop = change - market_change
        if excess_drop < EXCESS_DROP_LIMIT:
            continue                          # 超额跌幅

        # 星期过滤
        weekday = idx.weekday()  # 0=周一, 4=周五
        if weekday in EXCLUDE_WEEKDAYS:
            continue

        gross_return = row['next_open'] / row['close'] - 1
        cost = BUY_FEE + SELL_FEE + STAMP_TAX
        net_return = gross_return - cost

        all_records.append({
            'date': day_str,
            'weekday': weekday,
            'code': code,
            'industry': industry,
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
        print(f"    {code_idx+1}/{len(stock_daily)}, 已收集{len(all_records)}条")

trades_df = pd.DataFrame(all_records)
print(f"\n  ✅ 候选交易: {len(trades_df)} 条")
print(f"     {trades_df['date'].nunique()} 天, {trades_df['code'].nunique()} 只股票")
print(f"\n  毛收益统计:")
print(f"     均值: {trades_df['gross_return'].mean()*100:.3f}%")
print(f"     中位数: {trades_df['gross_return'].median()*100:.3f}%")
print(f"     胜率: {(trades_df['gross_return'] > 0).mean()*100:.1f}%")


# ============================================================
# Cell 4：市值 + ST过滤
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第四步：市值 & ST")
print(f"{'='*60}")

# 市值
monthly_cap = {}
current_month = None
codes_list = list(stock_daily.keys())

for day in bt_trade_days:
    day_str = str(day)
    month_key = day_str[:7]
    if month_key != current_month:
        current_month = month_key
        q = query(valuation.code, valuation.market_cap).filter(
            valuation.code.in_(codes_list))
        cap_df = get_fundamentals(q, date=day_str)
        monthly_cap[month_key] = dict(zip(cap_df['code'], cap_df['market_cap']))

trades_df['cap'] = trades_df.apply(
    lambda r: monthly_cap.get(r['date'][:7], {}).get(r['code'], 0), axis=1)

# ST
monthly_st = {}
current_month = None
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
        print(f"    {month_key}: {len(st_set)}只ST")

before = len(trades_df)
trades_df = trades_df[~trades_df.apply(
    lambda r: r['code'] in monthly_st.get(r['date'][:7], set()), axis=1
)].reset_index(drop=True)
print(f"\n  ✅ ST过滤: {before} → {len(trades_df)}")


# ============================================================
# Cell 5：星期效应分析（先看数据再决定是否排除）
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第五步：星期效应分析")
print(f"{'='*60}")

weekday_names = {0: '周一', 1: '周二', 2: '周三', 3: '周四', 4: '周五'}

print(f"\n  {'星期':<6} {'样本量':>6} {'均收益':>8} {'中位收益':>8} {'胜率':>6} {'盈亏比':>6}")
print(f"  {'-'*48}")

weekday_stats = {}
for wd in range(5):
    subset = trades_df[trades_df['weekday'] == wd]
    if len(subset) == 0:
        continue
    
    mean_r = subset['net_return'].mean()
    median_r = subset['net_return'].median()
    wr = (subset['net_return'] > 0).mean()
    wins = subset[subset['net_return'] > 0]['net_return']
    losses = subset[subset['net_return'] < 0]['net_return']
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.001
    plr = avg_win / avg_loss
    
    flag = ""
    if mean_r < 0:
        flag = " ⚠️ 负收益"
    elif wr >= 0.55:
        flag = " ✅ 表现优秀"
    
    print(f"  {weekday_names[wd]:<6} {len(subset):>6} {mean_r*100:>7.3f}% {median_r*100:>7.3f}% {wr*100:>5.1f}% {plr:>5.2f}{flag}")
    
    weekday_stats[wd] = {
        'count': len(subset), 'mean': mean_r, 'median': median_r,
        'win_rate': wr, 'pl_ratio': plr
    }

# 建议
print(f"\n  💡 建议:")
bad_days = [wd for wd, s in weekday_stats.items() if s['mean'] < 0]
good_days = [wd for wd, s in weekday_stats.items() if s['win_rate'] >= 0.55]

if bad_days:
    bad_names = [weekday_names[d] for d in bad_days]
    print(f"     ⚠️ {', '.join(bad_names)} 平均收益为负，建议排除")
    print(f"        修改Cell 1的 EXCLUDE_WEEKDAYS = {bad_days}")
if good_days:
    good_names = [weekday_names[d] for d in good_days]
    print(f"     ✅ {', '.join(good_names)} 表现最好（胜率≥55%）")


# ============================================================
# Cell 6：网格搜索（含仓位管理+行业分散）
# ============================================================

print(f"\n{'='*60}")
print(f"📊 第六步：网格搜索")
print(f"{'='*60}")

def apply_industry_limit(day_df, top_n, max_same=MAX_SAME_INDUSTRY):
    """
    行业分散选股：按跌幅排序，同行业最多选max_same只
    """
    day_df = day_df.sort_values('change_pct')  # 跌最多的排前面
    selected = []
    industry_count = {}
    
    for _, row in day_df.iterrows():
        ind = row['industry']
        cnt = industry_count.get(ind, 0)
        if cnt >= max_same:
            continue
        selected.append(row)
        industry_count[ind] = cnt + 1
        if len(selected) >= top_n:
            break
    
    return pd.DataFrame(selected) if selected else pd.DataFrame()

def calc_weights(selected_df, mode='equal'):
    """
    计算仓位权重
    - equal: 等权
    - signal: 按跌幅温和程度加权（跌得少的权重高，更安全）
    """
    n = len(selected_df)
    if n == 0:
        return []
    
    if mode == 'equal':
        return [1.0 / n] * n
    
    elif mode == 'signal':
        # 跌幅越接近0（温和），权重越高
        # change_pct 是负数，越接近0越温和
        # 用 1/(abs(change)+0.01) 作为原始权重
        changes = selected_df['change_pct'].values
        raw_weights = 1.0 / (np.abs(changes) + 0.01)
        return (raw_weights / raw_weights.sum()).tolist()
    
    return [1.0 / n] * n

results = []
total_combos = len(TOP_N_LIST) * len(MIN_DROP_LIST) * len(MIN_AMOUNT_LIST) * len(MIN_CAP_LIST) * len(WEIGHT_MODE_LIST)
combo_idx = 0

for top_n in TOP_N_LIST:
    for min_drop in MIN_DROP_LIST:
        for min_amount in MIN_AMOUNT_LIST:
            for min_cap in MIN_CAP_LIST:
                for weight_mode in WEIGHT_MODE_LIST:
                    combo_idx += 1
                    
                    filtered = trades_df[
                        (trades_df['change_pct'] <= min_drop) &
                        (trades_df['money'] >= min_amount * 10000) &
                        (trades_df['cap'] >= min_cap)
                    ]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # 每天：行业分散选股 + 权重计算
                    daily_selected = []
                    daily_weighted_returns = {}  # {date: 当天加权收益}
                    for date, day_df in filtered.groupby('date'):
                        sel = apply_industry_limit(day_df, top_n)
                        if len(sel) == 0:
                            continue
                        weights = calc_weights(sel, weight_mode)
                        sel = sel.copy()
                        sel['weight'] = weights
                        # 日收益 = Σ(各只收益 × 权重)，权重之和=1
                        daily_weighted_returns[date] = (sel['net_return'].values * np.array(weights)).sum()
                        daily_selected.append(sel)
                    
                    if not daily_selected:
                        continue
                    
                    selected = pd.concat(daily_selected, ignore_index=True)
                    
                    if len(selected) < 30:
                        continue
                    
                    # 统计
                    n_trades = len(selected)
                    n_days = len(daily_weighted_returns)
                    
                    # 日收益序列
                    daily_returns = pd.Series(daily_weighted_returns)
                    
                    mean_return = selected['net_return'].mean()
                    median_return = selected['net_return'].median()
                    win_rate = (selected['net_return'] > 0).mean()
                    
                    wins = selected[selected['net_return'] > 0]['net_return']
                    losses = selected[selected['net_return'] < 0]['net_return']
                    avg_win = wins.mean() if len(wins) > 0 else 0
                    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.001
                    plr = avg_win / avg_loss
                    
                    cumulative = (1 + daily_returns).cumprod()
                    total_return = cumulative.iloc[-1] - 1
                    peak = cumulative.expanding().max()
                    max_dd = ((cumulative - peak) / peak).min()
                    
                    daily_std = daily_returns.std()
                    sharpe = (daily_returns.mean() / daily_std * np.sqrt(250)) if daily_std > 0 else 0
                    
                    worst_trade = selected['net_return'].min()
                    n_industries = selected['industry'].nunique()
                    
                    label = f"Top{top_n}|跌≥{abs(min_drop)*100:.0f}%|额≥{min_amount}万|市值≥{min_cap}亿|{weight_mode}"
                    
                    results.append({
                        '策略': label,
                        'top_n': top_n,
                        'min_drop': min_drop,
                        'min_amount': min_amount,
                        'min_cap': min_cap,
                        'weight_mode': weight_mode,
                        '交易次数': n_trades,
                        '交易天数': n_days,
                        '日均笔数': round(n_trades / n_days, 1),
                        '平均净收益': mean_return,
                        '中位净收益': median_return,
                        '胜率': win_rate,
                        '盈亏比': plr,
                        '累计收益': total_return,
                        '最大回撤': max_dd,
                        '夏普比': sharpe,
                        '最大单笔亏': worst_trade,
                        '涉及行业数': n_industries,
                    })
                    
                    if combo_idx % 100 == 0:
                        print(f"    {combo_idx}/{total_combos}")

results_df = pd.DataFrame(results)
print(f"\n  ✅ 完成: {len(results_df)} 种有效组合")

# ---- 排行榜 ----
print(f"\n{'='*60}")
print(f"🏆 排行榜 Top 20（按夏普比）")
print(f"{'='*60}")

top20 = results_df.nlargest(20, '夏普比')
for rank, (_, row) in enumerate(top20.iterrows(), 1):
    print(f"\n  #{rank} {row['策略']}")
    print(f"    {row['交易次数']}笔/{row['交易天数']}天 | 胜率{row['胜率']*100:.1f}% | 盈亏比{row['盈亏比']:.2f}")
    print(f"    均收{row['平均净收益']*100:.3f}% | 累计{row['累计收益']*100:.1f}% | 回撤{row['最大回撤']*100:.1f}%")
    print(f"    夏普{row['夏普比']:.2f} | 最大亏{row['最大单笔亏']*100:.2f}% | {row['涉及行业数']}个行业")

# ---- 等权 vs 信号加权 对比 ----
print(f"\n{'='*60}")
print(f"📊 仓位模式对比：等权 vs 信号加权")
print(f"{'='*60}")

for mode in WEIGHT_MODE_LIST:
    subset = results_df[results_df['weight_mode'] == mode]
    if len(subset) == 0:
        continue
    best = subset.nlargest(1, '夏普比').iloc[0]
    print(f"\n  【{mode}】最佳: {best['策略']}")
    print(f"    夏普{best['夏普比']:.2f} | 胜率{best['胜率']*100:.1f}% | 累计{best['累计收益']*100:.1f}%")

# ---- 解读 ----
if len(top20) > 0:
    best = top20.iloc[0]
    print(f"\n{'='*60}")
    print(f"📝 综合解读")
    print(f"{'='*60}")
    
    if best['胜率'] >= 0.55:
        print(f"  ✅ 胜率{best['胜率']*100:.1f}% — 达标")
    elif best['胜率'] >= 0.50:
        print(f"  ⚠️ 胜率{best['胜率']*100:.1f}% — 需盈亏比补偿")
    else:
        print(f"  ❌ 胜率{best['胜率']*100:.1f}% — 不及格")
    
    if best['夏普比'] >= 1.5:
        print(f"  ✅ 夏普{best['夏普比']:.1f} — 优秀")
    elif best['夏普比'] >= 0.8:
        print(f"  ✅ 夏普{best['夏普比']:.1f} — 良好")
    else:
        print(f"  ⚠️ 夏普{best['夏普比']:.1f} — 需改进")
    
    if best['最大单笔亏'] > -0.05:
        print(f"  ✅ 尾部风险控制良好")
    else:
        print(f"  ⚠️ 最大单笔亏{best['最大单笔亏']*100:.1f}%，仍有尾部风险")
    
    print(f"\n  🛡️ v3.0风控效果:")
    print(f"     行业分散: 涉及{best['涉及行业数']}个行业")


# ============================================================
# Cell 7：资金模拟
# ============================================================
#
# 【资金流说明】
# 隔夜反转是完全串行的：
#   T日开盘卖出（昨天买的）→ 钱全回来
#   T日尾盘买入 → 钱全锁住
#   T+1日开盘卖出 → 钱全回来
#
# 不存在资金叠加占用的问题——买和卖不在同一时间，每天都是"全卖→全买"
# 所以直接用 capital += day_pnl 就行，不需要追踪holdings

print(f"\n{'='*60}")
print(f"💰 第七步：资金模拟")
print(f"{'='*60}")

if len(results_df) == 0:
    print("  ❌ 无有效策略")
else:
    bp = results_df.nlargest(1, '夏普比').iloc[0]
    sim_top_n = int(bp['top_n'])
    sim_min_drop = bp['min_drop']
    sim_min_amount = bp['min_amount']
    sim_min_cap = bp['min_cap']
    sim_weight_mode = bp['weight_mode']

    print(f"  策略: {bp['策略']}")

    INIT_CAPITAL = 50000
    MAX_PER_TRADE = 10000
    COMMISSION_PER_SIDE = 5
    CONSEC_LOSS_LIMIT = 10
    TOTAL_LOSS_LIMIT = 20000

    sim_filtered = trades_df[
        (trades_df['change_pct'] <= sim_min_drop) &
        (trades_df['money'] >= sim_min_amount * 10000) &
        (trades_df['cap'] >= sim_min_cap)
    ]

    capital = INIT_CAPITAL
    peak_capital = INIT_CAPITAL
    consecutive_loss_days = 0
    stopped = False
    stop_reason = ""
    daily_log = []
    equity_curve = []
    all_details = []

    for day_str in sorted(sim_filtered['date'].unique()):
        if stopped:
            break

        day_df = sim_filtered[sim_filtered['date'] == day_str]
        sel = apply_industry_limit(day_df, sim_top_n)
        if len(sel) == 0:
            continue

        weights = calc_weights(sel, sim_weight_mode)
        n_pos = len(sel)

        # 每只分配多少钱：按权重×可用资金，但单只不超过MAX_PER_TRADE
        investable = capital * 0.95  # 留5%缓冲

        day_pnl = 0
        day_commission = 0
        day_trade_count = 0

        for i, (_, trade) in enumerate(sel.iterrows()):
            alloc = investable * weights[i]
            alloc = min(alloc, MAX_PER_TRADE)

            shares = int(alloc / trade['close'] / 100) * 100
            if shares <= 0:
                continue

            buy_cost = shares * trade['close']
            sell_revenue = shares * trade['next_open']
            commission = COMMISSION_PER_SIDE * 2
            stamp = sell_revenue * STAMP_TAX
            pnl = sell_revenue - buy_cost - commission - stamp

            day_pnl += pnl
            day_commission += commission + stamp
            day_trade_count += 1

            all_details.append({
                'date': day_str,
                'code': trade['code'],
                'industry': trade['industry'],
                'shares': shares,
                'buy': trade['close'],
                'sell': trade['next_open'],
                'pnl': pnl,
                'weight': weights[i],
                'change': trade['change_pct'],
            })

        capital += day_pnl
        peak_capital = max(peak_capital, capital)

        daily_log.append({
            'date': day_str,
            'weekday': pd.to_datetime(day_str).weekday(),
            'n_trades': day_trade_count,
            'day_pnl': day_pnl,
            'commission': day_commission,
            'capital': capital,
        })
        equity_curve.append((day_str, capital))

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

    log_df = pd.DataFrame(daily_log)
    detail_df = pd.DataFrame(all_details)

    print(f"\n  {'='*50}")
    print(f"  📊 模拟结果")
    print(f"  {'='*50}")
    print(f"  初始: {INIT_CAPITAL:,.0f}元")
    print(f"  最终: {capital:,.0f}元")
    print(f"  收益: {capital - INIT_CAPITAL:+,.0f}元 ({(capital/INIT_CAPITAL-1)*100:+.1f}%)")
    print(f"  峰值: {peak_capital:,.0f}元")

    if len(log_df) > 0:
        max_dd = (peak_capital - log_df['capital'].min()) / peak_capital * 100
        print(f"  回撤: {max_dd:.1f}%")
        print(f"  手续费: {log_df['commission'].sum():,.0f}元")
        print(f"\n  交易: {len(log_df)}天, {len(detail_df)}笔")
        print(f"  盈利天: {(log_df['day_pnl']>0).sum()} ({(log_df['day_pnl']>0).mean()*100:.1f}%)")
        print(f"  日均: {log_df['day_pnl'].mean():+.1f}元")
        print(f"  最佳日: {log_df['day_pnl'].max():+.0f}元")
        print(f"  最差日: {log_df['day_pnl'].min():+.0f}元")

        # 星期效应（模拟中）
        print(f"\n  📅 模拟中的星期效应:")
        for wd in range(5):
            wd_log = log_df[log_df['weekday'] == wd]
            if len(wd_log) == 0:
                continue
            wd_wr = (wd_log['day_pnl'] > 0).mean()
            wd_avg = wd_log['day_pnl'].mean()
            flag = " ⚠️" if wd_avg < 0 else ""
            print(f"    {weekday_names[wd]}: {len(wd_log)}天, 胜率{wd_wr*100:.0f}%, 日均{wd_avg:+.0f}元{flag}")

        # 行业分布
        if len(detail_df) > 0:
            ind_stats = detail_df.groupby('industry').agg(
                笔数=('pnl', 'count'),
                总盈亏=('pnl', 'sum'),
                胜率=('pnl', lambda x: (x > 0).mean())
            ).sort_values('总盈亏', ascending=False)
            
            print(f"\n  🏭 行业盈亏 Top5 & Bottom5:")
            top5 = ind_stats.head(5)
            for ind, row in top5.iterrows():
                print(f"    ✅ {ind}: {row['笔数']:.0f}笔, {row['总盈亏']:+.0f}元, 胜率{row['胜率']*100:.0f}%")
            bottom5 = ind_stats.tail(5)
            for ind, row in bottom5.iterrows():
                if row['总盈亏'] < 0:
                    print(f"    ❌ {ind}: {row['笔数']:.0f}笔, {row['总盈亏']:+.0f}元, 胜率{row['胜率']*100:.0f}%")

        # 最大亏损
        if len(detail_df) > 0:
            worst = detail_df.nsmallest(3, 'pnl')
            print(f"\n  ⚠️ 最大亏损交易:")
            for _, w in worst.iterrows():
                print(f"    {w['date']} {w['code']}({w['industry']}): {w['pnl']:+.0f}元 (跌{w['change']*100:.1f}%)")

    if stopped:
        print(f"\n  🛑 风控停止: {stop_reason}")

    # 资金曲线
    print(f"\n  📈 月度资金:")
    eq_df = pd.DataFrame(equity_curve, columns=['date', 'capital'])
    eq_df['month'] = eq_df['date'].str[:7]
    monthly_eq = eq_df.groupby('month').last()
    for month, row in monthly_eq.iterrows():
        v = row['capital']
        pct = (v / INIT_CAPITAL - 1) * 100
        bar = "█" * max(0, min(int((v / INIT_CAPITAL - 0.5) * 40), 50))
        print(f"    {month}: {v:>8,.0f}元 ({pct:+5.1f}%) {bar}")
