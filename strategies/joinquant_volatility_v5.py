"""
波动率突破策略 v5 - 聚宽研究环境（纯信号验证版）
=================================

【策略验证三步走】
  第一步（本版本）：纯信号验证 — 每个信号假设都能成交，验证选股逻辑本身有没有edge
  第二步（后续）：加仓位管理 — 资金限制、最大持仓、手续费
  第三步（后续）：压力测试 — 牛熊分段、参数敏感性

【策略逻辑 v5.1】
  1. 选股：行业市值/营收前10% + 从1年高点回撤超2/3
  2. 买入信号：分钟K线涨幅突破历史极值 + 价格在1年高低点1/3分位以上
  3. 卖出：多日持有 + 移动止盈止损 + 保本机制 + 最长持有天数
  4. 回测：日K线级别止损模拟（省聚宽额度）

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
INDUSTRY_TOP_PCT = 0.10          # 行业内市值或营收前10%
DROP_RATIO = 1/3                 # 当前价 < 1年最高价 × 1/3（跌超2/3）
PRICE_POSITION_MIN = 1/3         # 价格必须在1年(最高-最低)的1/3分位以上

# ======== 回测参数 ========
END_DATE = '2026-02-25'
BACKTEST_YEARS = 3               # 回测3年
COOLDOWN_DAYS = 2                # 同一只股票信号冷却天数

# ======== 手续费估算 ========
COMMISSION_PER_SIDE = 5          # 买卖各5元
ASSUMED_TRADE_AMOUNT = 10000     # 假设每笔1万
FEE_RATE_PCT = COMMISSION_PER_SIDE * 2 / ASSUMED_TRADE_AMOUNT * 100  # ≈0.1%

# ======== 网格搜索参数空间 ========
# K线周期（用于信号触发判断）
FREQ_LIST = ['5m', '15m', '30m']

# 触发类型：只保留「突破历史最大值」（中位数×N已证明无edge）
# 回看周期
LOOKBACK_PERIODS = ['3m', '1y']

# ======== 卖出参数（多日持有 + 移动止盈止损）========
INITIAL_STOP_LIST = [0.05, 0.07]     # 初始止损：买入价下跌5%/7%
TRAILING_PROFIT_LIST = [0.08, 0.10]  # 移动止盈：从最高点回撤8%/10%
BREAKEVEN_TRIGGER = 0.05             # 浮盈超5%后止损上移到成本价
MAX_HOLD_DAYS_LIST = [10, 20]        # 最长持有天数

# ======== 计算参数组合数 ========
n_total = (len(FREQ_LIST) * len(LOOKBACK_PERIODS) *
           len(INITIAL_STOP_LIST) * len(TRAILING_PROFIT_LIST) *
           len(MAX_HOLD_DAYS_LIST))

print("=" * 60)
print("✅ Cell 1 配置完成（纯信号验证 v5.1）")
print("=" * 60)
print(f"  📌 选股: 行业市值/营收前{INDUSTRY_TOP_PCT*100:.0f}% + 从高点跌超{(1-DROP_RATIO)*100:.0f}%")
print(f"  📌 买入过滤: 价格在1年高低点{PRICE_POSITION_MIN*100:.0f}%分位以上（脱离底部）")
print(f"  📌 K线周期: {FREQ_LIST}")
print(f"  📌 触发: 突破历史最大涨幅（已砍掉中位数×N）")
print(f"  📌 回看周期: {LOOKBACK_PERIODS}")
print(f"  📌 卖出: 初始止损{[f'{x*100:.0f}%' for x in INITIAL_STOP_LIST]} | "
      f"移动止盈回撤{[f'{x*100:.0f}%' for x in TRAILING_PROFIT_LIST]} | "
      f"保本触发{BREAKEVEN_TRIGGER*100:.0f}% | "
      f"最长持有{MAX_HOLD_DAYS_LIST}天")
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 止损模拟: 日K线级别（省聚宽额度）")
print(f"  📌 参数组合: {n_total} 种")
print(f"  ⚠️ 纯信号模式：不模拟资金，每个信号假设都能成交")


# ============================================================
# Cell 2：构建滚动股票池（行业龙头 + 深度回撤）
# ============================================================

def build_rolling_pool(end_date, backtest_years=BACKTEST_YEARS,
                       top_pct=INDUSTRY_TOP_PCT, drop_ratio=DROP_RATIO,
                       price_pos_min=PRICE_POSITION_MIN):
    """
    构建滚动股票池：
      1. 按申万一级行业分组
      2. 每个行业取市值或营收前10%的股票
      3. 当前价 < 1年最高价 × 1/3（跌超2/3才入池）
    
    返回:
        pool_calendar: {股票代码: set(日期)}
        stock_info:    {股票代码: {'name', 'market_cap', 'industry'}}
    """
    print(f"\n{'='*60}")
    print(f"📊 第一步：构建滚动股票池（行业龙头+深度回撤）")
    print(f"{'='*60}")

    bt_start = pd.to_datetime(end_date) - timedelta(days=365 * backtest_years)
    data_start = bt_start - timedelta(days=365)  # 多拉1年算year_high
    bt_start_str = bt_start.strftime('%Y-%m-%d')
    data_start_str = data_start.strftime('%Y-%m-%d')

    all_trade_days = get_trade_days(start_date=bt_start_str, end_date=end_date)
    print(f"  回测区间: {bt_start_str} ~ {end_date} ({len(all_trade_days)}个交易日)")

    # ---- 获取所有非ST、上市>2年的股票 ----
    all_stocks = get_all_securities(types=['stock'], date=end_date)
    two_years_ago = (pd.to_datetime(end_date) - timedelta(days=365*2)).date()
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    valid_codes = [c for c in valid.index
                   if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]
    print(f"  非ST且上市>2年: {len(valid_codes)} 只")

    # ---- 按申万一级行业分组，每个行业取市值前10% ----
    trade_days_list = get_trade_days(end_date=end_date, count=5)
    last_trade = str(trade_days_list[-1])
    
    # 获取市值
    q = query(valuation.code, valuation.market_cap).filter(
        valuation.code.in_(valid_codes)
    )
    cap_df = get_fundamentals(q, date=last_trade)
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    
    # 获取行业分类（申万一级）
    industry_dict = {}
    for code in valid_codes:
        try:
            ind = get_industry(code, date=last_trade)
            if code in ind and 'sw_l1' in ind[code]:
                industry_dict[code] = ind[code]['sw_l1']['industry_name']
        except:
            continue
    
    # 每个行业取市值前10%
    industry_groups = {}
    for code, ind_name in industry_dict.items():
        if ind_name not in industry_groups:
            industry_groups[ind_name] = []
        cap = cap_dict.get(code, 0)
        if cap > 0:
            industry_groups[ind_name].append((code, cap))
    
    candidate_codes = []
    industry_stats = {}
    for ind_name, stocks in industry_groups.items():
        stocks_sorted = sorted(stocks, key=lambda x: -x[1])
        top_n = max(1, int(len(stocks_sorted) * top_pct))
        top_stocks = stocks_sorted[:top_n]
        candidate_codes.extend([s[0] for s in top_stocks])
        industry_stats[ind_name] = {
            'total': len(stocks_sorted),
            'selected': top_n,
            'min_cap': top_stocks[-1][1] if top_stocks else 0,
        }
    
    print(f"  行业数: {len(industry_groups)} 个")
    print(f"  行业前{top_pct*100:.0f}%筛选: {len(candidate_codes)} 只")
    
    # 显示各行业入选情况
    sorted_industries = sorted(industry_stats.items(), key=lambda x: -x[1]['selected'])
    print(f"\n  📋 各行业入选数量:")
    print(f"  {'行业':<12s}  {'总数':>4s}  {'入选':>4s}  {'最低市值(亿)':>10s}")
    for ind_name, stats in sorted_industries[:15]:
        print(f"  {ind_name:<12s}  {stats['total']:>4d}  {stats['selected']:>4d}  {stats['min_cap']:>10.0f}")
    if len(sorted_industries) > 15:
        print(f"  ... 还有 {len(sorted_industries)-15} 个行业")

    # ---- 逐只计算入池日期（从高点跌超2/3 + 价格在1/3分位以上）----
    print(f"\n  拉取日K线并计算入池日期...")
    pool_calendar = {}
    stock_info = {}
    total_pool_days = 0

    for i in range(0, len(candidate_codes), 50):
        batch = candidate_codes[i:i+50]
        prices = get_price(batch, start_date=data_start_str, end_date=end_date,
                           frequency='daily', fields=['high', 'low', 'close'], panel=True)
        for code in batch:
            try:
                if isinstance(prices['high'], pd.DataFrame):
                    highs = prices['high'][code].dropna()
                    lows = prices['low'][code].dropna()
                    closes = prices['close'][code].dropna()
                else:
                    continue
                if len(highs) < 300:
                    continue

                name = get_security_info(code).display_name
                cap = cap_dict.get(code, 0)
                ind = industry_dict.get(code, '未知')

                valid_dates = set()
                close_arr = closes.values
                high_arr = highs.values
                low_arr = lows.values
                dates_arr = closes.index

                for j in range(250, len(close_arr)):
                    year_high = high_arr[j-250:j].max()
                    year_low = low_arr[j-250:j].min()
                    current = close_arr[j]
                    d = str(dates_arr[j].date())
                    if d < bt_start_str:
                        continue
                    
                    # 条件1: 从高点跌超2/3
                    if current >= year_high * drop_ratio:
                        continue
                    
                    # 条件2: 价格在高低点的1/3分位以上（脱离底部）
                    price_range = year_high - year_low
                    if price_range <= 0:
                        continue
                    price_position = (current - year_low) / price_range
                    if price_position >= price_pos_min:
                        valid_dates.add(d)

                if valid_dates:
                    pool_calendar[code] = valid_dates
                    stock_info[code] = {'name': name, 'market_cap': cap, 'industry': ind}
                    total_pool_days += len(valid_dates)
            except:
                continue

        done = min(i+50, len(candidate_codes))
        print(f"    已处理 {done}/{len(candidate_codes)} ({done/len(candidate_codes)*100:.0f}%)")

    print(f"\n  ✅ 股票池构建完成!")
    print(f"  入池股票: {len(pool_calendar)} 只")
    if pool_calendar:
        print(f"  平均入池: {total_pool_days/len(pool_calendar):.0f} 天/只")
    
        # 按行业统计入池数
        ind_pool_count = {}
        for code in pool_calendar:
            ind = stock_info[code].get('industry', '未知')
            ind_pool_count[ind] = ind_pool_count.get(ind, 0) + 1
        
        print(f"\n  📋 入池股票行业分布:")
        for ind, cnt in sorted(ind_pool_count.items(), key=lambda x: -x[1])[:10]:
            print(f"    {ind}: {cnt}只")

        sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:20]
        print(f"\n  📋 入池天数TOP20:")
        print(f"  {'股票':>10s}  {'代码':>14s}  {'行业':>8s}  {'入池天数':>6s}  {'市值(亿)':>8s}")
        for code, dates in sorted_stocks:
            info = stock_info[code]
            print(f"  {info['name']:>10s}  {code:>14s}  {info['industry']:>8s}  {len(dates):>6d}天  {info['market_cap']:>8.0f}")

    return pool_calendar, stock_info

pool_calendar, stock_info = build_rolling_pool(END_DATE)
print(f"\n✅ Cell 2 完成")


# ============================================================
# Cell 3：核心引擎（纯信号验证 + 日K线多日止损模拟）
# ============================================================

def get_lookback_bars(freq, period):
    """计算回看需要多少根K线"""
    bars_per_day = {'5m': 48, '15m': 16, '30m': 8}
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def simulate_daily_exit(stock_code, buy_date, buy_price,
                        initial_stop_pct, trailing_profit_pct,
                        breakeven_trigger_pct, max_hold_days,
                        end_date=END_DATE):
    """
    用日K线模拟多日持有的止盈止损

    逻辑（每天按顺序检查）：
      1. 当日最低价 <= 止损价 → 以止损价卖出
      2. 当日最高价创新高 → 更新移动止盈线
      3. 如果浮盈曾超过breakeven_trigger → 止损上移到成本价
      4. 当日最低价 <= 移动止盈线 → 以止盈线价格卖出
      5. 持有天数到上限 → 以收盘价卖出

    注意：日K线无法区分盘中高低点先后顺序
      保守处理：同一天最高最低都触发时，优先触发止损（假设最坏情况）

    返回:
        (卖出价, 卖出方式, 持有天数, 盘中最高价, 最大浮盈%) 或 None
    """
    # 获取买入日之后的日K线
    buy_dt = pd.to_datetime(buy_date)
    fetch_end = min(
        (buy_dt + timedelta(days=max_hold_days * 2 + 10)).strftime('%Y-%m-%d'),
        end_date
    )
    
    daily = get_price(stock_code, start_date=buy_date, end_date=fetch_end,
                      frequency='daily', fields=['open', 'high', 'low', 'close'])
    if daily is None or len(daily) < 2:
        return None
    
    # 跳过买入当天，从次日开始
    buy_date_dt = pd.to_datetime(buy_date).date()
    daily = daily[daily.index.date > buy_date_dt]
    if len(daily) == 0:
        return None
    
    # 初始化
    stop_price = buy_price * (1 - initial_stop_pct)  # 初始止损价
    highest_since_buy = buy_price                     # 持有期间最高价
    trailing_sell_price = 0                           # 移动止盈触发价（未激活时为0）
    breakeven_activated = False                       # 保本机制是否激活
    
    trade_days = get_trade_days(start_date=buy_date, end_date=fetch_end)
    trade_days_after_buy = [d for d in trade_days if d > buy_date_dt]
    
    for day_idx, date in enumerate(trade_days_after_buy):
        if day_idx >= max_hold_days:
            break
        
        # 找当天的K线
        day_data = daily[daily.index.date == date]
        if len(day_data) == 0:
            continue
        
        bar = day_data.iloc[0]
        day_high = bar['high']
        day_low = bar['low']
        day_close = bar['close']
        hold_days = day_idx + 1
        
        # ---- 1. 检查止损 ----
        if day_low <= stop_price:
            max_profit_pct = (highest_since_buy - buy_price) / buy_price * 100
            sell_type = '保本止损' if breakeven_activated and stop_price >= buy_price else '初始止损'
            return (round(stop_price, 3), sell_type, hold_days,
                    round(highest_since_buy, 3), round(max_profit_pct, 2))
        
        # ---- 2. 更新最高价 ----
        if day_high > highest_since_buy:
            highest_since_buy = day_high
        
        # ---- 3. 检查保本触发 ----
        current_profit_pct = (highest_since_buy - buy_price) / buy_price
        if not breakeven_activated and current_profit_pct >= breakeven_trigger_pct:
            breakeven_activated = True
            stop_price = buy_price  # 止损上移到成本价
        
        # ---- 4. 计算并检查移动止盈 ----
        trailing_sell_price = highest_since_buy * (1 - trailing_profit_pct)
        
        # 移动止盈线高于止损线时才用移动止盈
        if trailing_sell_price > stop_price:
            if day_low <= trailing_sell_price:
                max_profit_pct = (highest_since_buy - buy_price) / buy_price * 100
                return (round(trailing_sell_price, 3), f'移动止盈(回撤{trailing_profit_pct*100:.0f}%)',
                        hold_days, round(highest_since_buy, 3), round(max_profit_pct, 2))
    
    # ---- 5. 到期卖出 ----
    if len(daily) > 0:
        last_bar = daily.iloc[min(max_hold_days - 1, len(daily) - 1)]
        sell_price = last_bar['close']
        max_profit_pct = (highest_since_buy - buy_price) / buy_price * 100
        return (round(sell_price, 3), f'到期卖出({max_hold_days}天)',
                min(max_hold_days, len(daily)), round(highest_since_buy, 3),
                round(max_profit_pct, 2))
    
    return None


def backtest_signals(pool_calendar, stock_info, max_stocks,
                     freq, period,
                     initial_stop_pct, trailing_profit_pct,
                     breakeven_trigger_pct, max_hold_days,
                     end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    纯信号回测：
      1. 分钟K线找买入信号（突破历史极值 + 价格分位过滤）
      2. 日K线模拟多日持有的止盈止损
    """
    lookback_days_map = {'3m': 63, '1y': 250}
    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:max_stocks]
    
    all_signals = []
    stock_summaries = []
    
    for si, (code, valid_dates) in enumerate(sorted_stocks):
        name = stock_info[code]['name']
        industry = stock_info[code].get('industry', '')
        
        try:
            earliest = min(valid_dates)
            data_start = (pd.to_datetime(earliest) - timedelta(
                days=lookback_days_map[period] * 2)).strftime('%Y-%m-%d')
            
            # 分钟K线（用于信号触发）
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
            last_sell_date = None  # 上一笔卖出日期，避免持仓重叠
            
            for i, date in enumerate(trade_dates):
                date_str = str(date)
                
                # 冷却期
                if i - last_signal_date_idx <= cooldown:
                    continue
                
                # 如果上一笔还没卖出，跳过
                if last_sell_date and date_str <= last_sell_date:
                    continue
                
                day_bars = min_df[min_df['date'] == date]
                if len(day_bars) < 5:
                    continue
                
                hist = min_df[min_df['date'] < date]
                if len(hist) < lookback_bars:
                    continue
                
                hist_returns = hist['bar_return'].iloc[-lookback_bars:]
                
                # 阈值：突破历史最大涨幅
                threshold = hist_returns.max()
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
                
                # ★ 日K线模拟多日止盈止损
                sell_result = simulate_daily_exit(
                    code, date_str, trigger_price,
                    initial_stop_pct, trailing_profit_pct,
                    breakeven_trigger_pct, max_hold_days,
                    end_date=end_date
                )
                
                if sell_result is None:
                    continue
                
                sell_price, sell_type, hold_days, highest, max_profit_pct = sell_result
                
                # 收益率
                raw_ret = (sell_price - trigger_price) / trigger_price * 100
                net_ret = raw_ret - FEE_RATE_PCT
                
                last_signal_date_idx = i
                # 计算卖出日期，避免持仓重叠
                sell_trade_days = get_trade_days(start_date=date_str, count=hold_days + 1)
                if len(sell_trade_days) > hold_days:
                    last_sell_date = str(sell_trade_days[hold_days])
                
                signal = {
                    '日期': date_str,
                    '股票': name,
                    '代码': code,
                    '行业': industry,
                    '触发时间': str(trigger_bar),
                    '买入价': trigger_price,
                    'K线涨幅%': round(trigger_return * 100, 2),
                    '阈值%': round(threshold * 100, 2),
                    '卖出价': sell_price,
                    '卖出方式': sell_type,
                    '持有天数': hold_days,
                    '盘中最高': highest,
                    '最大浮盈%': round(max_profit_pct, 2),
                    '毛收益率%': round(raw_ret, 2),
                    '净收益率%': round(net_ret, 2),
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
                avg_hold = np.mean([s['持有天数'] for s in stock_signals])
                
                stock_summaries.append({
                    '股票': name,
                    '代码': code,
                    '行业': industry,
                    '入池天数': len(valid_dates),
                    '信号数': total,
                    '胜率%': round(wins/total*100, 1),
                    '平均收益%': round(np.mean(rets), 2),
                    '平均盈利%': round(avg_win, 2),
                    '平均亏损%': round(avg_loss, 2),
                    '盈亏比': round(pl_ratio, 2) if pl_ratio != float('inf') else 999,
                    '平均持有天': round(avg_hold, 1),
                })
        except:
            continue
    
    if not all_signals:
        return None
    
    # ---- 汇总统计 ----
    rets = [s['净收益率%'] for s in all_signals]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    hold_days_list = [s['持有天数'] for s in all_signals]
    
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
    
    # 期望值
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
        '平均持有天数': round(np.mean(hold_days_list), 1),
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

MAX_STOCKS = 20  # 取入池天数最多的前20只

print(f"\n{'='*60}")
print(f"📊 第二步：网格搜索（前{MAX_STOCKS}只股票）")
print(f"{'='*60}")

# 构建参数网格
param_grid = []
for freq in FREQ_LIST:
    for period in LOOKBACK_PERIODS:
        for init_stop in INITIAL_STOP_LIST:
            for trail_profit in TRAILING_PROFIT_LIST:
                for max_days in MAX_HOLD_DAYS_LIST:
                    label = (f"{freq}|回看{period}|"
                             f"止损{init_stop*100:.0f}%|"
                             f"止盈回撤{trail_profit*100:.0f}%|"
                             f"持有{max_days}天")
                    param_grid.append({
                        'freq': freq,
                        'period': period,
                        'initial_stop': init_stop,
                        'trailing_profit': trail_profit,
                        'max_hold_days': max_days,
                        'label': label,
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
        initial_stop_pct=params['initial_stop'],
        trailing_profit_pct=params['trailing_profit'],
        breakeven_trigger_pct=BREAKEVEN_TRIGGER,
        max_hold_days=params['max_hold_days'],
    )
    
    if result:
        s = result['汇总']
        grid_results.append({
            '策略': label,
            'K线周期': params['freq'],
            '回看周期': params['period'],
            '初始止损': f"{params['initial_stop']*100:.0f}%",
            '止盈回撤': f"{params['trailing_profit']*100:.0f}%",
            '最长持有': f"{params['max_hold_days']}天",
            '信号数': s['总信号数'],
            '胜率%': s['胜率%'],
            '盈亏比': s['盈亏比'],
            '平均净收益%': s['平均净收益%'],
            '收益中位数%': s['收益中位数%'],
            '期望值%': s['期望值%'],
            '最大连亏': s['最大连亏次数'],
            '平均持有天': s['平均持有天数'],
            '平均盈利%': s['平均盈利%'],
            '平均亏损%': s['平均亏损%'],
            '最大单笔盈%': s['最大单笔盈利%'],
            '最大单笔亏%': s['最大单笔亏损%'],
            '有信号股票数': s['有信号股票数'],
            '卖出分布': s['卖出方式分布'],
            '_result': result,
        })
        print(f"      → 信号{s['总信号数']}个 | 胜率{s['胜率%']}% | 盈亏比{s['盈亏比']} | "
              f"期望值{s['期望值%']:+.3f}% | 均持有{s['平均持有天数']:.1f}天 | 连亏max{s['最大连亏次数']}")
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
    gdf['信号量得分'] = gdf['信号数'].apply(lambda x: min(x / 50, 1.0) * 100)
    gdf['期望值得分'] = (gdf['期望值%'] * 50).clip(-100, 100)  # 放大50倍（多日持有收益率更大）
    gdf['盈亏比得分'] = gdf['盈亏比'].clip(0, 5) * 20
    gdf['综合评分'] = (
        gdf['期望值得分'] * 0.4 +
        gdf['盈亏比得分'] * 0.2 +
        gdf['胜率%'] * 0.2 +
        gdf['信号量得分'] * 0.2
    ).round(1)
    
    gdf = gdf.sort_values('综合评分', ascending=False)
    
    print(f"\n{'='*60}")
    print(f"🏆 策略排行榜 TOP 20（纯信号验证 v5.1）")
    print(f"{'='*60}")
    print(f"  评分 = 期望值×40% + 盈亏比×20% + 胜率×20% + 信号量×20%")
    print(f"  ⚠️ 纯信号验证，假设每笔都能成交\n")
    
    show_cols = ['策略', '信号数', '胜率%', '盈亏比', '平均净收益%',
                 '期望值%', '平均持有天', '最大连亏', '综合评分']
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
              f"盈亏比{sub['盈亏比'].mean():.2f} | "
              f"均持有{sub['平均持有天'].mean():.1f}天")
    
    print(f"\n{'='*60}")
    print("📈 按初始止损汇总")
    print(f"{'='*60}")
    for stop in INITIAL_STOP_LIST:
        sub = gdf[gdf['初始止损'] == f"{stop*100:.0f}%"]
        if sub.empty:
            continue
        print(f"  止损{stop*100:.0f}%: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"盈亏比{sub['盈亏比'].mean():.2f}")
    
    print(f"\n{'='*60}")
    print("📈 按移动止盈回撤汇总")
    print(f"{'='*60}")
    for tp in TRAILING_PROFIT_LIST:
        sub = gdf[gdf['止盈回撤'] == f"{tp*100:.0f}%"]
        if sub.empty:
            continue
        print(f"  回撤{tp*100:.0f}%: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"盈亏比{sub['盈亏比'].mean():.2f}")
    
    print(f"\n{'='*60}")
    print("📈 按最长持有天数汇总")
    print(f"{'='*60}")
    for md in MAX_HOLD_DAYS_LIST:
        sub = gdf[gdf['最长持有'] == f"{md}天"]
        if sub.empty:
            continue
        print(f"  {md}天: 期望值{sub['期望值%'].mean():+.3f}% | "
              f"胜率{sub['胜率%'].mean():.1f}% | "
              f"均持有{sub['平均持有天'].mean():.1f}天")
    
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
    print(f"  平均持有:     {best['平均持有天']:.1f} 天")
    print(f"  最大连亏:     {best['最大连亏']:.0f} 次")
    print(f"  最大单笔盈:   {best['最大单笔盈%']:+.2f}%")
    print(f"  最大单笔亏:   {best['最大单笔亏%']:+.2f}%")
    
    # 卖出方式分布
    sell_dist = best.get('卖出分布', {})
    if sell_dist:
        total_sig = best['信号数']
        print(f"\n  卖出方式分布:")
        for st, cnt in sorted(sell_dist.items(), key=lambda x: -x[1]):
            print(f"    {st}: {cnt}笔 ({cnt/total_sig*100:.1f}%)")
    
    # 解读
    print(f"\n  💡 策略是否有edge?")
    ev = best['期望值%']
    if ev > 0.5:
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
        print(f"     ⚠️ 胜率高但盈亏比{pr:.1f}偏低，赚的不够多")
    
    avg_hold = best['平均持有天']
    print(f"     📅 平均持有{avg_hold:.1f}天，年化交易约{250/avg_hold:.0f}次（单只）")


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
        show = ['股票', '行业', '信号数', '胜率%', '盈亏比', '平均收益%', '平均持有天']
        print(sdf[show].to_string(index=False))
        
        profitable = len(sdf[sdf['平均收益%'] > 0])
        losing = len(sdf[sdf['平均收益%'] <= 0])
        print(f"\n  {profitable}只平均赚钱，{losing}只平均亏钱")
    
    # 信号明细
    if best_result['信号列表']:
        tlog = pd.DataFrame(best_result['信号列表'])
        print(f"\n{'='*60}")
        print(f"📋 信号明细（共{len(tlog)}笔）")
        print(f"{'='*60}")
        
        show_cols_detail = ['日期', '股票', '买入价', '卖出价', '卖出方式',
                           '持有天数', '最大浮盈%', '净收益率%']
        
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
        bins = [(-999, -7), (-7, -5), (-5, -3), (-3, 0),
                (0, 3), (3, 5), (5, 10), (10, 20), (20, 999)]
        labels = ['<-7%', '-7~-5%', '-5~-3%', '-3~0%',
                  '0~3%', '3~5%', '5~10%', '10~20%', '>20%']
        for (lo, hi), label in zip(bins, labels):
            cnt = len(rets[(rets > lo) & (rets <= hi)])
            bar = '█' * int(cnt / max(len(rets), 1) * 50)
            print(f"  {label:>8s}: {cnt:>3d}笔 ({cnt/max(len(rets),1)*100:>5.1f}%) {bar}")
        
        # 持有天数分布
        print(f"\n{'='*60}")
        print(f"📊 持有天数分布")
        print(f"{'='*60}")
        hdays = tlog['持有天数']
        for d in sorted(hdays.unique()):
            cnt = len(hdays[hdays == d])
            avg_r = tlog[hdays == d]['净收益率%'].mean()
            bar = '█' * int(cnt / max(len(hdays), 1) * 50)
            print(f"  {d:>2d}天: {cnt:>3d}笔 (均收{avg_r:+.2f}%) {bar}")
        
        # 按月统计
        tlog['月份'] = pd.to_datetime(tlog['日期']).dt.to_period('M').astype(str)
        monthly = tlog.groupby('月份').agg(
            信号数=('净收益率%', 'count'),
            胜率=('净收益率%', lambda x: f"{(x > 0).sum() / len(x) * 100:.0f}%"),
            平均收益=('净收益率%', lambda x: f"{x.mean():+.2f}%"),
            均持有天=('持有天数', lambda x: f"{x.mean():.1f}"),
        )
        print(f"\n{'='*60}")
        print(f"📅 按月统计")
        print(f"{'='*60}")
        print(monthly.to_string())
