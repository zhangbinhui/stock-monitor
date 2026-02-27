"""
波动率突破策略 v5 - 聚宽研究环境
=================================

【策略逻辑（白话版）】
1. 选股：从全A股里找「跌惨了的大票」（市值≥50亿，股价相对1年高点大幅回撤）
2. 盯盘：用分钟K线（5分/15分/30分）盘中监控，看有没有「异常大涨」
3. 买入：某根K线涨幅突破历史极值 → 信号触发 → 以当前价买入
4. 卖出：次日用「移动止损」跟踪盘中最高价，回撤到一定比例就卖
5. 资金模拟：5万本金，单笔不超过1万，带风控

【v5 相比 v4.2 的改动】
  - 合并信号回测和资金模拟（不再重复跑两遍）
  - 去掉均值触发（只保留中位数），去掉1分钟K线
  - 手续费统一为固定5元/次（更接近实际最低收费）
  - 卖出策略升级：固定「次日收盘卖」→「移动止损 + 保底止损 + 兜底收盘卖」
  - 移动止损回撤比例纳入参数搜索（3%、5%）

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
MIN_MARKET_CAP = 50e8           # 最低市值50亿
MAX_PRICE_RATIO_DEFAULT = 1/3   # 默认：当前价 < 1年最高价 × 1/3（跌67%）
MARKET_CAP_TIERS = [
    (100e8, 1/2),   # 100亿以上：跌50%入池
    (50e8,  1/3),   # 50亿以上：跌67%入池
]

# ======== 回测参数 ========
END_DATE = '2026-02-25'
BACKTEST_YEARS = 3              # 回测3年（2023.2~2026.2）
COOLDOWN_DAYS = 2               # 信号冷却天数

# ======== 手续费（固定金额）========
COMMISSION_PER_SIDE = 5         # 买卖各5元（券商最低收费标准）

# ======== 网格搜索参数空间 ========
# K线周期（去掉了1分钟，太频繁且实际来不及操作）
FREQ_LIST = ['5m', '15m', '30m']

# 触发类型
#   A类「突破最大值」: K线涨幅 > 历史最大值
#   B类「中位数×N倍」: K线涨幅 > 历史正涨幅中位数 × N倍（去掉了均值，中位数更稳定）
MULT_LIST = [1.5, 2.0, 2.5, 3.0]

# 回看周期
LOOKBACK_PERIODS = ['3m', '1y']

# ★ 卖出参数：移动止损回撤比例
TRAILING_STOP_LIST = [0.03, 0.05]  # 3% 和 5%
FLOOR_STOP = 0.03                   # 保底止损：跌破买入价3%无条件走

# ======== 资金模拟参数 ========
INIT_CAPITAL = 50000            # 初始资金5万
MAX_PER_TRADE = 5000            # 单笔最大5千
MAX_POSITIONS = 10              # 同时最多10只（5万÷5千）
DAILY_LOSS_LIMIT = -1000        # 日亏上限
MAX_CONSECUTIVE_LOSS = 10       # 连亏暂停
TOTAL_LOSS_LIMIT = -20000       # 总亏上限

# ======== 显示配置 ========
# 计算参数组合数
n_trigger = 1 + len(MULT_LIST)  # 1个A类 + N个B类
n_trailing = len(TRAILING_STOP_LIST)
n_total = len(FREQ_LIST) * n_trigger * len(LOOKBACK_PERIODS) * n_trailing

print("=" * 60)
print("✅ Cell 1 配置完成")
print("=" * 60)
print(f"  📌 股票池: 市值≥{MIN_MARKET_CAP/1e8:.0f}亿，从1年高点大幅回撤")
print(f"  📌 K线周期: {FREQ_LIST}（去掉了1分钟）")
print(f"  📌 触发方式: A类(突破最大值) + B类(中位数×{MULT_LIST})（去掉了均值）")
print(f"  📌 回看周期: {LOOKBACK_PERIODS}")
print(f"  📌 卖出策略: 移动止损(回撤{[f'{x*100:.0f}%' for x in TRAILING_STOP_LIST]}) + 保底止损({FLOOR_STOP*100:.0f}%) + 兜底收盘卖")
print(f"  📌 回测区间: {BACKTEST_YEARS}年（截止{END_DATE}）")
print(f"  📌 手续费: 买卖各{COMMISSION_PER_SIDE}元（固定）")
print(f"  📌 参数组合: {n_total} 种")
print(f"  📌 资金: {INIT_CAPITAL/10000:.0f}万本金, 单笔≤{MAX_PER_TRADE:.0f}元, 最多同时{MAX_POSITIONS}只")


# ============================================================
# Cell 2：构建滚动股票池（和v4.2完全一样）
# ============================================================

def build_rolling_pool(end_date, backtest_years=BACKTEST_YEARS,
                       min_cap=MIN_MARKET_CAP, tiers=MARKET_CAP_TIERS):
    """
    构建滚动股票池：每个交易日判断哪些股票「跌惨了」
    
    返回:
        pool_calendar: {股票代码: set(日期)} 每只股票哪些天在池子里
        stock_info:    {股票代码: {'name': 名称, 'market_cap': 市值(亿)}}
    """
    print(f"\n{'='*60}")
    print(f"📊 第一步：构建滚动股票池")
    print(f"{'='*60}")

    bt_start = pd.to_datetime(end_date) - timedelta(days=365 * backtest_years)
    data_start = bt_start - timedelta(days=365)
    bt_start_str = bt_start.strftime('%Y-%m-%d')
    data_start_str = data_start.strftime('%Y-%m-%d')

    all_trade_days = get_trade_days(start_date=bt_start_str, end_date=end_date)
    print(f"  回测区间: {bt_start_str} ~ {end_date} ({len(all_trade_days)}个交易日)")

    # 过滤ST、上市不满2年
    all_stocks = get_all_securities(types=['stock'], date=end_date)
    two_years_ago = (pd.to_datetime(end_date) - timedelta(days=365*2)).date()
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    valid_codes = [c for c in valid.index
                   if not get_security_info(c).display_name.startswith('ST')
                   and not get_security_info(c).display_name.startswith('*ST')]
    print(f"  非ST且上市>2年: {len(valid_codes)} 只")

    # 市值初筛
    trade_days_list = get_trade_days(end_date=end_date, count=5)
    last_trade = str(trade_days_list[-1])
    q = query(valuation.code, valuation.market_cap).filter(
        valuation.code.in_(valid_codes),
        valuation.market_cap >= min_cap / 1e8 * 0.5
    )
    cap_df = get_fundamentals(q, date=last_trade)
    candidate_codes = list(cap_df['code'])
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    print(f"  市值初筛(≥{min_cap/1e8*0.5:.0f}亿): {len(candidate_codes)} 只")

    # 逐只计算入池日期
    print(f"  拉取日K线并计算...")
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
                ratio = MAX_PRICE_RATIO_DEFAULT
                for tier_cap, tier_ratio in tiers:
                    if cap * 1e8 >= tier_cap:
                        ratio = tier_ratio
                        break

                valid_dates = set()
                close_arr = closes.values
                high_arr = highs.values
                dates_arr = closes.index
                for j in range(250, len(close_arr)):
                    year_high = high_arr[j-250:j].max()
                    current = close_arr[j]
                    d = str(dates_arr[j].date())
                    if d < bt_start_str:
                        continue
                    if current < year_high * ratio:
                        valid_dates.add(d)

                if valid_dates:
                    pool_calendar[code] = valid_dates
                    stock_info[code] = {'name': name, 'market_cap': cap}
                    total_pool_days += len(valid_dates)
            except:
                continue

        done = min(i+50, len(candidate_codes))
        print(f"    已处理 {done}/{len(candidate_codes)} ({done/len(candidate_codes)*100:.0f}%)")

    print(f"\n  ✅ 股票池构建完成!")
    print(f"  入池股票: {len(pool_calendar)} 只")
    print(f"  平均入池: {total_pool_days/max(len(pool_calendar),1):.0f} 天/只")

    if pool_calendar:
        sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:20]
        print(f"\n  📋 入池天数TOP20:")
        print(f"  {'股票':>10s}  {'代码':>14s}  {'入池天数':>6s}  {'市值(亿)':>8s}")
        for code, dates in sorted_stocks:
            info = stock_info[code]
            print(f"  {info['name']:>10s}  {code:>14s}  {len(dates):>6d}天  {info['market_cap']:>8.0f}")

    return pool_calendar, stock_info

pool_calendar, stock_info = build_rolling_pool(END_DATE)
print(f"\n✅ Cell 2 完成")


# ============================================================
# Cell 3：核心引擎（信号回测 + 资金模拟 合并版）
# ============================================================
#
# 【改动】
#   - 卖出策略：移动止损（跟踪次日盘中最高价，回撤N%就卖）
#   - 保底止损：跌破买入价3%无条件卖
#   - 兜底：次日收盘前都没触发 → 收盘卖
#   - 信号收集和资金模拟在一个流程里完成

def get_lookback_bars(freq, period):
    """计算回看需要多少根K线"""
    bars_per_day = {'5m': 48, '15m': 16, '30m': 8}
    days = {'3m': 63, '1y': 250}
    return bars_per_day[freq] * days[period]


def simulate_trailing_stop_sell(stock_code, buy_date, buy_price, freq, trailing_pct, floor_stop_pct):
    """
    模拟次日的移动止损卖出
    
    逻辑：
      1. 拿到次日的分钟K线
      2. 逐根跟踪盘中最高价
      3. 如果当前价从最高价回撤 trailing_pct → 卖出（移动止损触发）
      4. 如果当前价跌破买入价 × (1 - floor_stop_pct) → 卖出（保底止损触发）
      5. 收盘前都没触发 → 以收盘价卖出（兜底）
    
    参数:
        stock_code:     股票代码
        buy_date:       买入日期（字符串）
        buy_price:      买入价
        freq:           K线周期（用于拉次日分钟线）
        trailing_pct:   移动止损回撤比例（0.03=3%）
        floor_stop_pct: 保底止损比例（0.03=3%）
    
    返回:
        (卖出价, 卖出方式) 或 None（无法获取次日数据）
    """
    # 找次日日期
    next_days = get_trade_days(start_date=buy_date, count=3)  # 拿3天确保有次日
    if len(next_days) < 2:
        return None
    
    # 找到buy_date在列表中的位置，取下一个交易日
    buy_date_pd = pd.to_datetime(buy_date).date()
    next_day = None
    for d in next_days:
        if d > buy_date_pd:
            next_day = d
            break
    if next_day is None:
        return None
    
    next_day_str = str(next_day)
    
    # 拉取次日的分钟K线
    next_day_end = (pd.to_datetime(next_day_str) + timedelta(days=1)).strftime('%Y-%m-%d')
    min_bars = get_price(stock_code, start_date=next_day_str, end_date=next_day_end,
                         frequency=freq, fields=['open', 'close', 'high', 'low'])
    
    if min_bars is None or len(min_bars) == 0:
        return None
    
    # 只要次日的K线
    min_bars = min_bars[min_bars.index.date == next_day]
    if len(min_bars) == 0:
        return None
    
    # 保底止损价
    floor_price = buy_price * (1 - floor_stop_pct)
    
    # 逐根K线模拟
    intraday_high = 0  # 盘中最高价（实时更新）
    
    for idx in range(len(min_bars)):
        bar = min_bars.iloc[idx]
        bar_high = bar['high']
        bar_low = bar['low']
        bar_close = bar['close']
        
        # 更新盘中最高价
        if bar_high > intraday_high:
            intraday_high = bar_high
        
        # 检查保底止损（优先级最高）
        if bar_low <= floor_price:
            return (round(floor_price, 3), '保底止损')
        
        # 检查移动止损（盘中最高价回撤trailing_pct）
        if intraday_high > 0:
            trailing_price = intraday_high * (1 - trailing_pct)
            if bar_low <= trailing_price:
                # 以移动止损价卖出（取trailing_price和bar_close的较低者，更保守）
                sell_p = min(trailing_price, bar_close)
                return (round(sell_p, 3), f'移动止损{trailing_pct*100:.0f}%')
    
    # 收盘还没触发 → 以最后一根K线收盘价卖
    last_close = min_bars.iloc[-1]['close']
    return (round(last_close, 3), '次日收盘卖')


def backtest_and_simulate(pool_calendar, stock_info, max_stocks,
                          freq, period, signal_type, multiplier,
                          trailing_pct,
                          end_date=END_DATE, cooldown=COOLDOWN_DAYS):
    """
    单组参数的完整回测+资金模拟
    
    流程：
      1. 遍历股票池中的股票
      2. 对每只股票，扫描分钟K线找信号
      3. 信号触发后，模拟次日移动止损卖出
      4. 收集所有信号，按日期排序
      5. 用5万本金逐笔模拟资金变化
    
    返回:
        {
            '参数': 参数描述,
            '信号统计': {...},
            '资金模拟': {...},
            '交易明细': [...],
            '个股统计': [...],
        }
    """
    lookback_days_map = {'3m': 63, '1y': 250}
    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))[:max_stocks]
    
    # ---- 第一步：收集所有信号 ----
    all_signals = []
    stock_summaries = []
    
    for si, (code, valid_dates) in enumerate(sorted_stocks):
        name = stock_info[code]['name']
        
        try:
            earliest = min(valid_dates)
            data_start = (pd.to_datetime(earliest) - timedelta(days=lookback_days_map[period] * 2)).strftime('%Y-%m-%d')
            
            # 拉日K线
            daily = get_price(code, start_date=data_start, end_date=end_date,
                              frequency='daily', fields=['open', 'high', 'low', 'close', 'pre_close'])
            if daily is None or len(daily) < lookback_days_map[period] + 30:
                continue
            daily['date_str'] = daily.index.strftime('%Y-%m-%d')
            
            # 拉分钟K线
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
            
            for i, date in enumerate(trade_dates):
                date_str = str(date)
                
                if i - last_signal_date_idx <= cooldown:
                    continue
                
                day_bars = min_df[min_df['date'] == date]
                if len(day_bars) < 5:
                    continue
                
                hist = min_df[min_df['date'] < date]
                if len(hist) < lookback_bars:
                    continue
                
                hist_returns = hist['bar_return'].iloc[-lookback_bars:]
                
                # 计算阈值
                if signal_type == 'max_break':
                    threshold = hist_returns.max()
                elif signal_type == 'mult_break':
                    pos_returns = hist_returns[hist_returns > 0]
                    base = pos_returns.median() if len(pos_returns) > 0 else 0
                    if pd.isna(base) or base <= 0:
                        continue
                    threshold = base * multiplier
                else:
                    continue
                
                if pd.isna(threshold) or threshold <= 0:
                    continue
                
                # 扫描当天K线
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
                
                # ★ 模拟次日移动止损卖出
                sell_result = simulate_trailing_stop_sell(
                    code, date_str, trigger_price, freq, trailing_pct, FLOOR_STOP)
                
                if sell_result is None:
                    continue
                
                sell_price, sell_type = sell_result
                
                # 不再用费率，用固定手续费（在资金模拟时扣）
                # 这里算的是不含手续费的毛收益率
                raw_ret = (sell_price - trigger_price) / trigger_price * 100
                
                last_signal_date_idx = i
                
                signal = {
                    '日期': date_str,
                    '股票': name,
                    '代码': code,
                    '触发时间': str(trigger_bar),
                    '买入价': trigger_price,
                    'K线涨幅%': round(trigger_return * 100, 2),
                    '阈值%': round(threshold * 100, 2),
                    '卖出价': sell_price,
                    '卖出方式': sell_type,
                    '毛收益率%': round(raw_ret, 2),
                }
                stock_signals.append(signal)
                all_signals.append(signal)
            
            # 个股统计
            if stock_signals:
                rets = [s['毛收益率%'] for s in stock_signals]
                wins = len([r for r in rets if r > 0])
                total = len(rets)
                avg_win = np.mean([r for r in rets if r > 0]) if wins > 0 else 0
                avg_loss = np.mean([r for r in rets if r <= 0]) if total - wins > 0 else 0
                pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
                
                stock_summaries.append({
                    '股票': name,
                    '代码': code,
                    '入池天数': len(valid_dates),
                    '信号数': total,
                    '胜率%': round(wins/total*100, 1),
                    '平均收益%': round(np.mean(rets), 2),
                    '平均盈利%': round(avg_win, 2),
                    '平均亏损%': round(avg_loss, 2),
                    '盈亏比': round(pl_ratio, 2) if pl_ratio != float('inf') else 999,
                })
        except:
            continue
    
    if not all_signals:
        return None
    
    # ---- 第二步：资金模拟（考虑资金占用）----
    #
    # 【逻辑】
    #   - available_cash: 可用资金（没被持仓占用的钱）
    #   - holdings: 当前持仓列表，每笔记录 {代码, 买入日期, 卖出日期, 股数, 买入花费, 卖出收入}
    #   - 每处理一个新信号前，先结算「卖出日期 <= 当前信号日期」的持仓，资金回来
    #   - 买入时从 available_cash 扣钱
    #   - 这样同一天多个信号时，如果钱被占着就买不了，更接近真实
    
    # 信号里包含买入日期，但卖出是次日。需要知道次日是哪天。
    # 用 get_trade_days 找每个买入日的次日
    all_buy_dates = sorted(set(s['日期'] for s in all_signals))
    
    # 批量获取交易日（用于查找"次日"）
    if all_buy_dates:
        all_trade_days_list = get_trade_days(
            start_date=all_buy_dates[0],
            end_date=(pd.to_datetime(end_date) + timedelta(days=10)).strftime('%Y-%m-%d')
        )
        trade_day_strs = [str(d) for d in all_trade_days_list]
        
        # 建立 "买入日 → 次交易日" 的映射
        next_trade_day_map = {}
        for i, d in enumerate(trade_day_strs):
            if i + 1 < len(trade_day_strs):
                next_trade_day_map[d] = trade_day_strs[i + 1]
    else:
        next_trade_day_map = {}
    
    trades_df = pd.DataFrame(all_signals).sort_values('日期').reset_index(drop=True)
    
    # DEBUG: 查看信号价格范围和资金参数
    print(f"    DBG: {len(trades_df)}信号, 价格{trades_df['买入价'].min():.2f}~{trades_df['买入价'].max():.2f}, MAX_PER_TRADE={MAX_PER_TRADE}, INIT={INIT_CAPITAL}")
    
    available_cash = INIT_CAPITAL    # 可用现金（未被持仓占用）
    total_equity = INIT_CAPITAL      # 总权益（现金 + 持仓市值，简化为现金 + 冻结金额）
    peak_equity = INIT_CAPITAL
    max_drawdown = 0
    max_drawdown_pct = 0
    holdings = []                    # 当前持仓: [{代码, 买入日, 卖出日, 股数, 买入花费, 卖出收入, 盈亏}]
    trade_log = []
    consecutive_losses = 0
    strategy_paused = False
    pause_reason = None
    daily_pnl = {}
    skipped_no_cash = 0              # 因资金不足跳过的信号数
    skipped_max_pos = 0              # 因持仓满跳过的信号数
    
    for _, trade in trades_df.iterrows():
        if strategy_paused:
            continue
        
        buy_date = trade['日期']
        buy_price = trade['买入价']
        sell_price = trade['卖出价']
        code = trade['代码']
        
        # ---- 结算已到期的持仓（卖出日 <= 当前买入日）----
        still_holding = []
        for h in holdings:
            if h['卖出日'] <= buy_date:
                # 卖出，资金回来
                available_cash += h['卖出收入']
            else:
                still_holding.append(h)
        holdings = still_holding
        
        # ---- 检查持仓数量 ----
        if len(holdings) >= MAX_POSITIONS:
            skipped_max_pos += 1
            continue
        
        # ---- 检查是否已持有该股票 ----
        if any(h['代码'] == code for h in holdings):
            continue
        
        # ---- 计算能买多少股 ----
        max_afford = min(MAX_PER_TRADE, available_cash - COMMISSION_PER_SIDE)
        if max_afford < buy_price * 100:
            if skipped_no_cash < 3:
                print(f"    SKIP(资金): {trade['股票']} price={buy_price:.2f} need={buy_price*100:.0f} have={max_afford:.0f} holdings={len(holdings)}")
            skipped_no_cash += 1
            continue
        shares = int(max_afford / buy_price / 100) * 100
        if shares <= 0:
            if skipped_no_cash < 3:
                print(f"    SKIP(0股): {trade['股票']} price={buy_price:.2f} max_afford={max_afford:.0f}")
            skipped_no_cash += 1
            continue
        
        buy_cost = shares * buy_price + COMMISSION_PER_SIDE
        sell_revenue = shares * sell_price - COMMISSION_PER_SIDE
        pnl = sell_revenue - buy_cost
        
        # 确定卖出日期（买入日的下一个交易日）
        sell_date = next_trade_day_map.get(buy_date, buy_date)
        
        # ---- 扣钱、记录持仓 ----
        available_cash -= buy_cost
        
        holdings.append({
            '代码': code,
            '买入日': buy_date,
            '卖出日': sell_date,
            '股数': shares,
            '买入花费': buy_cost,
            '卖出收入': sell_revenue,
            '盈亏': pnl,
        })
        
        # 更新总权益 = 可用现金 + 所有持仓的卖出收入（预期）
        total_equity = available_cash + sum(h['卖出收入'] for h in holdings)
        
        trade_log.append({
            '买入日期': buy_date,
            '卖出日期': sell_date,
            '股票': trade['股票'],
            '代码': code,
            '股数': shares,
            '买入价': buy_price,
            '卖出价': sell_price,
            '卖出方式': trade['卖出方式'],
            '盈亏(元)': round(pnl, 2),
            '盈亏率%': round(pnl / buy_cost * 100, 2),
            '可用资金': round(available_cash, 2),
            '总权益': round(total_equity, 2),
        })
        
        daily_pnl[buy_date] = daily_pnl.get(buy_date, 0) + pnl
        
        if total_equity > peak_equity:
            peak_equity = total_equity
        dd = total_equity - peak_equity
        dd_pct = dd / peak_equity * 100 if peak_equity > 0 else 0
        if dd < max_drawdown:
            max_drawdown = dd
            max_drawdown_pct = dd_pct
        
        # 风控
        if daily_pnl.get(buy_date, 0) <= DAILY_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"当天亏损{daily_pnl[buy_date]:.0f}元，超过{abs(DAILY_LOSS_LIMIT)}元"
            break
        if pnl <= 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
        if consecutive_losses >= MAX_CONSECUTIVE_LOSS:
            strategy_paused = True
            pause_reason = f"连续亏损{consecutive_losses}笔"
            break
        if total_equity - INIT_CAPITAL <= TOTAL_LOSS_LIMIT:
            strategy_paused = True
            pause_reason = f"总亏损{total_equity - INIT_CAPITAL:.0f}元，超过{abs(TOTAL_LOSS_LIMIT)}元"
            break
    
    # 结算剩余持仓
    for h in holdings:
        available_cash += h['卖出收入']
    holdings = []
    final_capital = available_cash
    
    # ---- 汇总 ----
    total_pnl = final_capital - INIT_CAPITAL
    total_trades = len(trade_log)
    win_trades = len([t for t in trade_log if t['盈亏(元)'] > 0])
    
    # 信号层面统计（不受资金限制）
    all_rets = [s['毛收益率%'] for s in all_signals]
    sig_wins = len([r for r in all_rets if r > 0])
    sig_total = len(all_rets)
    
    return {
        '信号统计': {
            '总信号数': sig_total,
            '信号胜率%': round(sig_wins/sig_total*100, 1) if sig_total > 0 else 0,
            '信号平均收益%': round(np.mean(all_rets), 2),
            '有信号的股票数': len(stock_summaries),
        },
        '资金模拟': {
            '最终资金': round(final_capital, 2),
            '总盈亏': round(total_pnl, 2),
            '总收益率%': round(total_pnl / INIT_CAPITAL * 100, 1),
            '权益峰值': round(peak_equity, 2),
            '最大回撤(元)': round(max_drawdown, 2),
            '最大回撤%': round(max_drawdown_pct, 1),
            '实际交易笔数': total_trades,
            '实际胜率%': round(win_trades/total_trades*100, 1) if total_trades > 0 else 0,
            '因资金不足跳过': skipped_no_cash,
            '因持仓满跳过': skipped_max_pos,
            '风控暂停': pause_reason,
        },
        '交易明细': trade_log,
        '个股统计': stock_summaries,
    }


print("✅ Cell 3 引擎定义完成")


# ============================================================
# Cell 4：运行网格搜索
# ============================================================

MAX_STOCKS = 15  # ← 先15只验证，确认后改大

print(f"\n{'='*60}")
print(f"📊 第二步：网格搜索（{MAX_STOCKS}只股票）")
print(f"{'='*60}")

# 构建参数网格
param_grid = []
for freq in FREQ_LIST:
    for period in LOOKBACK_PERIODS:
        for trailing in TRAILING_STOP_LIST:
            trailing_label = f"回撤{trailing*100:.0f}%止损"
            # A类
            param_grid.append({
                'freq': freq, 'signal_type': 'max_break',
                'period': period, 'multiplier': None,
                'trailing_pct': trailing,
                'label': f"{freq}|突破最大值|回看{period}|{trailing_label}"
            })
            # B类（只有中位数）
            for mult in MULT_LIST:
                param_grid.append({
                    'freq': freq, 'signal_type': 'mult_break',
                    'period': period, 'multiplier': mult,
                    'trailing_pct': trailing,
                    'label': f"{freq}|中位数×{mult}|回看{period}|{trailing_label}"
                })

print(f"  参数组合: {len(param_grid)} 种")
print(f"  开始搜索...\n")

grid_results = []

for pi, params in enumerate(param_grid):
    label = params['label']
    print(f"  [{pi+1}/{len(param_grid)}] {label}")
    
    result = backtest_and_simulate(
        pool_calendar, stock_info, MAX_STOCKS,
        freq=params['freq'],
        period=params['period'],
        signal_type=params['signal_type'],
        multiplier=params['multiplier'],
        trailing_pct=params['trailing_pct'],
    )
    
    if result:
        sig = result['信号统计']
        cap = result['资金模拟']
        grid_results.append({
            '策略': label,
            'K线周期': params['freq'],
            '触发类型': '突破最大值' if params['signal_type'] == 'max_break' else f"中位数×{params['multiplier']}",
            '回看周期': params['period'],
            '止损回撤': f"{params['trailing_pct']*100:.0f}%",
            '信号数': sig['总信号数'],
            '信号胜率%': sig['信号胜率%'],
            '信号均收%': sig['信号平均收益%'],
            '有信号股票数': sig['有信号的股票数'],
            '最终资金': cap['最终资金'],
            '总收益率%': cap['总收益率%'],
            '最大回撤%': cap['最大回撤%'],
            '实际交易数': cap['实际交易笔数'],
            '实际胜率%': cap['实际胜率%'],
            '因资金不足跳过': cap['因资金不足跳过'],
            '因持仓满跳过': cap['因持仓满跳过'],
            '风控暂停': cap['风控暂停'],
            '_result': result,  # 保存完整结果，后面查看详情用
        })
        print(f"      → 信号{sig['总信号数']}个, 胜率{sig['信号胜率%']}%, "
              f"资金{cap['最终资金']:,.0f}元({cap['总收益率%']:+.1f}%), "
              f"最大回撤{cap['最大回撤%']:.1f}%")
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
    
    # 综合评分：收益率40% + 胜率20% + 回撤20% + 信号量20%
    gdf['回撤得分'] = (100 - gdf['最大回撤%'].abs()).clip(0, 100)  # 回撤越小越好
    gdf['信号量得分'] = gdf['信号数'].apply(lambda x: min(x / 50, 1.0) * 100)
    gdf['综合评分'] = (
        gdf['总收益率%'].clip(-50, 50) * 0.4 +  # 收益率权重最大
        gdf['信号胜率%'] * 0.2 +
        gdf['回撤得分'] * 0.2 +
        gdf['信号量得分'] * 0.2
    ).round(1)
    
    gdf = gdf.sort_values('综合评分', ascending=False)
    
    print(f"\n{'='*60}")
    print(f"📊 策略排行榜 TOP 20")
    print(f"{'='*60}")
    print(f"  评分 = 收益率×40% + 胜率×20% + 低回撤×20% + 信号量×20%\n")
    
    show_cols = ['策略', '信号数', '信号胜率%', '信号均收%',
                 '总收益率%', '最大回撤%', '实际交易数', '综合评分']
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
        print(f"  {freq_cn[freq]}: 平均收益{sub['总收益率%'].mean():+.1f}%, "
              f"平均胜率{sub['信号胜率%'].mean():.1f}%, "
              f"平均回撤{sub['最大回撤%'].mean():.1f}%")
    
    print(f"\n{'='*60}")
    print("📈 按止损回撤比例汇总")
    print(f"{'='*60}")
    for t in TRAILING_STOP_LIST:
        sub = gdf[gdf['止损回撤'] == f"{t*100:.0f}%"]
        if sub.empty:
            continue
        print(f"  回撤{t*100:.0f}%止损: 平均收益{sub['总收益率%'].mean():+.1f}%, "
              f"平均胜率{sub['信号胜率%'].mean():.1f}%, "
              f"平均回撤{sub['最大回撤%'].mean():.1f}%")
    
    print(f"\n{'='*60}")
    print("📈 按触发类型汇总")
    print(f"{'='*60}")
    for tt in gdf['触发类型'].unique():
        sub = gdf[gdf['触发类型'] == tt]
        print(f"  {tt}: 平均收益{sub['总收益率%'].mean():+.1f}%, "
              f"平均胜率{sub['信号胜率%'].mean():.1f}%")
    
    # ---- 最佳策略 ----
    best = gdf.iloc[0]
    print(f"\n{'='*60}")
    print(f"🏆 最佳策略: {best['策略']}")
    print(f"{'='*60}")
    print(f"  信号数:     {best['信号数']:.0f} 个")
    print(f"  信号胜率:   {best['信号胜率%']:.1f}%")
    print(f"  信号均收:   {best['信号均收%']:.2f}%")
    print(f"  总收益率:   {best['总收益率%']:+.1f}%")
    print(f"  最大回撤:   {best['最大回撤%']:.1f}%")
    print(f"  综合评分:   {best['综合评分']}")
    
    if best['因资金不足跳过'] > 0 or best['因持仓满跳过'] > 0:
        print(f"  跳过信号:   资金不足{best['因资金不足跳过']:.0f}次, 持仓满{best['因持仓满跳过']:.0f}次")
    if best['风控暂停']:
        print(f"  ⚠️ 风控触发: {best['风控暂停']}")
    
    # 解读
    print(f"\n  💡 解读:")
    if best['总收益率%'] > 0:
        print(f"     3年下来5万变成{best['最终资金']:,.0f}元，赚了{best['总收益率%']:.1f}%")
    else:
        print(f"     3年下来5万变成{best['最终资金']:,.0f}元，亏了{abs(best['总收益率%']):.1f}%")
    print(f"     过程中最多从高点回撤{abs(best['最大回撤%']):.1f}%")
    if best['信号胜率%'] > 50:
        print(f"     胜率{best['信号胜率%']:.1f}%，赢多输少 ✅")
    else:
        print(f"     胜率{best['信号胜率%']:.1f}%，不到半数，需要靠大赚弥补 ⚠️")


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
        print(sdf.to_string(index=False))
        
        profitable = len(sdf[sdf['平均收益%'] > 0])
        losing = len(sdf[sdf['平均收益%'] <= 0])
        print(f"\n  {profitable}只平均赚钱，{losing}只平均亏钱")
    
    # 交易明细
    if best_result['交易明细']:
        tlog = pd.DataFrame(best_result['交易明细'])
        print(f"\n{'='*60}")
        print(f"📋 交易明细（共{len(tlog)}笔）")
        print(f"{'='*60}")
        
        # 卖出方式分布
        print(f"\n  卖出方式分布:")
        for st, cnt in tlog['卖出方式'].value_counts().items():
            print(f"    {st}: {cnt}笔 ({cnt/len(tlog)*100:.1f}%)")
        
        if len(tlog) <= 30:
            print(f"\n{tlog.to_string(index=False)}")
        else:
            print(f"\n  前15笔:")
            print(tlog.head(15).to_string(index=False))
            print(f"\n  ...省略 {len(tlog)-25} 笔...")
            print(f"\n  后10笔:")
            print(tlog.tail(10).to_string(index=False))
        
        # 资金曲线
        print(f"\n{'='*60}")
        print(f"📈 资金曲线")
        print(f"{'='*60}")
        print(f"  起始: {INIT_CAPITAL:,.0f}元")
        print(f"  结束: {tlog['总权益'].iloc[-1]:,.0f}元")
        print(f"  最高: {tlog['总权益'].max():,.0f}元")
        print(f"  最低: {tlog['总权益'].min():,.0f}元")
        
        # 按月统计
        tlog['月份'] = pd.to_datetime(tlog['买入日期']).dt.to_period('M').astype(str)
        monthly = tlog.groupby('月份').agg(
            交易笔数=('盈亏(元)', 'count'),
            月盈亏=('盈亏(元)', 'sum'),
            月胜率=('盈亏(元)', lambda x: (x > 0).sum() / len(x) * 100),
            月末权益=('总权益', 'last'),
        ).round(1)
        print(f"\n  按月统计:")
        print(monthly.to_string())
