"""
聚宽数据缓存工具 v2 - 省额度版
优化：批量接口替代逐只查询，先筛后拉

额度预算（试用账号100万/天）：
  Step 1 (pool): ~15万条（日K线 + fundamentals）
  Step 2 (minutes): 按周期分天拉
    30m: ~2000条/只, 15m: ~4000条/只, 5m: ~12000条/只, 1m: ~60000条/只

使用：
  python jq_data_cache.py --step pool      # Day1: 构建股票池（~15万额度）
  python jq_data_cache.py --step minutes   # Day1+: 下载分钟K线
  python jq_data_cache.py --step status    # 查看缓存状态
"""

import warnings
warnings.filterwarnings('ignore')
import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 压掉 jqdatasdk 的 pickle warning
import logging
logging.disable(logging.WARNING)
os.environ['PYTHONWARNINGS'] = 'ignore'

import jqdatasdk as jq

# ---- 配置 ----
JQ_USER = '13764000563'
JQ_PASS = 'Guigui911014#'

# 数据范围（试用账号限制）
DATA_START = '2024-11-18'
DATA_END = '2025-11-25'

# 股票池参数
MIN_MARKET_CAP = 50e8
MARKET_CAP_TIERS = [
    (100e8, 1/2),
    (50e8,  1/3),
]
MAX_PRICE_RATIO_DEFAULT = 1/3

# 缓存目录
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jq_cache')
DAILY_DIR = os.path.join(CACHE_DIR, 'daily')
MINUTE_DIR = os.path.join(CACHE_DIR, 'minutes')
POOL_FILE = os.path.join(CACHE_DIR, 'rolling_pool.json')
CANDIDATES_FILE = os.path.join(CACHE_DIR, 'candidates.json')

FREQ_LIST = ['30m', '15m', '5m', '1m']
BARS_PER_DAY = {'1m': 240, '5m': 48, '15m': 16, '30m': 8}


def ensure_dirs():
    for d in [CACHE_DIR, DAILY_DIR, MINUTE_DIR]:
        os.makedirs(d, exist_ok=True)
    for freq in FREQ_LIST:
        os.makedirs(os.path.join(MINUTE_DIR, freq), exist_ok=True)


def auth():
    jq.auth(JQ_USER, JQ_PASS)
    quota = jq.get_query_count()
    print(f"✅ 认证成功 | 额度: {quota['spare']:,}/{quota['total']:,}")
    return quota


def check_quota(min_needed=1000):
    """检查剩余额度"""
    q = jq.get_query_count()
    if q['spare'] < min_needed:
        print(f"⚠️ 额度不足: 剩余{q['spare']:,}, 需要{min_needed:,}")
        return False
    return True


def step_pool():
    """Step 1: 构建滚动股票池（省额度版）
    
    策略：
    1. get_all_securities 获取股票列表（1次调用）
    2. get_fundamentals 批量筛市值（1次调用）
    3. get_price 批量获取日K线（按批次，每批50只）
    
    预估额度：~15万条
    """
    ensure_dirs()
    quota = auth()

    print(f"\n{'='*60}")
    print(f"Step 1: 构建滚动股票池 ({DATA_START} ~ {DATA_END})")
    print(f"{'='*60}")

    # 1. 获取所有A股（不消耗额度）
    all_stocks = jq.get_all_securities(types=['stock'], date=DATA_END)
    print(f"全市场: {len(all_stocks)} 只")

    # 2. 过滤ST和次新股（本地过滤，不消耗额度）
    two_years_ago = pd.to_datetime(DATA_START) - timedelta(days=365)
    valid = all_stocks[all_stocks['start_date'] <= two_years_ago]
    
    # 从 all_stocks 的 display_name 列直接过滤ST，不用逐只调 get_security_info
    valid_codes = [c for c in valid.index 
                   if not valid.loc[c, 'display_name'].startswith('ST')
                   and not valid.loc[c, 'display_name'].startswith('*ST')]
    print(f"非ST/上市>2年: {len(valid_codes)} 只")

    # 3. 批量获取市值（1次 fundamentals 查询）
    trade_days = jq.get_trade_days(end_date=DATA_END, count=5)
    last_trade = str(trade_days[-1])

    q = jq.query(
        jq.valuation.code,
        jq.valuation.market_cap
    ).filter(
        jq.valuation.code.in_(valid_codes),
        jq.valuation.market_cap >= MIN_MARKET_CAP / 1e8 * 0.5
    )
    cap_df = jq.get_fundamentals(q, date=last_trade)
    cap_dict = dict(zip(cap_df['code'], cap_df['market_cap']))
    candidate_codes = list(cap_df['code'])
    
    # 构建 code -> name 映射（从 all_stocks 取，不调 API）
    name_dict = {c: valid.loc[c, 'display_name'] for c in candidate_codes if c in valid.index}
    
    print(f"市值≥{MIN_MARKET_CAP/1e8*0.5:.0f}亿: {len(candidate_codes)} 只")

    # 4. 批量下载日K线（支持分批，跨天累积）
    # 需要从 DATA_START 往前推250天算滚动高点
    daily_start = (pd.to_datetime(DATA_START) - timedelta(days=400)).strftime('%Y-%m-%d')
    print(f"\n下载日K线 ({daily_start} ~ {DATA_END})...")
    
    # 统计已缓存数量，只预估未缓存的额度
    already_cached = sum(1 for c in candidate_codes 
                         if os.path.exists(os.path.join(DAILY_DIR, f"{c.replace('.', '_')}.parquet")))
    to_download_count = len(candidate_codes) - already_cached
    est_rows = to_download_count * 500
    print(f"总候选: {len(candidate_codes)} 只, 已缓存: {already_cached}, 待下载: {to_download_count}")
    print(f"预估消耗: {est_rows:,} 条")
    
    if to_download_count == 0:
        print("日K线全部已缓存 ✅")
    elif not check_quota(500):  # 只要还有最少1只的额度就继续
        print("额度不足，请明天再试")
        return

    pool_calendar = {}
    stock_info = {}
    downloaded = 0
    cached = 0
    quota_exhausted = False

    for i in range(0, len(candidate_codes), 50):
        if quota_exhausted:
            break
        batch = candidate_codes[i:i+50]

        for code in batch:
            cache_file = os.path.join(DAILY_DIR, f"{code.replace('.', '_')}.parquet")

            if os.path.exists(cache_file):
                df = pd.read_parquet(cache_file)
                cached += 1
            else:
                # 每20只检查一次额度
                if downloaded > 0 and downloaded % 20 == 0:
                    remaining = jq.get_query_count()['spare']
                    if remaining < 1000:
                        print(f"\n  ⚠️ 额度不足({remaining:,}), 已下载{downloaded}只, 明天继续")
                        quota_exhausted = True
                        break
                
                try:
                    df = jq.get_price(code, start_date=daily_start, end_date=DATA_END,
                                      frequency='daily',
                                      fields=['open', 'high', 'low', 'close', 'volume', 'pre_close'])
                    if df is None or len(df) == 0:
                        if downloaded == 0 and cached == 0:
                            # 第一只就失败，打印调试信息
                            print(f"  ⚠️ {code} 返回空数据: type={type(df)}, 可能是试用账号数据范围限制")
                        continue
                    df.to_parquet(cache_file)
                    downloaded += 1
                    if downloaded <= 3:
                        print(f"  ✅ {code}: {len(df)}条 ({df.index[0]} ~ {df.index[-1]})")
                except Exception as e:
                    if downloaded == 0 and cached == 0:
                        print(f"  ❌ {code} 异常: {e}")
                    continue

            if len(df) < 300:
                continue

            name = name_dict.get(code, code)
            cap = cap_dict.get(code, 0)

            # 确定回撤要求
            ratio = MAX_PRICE_RATIO_DEFAULT
            for tier_cap, tier_ratio in MARKET_CAP_TIERS:
                if cap * 1e8 >= tier_cap:
                    ratio = tier_ratio
                    break

            # 滚动计算
            close_arr = df['close'].values
            high_arr = df['high'].values
            dates_arr = df.index

            valid_dates = []
            for j in range(250, len(close_arr)):
                year_high = high_arr[j-250:j].max()
                current = close_arr[j]
                d = str(dates_arr[j])[:10]

                if d < DATA_START or d > DATA_END:
                    continue
                if current < year_high * ratio:
                    valid_dates.append(d)

            if valid_dates:
                pool_calendar[code] = valid_dates
                stock_info[code] = {
                    'name': name,
                    'market_cap': round(cap, 1),
                    'pool_days': len(valid_dates),
                }

        done = min(i + 50, len(candidate_codes))
        print(f"  [{done}/{len(candidate_codes)}] 入池{len(pool_calendar)}只 | 新下载{downloaded} 已缓存{cached}")

    # 保存（即使没全部下完也保存进度，下次接着来）
    processed = downloaded + cached
    total = len(candidate_codes)
    if quota_exhausted:
        print(f"\n⏸️ 额度用完，已处理 {processed}/{total} 只 ({processed*100//total}%)")
        print(f"   明天凌晨0:30自动继续")
    
    with open(POOL_FILE, 'w', encoding='utf-8') as f:
        json.dump({'pool_calendar': pool_calendar, 'stock_info': stock_info}, f, ensure_ascii=False, indent=2)

    sorted_stocks = sorted(pool_calendar.items(), key=lambda x: -len(x[1]))
    candidates = [{'code': code, 'name': stock_info[code]['name'],
                    'market_cap': stock_info[code]['market_cap'],
                    'pool_days': len(dates)} for code, dates in sorted_stocks]

    with open(CANDIDATES_FILE, 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 滚动股票池构建完成:")
    print(f"   入池股票: {len(pool_calendar)} 只")
    total_days = sum(len(v) for v in pool_calendar.values())
    print(f"   总池日数: {total_days}")
    print(f"\n   入池天数TOP20:")
    for code, dates in sorted_stocks[:20]:
        info = stock_info[code]
        print(f"     {info['name']:>8s} ({code}): {len(dates):>3d}天, 市值{info['market_cap']:.0f}亿")

    quota = jq.get_query_count()
    print(f"\n   剩余额度: {quota['spare']:,}/{quota['total']:,}")


def step_minutes():
    """Step 2: 下载入池股票的分钟K线（支持断点续传）"""
    ensure_dirs()
    quota = auth()

    if not os.path.exists(CANDIDATES_FILE):
        print("❌ 请先运行 --step pool")
        return

    with open(CANDIDATES_FILE, 'r') as f:
        candidates = json.load(f)

    print(f"\n{'='*60}")
    print(f"Step 2: 下载分钟K线 ({len(candidates)} 只入池股票)")
    print(f"{'='*60}")

    trading_days = len(jq.get_trade_days(start_date=DATA_START, end_date=DATA_END))
    print(f"交易日: {trading_days} 天\n")

    # 额度预估
    for freq in FREQ_LIST:
        freq_dir = os.path.join(MINUTE_DIR, freq)
        cached = len([f for f in os.listdir(freq_dir) if f.endswith('.parquet')]) if os.path.exists(freq_dir) else 0
        remaining = len(candidates) - cached
        est = remaining * BARS_PER_DAY[freq] * trading_days
        print(f"  {freq}: 待下载{remaining}只, 预估{est:,}条 ({est/10000:.1f}万)")

    print(f"\n  当前额度: {quota['spare']:,} 条")

    for freq in FREQ_LIST:
        freq_dir = os.path.join(MINUTE_DIR, freq)
        est_per_stock = BARS_PER_DAY[freq] * trading_days

        # 统计已缓存
        cached_codes = set()
        if os.path.exists(freq_dir):
            for f in os.listdir(freq_dir):
                if f.endswith('.parquet'):
                    cached_codes.add(f.replace('_XSHE.parquet', '.XSHE').replace('_XSHG.parquet', '.XSHG'))

        to_download = [c for c in candidates if c['code'] not in cached_codes]

        if not to_download:
            print(f"\n--- {freq}: 全部已缓存 ✅ ---")
            continue

        total_est = len(to_download) * est_per_stock
        remaining_quota = jq.get_query_count()['spare']

        print(f"\n--- {freq}: {len(to_download)}只待下载, 约{total_est:,}条, 额度{remaining_quota:,} ---")

        if remaining_quota < est_per_stock * 2:
            print(f"  ⚠️ 额度不足，跳过")
            continue

        downloaded = 0
        for ci, cand in enumerate(to_download):
            code = cand['code']
            cache_file = os.path.join(freq_dir, f"{code.replace('.', '_')}.parquet")

            # 每10只检查一次额度
            if (ci + 1) % 10 == 0:
                remaining = jq.get_query_count()['spare']
                if remaining < est_per_stock * 2:
                    print(f"  ⚠️ 额度不足({remaining:,}), 暂停{freq}, 明天继续")
                    break

            try:
                df = jq.get_price(code, start_date=DATA_START, end_date=DATA_END,
                                  frequency=freq,
                                  fields=['open', 'close', 'high', 'low', 'volume'])
                if df is not None and len(df) > 0:
                    df.to_parquet(cache_file)
                    downloaded += 1
                    if downloaded % 5 == 0 or downloaded == 1:
                        print(f"  [{downloaded}/{len(to_download)}] {cand['name']}: {len(df)}条 ✅")
            except Exception as e:
                print(f"  {cand['name']} 失败: {e}")

        print(f"  {freq} 本轮下载: {downloaded}只")

    quota = jq.get_query_count()
    print(f"\n✅ 完成 | 剩余额度: {quota['spare']:,}/{quota['total']:,}")


def step_status():
    """查看缓存状态"""
    ensure_dirs()

    print(f"\n{'='*60}")
    print(f"缓存状态 ({CACHE_DIR})")
    print(f"{'='*60}")

    daily_files = [f for f in os.listdir(DAILY_DIR) if f.endswith('.parquet')]
    print(f"\n日K线: {len(daily_files)} 只")

    for freq in FREQ_LIST:
        freq_dir = os.path.join(MINUTE_DIR, freq)
        if os.path.exists(freq_dir):
            files = [f for f in os.listdir(freq_dir) if f.endswith('.parquet')]
            print(f"{freq} K线: {len(files)} 只")
        else:
            print(f"{freq} K线: 0 只")

    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            data = json.load(f)
        pool = data['pool_calendar']
        total_days = sum(len(v) for v in pool.values())
        print(f"\n滚动股票池: {len(pool)} 只, 总池日{total_days}")

        if os.path.exists(CANDIDATES_FILE):
            with open(CANDIDATES_FILE, 'r') as f:
                cands = json.load(f)
            print(f"候选列表: {len(cands)} 只")
    else:
        print(f"\n滚动股票池: 未构建")

    try:
        jq.auth(JQ_USER, JQ_PASS)
        q = jq.get_query_count()
        print(f"\n额度: {q['spare']:,}/{q['total']:,}")
    except:
        print("\n额度查询失败")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='聚宽数据缓存工具')
    parser.add_argument('--step', choices=['pool', 'minutes', 'status'], required=True)
    args = parser.parse_args()

    if args.step == 'pool':
        step_pool()
    elif args.step == 'minutes':
        step_minutes()
    elif args.step == 'status':
        step_status()
