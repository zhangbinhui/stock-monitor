#!/usr/bin/env python3
"""
动态核心指标引擎

根据持仓的林奇分类+行业+当前状态，自动推断需要跟踪的指标并抓取最新数据。
- 新股入池 → 自动生成指标
- 数据滚动 → 2月产销出了自动等3月
- 催化剂到期 → 年报出了切到Q1报
"""

import json
import os
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional

log = logging.getLogger("core_indicators")

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "indicator_cache.json")

# ============================================================
# 缓存
# ============================================================

def _load_cache() -> Dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {}


def _save_cache(cache: Dict):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _get_cached(key: str, max_age_hours: int = 12) -> Optional[Dict]:
    """获取缓存，超过 max_age_hours 过期"""
    cache = _load_cache()
    item = cache.get(key)
    if item:
        ts = item.get('timestamp', 0)
        age = (datetime.now().timestamp() - ts) / 3600
        if age < max_age_hours:
            return item.get('data')
    return None


def _set_cached(key: str, data):
    cache = _load_cache()
    cache[key] = {'data': data, 'timestamp': datetime.now().timestamp()}
    _save_cache(cache)


# ============================================================
# 数据抓取函数
# ============================================================

def fetch_southbound_flow() -> Optional[Dict]:
    """南向资金净流入"""
    cached = _get_cached('southbound_flow')
    if cached:
        return cached
    
    try:
        import akshare as ak
        df = ak.stock_hsgt_hist_em(symbol="南向资金")
        if df.empty:
            return None
        
        df = df.sort_values('日期', ascending=False)
        latest = df.iloc[0]
        last_5 = df.head(5)
        
        result = {
            'date': str(latest['日期']),
            'daily_net': round(float(latest['当日成交净买额']), 2),
            'sum_5d': round(float(last_5['当日成交净买额'].sum()), 2),
            'cumulative': round(float(latest['历史累计净买额']), 2),
        }
        _set_cached('southbound_flow', result)
        return result
    except Exception as e:
        log.warning(f"南向资金获取失败: {e}")
        return None


def fetch_report_disclosure(code: str) -> Optional[Dict]:
    """查询下一份财报的预计披露时间和是否已发布"""
    cache_key = f'report_disclosure_{code}'
    cached = _get_cached(cache_key, max_age_hours=24)
    if cached:
        return cached
    
    try:
        import akshare as ak
        now = datetime.now()
        
        # 确定当前应该等哪份报告
        # 1-4月等年报，4-8月等半年报(或Q1)，8-10月等Q3
        if now.month <= 4:
            period = f"{now.year - 1}年报"
            report_name = f"{now.year - 1}年报"
        elif now.month <= 8:
            period = f"{now.year}半年报"
            report_name = f"{now.year}半年报"
        else:
            period = f"{now.year}三季报"
            report_name = f"{now.year}Q3"
        
        # 先试年报，如果年报已出就查下一份
        df = ak.stock_report_disclosure(market="沪深京", period=period)
        match = df[df['股票代码'].astype(str).str.contains(code)]
        
        if match.empty:
            return None
        
        row = match.iloc[0]
        scheduled = str(row['首次预约']) if row['首次预约'] is not None else None
        actual = str(row['实际披露']) if row['实际披露'] is not None and str(row['实际披露']) != 'NaT' else None
        
        result = {
            'report_type': report_name,
            'scheduled_date': scheduled,
            'actual_date': actual,
            'is_published': actual is not None and actual != 'None' and actual != 'NaT',
        }
        
        # 如果已发布，查下一份
        if result['is_published']:
            # 年报出了→等Q1，半年报出了→等Q3
            if '年报' in period:
                next_period = f"{now.year}一季报"
            elif '半年报' in period:
                next_period = f"{now.year}三季报"
            else:
                next_period = f"{now.year}年报"
            
            try:
                df2 = ak.stock_report_disclosure(market="沪深京", period=next_period)
                match2 = df2[df2['股票代码'].astype(str).str.contains(code)]
                if not match2.empty:
                    row2 = match2.iloc[0]
                    result['next_report'] = next_period
                    result['next_scheduled'] = str(row2['首次预约']) if row2['首次预约'] is not None else None
            except:
                pass
        
        _set_cached(cache_key, result)
        return result
    except Exception as e:
        log.warning(f"财报披露查询失败 {code}: {e}")
        return None


def fetch_monthly_sales_data(code: str, company_name: str, months: int = 6) -> Optional[Dict]:
    """下载并解析月度产销快报PDF，提取销量数据和趋势分析"""
    cache_key = f'monthly_sales_{code}'
    cached = _get_cached(cache_key, max_age_hours=24)
    if cached:
        return cached
    
    try:
        import pdfplumber
        import io
        import re
        
        # 搜索产销快报公告
        url = "http://www.cninfo.com.cn/new/fulltextSearch/full"
        headers = {"User-Agent": "Mozilla/5.0"}
        params = {
            "searchkey": f"{company_name} 产销快报",
            "sdate": (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
            "edate": datetime.now().strftime('%Y-%m-%d'),
            "isfulltext": "false", "sortName": "pubdate", "sortType": "desc",
            "pageNum": "1", "pageSize": str(months + 2),
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        anns = r.json().get("announcements") or []
        
        if not anns:
            return None
        
        monthly = []
        for ann in anns[:months]:
            title = re.sub(r'<[^>]+>', '', ann.get('announcementTitle', ''))
            month_match = re.search(r'(\d{4})年(\d{1,2})月', title)
            if not month_match:
                continue
            
            year, month = int(month_match.group(1)), int(month_match.group(2))
            pdf_url = f"http://static.cninfo.com.cn/{ann.get('adjunctUrl', '')}"
            
            try:
                pdf_r = requests.get(pdf_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                with pdfplumber.open(io.BytesIO(pdf_r.content)) as pdf:
                    tables = pdf.pages[0].extract_tables()
                    if tables:
                        for row in tables[0]:
                            if row and row[0] and '合计' in str(row[0]):
                                # 处理千分位逗号
                                def parse_int(s):
                                    if s is None: return 0
                                    return int(str(s).replace(',', '').strip())
                                
                                monthly.append({
                                    'period': f"{year}-{month:02d}",
                                    'year': year, 'month': month,
                                    'production': parse_int(row[1]),
                                    'prod_yoy': str(row[3] or ''),
                                    'sales': parse_int(row[7]),
                                    'sales_yoy': str(row[9] or ''),
                                })
                                break
            except Exception as e:
                log.debug(f"PDF解析失败 {year}-{month:02d}: {e}")
        
        if not monthly:
            return None
        
        monthly.sort(key=lambda x: (x['year'], x['month']))
        
        # === 分析 ===
        latest = monthly[-1]
        analysis = {
            'monthly_data': monthly,
            'latest_period': latest['period'],
            'latest_sales': latest['sales'],
            'latest_sales_yoy': latest['sales_yoy'],
            'latest_production': latest['production'],
        }
        
        # 环比
        if len(monthly) >= 2:
            prev = monthly[-2]
            if prev['sales'] > 0:
                mom = (latest['sales'] - prev['sales']) / prev['sales'] * 100
                analysis['mom_change'] = round(mom, 1)
                analysis['prev_period'] = prev['period']
                analysis['prev_sales'] = prev['sales']
        
        # 近3月趋势
        if len(monthly) >= 3:
            recent3 = [d['sales'] for d in monthly[-3:]]
            if all(recent3[i] >= recent3[i-1] for i in range(1, len(recent3))):
                analysis['trend'] = '连续上升'
            elif all(recent3[i] <= recent3[i-1] for i in range(1, len(recent3))):
                analysis['trend'] = '连续下降'
            else:
                analysis['trend'] = '波动'
        
        # 月销万辆里程碑
        analysis['above_10k'] = latest['sales'] >= 10000
        if not analysis['above_10k']:
            analysis['gap_to_10k'] = 10000 - latest['sales']
        
        _set_cached(cache_key, analysis)
        return analysis
    except Exception as e:
        log.warning(f"产销数据获取失败 {code}: {e}")
        return None


def fetch_announcement_search(code: str, keyword: str, company_name: str = "", limit: int = 3) -> Optional[List[Dict]]:
    """巨潮公告全文搜索"""
    cache_key = f'ann_{code}_{keyword}'
    cached = _get_cached(cache_key, max_age_hours=12)
    if cached:
        return cached
    
    try:
        import re as _re
        search_term = f"{company_name} {keyword}" if company_name else keyword
        
        url = "http://www.cninfo.com.cn/new/fulltextSearch/full"
        headers = {"User-Agent": "Mozilla/5.0"}
        params = {
            "searchkey": search_term,
            "sdate": (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
            "edate": datetime.now().strftime('%Y-%m-%d'),
            "isfulltext": "false",
            "sortName": "pubdate",
            "sortType": "desc",
            "pageNum": "1",
            "pageSize": str(limit),
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        result = r.json()
        announcements = result.get("announcements") or []
        
        parsed = []
        for ann in announcements[:limit]:
            ts = ann.get('announcementTime', 0)
            date = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d') if ts else None
            title = _re.sub(r'<[^>]+>', '', ann.get('announcementTitle', ''))
            parsed.append({
                'title': title,
                'date': date,
            })
        
        _set_cached(cache_key, parsed)
        return parsed
    except Exception as e:
        log.warning(f"公告搜索失败 {code} {keyword}: {e}")
        return None


def fetch_commodity_prices(products: List[str], cost_driver: str = None) -> Dict[str, Optional[Dict]]:
    """获取商品现货价格
    
    策略：
    1. 有期货的品种（玉米）→ akshare期货行情
    2. 无期货的化工品（味精/赖氨酸/苏氨酸）→ 读取缓存文件 commodity_prices.json
       缓存由外部定时更新（cron web搜索 或 手动维护）
    """
    FUTURES_MAP = {
        '玉米': 'C0',
        '豆粕': 'M0',
        '棉花': 'CF0',
    }
    
    COMMODITY_PRICES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "commodity_prices.json")
    
    # 加载手动/半自动维护的价格缓存
    manual_prices = {}
    if os.path.exists(COMMODITY_PRICES_FILE):
        try:
            with open(COMMODITY_PRICES_FILE, 'r', encoding='utf-8') as f:
                manual_prices = json.load(f)
        except:
            pass
    
    all_products = list(products) + ([cost_driver] if cost_driver and cost_driver not in products else [])
    results = {}
    
    for product in all_products:
        cache_key = f'commodity_{product}'
        cached = _get_cached(cache_key, max_age_hours=12)
        if cached:
            results[product] = cached
            continue
        
        # 1. 期货品种
        if product in FUTURES_MAP:
            try:
                import akshare as ak
                df = ak.futures_zh_daily_sina(symbol=FUTURES_MAP[product])
                if not df.empty:
                    df = df.sort_values('date', ascending=False)
                    latest = df.iloc[0]
                    prev_30 = df.iloc[min(22, len(df)-1)] if len(df) > 22 else df.iloc[-1]
                    price = float(latest['close'])
                    prev_price = float(prev_30['close'])
                    change_30d = (price - prev_price) / prev_price * 100 if prev_price > 0 else 0
                    
                    data = {
                        'price': price,
                        'unit': '元/吨',
                        'change_30d': round(change_30d, 1),
                        'date': str(latest['date']),
                        'source': '期货主力',
                    }
                    _set_cached(cache_key, data)
                    results[product] = data
                    continue
            except Exception as e:
                log.debug(f"期货行情获取失败 {product}: {e}")
        
        # 2. 手动缓存文件
        if product in manual_prices:
            mp = manual_prices[product]
            # 检查是否太旧（超过7天告警）
            date_str = mp.get('date', '')
            age_days = None
            if date_str:
                try:
                    age_days = (datetime.now() - datetime.strptime(date_str, '%Y-%m-%d')).days
                except:
                    pass
            
            data = {
                'price': mp.get('price', 0),
                'unit': mp.get('unit', '元/吨'),
                'change_30d': mp.get('change_30d', 0),
                'date': date_str,
                'source': mp.get('source', '行业报价'),
                'stale': age_days is not None and age_days > 7,
            }
            results[product] = data
            continue
        
        results[product] = None
    
    return results


# ============================================================
# 指标引擎
# ============================================================

class IndicatorEngine:
    """动态核心指标引擎"""
    
    def generate_indicators(self, holding: Dict, fundamentals: Dict = None) -> List[Dict]:
        """
        根据持仓的 stock_class + 行业 + 当前状态，自动生成指标
        
        返回: [{name, icon, text, status}]
        """
        stock_class = holding.get('stock_class', '')
        h_type = holding.get('type', 'stock')
        code = holding.get('code', '')
        notes = holding.get('notes', '')
        
        indicators = []
        
        # 代持/跟随 → 不跟踪
        if '操盘手' in notes or '代持' in notes or '跟随' in notes:
            indicators.append({
                'name': '跟随',
                'icon': '👤',
                'text': '跟随操盘手指令，不做独立跟踪',
                'status': 'info',
            })
            return indicators
        
        # ETF → 资金流向
        if h_type == 'etf':
            indicators.extend(self._etf_indicators(holding))
            return indicators
        
        # 按林奇分类分发
        if stock_class == '成长股':
            indicators.extend(self._growth_indicators(holding, fundamentals))
        elif stock_class == '周期股':
            indicators.extend(self._cyclical_indicators(holding, fundamentals))
        elif stock_class == '价值股':
            indicators.extend(self._value_indicators(holding, fundamentals))
        elif stock_class == '困境反转':
            indicators.extend(self._turnaround_indicators(holding, fundamentals))
        else:
            # 通用：财报监控
            indicators.extend(self._common_indicators(holding, fundamentals))
        
        return indicators
    
    def _etf_indicators(self, holding: Dict) -> List[Dict]:
        """ETF指标：资金流向"""
        indicators = []
        code = holding.get('code', '')
        name = holding.get('name', '')
        
        # 恒生科技 → 南向资金
        if '恒生' in name or '港股' in name:
            flow = fetch_southbound_flow()
            if flow:
                daily = flow['daily_net']
                sum5 = flow['sum_5d']
                daily_icon = '📈' if daily > 0 else '📉'
                indicators.append({
                    'name': '南向资金',
                    'icon': '💰',
                    'text': f"最新{daily_icon}{daily:+.1f}亿 | 近5日{sum5:+.1f}亿",
                    'status': 'good' if daily > 0 else 'warn',
                })
            else:
                indicators.append({
                    'name': '南向资金',
                    'icon': '💰',
                    'text': '数据获取中...',
                    'status': 'loading',
                })
        
        return indicators
    
    def _growth_indicators(self, holding: Dict, fundamentals: Dict) -> List[Dict]:
        """成长股：财报披露 + 增速趋势"""
        indicators = []
        code = holding.get('code', '')
        
        # 下一份财报
        report = fetch_report_disclosure(code)
        if report:
            if report['is_published']:
                next_info = report.get('next_report', '')
                next_date = report.get('next_scheduled', '')
                indicators.append({
                    'name': '财报',
                    'icon': '📅',
                    'text': f"{report['report_type']}已发布 | 下一份: {next_info} 预计{next_date}",
                    'status': 'info',
                })
            else:
                scheduled = report['scheduled_date']
                days_left = None
                if scheduled and scheduled != 'None':
                    try:
                        target = datetime.strptime(scheduled[:10], '%Y-%m-%d')
                        days_left = (target - datetime.now()).days
                    except:
                        pass
                
                urgency = ''
                if days_left is not None:
                    if days_left <= 7:
                        urgency = f' ⚠️仅剩{days_left}天'
                    elif days_left <= 30:
                        urgency = f' ({days_left}天后)'
                
                indicators.append({
                    'name': '财报',
                    'icon': '📅',
                    'text': f"{report['report_type']}未发布 预计{scheduled}{urgency}",
                    'status': 'wait',
                })
        
        return indicators
    
    def _cyclical_indicators(self, holding: Dict, fundamentals: Dict) -> List[Dict]:
        """周期股：产品价格 + 财报"""
        indicators = []
        code = holding.get('code', '')
        hints = holding.get('indicator_hints', {})
        
        # 产品价格
        products = hints.get('products', [])
        cost_driver = hints.get('cost_driver')
        
        if products:
            prices = fetch_commodity_prices(products, cost_driver=cost_driver)
            
            price_parts = []
            for p in products:
                data = prices.get(p)
                if data:
                    chg = data.get('change_30d', 0)
                    icon = '📈' if chg > 0 else '📉' if chg < 0 else '➡️'
                    stale = '⚠️' if data.get('stale') else ''
                    price_parts.append(f"{p}{icon}{data['price']}{data['unit']}(月{chg:+.1f}%){stale}")
                else:
                    price_parts.append(f"{p}: 暂无数据")
            
            if cost_driver:
                cost_data = prices.get(cost_driver)
                if cost_data:
                    chg = cost_data.get('change_30d', 0)
                    icon = '📈' if chg > 0 else '📉'
                    price_parts.append(f"成本({cost_driver}){icon}{cost_data['price']}(月{chg:+.1f}%)")
            
            indicators.append({
                'name': '产品价格',
                'icon': '📊',
                'text': ' | '.join(price_parts),
                'status': 'info',
            })
        
        # 财报
        indicators.extend(self._growth_indicators(holding, fundamentals))
        
        return indicators
    
    def _value_indicators(self, holding: Dict, fundamentals: Dict) -> List[Dict]:
        """价值股：股息率/分红 + 财报"""
        indicators = []
        code = holding.get('code', '')
        name = holding.get('name', '')
        
        # 银行股特有：净息差
        industry = ''
        if fundamentals:
            industry = fundamentals.get('industry', '') or ''
        if any(kw in name or kw in industry for kw in ['银行', '农商', '金融']):
            indicators.append({
                'name': '关注',
                'icon': '🏦',
                'text': '核心看净息差+分红方案（待年报披露）',
                'status': 'info',
            })
        
        # 财报披露
        indicators.extend(self._growth_indicators(holding, fundamentals))
        
        return indicators
    
    def _turnaround_indicators(self, holding: Dict, fundamentals: Dict) -> List[Dict]:
        """困境反转股：月度产销数据分析 + 毛利率"""
        indicators = []
        code = holding.get('code', '')
        name = holding.get('name', '')
        hints = holding.get('indicator_hints', {})
        
        # 月度产销数据（下载PDF提取+分析）
        sales_data = fetch_monthly_sales_data(code, name, months=6)
        if sales_data:
            latest_sales = sales_data['latest_sales']
            yoy = sales_data['latest_sales_yoy']
            period = sales_data['latest_period']
            
            # 销量概况
            parts = [f"{period} 销量{latest_sales:,}辆 同比{yoy}"]
            
            # 环比
            mom = sales_data.get('mom_change')
            if mom is not None:
                mom_icon = '📈' if mom > 0 else '📉'
                parts.append(f"环比{mom_icon}{mom:+.1f}%")
            
            indicators.append({
                'name': '产销',
                'icon': '🚗',
                'text': ' | '.join(parts),
                'status': 'good' if latest_sales >= 10000 else 'info',
            })
            
            # 近几月趋势
            monthly = sales_data.get('monthly_data', [])
            if len(monthly) >= 3:
                trend_parts = [f"{d['period'][-2:]}月:{d['sales']:,}" for d in monthly[-4:]]
                trend = sales_data.get('trend', '')
                trend_icon = '📈' if trend == '连续上升' else '📉' if trend == '连续下降' else '↔️'
                indicators.append({
                    'name': '趋势',
                    'icon': trend_icon,
                    'text': f"{'→'.join(trend_parts)} ({trend})",
                    'status': 'good' if trend == '连续上升' else 'warn' if trend == '连续下降' else 'info',
                })
            
            # 智能点评
            comment = self._analyze_sales_context(sales_data)
            if comment:
                indicators.append({
                    'name': '点评',
                    'icon': '💬',
                    'text': comment,
                    'status': 'analysis',
                })
            
            # 关键里程碑判断
            if sales_data.get('above_10k'):
                indicators.append({
                    'name': '里程碑',
                    'icon': '🎯',
                    'text': '月销破万✅ → 加仓信号！',
                    'status': 'milestone',
                })
            else:
                gap = sales_data.get('gap_to_10k', 0)
                indicators.append({
                    'name': '里程碑',
                    'icon': '🎯',
                    'text': f'距月销万辆还差{gap:,}辆',
                    'status': 'wait',
                })
            
            # 下期预计
            if monthly:
                last_month = monthly[-1]['month']
                last_year = monthly[-1]['year']
                next_month = last_month + 1
                next_year = last_year
                if next_month > 12:
                    next_month = 1
                    next_year += 1
                indicators.append({
                    'name': '下期',
                    'icon': '⏳',
                    'text': f'{next_year}年{next_month}月产销快报预计{next_month+1}月初发布',
                    'status': 'wait',
                })
        else:
            # fallback: 公告搜索
            ops_keyword = hints.get('ops_keyword', '产销')
            anns = fetch_announcement_search(code, ops_keyword, company_name=name, limit=3)
            if anns:
                indicators.append({
                    'name': '经营数据',
                    'icon': '📋',
                    'text': f"最新: [{anns[0]['date']}] {anns[0]['title'][:30]}",
                    'status': 'info',
                })
            else:
                indicators.append({
                    'name': '经营数据',
                    'icon': '📋',
                    'text': '未找到产销数据',
                    'status': 'warn',
                })
        
        # 财报
        indicators.extend(self._growth_indicators(holding, fundamentals))
        
        return indicators
    
    def _analyze_sales_context(self, sales_data: Dict) -> Optional[str]:
        """根据产销数据给出智能点评"""
        latest_sales = sales_data.get('latest_sales', 0)
        mom = sales_data.get('mom_change')
        yoy_str = sales_data.get('latest_sales_yoy', '')
        period = sales_data.get('latest_period', '')
        monthly = sales_data.get('monthly_data', [])
        
        # 解析同比数值
        yoy = None
        try:
            yoy = float(yoy_str.replace('%', '').replace('+', ''))
        except:
            pass
        
        comments = []
        
        # 春节月判断（1-2月）
        month = int(period.split('-')[1]) if '-' in period else 0
        if month in [1, 2] and mom is not None and mom < -30:
            comments.append(f"春节月环比大跌属季节性正常")
            # 跟去年同期比更有意义
            if yoy is not None:
                if yoy > 20:
                    comments.append(f"同比+{yoy:.0f}%表现强劲")
                elif yoy > 0:
                    comments.append(f"同比仍正增长，基本面未恶化")
                else:
                    comments.append(f"⚠️同比转负需警惕")
        else:
            # 非春节月
            if mom is not None:
                if mom > 20:
                    comments.append("环比大增，放量信号")
                elif mom < -20:
                    comments.append("⚠️环比大跌，关注是否趋势性下滑")
            
            if yoy is not None:
                if yoy > 50:
                    comments.append("同比高增长，反转加速")
                elif yoy > 0:
                    comments.append("同比正增长")
                else:
                    comments.append("⚠️同比下滑")
        
        # 连续趋势判断
        if len(monthly) >= 4:
            # 排除最新月(可能春节)，看之前3个月趋势
            prev3 = [d['sales'] for d in monthly[-4:-1]]
            if all(prev3[i] > prev3[i-1] for i in range(1, len(prev3))):
                comments.append("前3月销量持续攀升")
        
        return '；'.join(comments) if comments else None
    
    def _common_indicators(self, holding: Dict, fundamentals: Dict) -> List[Dict]:
        """通用指标"""
        return self._growth_indicators(holding, fundamentals)


# ============================================================
# 格式化
# ============================================================

def format_indicators(indicators: List[Dict]) -> List[str]:
    """将指标列表格式化为报告行"""
    if not indicators:
        return []
    
    lines = []
    for ind in indicators:
        icon = ind.get('icon', '🔍')
        text = ind.get('text', '')
        lines.append(f"   {icon} {text}")
    
    return lines


# ============================================================
# 测试
# ============================================================

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    # 加载 portfolio.json 测试
    import json
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'portfolio.json'), 'r') as f:  # portfolio.json stays in root
        portfolio = json.load(f)
    
    engine = IndicatorEngine()
    
    for h in portfolio['accounts'][0]['holdings']:
        print(f"\n=== {h['name']} ({h['code']}) [{h.get('stock_class', h.get('type'))}] ===")
        indicators = engine.generate_indicators(h)
        for line in format_indicators(indicators):
            print(line)
