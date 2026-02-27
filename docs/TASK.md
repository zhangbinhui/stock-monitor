# 任务：持仓核心指标自动跟踪

## 背景
portfolio_monitor.py 是持仓监控系统，当前已有：实时行情、均线判断、三重过滤复验、公告扫描。
现在需要给每只持仓加上"核心跟踪指标"的自动抓取，在日报中展示最新数据。

## 需求

### 新增模块 `core_indicators.py`
为每只持仓定义需要跟踪的指标类型，自动抓取最新数据。

### 各持仓的核心指标

#### 1. 恒生科技ETF (513130) — 南向资金
- 抓取：港股通南向资金净流入（最近交易日 + 近5日累计）
- 数据源：`akshare` 的 `stock_hsgt_south_net_flow_in_em` 或类似接口
- 展示：`📊 南向资金: 昨日净流入+XX亿 | 近5日累计+XX亿`

#### 2. 西大门 (605155) — 财报发布监控
- 抓取：检查2025年报是否已发布（预计3/20）
- 数据源：巨潮公告API搜索"年度报告"关键词
- 展示：`📊 年报: 未发布（预计3/20）` 或 `📊 年报: 已发布！`
- 同时检查是否有业绩快报/预告

#### 3. 北汽蓝谷 (600733) — 月度产销数据
- 抓取：最新月度产销快报
- 数据源：巨潮公告API搜索"产销快报"关键词
- 展示：`📊 最新产销: X月 销量XXXX辆 同比+XX%`

#### 4. 渝农商行 (601077) — 已有数据补充展示
- 净息差和股息率已在财报数据中，只需在展示时提取并格式化
- 展示：`📊 净息差: 1.59% | PE: 4.9 | 待年报分红方案`

#### 5. 梅花生物 (600873) — 大宗商品价格
- 抓取：味精、赖氨酸的现货/期货价格走势
- 数据源：尝试 akshare 的商品现货接口，如 `spot_goods` 系列
- 如果 akshare 没有直接接口，尝试抓取生意社(100ppi.com)的价格数据
- 展示：`📊 味精: XX元/吨(近1月+X%) | 赖氨酸: XX元/吨(近1月+X%)`

#### 6. 亿纬锂能 (300014) — 跳过
- 操盘手管理，不做自动跟踪

### portfolio.json 改造
将 `core_track` 从静态字符串改为配置对象：
```json
"core_track": {
  "type": "southbound_flow",  // 或 "annual_report_watch", "monthly_sales", "commodity_price" 等
  "params": { ... }  // 类型相关参数
}
```

### portfolio_monitor.py 集成
- 在 analyze_portfolio 中调用 core_indicators 模块
- 结果传入 format_report
- 在每只股票的 🔍 行展示实际数据（替代静态文字）
- 抓取失败时 graceful fallback 到静态文字

### 注意事项
- 虚拟环境：`.venv_new/`，已安装 akshare
- 网络：WSL2 环境，有代理(localhost:10808)，国内接口直连即可
- 抓取要有超时和错误处理，单个指标失败不影响整体日报
- 每个数据源加缓存，同一天内不重复请求（写入 `indicator_cache.json`）
- 日志用 logging，和现有风格一致

## 验证
改完后运行：
```bash
source .venv_new/bin/activate
python portfolio_monitor.py --mode daily --stdout
```
确认新指标正常展示。

完成后运行:
```bash
openclaw system event --text "Done: 核心指标自动跟踪模块完成" --mode now
```
