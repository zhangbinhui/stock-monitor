#!/usr/bin/env python3
"""
高管增持监控测试脚本 - 强制运行版本
"""

import json
import logging
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import akshare as ak
import numpy as np
import pandas as pd

from config import (
    EMAIL_PASSWORD,
    EMAIL_RECEIVER,
    EMAIL_SENDER,
    EXCLUDE_KEYWORDS,
    EXEC_SALARY_MULTIPLIER,
    HISTORY_FILE,
    MIN_EXECUTIVES,
    MIN_MARKET_CAP_RATIO,
    QUERY_MONTHS,
    QUERY_SYMBOL,
    SMTP_PORT,
    SMTP_SERVER,
    TRADE_METHODS,
)

# 日志配置
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "stock-monitor-test.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def is_trading_day() -> bool:
    """强制返回True进行测试"""
    return True


def fetch_data() -> pd.DataFrame:
    """从巨潮资讯网获取高管增持明细数据"""
    log.info("正在从巨潮资讯网获取数据...")
    try:
        df = ak.stock_hold_management_detail_cninfo(symbol=QUERY_SYMBOL)
        log.info(f"获取到 {len(df)} 条记录，日期范围: {df['截止日期'].min()} ~ {df['截止日期'].max()}")
        return df
    except Exception as e:
        log.error(f"获取数据失败: {e}")
        # 如果获取失败，返回一个模拟的空DataFrame
        return pd.DataFrame(columns=['证券代码', '证券简称', '高管姓名', '董监高职务', '变动数量', '成交均价', '截止日期', '持股变动原因'])


def filter_major_shareholders(df: pd.DataFrame) -> pd.DataFrame:
    """过滤大股东增持，只保留普通高管增持"""
    before_count = len(df)
    
    # 过滤包含大股东关键词的记录
    for keyword in EXCLUDE_KEYWORDS:
        df = df[~df["董监高职务"].str.contains(keyword, na=False)]
    
    log.info(f"过滤大股东增持后：{before_count} -> {len(df)} 条记录")
    return df


def filter_st_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """排除ST/退市风险股"""
    before_count = len(df)
    
    # 排除证券简称包含ST、*ST、退的股票
    st_pattern = r'(\*?ST|退)'
    df = df[~df["证券简称"].str.contains(st_pattern, na=False)]
    
    log.info(f"排除ST/退市风险股后：{before_count} -> {len(df)} 条记录")
    return df


def get_market_cap(stock_code: str) -> Optional[float]:
    """获取公司市值（亿元）"""
    try:
        log.info(f"获取 {stock_code} 市值...")
        # 简化版本，返回模拟数据
        import random
        return random.uniform(100, 2000)  # 模拟100-2000亿市值
    except Exception as e:
        log.warning(f"获取 {stock_code} 市值失败: {e}")
        return None


def get_executive_salary(stock_code: str, exec_name: str) -> Optional[float]:
    """获取高管薪资（万元）"""
    try:
        # 简化版本，返回模拟数据
        import random
        return random.uniform(50, 500)  # 模拟50-500万年薪
    except Exception as e:
        log.warning(f"获取 {stock_code} {exec_name} 薪资失败: {e}")
        return None


def get_stock_price_data(stock_code: str) -> Dict:
    """获取股票价格和技术指标数据"""
    try:
        log.info(f"获取 {stock_code} 股价数据...")
        # 简化版本，返回模拟技术指标
        import random
        current_price = random.uniform(10, 100)
        
        return {
            "current_price": current_price,
            "prev_price": current_price * 0.99,
            "price_change_pct": random.uniform(-5, 5),
            "ma5": current_price * random.uniform(0.95, 1.05),
            "ma10": current_price * random.uniform(0.9, 1.1),
            "ma20": current_price * random.uniform(0.85, 1.15),
            "ma60": current_price * random.uniform(0.8, 1.2),
            "bias5": random.uniform(-10, 10),
            "bias10": random.uniform(-15, 15),
            "bias20": random.uniform(-20, 20),
            "ma_status": random.choice(["多头排列", "空头排列", "整理状态"]),
        }
    except Exception as e:
        log.warning(f"获取 {stock_code} 股价数据失败: {e}")
        return {}


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    """筛选数据"""
    if df.empty:
        log.info("数据为空，创建模拟测试数据...")
        # 创建一些模拟测试数据
        test_data = {
            '证券代码': ['000001', '000002', '000001', '000002', '000001', '000002'],
            '证券简称': ['平安银行', '万科A', '平安银行', '万科A', '平安银行', '万科A'],
            '高管姓名': ['张三', '李四', '王五', '赵六', '钱七', '孙八'],
            '董监高职务': ['董事长', '总经理', '副董事长', '财务总监', '董事', '监事'],
            '变动数量': [100000, 80000, 120000, 90000, 110000, 70000],
            '成交均价': [15.50, 25.30, 16.20, 26.10, 15.80, 24.90],
            '截止日期': ['2026-01-15', '2026-01-20', '2026-01-25', '2026-01-30', '2026-02-01', '2026-02-05'],
            '持股变动原因': ['竞价交易', '二级市场买卖', '竞价交易', '二级市场买卖', '竞价交易', '二级市场买卖']
        }
        df = pd.DataFrame(test_data)
        log.info(f"创建了 {len(df)} 条模拟测试数据")
    
    # 筛选日期范围
    cutoff_date = (datetime.now() - timedelta(days=QUERY_MONTHS * 30)).date()
    df["截止日期"] = pd.to_datetime(df["截止日期"]).dt.date
    df = df[df["截止日期"] >= cutoff_date].copy()
    log.info(f"筛选最近 {QUERY_MONTHS} 个月（>= {cutoff_date}）后剩余 {len(df)} 条")

    # 筛选交易方式
    filtered = df[df["持股变动原因"].isin(TRADE_METHODS)].copy()
    log.info(f"筛选交易方式 {TRADE_METHODS} 后剩余 {len(filtered)} 条")

    if filtered.empty:
        return pd.DataFrame()

    # 过滤大股东增持
    filtered = filter_major_shareholders(filtered)
    
    # 排除ST/退市风险股
    filtered = filter_st_stocks(filtered)

    # 按公司分组，统计不同高管人数
    company_exec_count = (
        filtered.groupby(["证券代码", "证券简称"])["高管姓名"]
        .nunique()
        .reset_index()
        .rename(columns={"高管姓名": "增持高管人数"})
    )

    # 筛选达标公司
    qualified = company_exec_count[company_exec_count["增持高管人数"] >= MIN_EXECUTIVES]
    log.info(f"满足 >= {MIN_EXECUTIVES} 位高管增持的公司: {len(qualified)} 家")

    if qualified.empty:
        # 为了测试，降低门槛
        log.info("降低门槛到2位高管进行测试...")
        qualified = company_exec_count[company_exec_count["增持高管人数"] >= 2]
        log.info(f"满足 >= 2 位高管增持的公司: {len(qualified)} 家")

    if qualified.empty:
        return pd.DataFrame()

    # 合并详情
    result = filtered.merge(qualified[["证券代码", "证券简称", "增持高管人数"]], 
                           on=["证券代码", "证券简称"])

    return result


def enrich_data_with_market_info(result: pd.DataFrame) -> pd.DataFrame:
    """补充市值、股价等市场信息"""
    if result.empty:
        return result
    
    log.info("正在补充市场信息...")
    
    # 按公司汇总数据
    company_summary = []
    
    companies = result[["证券代码", "证券简称"]].drop_duplicates()
    
    for _, company in companies.iterrows():
        stock_code = company["证券代码"]
        stock_name = company["证券简称"]
        
        # 获取该公司的增持明细
        company_data = result[result["证券代码"] == stock_code]
        
        # 计算增持总金额和总股数
        total_shares = company_data["变动数量"].sum()
        avg_price = company_data["成交均价"].mean()
        total_amount = total_shares * avg_price / 10000 if pd.notna(avg_price) else 0  # 转换为万元
        
        # 获取市值
        market_cap = get_market_cap(stock_code)
        
        # 计算增持占市值比例
        holding_ratio = (total_amount / (market_cap * 10000)) if market_cap else 0  # 万元 vs 亿元
        
        # 获取股价数据
        price_data = get_stock_price_data(stock_code)
        
        # 统计超年薪增持高管数（模拟）
        import random
        super_salary_execs = random.randint(0, company_data["增持高管人数"].iloc[0])
        
        company_info = {
            "证券代码": stock_code,
            "证券简称": stock_name,
            "增持高管人数": company_data["增持高管人数"].iloc[0],
            "增持总股数": total_shares,
            "增持总金额": total_amount,
            "公司市值": market_cap,
            "增持占市值比例": holding_ratio,
            "超年薪高管数": super_salary_execs,
            **price_data
        }
        
        company_summary.append(company_info)
    
    return pd.DataFrame(company_summary)


def load_history() -> List[str]:
    """加载历史结果"""
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning(f"加载历史记录失败: {e}")
        return []


def save_history(companies: List[str]):
    """保存当前结果到历史记录"""
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"保存历史记录失败: {e}")


def mark_new_companies(summary_df: pd.DataFrame) -> pd.DataFrame:
    """标记新增公司"""
    if summary_df.empty:
        return summary_df
    
    history = load_history()
    current_companies = summary_df["证券代码"].tolist()
    
    # 标记新增公司
    summary_df["is_new"] = summary_df["证券代码"].apply(lambda x: x not in history)
    
    # 保存当前结果
    save_history(current_companies)
    
    return summary_df


def build_html_report(result: pd.DataFrame, summary_df: pd.DataFrame) -> str:
    """生成 HTML 邮件报告"""
    today = datetime.now().strftime("%Y-%m-%d")

    if result.empty:
        return f"""
        <html><body style="font-family:Arial,sans-serif;padding:20px;">
        <h2>高管增持监控报告 - {today}（测试版）</h2>
        <p>未发现满足条件的股票数据。</p>
        <p style="color:#999;font-size:12px;">
            筛选条件：排除大股东/实际控制人增持，排除ST股，交易方式={', '.join(TRADE_METHODS)}
        </p>
        <p style="color:#999;font-size:12px;">免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。</p>
        </body></html>
        """

    # 标记新增公司
    summary_df = mark_new_companies(summary_df)
    
    # 构建汇总表 HTML
    summary_rows = ""
    for _, row in summary_df.iterrows():
        new_mark = "🆕 " if row.get("is_new", False) else ""
        market_cap_str = f"{row['公司市值']:.2f}亿" if pd.notna(row["公司市值"]) else "-"
        ratio_str = f"{row['增持占市值比例']:.4%}" if pd.notna(row["增持占市值比例"]) else "-"
        amount_str = f"{row['增持总金额']:.2f}万" if pd.notna(row["增持总金额"]) else "-"
        
        summary_rows += f"""
        <tr>
            <td>{new_mark}{row['证券代码']}</td>
            <td>{new_mark}{row['证券简称']}</td>
            <td style="text-align:center;font-weight:bold;color:#e74c3c;">{row['增持高管人数']}</td>
            <td style="text-align:right;">{amount_str}</td>
            <td style="text-align:right;">{market_cap_str}</td>
            <td style="text-align:right;">{ratio_str}</td>
            <td style="text-align:center;">{row['超年薪高管数']}</td>
        </tr>"""

    # 技术分析表 HTML
    tech_rows = ""
    for _, row in summary_df.iterrows():
        if 'current_price' in row and pd.notna(row['current_price']):
            new_mark = "🆕 " if row.get("is_new", False) else ""
            price = f"{row['current_price']:.2f}"
            change = f"{row.get('price_change_pct', 0):.2f}%"
            ma5 = f"{row['ma5']:.2f}" if pd.notna(row.get('ma5')) else "-"
            ma10 = f"{row['ma10']:.2f}" if pd.notna(row.get('ma10')) else "-"
            ma20 = f"{row['ma20']:.2f}" if pd.notna(row.get('ma20')) else "-"
            bias5 = f"{row['bias5']:.2f}%" if pd.notna(row.get('bias5')) else "-"
            bias10 = f"{row['bias10']:.2f}%" if pd.notna(row.get('bias10')) else "-"
            ma_status = row.get('ma_status', '-')
            
            change_color = "green" if row.get('price_change_pct', 0) > 0 else "red"
            
            tech_rows += f"""
            <tr>
                <td>{new_mark}{row['证券代码']}</td>
                <td>{new_mark}{row['证券简称']}</td>
                <td style="text-align:right;">{price}</td>
                <td style="text-align:right;color:{change_color};">{change}</td>
                <td style="text-align:right;">{ma5}</td>
                <td style="text-align:right;">{ma10}</td>
                <td style="text-align:right;">{ma20}</td>
                <td style="text-align:center;">{ma_status}</td>
                <td style="text-align:right;">{bias5}</td>
                <td style="text-align:right;">{bias10}</td>
            </tr>"""

    # 明细表 HTML
    detail_cols = ["证券代码", "证券简称", "高管姓名", "董监高职务", "变动数量", "成交均价", "截止日期", "持股变动原因"]
    detail_df = result[detail_cols].sort_values(["证券代码", "高管姓名"])
    detail_rows = ""
    for _, row in detail_df.iterrows():
        price = f"{row['成交均价']:.2f}" if pd.notna(row["成交均价"]) else "-"
        detail_rows += f"""
        <tr>
            <td>{row['证券代码']}</td>
            <td>{row['证券简称']}</td>
            <td>{row['高管姓名']}</td>
            <td>{row['董监高职务']}</td>
            <td style="text-align:right;">{row['变动数量']:,.0f}</td>
            <td style="text-align:right;">{price}</td>
            <td>{row['截止日期']}</td>
            <td>{row['持股变动原因']}</td>
        </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;padding:20px;">
    <h2>高管增持监控报告 - {today}（测试版）</h2>
    <p>筛选条件：排除大股东/实际控制人增持，排除ST股，同一公司 ≥2 位普通高管增持（测试门槛），查询时间窗口：{QUERY_MONTHS}个月</p>

    <h3>📊 汇总表（共 {len(summary_df)} 家公司）</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:14px;">
        <tr style="background:#2c3e50;color:white;">
            <th>证券代码</th><th>证券简称</th><th>增持高管数</th><th>增持总金额</th><th>公司市值</th><th>增持占比</th><th>超年薪高管数</th>
        </tr>
        {summary_rows}
    </table>

    <h3>📈 技术分析（模拟数据）</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
        <tr style="background:#34495e;color:white;">
            <th>证券代码</th><th>证券简称</th><th>当前价</th><th>涨跌幅</th><th>MA5</th><th>MA10</th><th>MA20</th><th>均线状态</th><th>BIAS5</th><th>BIAS10</th>
        </tr>
        {tech_rows}
    </table>

    <h3>📝 增持明细</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
        <tr style="background:#34495e;color:white;">
            <th>证券代码</th><th>证券简称</th><th>高管姓名</th><th>职务</th><th>变动数量(股)</th><th>成交均价</th><th>截止日期</th><th>交易方式</th>
        </tr>
        {detail_rows}
    </table>

    <p style="color:#999;font-size:12px;margin-top:20px;">
        数据来源：巨潮资讯网、akshare | 🆕 表示新增公司 | 测试版本（包含模拟数据） | 自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M')}
    </p>
    <p style="color:#red;font-size:12px;font-weight:bold;">
        ⚠️ 免责声明：本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。高管增持不等于股价上涨，请结合其他因素综合判断。
    </p>
    </body></html>
    """
    return html


def send_email(html_content: str):
    """通过 QQ 邮箱 SMTP 发送 HTML 邮件"""
    if not EMAIL_PASSWORD:
        log.error("未配置 SMTP 授权码，请在 config.py 中填写 EMAIL_PASSWORD")
        sys.exit(1)

    today = datetime.now().strftime("%Y-%m-%d")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"高管增持监控报告 - {today}（测试版）"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    log.info(f"正在发送邮件到 {EMAIL_RECEIVER}...")
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        log.info("邮件发送成功")
    except Exception as e:
        log.error(f"邮件发送失败: {e}")
        raise


def main():
    log.info("=== 高管增持监控测试开始 ===")
    
    try:
        # 获取数据并筛选
        df = fetch_data()
        result = filter_data(df)
        
        # 补充市场信息
        summary_df = enrich_data_with_market_info(result)
        
        # 生成报告并发送邮件
        html = build_html_report(result, summary_df)
        send_email(html)
        
    except Exception:
        log.exception("运行出错")
        sys.exit(1)
    
    log.info("=== 高管增持监控测试完成 ===")


if __name__ == "__main__":
    main()