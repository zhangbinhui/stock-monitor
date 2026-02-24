import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# QQ 邮箱 SMTP 配置（从环境变量读取）
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.qq.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))

# 筛选参数
MIN_EXECUTIVES = 5  # 同一公司最少增持高管人数
TRADE_METHODS = ["竞价交易", "二级市场买卖"]  # 交易方式筛选

# 查询参数
QUERY_SYMBOL = "增持"
QUERY_MONTHS = 2  # 查询最近几个月的数据

# 新增筛选参数
# 过滤大股东关键词
EXCLUDE_KEYWORDS = ["控股股东", "实际控制人", "第一大股东", "控股", "实控人", "大股东"]

# 金额门槛设置
MIN_MARKET_CAP_RATIO = 0.0001  # 增持金额占市值的最低比例（0.01%）
EXEC_SALARY_MULTIPLIER = 0.5   # 增持金额 vs 年薪比例阈值（0.5倍年薪）

# 历史记录文件
HISTORY_FILE = "history_results.json"
