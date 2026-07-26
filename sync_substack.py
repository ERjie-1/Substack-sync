#!/usr/bin/env python3
"""
Substack to Notion 同步脚本 (GitHub Actions 版)
整合自 sync_v6_translate.py + 用户配置

功能：
1. 从 Gmail 获取 Substack 邮件
2. DeepSeek 聚合翻译（保持上下文）
3. Google Translate Fallback
4. 同步到两个 Notion 数据库
5. 智能去重和 Ticker 提取

环境变量配置：
- NOTION_API_TOKEN: Notion API Token (数据库1)
- NOTION_DATABASE_ID: Notion 数据库 ID (数据库1)
- NOTION_API_TOKEN_2: Notion API Token (数据库2，可选)
- NOTION_DATABASE_ID_2: Notion 数据库 ID (数据库2，可选)
- GMAIL_TOKEN: Gmail OAuth Token (JSON 格式)
- DEEPSEEK_API_KEY: DeepSeek API Key
- ENABLE_TRANSLATION: 是否启用翻译 (true/false)
"""

import os
import re
import json
import base64
import html
import quopri
import hashlib
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode

import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ============ 配置区域 ============
# Notion 配置 (从环境变量读取)
NOTION_API_TOKEN = os.environ.get("NOTION_API_TOKEN", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
NOTION_API_TOKEN_2 = os.environ.get("NOTION_API_TOKEN_2", "")
NOTION_DATABASE_ID_2 = os.environ.get("NOTION_DATABASE_ID_2", "")

# DeepSeek 配置
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1/chat/completions"
MAX_CHARS_PER_BATCH = 6000
MAX_BLOCKS_PER_BATCH = 80
MIN_TEXT_LENGTH = 20
MIN_TITLE_LENGTH = 5
DEFAULT_SYNC_LOOKBACK_DAYS = 21
DEFAULT_NOTION_TIMEOUT_SECONDS = 30
NOTION_TITLE_PREFIX = os.environ.get("NOTION_TITLE_PREFIX", "").strip()
RENAME_NOTION_PAGE_ID = os.environ.get("RENAME_NOTION_PAGE_ID", "").strip()
RENAME_NOTION_TITLE = os.environ.get("RENAME_NOTION_TITLE", "").strip()
SYNC_RECEIPT_DIR = Path(os.environ.get("SYNC_RECEIPT_DIR", "sync_receipts"))
SYNC_LEDGER_PATH = Path(os.environ.get("SYNC_LEDGER_PATH", str(SYNC_RECEIPT_DIR / "message_ledger.json")))
NOTION_GMAIL_MESSAGE_ID_PROPERTY = os.environ.get("NOTION_GMAIL_MESSAGE_ID_PROPERTY", "").strip()
SYNC_SENDER_EMAIL = os.environ.get("SYNC_SENDER_EMAIL", "").strip().lower()
SYNC_MESSAGE_IDS = {
    value.strip() for value in os.environ.get("SYNC_MESSAGE_IDS", "").split(",") if value.strip()
}
TRANSLATION_DIAGNOSTIC_ONLY = os.environ.get("TRANSLATION_DIAGNOSTIC_ONLY", "false").lower() == "true"
# Bounded recovery must not mutate unrelated recent pages (for example, empty
# status refresh). Keep the explicit env override for test/manual callers, but
# make the narrow message-id mode fail closed by default.
DISABLE_STATUS_SIDE_EFFECTS = (
    os.environ.get("DISABLE_STATUS_SIDE_EFFECTS", "false").lower() == "true"
    or bool(SYNC_MESSAGE_IDS)
    or TRANSLATION_DIAGNOSTIC_ONLY
)

# 翻译开关
ENABLE_TRANSLATION = os.environ.get("ENABLE_TRANSLATION", "true").lower() == "true"

# Gmail API
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class NotionRequestError(Exception):
    """HTTP/API failure with safe diagnostics for receipts and logs."""

    def __init__(self, message: str, *, status_code=None, path="", payload_sha256="",
                 request_id="", notion_code="", notion_message=""):
        super().__init__(message)
        self.status_code = status_code
        self.path = path
        self.payload_sha256 = payload_sha256
        self.request_id = request_id
        self.notion_code = notion_code
        self.notion_message = notion_message


class NotionWriteError(Exception):
    """Identifies the write phase and whether a page was already created."""

    def __init__(self, phase: str, cause: Exception, page_id: str = "", blocks_appended: int = 0,
                 total_blocks: int = 0):
        self.phase = phase
        self.cause = cause
        self.page_id = page_id
        self.partial_page_created = bool(page_id)
        self.blocks_appended = blocks_appended
        self.total_blocks = total_blocks
        super().__init__(f"Notion write failed in {phase}: {cause}")


class TranslationError(Exception):
    """DeepSeek translation failure with body-free diagnostics."""

    def __init__(self, message: str, diagnostics: Optional[Dict] = None):
        super().__init__(message)
        self.diagnostics = diagnostics or {}


def _sha256_json(value) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _redact_sensitive(value):
    """Redact credential-shaped fields without logging email bodies."""
    sensitive = ("authorization", "token", "cookie", "secret", "password", "api_key", "apikey")
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if any(part in key.lower() for part in sensitive)
            else _redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value[:10]]
    if isinstance(value, str):
        return value[:1000]
    return value


def _safe_subject(subject: str) -> dict:
    return {
        "subject_sha256": hashlib.sha256(subject.encode("utf-8", errors="ignore")).hexdigest(),
        "subject_prefix": subject[:100],
    }

# ============ 发件人配置 ============
# 你的 Substack 订阅源
GMAIL_QUERY = '''from:(
    lobwedge@substack.com OR
    robonomics@substack.com OR
    purpledrink@substack.com OR
    nathanbancroft@substack.com OR
    jamesbulltard@substack.com OR
    globalsemiresearch@substack.com OR
    wukong123@substack.com OR
    robs@substack.com OR
    oreo521@substack.com OR
    franktrading@substack.com OR
    tmtbreakout@substack.com OR
    semianalysis@substack.com OR
    capitalflows@substack.com OR
    sleepysol@substack.com OR
    globaltechresearch@substack.com OR
    citrini@substack.com OR
    swyx@substack.com OR
    swyx+ainews@substack.com OR
    streetsignal@substack.com OR
    alphaseeker84@substack.com OR
    benjaminusagi267@substack.com
) -"sign in to substack" -"upgrade to a paid subscription" -"your payment receipt from"'''

# 发件人显示名称映射
SOURCE_MAPPING = {
    'lobwedge@substack.com': 'LW Research',
    'robonomics@substack.com': 'Robonomics',
    'purpledrink@substack.com': 'Purple Drinks',
    'nathanbancroft@substack.com': 'Nathan',
    'jamesbulltard@substack.com': 'Bulltrad',
    'globalsemiresearch@substack.com': 'GlobalSemiResearch',
    'wukong123@substack.com': 'Wukong',
    'robs@substack.com': 'Robs',
    'oreo521@substack.com': 'Oreo',
    'franktrading@substack.com': 'Frank',
    'tmtbreakout@substack.com': 'TMTB',
    'semianalysis@substack.com': 'SemiAnalysis',
    'capitalflows@substack.com': 'CapitalFlows',
    'sleepysol@substack.com': 'SleepySol',
    'globaltechresearch@substack.com': 'GlobalTechResearch',
    'citrini@substack.com': 'Citrini',
    'swyx@substack.com': 'LatentSpace',
    'swyx+ainews@substack.com': 'LatentSpace',
    'streetsignal@substack.com': 'streetsignal',
    'alphaseeker84@substack.com': 'Elliot',
    'benjaminusagi267@substack.com': '本杰明',
}

# Sources intentionally archived to the primary Notion database only.
DB2_EXCLUDED_SOURCES = {"Robs", "LatentSpace", "本杰明"}

# ============ 股票 Ticker 列表 ============
STOCK_TICKERS = {
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NFLX", "NVDA", "AMD", "INTC",
    "TSM", "ASML", "AVGO", "QCOM", "AMAT", "LRCX", "KLAC", "MRVL", "ADI", "NXPI",
    "TXN", "MCHP", "TER", "SNPS", "CDNS", "ARM", "SWKS", "MPWR",
    "COHR", "LITE", "CIEN", "ANET", "CSCO", "KEYS", "FFIV", "JNPR",
    "SMCI", "DELL", "HPE", "HPQ", "IBM", "NTAP", "WDC", "STX",
    "CRM", "ORCL", "NOW", "SNOW", "PLTR", "PATH", "WDAY", "ADBE", "INTU", "PANW", "CRWD",
    "FTNT", "NET", "MDB", "DDOG", "TEAM", "VEEV", "AKAM", "EPAM", "CTSH",
    "ACN", "GDDY", "VRSN", "CSGP", "MSCI", "FICO", "PAYC", "PAYX", "ADP",
    "FDS", "JKHY", "FIS", "FISV", "GPN", "CPAY",
    "APP", "UBER", "ABNB", "BKNG", "EXPE", "DASH", "EBAY", "ETSY", "PYPL", "COIN",
    "HOOD", "TTD", "ROKU", "SPOT", "PINS", "SNAP", "MTCH", "TTWO", "RBLX",
    "BABA", "PDD", "BIDU", "NIO", "XPEV", "BILI", "TME", "NTES",
    "RIVN", "LCID", "APTV",
    "LLY", "UNH", "JNJ", "MRK", "ABBV", "PFE", "BMY", "AMGN", "GILD", "VRTX", "REGN",
    "JPM", "BAC", "WFC", "BLK", "KKR", "APO", "ARES", "SCHW",
    "GEV", "HON", "CAT", "RTX", "LMT", "NOC", "LHX", "HII",
    "XOM", "CVX", "COP", "OXY", "EOG", "DVN", "FANG", "MPC", "VLO", "PSX", "SLB",
    "NEE", "DUK", "AEP", "EXC", "SRE", "PCG", "XEL", "WEC", "VST", "CEG",
    "LIN", "APD", "SHW", "ECL", "DOW", "PPG", "NUE", "STLD", "VMC", "MLM",
    "KO", "PEP", "COST", "WMT", "TGT", "LOW", "DLTR",
    "AMT", "CCI", "SBAC", "PLD", "EQIX", "DLR", "PSA", "EXR", "SPG", "VICI",
    "DIS", "CMCSA", "CHTR", "WBD", "PARA", "FOX", "FOXA", "NWS", "NWSA", "LYV", "TKO",
}

COMPANY_MAPPINGS = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN", "meta": "META", "facebook": "META", "nvidia": "NVDA",
    "tesla": "TSLA", "netflix": "NFLX", "adobe": "ADBE", "salesforce": "CRM",
    "oracle": "ORCL", "intel": "INTC", "amd": "AMD", "advanced micro devices": "AMD",
    "qualcomm": "QCOM", "broadcom": "AVGO", "cisco": "CSCO", "ibm": "IBM",
    "asml": "ASML", "tsmc": "TSM", "taiwan semiconductor": "TSM",
    "micron": "MU", "applied materials": "AMAT", "lam research": "LRCX",
    "marvell": "MRVL", "arm": "ARM", "synopsys": "SNPS", "cadence": "CDNS",
    "jpmorgan": "JPM", "jp morgan": "JPM", "goldman": "GS", "goldman sachs": "GS",
    "morgan stanley": "MS", "bank of america": "BAC", "citigroup": "C",
    "wells fargo": "WFC", "blackrock": "BLK", "visa": "V", "mastercard": "MA",
    "disney": "DIS", "warner": "WBD", "comcast": "CMCSA", "spotify": "SPOT",
    "walmart": "WMT", "costco": "COST", "target": "TGT", "home depot": "HD",
    "starbucks": "SBUX", "mcdonald": "MCD", "nike": "NKE", "lululemon": "LULU",
    "alibaba": "BABA", "tencent": "TCEHY", "baidu": "BIDU", "pinduoduo": "PDD",
    "palantir": "PLTR", "snowflake": "SNOW", "datadog": "DDOG", "crowdstrike": "CRWD",
    "airbnb": "ABNB", "uber": "UBER", "doordash": "DASH", "applovin": "APP",
}

# ============ DeepSeek 翻译 Prompt ============
TRANSLATION_SYSTEM_PROMPT = """你是一位专业的金融科技翻译专家，负责翻译投资研究报告和科技新闻。

## 翻译规则

### 1. 金融术语
- bull/bear market → 牛市/熊市
- long/short position → 多头/空头
- yield curve → 收益率曲线
- forward guidance → 前瞻指引
- rate cut/hike → 降息/加息
- earnings call → 财报电话会
- guidance → 指引
- capex → 资本开支
- gross margin → 毛利率
- TAM → 潜在市场规模

### 2. 科技术语
- data center → 数据中心
- hyperscaler → 超大规模云厂商
- inference → 推理（AI语境）
- training → 训练（AI语境）
- agentic AI → AI Agent / 智能体

### 3. 保持原文不翻译
- 公司名：NVIDIA, Apple, Meta, Google, Microsoft, Alibaba 等
- 产品名：ChatGPT, Claude, iPhone, AWS 等
- 股票代码：$NVDA, $AAPL, TSLA 等
- 专业术语：forward P/E, EV/EBITDA 等
- 数字/百分比：+2.5%, $100B, 3Q24 等
- 缩写：CEO, CFO, IPO, AI, ML 等

### 4. 翻译格式要求
- 输入格式：每段以 [Pn] 标记开头
- 输出格式：必须保持相同的 [Pn] 标记，翻译紧跟标记后
- 不要添加、删除或合并任何段落标记

### 5. 输出示例
输入：
[P1] NVIDIA reported strong Q3 results, with revenue up 94% YoY to $35.1B.
[P2] Management raised FY25 guidance, citing continued demand for H100/H200.

输出：
[P1] 英伟达公布了强劲的第三季度业绩，营收同比增长94%至351亿美元。
[P2] 管理层上调了FY25指引，理由是H100/H200的需求持续强劲。

现在请翻译以下内容："""


# ============ 工具函数 ============
def clean_url(url: str) -> str:
    if not url:
        return ""
    return url.split('?')[0]

def normalize_url(url: str) -> str:
    """用于去重的 URL 规范化"""
    url = clean_url(url).strip()
    if url.endswith('/'):
        url = url[:-1]
    return url


def validate_and_fix_url(url: str) -> Optional[str]:
    """验证并修复 URL"""
    if not url:
        return None

    url = url.strip()
    url = url.replace('=\n', '').replace('=\r\n', '')
    url = re.sub(r'\s+', '', url)

    if url.startswith('//'):
        url = 'https:' + url
    elif not url.startswith(('http://', 'https://', 'mailto:')):
        if re.match(r'^[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,}', url):
            url = 'https://' + url
        else:
            return None

    if url.startswith(('http://', 'https://')):
        match = re.match(r'https?://([a-zA-Z0-9][-a-zA-Z0-9.]*[a-zA-Z0-9])', url)
        if not match:
            return None
        if len(url) > 2000:
            url = url[:2000]
        return url

    if url.startswith('mailto:'):
        return url

    return None


def convert_image_url(url: str) -> str:
    """转换特殊图片 URL"""
    if not url:
        return url

    # Beehiiv CDN
    if 'media.beehiiv.com/cdn-cgi' in url:
        match = re.search(r'(https://media\.beehiiv\.com/)cdn-cgi/image/[^/]+/(.*?)(?:\?.*)?$', url)
        if match:
            return match.group(1) + match.group(2)

    # Stratechery
    match = re.match(r'https://i\d\.wp\.com/(stratechery\.com/[^?]+)', url)
    if match:
        return 'https://' + match.group(1)

    return url


def decode_quoted_printable(text: str) -> str:
    try:
        text = re.sub(r'=\r?\n', '', text)
        decoded = quopri.decodestring(text.encode('utf-8', errors='ignore')).decode('utf-8', errors='ignore')
        return decoded
    except:
        return text


def decode_html_entities(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r'[\u034f\u200b-\u200f\u2028-\u202f\u205f-\u206f\ufeff]', '', text)
    return text


def extract_sender_tag(email_addr: str) -> str:
    """从邮件地址提取发件人标签"""
    if not email_addr:
        return "unknown"

    match = re.search(r'<([^>]+)>', email_addr)
    if match:
        email_addr = match.group(1)

    email_lower = email_addr.lower()

    # 检查映射表
    for email_key, display_name in SOURCE_MAPPING.items():
        if email_key.lower() in email_lower:
            return display_name

    # Fallback: 使用 @ 前的部分
    match = re.match(r"([^@]+)@", email_addr)
    if match:
        tag = match.group(1).lower()
        if '+' in tag:
            tag = tag.split('+')[0]
        return tag

    return "unknown"


def normalize_sender(sender: str) -> str:
    """标准化发件人名称，避免大小写/空格导致重复"""
    if not sender:
        return ""
    sender = sender.strip().lower()
    sender = re.sub(r'[^a-z0-9]+', '', sender)
    return sender


def generate_unique_id(subject: str, sender: str, date_str: str) -> str:
    """生成唯一 ID 用于去重"""
    date_only = date_str[:10] if date_str else ""
    sender_norm = normalize_sender(sender)
    content = f"{subject}|{sender_norm}|{date_only}"
    return hashlib.md5(content.encode()).hexdigest()[:16]


def match_company_to_ticker(name: str) -> Optional[str]:
    name_lower = name.lower().strip()
    return COMPANY_MAPPINGS.get(name_lower)


def extract_tickers(subject: str, html_content: str, source: str) -> List[str]:
    """提取股票代码"""
    found_tickers = set()
    exclude = {
        'CEO', 'CFO', 'COO', 'CTO', 'IPO', 'GDP', 'CPI', 'PPI',
        'ETF', 'USD', 'EUR', 'JPY', 'GBP', 'CNY', 'API', 'AI',
        'YTD', 'QOQ', 'YOY', 'MOM', 'BPS', 'EPS', 'ROE', 'ROA',
        'SEC', 'FED', 'ECB', 'BOJ', 'PMI', 'ISM', 'FOMC',
        'BUY', 'SELL', 'HOLD', 'NEW', 'THE', 'AND', 'FOR',
        'GPU', 'CPU', 'TPU', 'RAM', 'SSD', 'LLM', 'NLP',
        'OIL', 'GAS', 'GOLD', 'COAL', 'CES', 'USA', 'UK', 'EU',
    }

    # 从标题提取
    for m in re.finditer(r'\$([A-Z]{2,6})\b', subject + ' ' + html_content):
        ticker = m.group(1)
        if ticker not in exclude and ticker in STOCK_TICKERS:
            found_tickers.add(ticker)

    # Research 格式
    match = re.search(r'Research\|([A-Z]{2,6}):', subject)
    if match and match.group(1) not in exclude:
        found_tickers.add(match.group(1))

    return sorted(found_tickers)


def extract_article_url(text: str) -> str:
    """提取文章 URL"""
    patterns = [
        r'View in browser\s*\(\s*(https://[^\s\)]+)',
        r'x-newsletter:\s*(https://[^\s]+)',
        r'View this post on the web at\s+(https://[^\s<>"]+)',
        r'https://[a-zA-Z0-9-]+\.substack\.com/p/[a-zA-Z0-9-]+',
        r'https://newsletter\.[a-zA-Z0-9-]+\.com/p/[a-zA-Z0-9-]+',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_url(match.group(1) if '(' in pattern else match.group(0))

    return ""


def get_sync_lookback_days() -> int:
    """同步窗口：同时约束 Gmail 抓取和 Notion 去重，避免窗口错位造成重复"""
    raw_value = os.environ.get("SYNC_LOOKBACK_DAYS", str(DEFAULT_SYNC_LOOKBACK_DAYS))
    try:
        value = int(raw_value)
        if value < 1:
            raise ValueError
        return value
    except ValueError:
        print(f"Invalid SYNC_LOOKBACK_DAYS={raw_value!r}, fallback to {DEFAULT_SYNC_LOOKBACK_DAYS}")
        return DEFAULT_SYNC_LOOKBACK_DAYS


# ============ 翻译函数 ============
def is_numeric_list_item(text: str) -> bool:
    """检测是否为数字列表项"""
    if not text:
        return False
    text = text.strip()

    if re.match(r'^[\$]?[A-Z]{2,5}\s+[+-]?\d+', text):
        return True

    digits = len(re.findall(r'[\d$%+\-.,]', text))
    if len(text) > 0 and digits / len(text) > 0.3:
        return True

    return False


def should_translate_block(block: Dict) -> Tuple[bool, str]:
    """判断 block 是否需要翻译"""
    block_type = block.get("type", "")

    if block_type == "image":
        return False, "image"

    if block_type not in ["paragraph", "quote", "bulleted_list_item",
                          "numbered_list_item", "heading_1", "heading_2", "heading_3"]:
        return False, f"unsupported:{block_type}"

    rich_text = block.get(block_type, {}).get("rich_text", [])
    text = "".join(rt.get("text", {}).get("content", "") for rt in rich_text)

    if not text or not text.strip():
        return False, "empty"

    min_len = MIN_TITLE_LENGTH if block_type.startswith("heading_") else MIN_TEXT_LENGTH
    if len(text.strip()) < min_len:
        return False, f"short:{len(text)}"

    # 中文检测
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    if chinese_chars > len(text) * 0.3:
        return False, "chinese"

    # 数字列表检测
    if block_type in ["bulleted_list_item", "numbered_list_item"]:
        if is_numeric_list_item(text):
            return False, "numeric"

    return True, "ok"


def _deepseek_batch_diagnostics(texts: List[str], marked_input: str) -> Dict:
    lengths = [len(text) for text in texts]
    return {
        "provider": "deepseek",
        "model": DEEPSEEK_MODEL,
        "batch_size": len(texts),
        "batch_chars": sum(lengths),
        "batch_max_chars": max(lengths) if lengths else 0,
        "request_text_sha256": hashlib.sha256(marked_input.encode("utf-8", errors="ignore")).hexdigest(),
    }


def _safe_deepseek_error(response) -> Dict:
    diagnostics = {
        "http_status": response.status_code,
        "response_body_sha256": hashlib.sha256(response.text.encode("utf-8", errors="ignore")).hexdigest(),
    }
    try:
        payload = response.json()
    except ValueError:
        return diagnostics
    error_payload = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error_payload, dict):
        code = error_payload.get("code")
        message = error_payload.get("message")
        error_type = error_payload.get("type")
        if code:
            diagnostics["deepseek_error_code"] = str(code)[:120]
        if message:
            diagnostics["deepseek_error_message"] = str(message)[:500]
        if error_type:
            diagnostics["deepseek_error_type"] = str(error_type)[:120]
    return diagnostics


def call_deepseek_api(texts: List[str], timeout: int = 60) -> Optional[str]:
    """调用 DeepSeek API"""
    if not DEEPSEEK_API_KEY:
        return None

    marked_input = "\n".join([f"[P{i+1}] {t}" for i, t in enumerate(texts)])
    diagnostics = _deepseek_batch_diagnostics(texts, marked_input)

    try:
        response = requests.post(
            DEEPSEEK_BASE_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": TRANSLATION_SYSTEM_PROMPT},
                    {"role": "user", "content": marked_input}
                ],
                "temperature": 0.3,
                "max_tokens": 8000,
                "stream": False
            },
            timeout=timeout
        )

        if response.status_code != 200:
            diagnostics.update(_safe_deepseek_error(response))
            print(f"    DeepSeek error: {json.dumps(diagnostics, ensure_ascii=False, sort_keys=True)}")
            raise TranslationError("DeepSeek translation request failed", diagnostics)

        try:
            return response.json()["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            diagnostics["http_status"] = response.status_code
            diagnostics["response_body_sha256"] = hashlib.sha256(
                response.text.encode("utf-8", errors="ignore")
            ).hexdigest()
            print(f"    DeepSeek malformed response: {json.dumps(diagnostics, ensure_ascii=False, sort_keys=True)}")
            raise TranslationError("DeepSeek translation response malformed", diagnostics) from exc

    except requests.exceptions.Timeout:
        diagnostics["error_type"] = "Timeout"
        print(f"    DeepSeek timeout: {json.dumps(diagnostics, ensure_ascii=False, sort_keys=True)}")
        raise TranslationError("DeepSeek translation request timed out", diagnostics)
    except TranslationError:
        raise
    except Exception as e:
        diagnostics["error_type"] = type(e).__name__
        diagnostics["error"] = str(e)[:500]
        print(f"    DeepSeek exception: {json.dumps(diagnostics, ensure_ascii=False, sort_keys=True)}")
        raise TranslationError("DeepSeek translation request raised exception", diagnostics) from e


def parse_translation_response(response: str, count: int) -> List[Optional[str]]:
    """解析 DeepSeek 返回"""
    translations = [None] * count
    if not response:
        return translations

    pattern = r'\[P(\d+)\]\s*(.+?)(?=\[P\d+\]|$)'
    matches = re.findall(pattern, response, re.DOTALL)

    for idx_str, content in matches:
        try:
            idx = int(idx_str) - 1
            if 0 <= idx < count:
                translations[idx] = content.strip()
        except ValueError:
            continue

    return translations


def translate_blocks_deepseek(blocks: List[Dict]) -> List[Dict]:
    """DeepSeek 聚合翻译"""
    if not blocks or not ENABLE_TRANSLATION:
        return blocks

    # 收集需要翻译的文本
    texts_to_translate = []
    block_indices = []

    for i, block in enumerate(blocks):
        should_trans, reason = should_translate_block(block)
        if should_trans:
            block_type = block.get("type", "")
            rich_text = block.get(block_type, {}).get("rich_text", [])
            text = "".join(rt.get("text", {}).get("content", "") for rt in rich_text)
            texts_to_translate.append(text)
            block_indices.append(i)

    if not texts_to_translate:
        return blocks

    if not DEEPSEEK_API_KEY:
        raise TranslationError("DeepSeek API key missing", {
            "provider": "deepseek",
            "model": DEEPSEEK_MODEL,
            "batch_size": len(texts_to_translate),
            "batch_chars": sum(len(text) for text in texts_to_translate),
            "error_code": "missing_api_key",
        })

    print(f"    Translating {len(texts_to_translate)} blocks...")

    # 分批翻译
    translations = [None] * len(texts_to_translate)
    batch_start = 0
    char_count = 0

    for i, text in enumerate(texts_to_translate):
        char_count += len(text)
        is_last = (i == len(texts_to_translate) - 1)
        batch_full = (char_count > MAX_CHARS_PER_BATCH or
                      (i - batch_start + 1) >= MAX_BLOCKS_PER_BATCH)

        if batch_full or is_last:
            batch = texts_to_translate[batch_start:i+1]

            if DEEPSEEK_API_KEY:
                deepseek_response = call_deepseek_api(batch)
                batch_translations = parse_translation_response(deepseek_response, len(batch))
                if any(trans is None for trans in batch_translations):
                    diagnostics = {
                        "provider": "deepseek",
                        "model": DEEPSEEK_MODEL,
                        "batch_size": len(batch),
                        "batch_chars": sum(len(item) for item in batch),
                        "translated_count": sum(1 for trans in batch_translations if trans),
                        "expected_count": len(batch),
                        "response_text_sha256": hashlib.sha256(
                            (deepseek_response or "").encode("utf-8", errors="ignore")
                        ).hexdigest(),
                    }
                    print(
                        "    DeepSeek incomplete translation: "
                        f"{json.dumps(diagnostics, ensure_ascii=False, sort_keys=True)}"
                    )
                    raise TranslationError("DeepSeek translation response incomplete", diagnostics)

                for j, trans in enumerate(batch_translations):
                    translations[batch_start + j] = trans

            batch_start = i + 1
            char_count = 0

            if not is_last:
                time.sleep(0.3)

    # 映射回 blocks
    translated_blocks = []

    for i, block in enumerate(blocks):
        if i in block_indices:
            trans_idx = block_indices.index(i)
            translation = translations[trans_idx]

            if translation:
                block_type = block.get("type", "")
                original_rt = block.get(block_type, {}).get("rich_text", [])

                new_rt = list(original_rt)
                new_rt.append({
                    "type": "text",
                    "text": {"content": "\n"},
                    "annotations": {"bold": False, "italic": False, "strikethrough": False,
                                   "underline": False, "code": False, "color": "default"}
                })
                new_rt.append({
                    "type": "text",
                    "text": {"content": translation[:1900]},
                    "annotations": {"bold": False, "italic": True, "strikethrough": False,
                                   "underline": False, "code": False, "color": "gray"}
                })

                new_block = {
                    "object": "block",
                    "type": block_type,
                    block_type: {"rich_text": new_rt}
                }
                translated_blocks.append(new_block)
            else:
                translated_blocks.append(block)
        else:
            translated_blocks.append(block)

    return translated_blocks


# ============ HTML 转 Notion Blocks ============
def html_to_notion_blocks(html_content: str) -> List[Dict]:
    """将 HTML 转换为 Notion blocks"""
    if not html_content:
        return []

    html_content = decode_quoted_printable(html_content)
    html_content = decode_html_entities(html_content)

    # 移除样式和脚本
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<div[^>]*class="preview"[^>]*>.*?</div>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    # 移除页脚
    footer_patterns = [
        r'<div[^>]*class="[^"]*footer[^"]*"[^>]*>.*?</div>',
        r'Forwarded this email\?[^<]{0,200}',
        r'Unsubscribe[^<]{0,500}',
    ]
    for pattern in footer_patterns:
        html_content = re.sub(pattern, '', html_content, flags=re.DOTALL | re.IGNORECASE)

    blocks = []

    # 提取图片
    img_positions = []
    for match in re.finditer(r'<img[^>]+>', html_content, re.IGNORECASE):
        src_match = re.search(r'src=["\']([^"\']+)["\']', match.group(0), re.IGNORECASE)
        if src_match:
            src = html.unescape(src_match.group(1))
            if src.startswith('http') and not any(x in src.lower() for x in ['tracking', 'pixel', '1x1', 'spacer', 'blank']):
                img_positions.append((match.start(), src))

    # 解析元素
    element_pattern = re.compile(
        r'(<h[1-6][^>]*>.*?</h[1-6]>)|'
        r'(<blockquote[^>]*>.*?</blockquote>)|'
        r'(<ul[^>]*>.*?</ul>)|'
        r'(<ol[^>]*>.*?</ol>)|'
        r'(<p[^>]*>.*?</p>)',
        flags=re.DOTALL | re.IGNORECASE
    )

    processed_img_positions = set()
    last_end = 0

    for match in element_pattern.finditer(html_content):
        # 添加中间的图片
        for img_pos, img_src in img_positions:
            if last_end <= img_pos < match.start() and img_pos not in processed_img_positions:
                blocks.append({
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "external",
                        "external": {"url": convert_image_url(img_src)}
                    }
                })
                processed_img_positions.add(img_pos)

        element = match.group(0)
        block = parse_element_to_block(element)
        if block:
            if isinstance(block, list):
                blocks.extend(block)
            else:
                blocks.append(block)

        last_end = match.end()

    # 添加剩余图片
    for img_pos, img_src in img_positions:
        if img_pos not in processed_img_positions and img_pos >= last_end:
            blocks.append({
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": convert_image_url(img_src)}
                }
            })

    # 去重
    seen_content = set()
    unique_blocks = []
    for block in blocks:
        block_type = block.get("type", "")
        if block_type in ["paragraph", "heading_1", "heading_2", "heading_3", "quote"]:
            rich_text = block.get(block_type, {}).get("rich_text", [])
            content = "".join(rt.get("text", {}).get("content", "") for rt in rich_text)
            fingerprint = f"{block_type}:{content[:100].lower().strip()}"
            if fingerprint and fingerprint not in seen_content:
                seen_content.add(fingerprint)
                unique_blocks.append(block)
        else:
            unique_blocks.append(block)

    return unique_blocks


def parse_element_to_block(element: str) -> Optional[Dict]:
    """解析 HTML 元素为 Notion block"""
    # 标题
    h_match = re.match(r'<h([1-6])[^>]*>(.*?)</h\1>', element, re.DOTALL | re.IGNORECASE)
    if h_match:
        level = int(h_match.group(1))
        inner_html = h_match.group(2)
        rich_text = parse_rich_text(inner_html)
        if rich_text:
            block_type = f"heading_{min(level, 3)}"
            return {
                "object": "block",
                "type": block_type,
                block_type: {"rich_text": rich_text}
            }
        return None

    # 引用
    if element.lower().startswith('<blockquote'):
        inner_match = re.search(r'<blockquote[^>]*>(.*?)</blockquote>', element, re.DOTALL | re.IGNORECASE)
        if inner_match:
            inner_html = inner_match.group(1)
            rich_text = parse_rich_text(inner_html)
            if rich_text:
                return {
                    "object": "block",
                    "type": "quote",
                    "quote": {"rich_text": rich_text}
                }
        return None

    # 无序列表
    if element.lower().startswith('<ul'):
        items = re.findall(r'<li[^>]*>(.*?)</li>', element, re.DOTALL | re.IGNORECASE)
        blocks = []
        for item in items:
            rich_text = parse_rich_text(item)
            if rich_text:
                blocks.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": rich_text}
                })
        return blocks if blocks else None

    # 有序列表
    if element.lower().startswith('<ol'):
        items = re.findall(r'<li[^>]*>(.*?)</li>', element, re.DOTALL | re.IGNORECASE)
        blocks = []
        for item in items:
            rich_text = parse_rich_text(item)
            if rich_text:
                blocks.append({
                    "object": "block",
                    "type": "numbered_list_item",
                    "numbered_list_item": {"rich_text": rich_text}
                })
        return blocks if blocks else None

    # 段落
    rich_text = parse_rich_text(element)
    if rich_text:
        all_text = ''.join(item.get('text', {}).get('content', '') for item in rich_text)
        if all_text.strip():
            return {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": rich_text}
            }

    return None


def parse_rich_text(html_content: str) -> List[Dict]:
    """解析 HTML 为 Notion rich_text"""
    if not html_content:
        return []

    html_content = re.sub(r'<br\s*/?>', '\n', html_content, flags=re.IGNORECASE)

    rich_texts = []
    parts = re.split(r'(<[^>]+>)', html_content)

    current_text = ""
    bold = False
    italic = False
    underline = False
    code = False
    link = None

    def save_current():
        nonlocal current_text
        if current_text:
            text = html.unescape(current_text)
            text = re.sub(r'[\u00ad\u200b\u200c\u200d\u2060\ufeff\u034f]', '', text)
            if text:
                rt = {
                    "type": "text",
                    "text": {"content": text[:2000]},
                    "annotations": {
                        "bold": bold, "italic": italic, "underline": underline,
                        "strikethrough": False, "code": code, "color": "default"
                    }
                }
                if link:
                    rt["text"]["link"] = {"url": link}
                rich_texts.append(rt)
            current_text = ""

    for part in parts:
        if not part:
            continue

        if part.startswith('<'):
            tag_lower = part.lower()

            if tag_lower.startswith('</'):
                tag_match = re.match(r'</(\w+)', tag_lower)
                if tag_match:
                    name = tag_match.group(1)
                    if name in ['strong', 'b']:
                        save_current()
                        bold = False
                    elif name in ['em', 'i']:
                        save_current()
                        italic = False
                    elif name == 'u':
                        save_current()
                        underline = False
                    elif name == 'code':
                        save_current()
                        code = False
                    elif name == 'a':
                        save_current()
                        link = None
            else:
                tag_match = re.match(r'<(\w+)', tag_lower)
                if tag_match:
                    name = tag_match.group(1)
                    if name in ['strong', 'b']:
                        save_current()
                        bold = True
                    elif name in ['em', 'i']:
                        save_current()
                        italic = True
                    elif name == 'u':
                        save_current()
                        underline = True
                    elif name == 'code':
                        save_current()
                        code = True
                    elif name == 'a':
                        save_current()
                        href_match = re.search(r'href=["\']([^"\']+)["\']', part, re.IGNORECASE)
                        if href_match:
                            raw_url = href_match.group(1)
                            validated_url = validate_and_fix_url(raw_url)
                            link = validated_url
        else:
            current_text += part

    save_current()
    rich_texts = [rt for rt in rich_texts if rt.get("text", {}).get("content", "").strip()]
    return rich_texts if rich_texts else []


# ============ Gmail API ============
def get_gmail_service():
    """获取 Gmail 服务"""
    token_json = os.environ.get("GMAIL_TOKEN")
    if not token_json:
        raise Exception("GMAIL_TOKEN environment variable not set")

    creds_data = json.loads(token_json)
    creds = Credentials.from_authorized_user_info(creds_data, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        raise Exception("Gmail credentials are invalid")

    return build('gmail', 'v1', credentials=creds)


def get_emails(
    service,
    query: str,
    max_results: int = 50,
    message_ids: Optional[Set[str]] = None,
) -> List[Dict]:
    """获取邮件列表"""
    emails = []
    if message_ids:
        # Bounded recovery must fetch the requested messages directly; a
        # latest-N list window can omit older known IDs and falsely fail closed.
        messages = [{'id': message_id} for message_id in sorted(message_ids)]
    else:
        results = service.users().messages().list(
            userId='me', q=query, maxResults=max_results
        ).execute()
        messages = results.get('messages', [])

    for msg in messages:
        message = service.users().messages().get(
            userId='me', id=msg['id'], format='full'
        ).execute()

        headers = message.get('payload', {}).get('headers', [])
        internal_date_ms = message.get('internalDate', '')

        email_data = {
            'id': msg['id'],
            'subject': '',
            'from': '',
            'date': '',
            'internal_date': internal_date_ms,
            'body_text': '',
            'body_html': ''
        }

        for header in headers:
            name = header.get('name', '').lower()
            value = header.get('value', '')
            if name == 'subject':
                email_data['subject'] = decode_html_entities(value)
            elif name == 'from':
                email_data['from'] = value
            elif name == 'date':
                email_data['date'] = value

        payload = message.get('payload', {})
        text_body, html_body = get_email_body(payload)
        email_data['body_text'] = text_body
        email_data['body_html'] = html_body

        emails.append(email_data)

    return emails


def normalized_sender_email(header_value: str) -> str:
    match = re.search(r'<([^>]+)>', header_value or "")
    return (match.group(1) if match else (header_value or "")).strip().lower()


def get_email_body(payload: Dict) -> Tuple[str, str]:
    """提取邮件正文"""
    text_body = ""
    html_body = ""

    def extract_parts(payload):
        nonlocal text_body, html_body
        mime_type = payload.get('mimeType', '')

        if 'body' in payload and payload['body'].get('data'):
            data = payload['body']['data']
            decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            if mime_type == 'text/plain':
                text_body = decoded
            elif mime_type == 'text/html':
                html_body = decoded

        if 'parts' in payload:
            for part in payload['parts']:
                extract_parts(part)

    extract_parts(payload)
    return text_body, html_body


# ============ Notion API ============
class NotionAPI:
    BASE_URL = "https://api.notion.com/v1"

    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }

    def _request(self, method: str, path: str, body: Dict) -> Dict:
        response = requests.request(
            method,
            f"{self.BASE_URL}{path}",
            headers=self.headers,
            json=body,
            timeout=DEFAULT_NOTION_TIMEOUT_SECONDS
        )
        payload_sha256 = _sha256_json(body)
        if not response.ok:
            try:
                raw_error = response.json()
            except ValueError:
                raw_error = {"raw": response.text[:1000]}
            safe_error = _redact_sensitive(raw_error)
            request_id = response.headers.get("x-request-id", "")
            if isinstance(raw_error, dict):
                request_id = request_id or raw_error.get("request_id", "")
            notion_code = raw_error.get("code", "") if isinstance(raw_error, dict) else ""
            notion_message = raw_error.get("message", "") if isinstance(raw_error, dict) else ""
            diagnostic = {
                "status_code": response.status_code,
                "method": method,
                "path": path,
                "request_id": request_id,
                "notion_code": notion_code,
                "notion_message": notion_message,
                "payload_sha256": payload_sha256,
                "response": safe_error,
            }
            print(f"[NOTION_HTTP_ERROR] {json.dumps(diagnostic, ensure_ascii=False, sort_keys=True)}")
            raise NotionRequestError(
                f"HTTP {response.status_code} for {method} {path}: {notion_message or 'Notion request failed'}",
                status_code=response.status_code,
                path=path,
                payload_sha256=payload_sha256,
                request_id=request_id,
                notion_code=notion_code,
                notion_message=notion_message,
            )
        data = response.json()
        if isinstance(data, dict) and data.get("object") == "error":
            safe_error = _redact_sensitive(data)
            print(f"[NOTION_API_ERROR] {json.dumps(safe_error, ensure_ascii=False, sort_keys=True)}")
            raise NotionRequestError(
                f"Notion API error: {data.get('code', 'unknown')} - {data.get('message', data)}",
                status_code=response.status_code,
                path=path,
                payload_sha256=payload_sha256,
                request_id=data.get("request_id", ""),
                notion_code=data.get("code", ""),
                notion_message=data.get("message", ""),
            )
        return data

    def query_database(self, database_id: str, start_cursor: str = None, payload: Dict = None) -> Dict:
        body = dict(payload) if payload else {}
        if start_cursor:
            body["start_cursor"] = start_cursor
        return self._request("POST", f"/databases/{database_id}/query", body)

    def create_page(self, database_id: str, properties: Dict, children: List[Dict] = None) -> Dict:
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if children:
            body["children"] = children[:100]
        return self._request("POST", "/pages", body)

    def append_blocks(self, page_id: str, children: List[Dict]) -> Dict:
        body = {"children": children[:100]}
        return self._request("PATCH", f"/blocks/{page_id}/children", body)

    def update_page(self, page_id: str, properties: Dict) -> Dict:
        body = {"properties": properties}
        return self._request("PATCH", f"/pages/{page_id}", body)

    def count_block_children(self, page_id: str) -> int:
        """Count all top-level children, following Notion pagination."""
        count = 0
        start_cursor = None
        while True:
            query = {"page_size": "100"}
            if start_cursor:
                query["start_cursor"] = start_cursor
            path = f"/blocks/{page_id}/children?{urlencode(query)}"
            result = self._request("GET", path, {})
            count += len(result.get("results", []))
            if not result.get("has_more"):
                return count
            start_cursor = result.get("next_cursor")
            if not start_cursor:
                raise RuntimeError("Notion block children response has_more=true without next_cursor")

    def create_page_with_all_blocks(self, database_id: str, properties: Dict, children: List[Dict],
                                    progress_callback=None) -> Dict:
        if not children:
            children = []

        try:
            # Create the page without content first so property and block errors
            # are isolated and a partial-page failure is explicit.
            result = self.create_page(database_id, properties)
        except Exception as exc:
            raise NotionWriteError("create_page", exc) from exc

        if not result.get("id"):
            return result

        page_id = result["id"]
        remaining = children[100:]
        first_batch = children[:100]
        if first_batch:
            remaining = first_batch + remaining
        blocks_appended = 0
        if progress_callback:
            progress_callback(page_id, blocks_appended, len(children))
        while remaining:
            batch = remaining[:100]
            remaining = remaining[100:]
            try:
                self.append_blocks(page_id, batch)
            except Exception as exc:
                raise NotionWriteError(
                    "append_blocks", exc, page_id=page_id,
                    blocks_appended=blocks_appended, total_blocks=len(children)
                ) from exc
            blocks_appended += len(batch)
            if progress_callback:
                progress_callback(page_id, blocks_appended, len(children))

        return result


def _split_notion_text(content: str, max_utf16_units: int = 2000) -> List[str]:
    """Split text using Notion's UTF-16 length accounting."""
    fragments = []
    current = []
    units = 0
    for char in content:
        char_units = len(char.encode("utf-16-le")) // 2
        if current and units + char_units > max_utf16_units:
            fragments.append("".join(current))
            current = []
            units = 0
        current.append(char)
        units += char_units
    if current:
        fragments.append("".join(current))
    return fragments


def sanitize_blocks_for_notion(blocks: List[Dict]) -> List[Dict]:
    """清理 blocks 中的无效链接并满足 Notion rich_text 限制"""
    sanitized = []

    for block in blocks:
        block_type = block.get("type", "")

        if block_type == "image":
            img_url = block.get("image", {}).get("external", {}).get("url", "")
            if img_url and img_url.startswith(('http://', 'https://')):
                sanitized.append(block)
            continue

        if block_type in ["paragraph", "heading_1", "heading_2", "heading_3",
                          "quote", "bulleted_list_item", "numbered_list_item"]:
            rich_text = block.get(block_type, {}).get("rich_text", [])
            cleaned_rich_text = []

            for rt in rich_text:
                if rt.get("type") == "text":
                    text_payload = rt.get("text", {})
                    content = text_payload.get("content", "")
                    if "link" in text_payload:
                        link_url = text_payload["link"].get("url", "")
                        validated = validate_and_fix_url(link_url)
                        if validated:
                            text_payload = dict(text_payload)
                            text_payload["link"] = {"url": validated}
                        else:
                            text_payload = dict(text_payload)
                            text_payload.pop("link", None)
                    if len(content.encode("utf-16-le")) // 2 > 2000:
                        # Translation can append text to an already parsed
                        # segment. Notion rejects any single rich_text content
                        # over 2000 chars, so split while retaining annotations
                        # and link metadata on each fragment.
                        for fragment_content in _split_notion_text(content):
                            fragment = dict(rt)
                            fragment["text"] = dict(text_payload)
                            fragment["text"]["content"] = fragment_content
                            cleaned_rich_text.append(fragment)
                        continue
                    rt = dict(rt)
                    rt["text"] = text_payload
                    cleaned_rich_text.append(rt)
                else:
                    cleaned_rich_text.append(rt)

            if cleaned_rich_text:
                new_block = {
                    "object": "block",
                    "type": block_type,
                    block_type: {"rich_text": cleaned_rich_text}
                }
                sanitized.append(new_block)
        else:
            sanitized.append(block)

    return sanitized


def update_recent_empty_statuses(notion: NotionAPI, database_id: str, limit: int = 20) -> None:
    """将最近的空状态条目设置为“待处理”"""
    print(f"Checking recent {limit} Notion items for empty status...")
    try:
        payload = {
            "page_size": limit,
            "sorts": [{"property": "Date", "direction": "descending"}],
        }
        result = notion.query_database(database_id, payload=payload)
    except Exception as e:
        print(f"Error querying recent items: {e}")
        return

    pages = result.get("results", [])
    updated = 0
    skipped = 0
    missing_status = 0

    for page in pages:
        page_id = page.get("id")
        props = page.get("properties", {})
        status_prop = props.get("状态")
        if not status_prop or "select" not in status_prop:
            missing_status += 1
            continue

        select_value = status_prop.get("select")
        if select_value is None:
            if not page_id:
                continue
            try:
                notion.update_page(page_id, {"状态": {"select": {"name": "待处理"}}})
                updated += 1
            except Exception as e:
                print(f"Error updating page {page_id}: {e}")
        else:
            skipped += 1

    print(f"Status update done. Updated: {updated}, Skipped: {skipped}, Missing status: {missing_status}")


def url_exists_in_notion(notion: NotionAPI, database_id: str, url: str) -> Tuple[bool, Dict]:
    """Create 前的 URL 精确查询 guard。"""
    payload = {
        "page_size": 1,
        "filter": {
            "property": "URL",
            "url": {"equals": url}
        }
    }
    result = notion.query_database(database_id, payload=payload)
    if result.get("object") == "error":
        raise Exception(f"Notion query error: {result.get('message', result)}")
    rows = len(result.get("results", []))
    return rows > 0, {"rows": rows, "has_more": result.get("has_more", False)}


def _property_text(properties: Dict, property_name: str) -> str:
    """Read a title/rich_text property without assuming one Notion type."""
    prop = properties.get(property_name, {})
    for key in ("rich_text", "title"):
        values = prop.get(key, []) or []
        if values:
            return values[0].get("plain_text") or values[0].get("text", {}).get("content", "")
    return ""


def write_sync_receipt(receipt: Dict) -> str:
    SYNC_RECEIPT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = SYNC_RECEIPT_DIR / f"substack_sync_{stamp}.json"
    path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[RECEIPT] path={path} status={receipt.get('status')} failures={receipt.get('failure_count', 0)}")
    return str(path)


def load_message_ledger() -> Dict[str, Dict]:
    if not SYNC_LEDGER_PATH.exists():
        return {}
    try:
        data = json.loads(SYNC_LEDGER_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError) as exc:
        print(f"[LEDGER] unreadable; failing closed: {exc}")
        raise SystemExit(1)


def write_message_ledger(ledger: Dict[str, Dict]) -> None:
    SYNC_LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    SYNC_LEDGER_PATH.write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[LEDGER] path={SYNC_LEDGER_PATH} entries={len(ledger)}")


def failure_record(email: Dict, *, phase: str, error: Exception, page_id: str = "",
                   partial_page_created: bool = False) -> Dict:
    if isinstance(error, NotionWriteError):
        phase = error.phase
        page_id = error.page_id
        partial_page_created = error.partial_page_created
        error = error.cause
    subject = email.get("subject", "")
    record = {
        "gmail_message_id": email.get("id", ""),
        "phase": phase,
        "page_id": page_id,
        "partial_page_created": partial_page_created,
        **_safe_subject(subject),
        "body_sha256": hashlib.sha256(email.get("body_html", "").encode("utf-8", errors="ignore")).hexdigest(),
        "error_type": type(error).__name__,
        "error": str(error)[:500],
    }
    if isinstance(error, NotionRequestError):
        record.update({
            "status_code": error.status_code,
            "path": error.path,
            "request_id": error.request_id,
            "notion_code": error.notion_code,
            "notion_message": error.notion_message,
            "payload_sha256": error.payload_sha256,
        })
    if isinstance(error, TranslationError):
        record["translation_diagnostics"] = error.diagnostics
    return record


# ============ 主同步函数 ============
def sync_gmail_to_notion():
    """主同步函数"""
    print(f"=" * 60)
    print(f"Substack to Notion Sync - {datetime.now().isoformat()}")
    print(f"=" * 60)
    print(f"Translation: {'Enabled (DeepSeek)' if ENABLE_TRANSLATION and DEEPSEEK_API_KEY else 'Disabled'}")

    max_results = int(os.environ.get("MAX_EMAIL_LIMIT", "50"))
    lookback_days = get_sync_lookback_days()
    sender_query = f"from:{SYNC_SENDER_EMAIL} " if SYNC_SENDER_EMAIL else ""
    gmail_query = f"{sender_query}{GMAIL_QUERY} newer_than:{lookback_days}d"
    print(f"Max emails to fetch: {max_results}")
    print(f"Sync lookback days: {lookback_days}")

    receipt = {
        "schema_version": "substack_sync_receipt_v2",
        "run_started_at": datetime.now().isoformat(),
        "status": "RUNNING",
        "max_email_limit": max_results,
        "lookback_days": lookback_days,
        "idempotency_mode": "gmail_message_id_property" if NOTION_GMAIL_MESSAGE_ID_PROPERTY else "legacy_url_title_date_plus_in_run_gmail_id",
        "message_id_property": NOTION_GMAIL_MESSAGE_ID_PROPERTY or None,
        "ledger_path": str(SYNC_LEDGER_PATH),
        "status_side_effects_disabled": DISABLE_STATUS_SIDE_EFFECTS,
        "translation_diagnostic_only": TRANSLATION_DIAGNOSTIC_ONLY,
        "translation_diagnostic_results": [],
        "failures": [],
        "db2_failures": [],
        "message_results": [],
        "requested_message_ids": sorted(SYNC_MESSAGE_IDS),
        "sender_filter": SYNC_SENDER_EMAIL or None,
    }

    def record_message_result(email: Dict, **fields):
        if not SYNC_MESSAGE_IDS:
            return
        receipt["message_results"].append({
            "gmail_message_id": email.get("id", ""),
            "subject_prefix": email.get("subject", "")[:120],
            **fields,
        })

    def fail_closed(phase: str, error: Exception):
        record = {
            "phase": phase,
            "error_type": type(error).__name__,
            "error": str(error)[:500],
        }
        receipt.update({
            "run_finished_at": datetime.now().isoformat(),
            "status": "FAIL",
            "failures": [record],
            "failure_count": 1,
            "db2_failure_count": 0,
        })
        write_sync_receipt(receipt)
        print(f"[SETUP_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
        raise SystemExit(1)

    # The durable Gmail message-id property is optional. When absent, retain
    # the existing URL/title-date deduplication and record that mode explicitly
    # in the receipt; the local ledger still covers same-run/partial recovery.

    # 初始化 Notion API
    notion = NotionAPI(NOTION_API_TOKEN)

    if TRANSLATION_DIAGNOSTIC_ONLY and (RENAME_NOTION_PAGE_ID or RENAME_NOTION_TITLE):
        fail_closed(
            "translation_diagnostic_config",
            RuntimeError("translation diagnostic mode does not allow Notion rename inputs"),
        )
    notion2 = NotionAPI(NOTION_API_TOKEN_2) if NOTION_API_TOKEN_2 and NOTION_DATABASE_ID_2 else None
    if bool(RENAME_NOTION_PAGE_ID) != bool(RENAME_NOTION_TITLE):
        fail_closed("notion_rename_setup", RuntimeError("RENAME_NOTION_PAGE_ID and RENAME_NOTION_TITLE must be provided together"))
    if RENAME_NOTION_PAGE_ID:
        try:
            notion.update_page(RENAME_NOTION_PAGE_ID, {
                "Name": {"title": [{"type": "text", "text": {"content": RENAME_NOTION_TITLE[:200]}}]}
            })
            receipt["rename_result"] = {
                "status": "PASS",
                "page_id": RENAME_NOTION_PAGE_ID,
                "title": RENAME_NOTION_TITLE[:200],
            }
            print(f"[NOTION_RENAME] page_id={RENAME_NOTION_PAGE_ID} status=PASS")
        except Exception as exc:
            fail_closed("notion_rename", exc)
    if notion2:
        print("DB2: Enabled")
    else:
        print("DB2: Disabled (missing NOTION_API_TOKEN_2 or NOTION_DATABASE_ID_2)")

    # Smoke runs must not mutate unrelated pages before processing the candidate.
    if DISABLE_STATUS_SIDE_EFFECTS:
        print("[SIDE_EFFECTS] update_recent_empty_statuses disabled")
    else:
        update_recent_empty_statuses(notion, NOTION_DATABASE_ID, limit=20)

    # 获取已存在的文章（用于去重，只查同步窗口内）
    existing_items = set()
    existing_urls = set()
    existing_pages_by_url = {}
    existing_message_ids = set()
    message_ledger = load_message_ledger()
    existing_message_ids.update(message_ledger.keys())
    dedup_pages_fetched = 0
    dedup_rows_fetched = 0
    url_guard_calls = 0
    url_guard_hits = 0
    url_guard_fail_closed = 0
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    dedup_filter = {
        "filter": {
            "property": "Date",
            "date": {"on_or_after": cutoff_date}
        }
    }
    try:
        has_more = True
        start_cursor = None

        while has_more:
            result = notion.query_database(NOTION_DATABASE_ID, start_cursor=start_cursor, payload=dedup_filter)
            if result.get("object") == "error":
                raise Exception(f"Notion query error: {result.get('message', result)}")
            dedup_pages_fetched += 1
            dedup_rows_fetched += len(result.get("results", []))

            for page in result.get("results", []):
                props = page.get("properties", {})
                title_prop = props.get("Name", {}).get("title", [])
                sender_prop = props.get("发件人", {}).get("select", {})
                date_prop = props.get("Date", {}).get("date", {})
                url_prop = props.get("URL", {}).get("url", "")

                title = title_prop[0].get("text", {}).get("content", "") if title_prop else ""
                sender_name = sender_prop.get("name", "") if sender_prop else ""
                date_str = date_prop.get("start", "") if date_prop else ""

                if title and sender_name and date_str:
                    existing_items.add(generate_unique_id(title, sender_name, date_str))
                if url_prop:
                    norm_url = normalize_url(url_prop)
                    if norm_url:
                        existing_urls.add(norm_url)
                        existing_pages_by_url.setdefault(norm_url, page.get("id", ""))
                if NOTION_GMAIL_MESSAGE_ID_PROPERTY:
                    message_id = _property_text(props, NOTION_GMAIL_MESSAGE_ID_PROPERTY)
                    if message_id:
                        existing_message_ids.add(message_id)

            has_more = result.get("has_more", False)
            start_cursor = result.get("next_cursor")

    except Exception as e:
        print(f"Error fetching existing items: {e}")
        fail_closed("dedup_query", e)

    print(f"Existing articles in Notion: {len(existing_items)}")
    print(f"Existing URLs in Notion: {len(existing_urls)}")
    print(
        f"Dedup query stats: cutoff={cutoff_date}, pages_fetched={dedup_pages_fetched}, "
        f"rows_fetched={dedup_rows_fetched}, existing_items={len(existing_items)}, existing_urls={len(existing_urls)}"
    )

    # Safety check: 同步窗口内应有文章，空结果可能是 API 异常
    if len(existing_items) == 0:
        print("WARNING: 0 existing articles found, retrying dedup query...")
        existing_items = set()
        existing_urls = set()
        existing_pages_by_url = {}
        dedup_pages_fetched = 0
        dedup_rows_fetched = 0
        try:
            has_more = True
            start_cursor = None
            while has_more:
                result = notion.query_database(NOTION_DATABASE_ID, start_cursor=start_cursor, payload=dedup_filter)
                response_obj = result.get("object")
                if response_obj == "error":
                    raise Exception(f"Notion query error: {result.get('message', result)}")
                dedup_pages_fetched += 1
                dedup_rows_fetched += len(result.get("results", []))
                for page in result.get("results", []):
                    props = page.get("properties", {})
                    title_prop = props.get("Name", {}).get("title", [])
                    sender_prop = props.get("发件人", {}).get("select", {})
                    date_prop = props.get("Date", {}).get("date", {})
                    url_prop = props.get("URL", {}).get("url", "")
                    title = title_prop[0].get("text", {}).get("content", "") if title_prop else ""
                    sender_name = sender_prop.get("name", "") if sender_prop else ""
                    date_str = date_prop.get("start", "") if date_prop else ""
                    if title and sender_name and date_str:
                        existing_items.add(generate_unique_id(title, sender_name, date_str))
                    if url_prop:
                        norm_url = normalize_url(url_prop)
                        if norm_url:
                            existing_urls.add(norm_url)
                            existing_pages_by_url.setdefault(norm_url, page.get("id", ""))
                    if NOTION_GMAIL_MESSAGE_ID_PROPERTY:
                        message_id = _property_text(props, NOTION_GMAIL_MESSAGE_ID_PROPERTY)
                        if message_id:
                            existing_message_ids.add(message_id)
                has_more = result.get("has_more", False)
                start_cursor = result.get("next_cursor")
        except Exception as e:
            print(f"Retry also failed: {e}")
            fail_closed("dedup_query_retry", e)
        print(f"Retry result - Existing articles: {len(existing_items)}, URLs: {len(existing_urls)}")
        print(
            f"Retry dedup stats: cutoff={cutoff_date}, pages_fetched={dedup_pages_fetched}, "
            f"rows_fetched={dedup_rows_fetched}, existing_items={len(existing_items)}, existing_urls={len(existing_urls)}"
        )
        if len(existing_items) == 0:
            print("ERROR: Dedup query returned 0 articles twice. Aborting to prevent duplicates.")
            fail_closed("dedup_empty_twice", RuntimeError("dedup query returned zero existing articles twice"))

    # 获取邮件
    try:
        gmail_service = get_gmail_service()
        emails = get_emails(
            gmail_service,
            gmail_query,
            max_results=max_results,
            message_ids=SYNC_MESSAGE_IDS or None,
        )
        if SYNC_MESSAGE_IDS:
            fetched_ids = {email.get("id", "") for email in emails}
            missing_ids = sorted(SYNC_MESSAGE_IDS - fetched_ids)
            receipt["requested_message_ids_missing"] = missing_ids
            if missing_ids:
                print(f"[CANDIDATE_MISSING] requested Gmail ids not fetched: {missing_ids}")
                raise RuntimeError("bounded candidate list contains Gmail ids not present in fetched window")
            emails = [email for email in emails if email.get("id", "") in SYNC_MESSAGE_IDS]
            print(f"[CANDIDATES] bounded Gmail ids: {[email.get('id', '') for email in emails]}")
        print(f"Fetched {len(emails)} emails from Gmail")
    except Exception as e:
        print(f"Error fetching emails: {e}")
        fail_closed("gmail_fetch", e)

    # 同步邮件
    synced_count = 0
    processed_count = 0
    skipped_count = 0
    diagnosed_count = 0

    for email in emails:
        try:
            processed_count += 1
            subject = email['subject']
            sender = email['from']
            if SYNC_SENDER_EMAIL and normalized_sender_email(sender) != SYNC_SENDER_EMAIL:
                record_message_result(email, result="skipped_sender_mismatch")
                skipped_count += 1
                continue
            body_html = email['body_html']
            body_text = email['body_text']
            sender_tag = extract_sender_tag(sender)
            gmail_message_id = email.get("id", "")
            ledger_entry = message_ledger.get(gmail_message_id, {})
            recovery_mode = ledger_entry.get("db1_state") in ("page_created", "partial_page_created", "appending") or ledger_entry.get("db2_state") in ("pending", "partial", "failed")
            print(f"[DEBUG] from='{sender}' -> sender_tag='{sender_tag}'")

            # 跳过欢迎邮件
            if subject.lower().startswith('welcome to '):
                print(f"[SKIP] Welcome email: {subject[:50]}...")
                skipped_count += 1
                continue

            # 解析日期
            try:
                if email.get('internal_date'):
                    timestamp_ms = int(email['internal_date'])
                    email_date = datetime.fromtimestamp(timestamp_ms / 1000)
                else:
                    email_date = parsedate_to_datetime(email['date'])
                date_str = email_date.strftime("%Y-%m-%dT%H:%M")
            except:
                date_str = datetime.now().strftime("%Y-%m-%dT%H:%M")

            # 提取文章 URL
            article_url = extract_article_url(body_text) or extract_article_url(body_html)
            article_url_norm = normalize_url(article_url) if article_url else ""
            validated_url = validate_and_fix_url(article_url) if article_url else None

            if TRANSLATION_DIAGNOSTIC_ONLY:
                content_blocks = html_to_notion_blocks(body_html) if body_html else []
                if ENABLE_TRANSLATION and content_blocks:
                    try:
                        translated_blocks = translate_blocks_deepseek(content_blocks)
                    except TranslationError as exc:
                        record = failure_record(email, phase="translate_blocks", error=exc)
                        receipt["failures"].append(record)
                        print(f"[TRANSLATION_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                        continue
                    receipt["translation_diagnostic_results"].append({
                        "gmail_message_id": gmail_message_id,
                        **_safe_subject(subject),
                        "result": "translation_ok_no_write",
                        "content_block_count": len(translated_blocks),
                    })
                    print(f"[TRANSLATION_DIAGNOSTIC_OK] gmail_message_id={gmail_message_id}")
                    diagnosed_count += 1
                    continue
                receipt["translation_diagnostic_results"].append({
                    "gmail_message_id": gmail_message_id,
                    **_safe_subject(subject),
                    "result": "no_translatable_content",
                    "content_block_count": len(content_blocks),
                })
                print(f"[TRANSLATION_DIAGNOSTIC_SKIP] gmail_message_id={gmail_message_id}")
                diagnosed_count += 1
                continue

            # A bounded recovery run is allowed to reuse an already-created
            # partial page found by its stable URL. Normal runs retain the
            # existing URL dedup behavior and never inspect page children.
            bounded_recovery_page_id = (
                existing_pages_by_url.get(article_url_norm, "")
                if SYNC_MESSAGE_IDS and article_url_norm else ""
            )
            if bounded_recovery_page_id and not ledger_entry:
                try:
                    existing_block_count = notion.count_block_children(bounded_recovery_page_id)
                    ledger_entry = {
                        "page_id": bounded_recovery_page_id,
                        "db1_state": "partial_page_created",
                        "db2_state": "pending" if notion2 else "not_applicable",
                        "blocks_appended": existing_block_count,
                        "total_blocks": 0,
                        **_safe_subject(subject),
                        "date": date_str,
                        "url": validated_url or "",
                    }
                    message_ledger[gmail_message_id] = ledger_entry
                    write_message_ledger(message_ledger)
                    recovery_mode = True
                    print(
                        f"[BOUNDED_RECOVERY_PAGE] gmail_id={gmail_message_id} "
                        f"page_id={bounded_recovery_page_id} blocks_existing={existing_block_count}"
                    )
                except Exception as exc:
                    record = failure_record(
                        email, phase="bounded_recovery_page_lookup", error=exc,
                        page_id=bounded_recovery_page_id, partial_page_created=True,
                    )
                    receipt["failures"].append(record)
                    print(f"[BOUNDED_RECOVERY_LOOKUP_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                    continue

            # 优先使用 URL 去重；URL 比标题更稳定
            if article_url_norm and not recovery_mode:
                if article_url_norm in existing_urls:
                    print(f"[SKIP] Duplicate (URL): {subject[:50]}...")
                    skipped_count += 1
                    continue

            if gmail_message_id in message_ledger and not recovery_mode:
                print(f"[SKIP] Duplicate (local Gmail ledger): {gmail_message_id}")
                skipped_count += 1
                continue

            if NOTION_GMAIL_MESSAGE_ID_PROPERTY and gmail_message_id in existing_message_ids and not recovery_mode:
                print(f"[SKIP] Duplicate (Gmail message id): {gmail_message_id}")
                skipped_count += 1
                continue

            # 检查是否已存在 (默认逻辑)
            unique_id = generate_unique_id(subject[:200], sender_tag, date_str)
            if unique_id in existing_items and not recovery_mode:
                print(f"[SKIP] Duplicate: {subject[:50]}...")
                skipped_count += 1
                continue

            # P0: create 前按 URL 精确查询一次；query 失败时 fail-closed
            if validated_url and not recovery_mode:
                url_guard_calls += 1
                try:
                    exists, meta = url_exists_in_notion(notion, NOTION_DATABASE_ID, validated_url)
                except Exception as e:
                    url_guard_fail_closed += 1
                    print(f"[WARN] URL guard query failed, fail-closed skip: {subject[:50]}... - {e}")
                    skipped_count += 1
                    continue
                if exists:
                    url_guard_hits += 1
                    print(
                        f"[SKIP] Duplicate (URL guard): {subject[:50]}... "
                        f"(rows={meta.get('rows', 0)}, has_more={meta.get('has_more', False)})"
                    )
                    existing_urls.add(article_url_norm)
                    existing_items.add(unique_id)
                    skipped_count += 1
                    continue

            # 判断类型
            is_chat = 'new thread from' in subject.lower() or '/chat/' in (article_url or '')
            email_type = "Chat" if is_chat else "Article"

            # 转换为 Notion blocks
            content_blocks = html_to_notion_blocks(body_html) if body_html else []

            # 翻译
            if ENABLE_TRANSLATION and content_blocks:
                try:
                    content_blocks = translate_blocks_deepseek(content_blocks)
                except TranslationError as exc:
                    record = failure_record(email, phase="translate_blocks", error=exc)
                    receipt["failures"].append(record)
                    print(f"[TRANSLATION_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                    continue

            # 提取 Ticker
            tickers = extract_tickers(subject, body_html if body_html else "", sender_tag)

            # 构建基础属性
            notion_title = f"{NOTION_TITLE_PREFIX}{subject}" if NOTION_TITLE_PREFIX else subject
            properties = {
                "Name": {"title": [{"type": "text", "text": {"content": notion_title[:200]}}]},
                "Date": {"date": {"start": date_str}},
                "发件人": {"select": {"name": sender_tag[:100]}},
                "类型": {"select": {"name": email_type}},
            }

            if validated_url:
                properties["URL"] = {"url": validated_url}

            if tickers:
                properties["提及公司"] = {
                    "multi_select": [{"name": t} for t in tickers[:10]]
                }

            if NOTION_GMAIL_MESSAGE_ID_PROPERTY:
                properties[NOTION_GMAIL_MESSAGE_ID_PROPERTY] = {
                    "rich_text": [{"type": "text", "text": {"content": gmail_message_id}}]
                }

            # 数据库1：增加“状态=待处理”，数据库2不加
            properties_db1 = dict(properties)
            properties_db1["状态"] = {"select": {"name": "待处理"}}
            properties_db2 = properties

            # 清理无效链接
            content_blocks = sanitize_blocks_for_notion(content_blocks)

            db2_applicable = bool(notion2 and sender_tag not in DB2_EXCLUDED_SOURCES)
            bounded_page_complete = False

            # A bounded URL lookup must prove the page is incomplete before
            # appending. If the consumer already has the full top-level block
            # count, do not duplicate content (or create a second DB2 page).
            if (
                recovery_mode
                and bounded_recovery_page_id
                and int(ledger_entry.get("blocks_appended", 0)) >= len(content_blocks)
            ):
                message_ledger[gmail_message_id].update({
                    "db1_state": "synced",
                    "total_blocks": len(content_blocks),
                })
                write_message_ledger(message_ledger)
                bounded_page_complete = True
                print(f"[BOUNDED_RECOVERY_ALREADY_COMPLETE] page_id={bounded_recovery_page_id} blocks={len(content_blocks)}")

            if bounded_page_complete and not db2_applicable:
                record_message_result(
                    email,
                    page_id=bounded_recovery_page_id,
                    existing_blocks=int(ledger_entry.get("blocks_appended", 0)),
                    expected_blocks=len(content_blocks),
                    blocks_appended=0,
                    db1_state="synced",
                    db2_state="not_applicable",
                    result="already_complete_db2_not_applicable",
                )
                skipped_count += 1
                continue

            # A DB1-success/DB2-failed ledger entry is a DB2-only retry. Never
            # create a second DB1 page for this bounded recovery path.
            if recovery_mode and ledger_entry.get("db1_state") == "synced" and ledger_entry.get("db2_state") in ("pending", "partial", "failed"):
                if not db2_applicable:
                    record = failure_record(email, phase="db2", error=RuntimeError("DB2 retry requested but DB2 is unavailable"), page_id=ledger_entry.get("page_id", ""))
                    receipt["db2_failures"].append(record)
                    receipt["failures"].append(record)
                    record_message_result(
                        email,
                        page_id=ledger_entry.get("page_id", ""),
                        existing_blocks=int(ledger_entry.get("blocks_appended", 0)),
                        expected_blocks=len(content_blocks),
                        blocks_appended=0,
                        db1_state="synced",
                        db2_state="not_applicable",
                        result="db2_not_applicable",
                    )
                    continue
                try:
                    db2_page_id = ledger_entry.get("db2_page_id", "")
                    db2_start = int(ledger_entry.get("db2_blocks_appended", 0))
                    if db2_page_id:
                        for offset in range(db2_start, len(content_blocks), 100):
                            batch = content_blocks[offset:offset + 100]
                            notion2.append_blocks(db2_page_id, batch)
                            message_ledger[gmail_message_id].update({
                                "db2_state": "partial",
                                "db2_page_id": db2_page_id,
                                "db2_blocks_appended": offset + len(batch),
                                "db2_total_blocks": len(content_blocks),
                            })
                            write_message_ledger(message_ledger)
                        result2 = {"id": db2_page_id}
                    else:
                        def persist_db2_progress(page_id, blocks_appended, total_blocks):
                            message_ledger[gmail_message_id].update({
                                "db2_state": "partial",
                                "db2_page_id": page_id,
                                "db2_blocks_appended": blocks_appended,
                                "db2_total_blocks": total_blocks,
                            })
                            write_message_ledger(message_ledger)
                        result2 = notion2.create_page_with_all_blocks(
                            database_id=NOTION_DATABASE_ID_2,
                            properties=properties_db2,
                            children=content_blocks,
                            progress_callback=persist_db2_progress,
                        )
                    if not result2.get("id"):
                        raise RuntimeError(result2.get("message", str(result2)))
                    message_ledger[gmail_message_id]["db2_state"] = "synced"
                    write_message_ledger(message_ledger)
                    print(f"[DB2_RECOVERED] {subject[:50]}...")
                    record_message_result(
                        email,
                        page_id=ledger_entry.get("page_id", ""),
                        existing_blocks=int(ledger_entry.get("blocks_appended", 0)),
                        expected_blocks=len(content_blocks),
                        blocks_appended=0,
                        db1_state="synced",
                        db2_state="synced",
                        result="already_complete_db1_db2_recovered" if bounded_page_complete else "db2_recovered",
                    )
                    skipped_count += 1
                    continue
                except Exception as exc:
                    record = failure_record(email, phase="db2", error=exc, page_id=ledger_entry.get("page_id", ""))
                    receipt["db2_failures"].append(record)
                    receipt["failures"].append(record)
                    message_ledger[gmail_message_id]["db2_state"] = "failed"
                    write_message_ledger(message_ledger)
                    print(f"[DB2_RECOVERY_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                    continue

            def persist_progress(page_id, blocks_appended, total_blocks):
                message_ledger[gmail_message_id] = {
                    **ledger_entry,
                    "page_id": page_id,
                    "db1_state": "page_created" if blocks_appended == 0 else "appending",
                    "db2_state": ledger_entry.get("db2_state", "pending" if db2_applicable else "not_applicable"),
                    "blocks_appended": blocks_appended,
                    "total_blocks": total_blocks,
                    **_safe_subject(subject),
                    "date": date_str,
                    "url": validated_url or "",
                }
                write_message_ledger(message_ledger)

            # Recover a known partial page by appending only the missing blocks.
            if recovery_mode and ledger_entry.get("db1_state") in ("page_created", "partial_page_created", "appending") and ledger_entry.get("page_id"):
                page_id = ledger_entry["page_id"]
                start = int(ledger_entry.get("blocks_appended", 0))
                try:
                    for offset in range(start, len(content_blocks), 100):
                        batch = content_blocks[offset:offset + 100]
                        notion.append_blocks(page_id, batch)
                        persist_progress(page_id, offset + len(batch), len(content_blocks))
                    message_ledger[gmail_message_id].update({"db1_state": "synced", "db2_state": ledger_entry.get("db2_state", "pending")})
                    write_message_ledger(message_ledger)
                    print(f"[DB1_RECOVERED] page_id={page_id} blocks={len(content_blocks)}")
                except Exception as exc:
                    record = failure_record(email, phase="append_blocks", error=exc, page_id=page_id, partial_page_created=True)
                    receipt["failures"].append(record)
                    print(f"[DB1_RECOVERY_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                    continue
                if not db2_applicable or message_ledger[gmail_message_id].get("db2_state") == "synced":
                    record_message_result(
                        email,
                        page_id=message_ledger[gmail_message_id].get("page_id", ""),
                        existing_blocks=start,
                        expected_blocks=len(content_blocks),
                        blocks_appended=max(0, len(content_blocks) - start),
                        db1_state=message_ledger[gmail_message_id].get("db1_state", "synced"),
                        db2_state=message_ledger[gmail_message_id].get("db2_state", "not_applicable"),
                        result="recovered",
                    )
                    skipped_count += 1
                    continue
                # Complete DB2 in the same bounded recovery run. The prior
                # implementation deferred this to a later run, but the
                # Actions workspace ledger is ephemeral; deferral made a
                # successful DB1 recovery look incomplete and lost the DB2
                # retry state across runs.
                if message_ledger[gmail_message_id].get("db2_state") in ("pending", "partial", "failed"):
                    try:
                        def persist_db2_progress(page_id, blocks_appended, total_blocks):
                            message_ledger[gmail_message_id].update({
                                "db2_state": "partial",
                                "db2_page_id": page_id,
                                "db2_blocks_appended": blocks_appended,
                                "db2_total_blocks": total_blocks,
                            })
                            write_message_ledger(message_ledger)
                        result2 = notion2.create_page_with_all_blocks(
                            database_id=NOTION_DATABASE_ID_2,
                            properties=properties_db2,
                            children=content_blocks,
                            progress_callback=persist_db2_progress,
                        )
                        if not result2.get("id"):
                            raise RuntimeError(result2.get("message", str(result2)))
                        message_ledger[gmail_message_id]["db2_state"] = "synced"
                        write_message_ledger(message_ledger)
                        print(f"[DB2_RECOVERED] {subject[:50]}...")
                        record_message_result(
                            email,
                            page_id=message_ledger[gmail_message_id].get("page_id", ""),
                            existing_blocks=start,
                            expected_blocks=len(content_blocks),
                            blocks_appended=max(0, len(content_blocks) - start),
                            db1_state=message_ledger[gmail_message_id].get("db1_state", "synced"),
                            db2_state="synced",
                            result="recovered",
                        )
                    except Exception as exc:
                        record = failure_record(email, phase="db2", error=exc, page_id=message_ledger[gmail_message_id].get("page_id", ""))
                        receipt["db2_failures"].append(record)
                        receipt["failures"].append(record)
                        message_ledger[gmail_message_id]["db2_state"] = "failed"
                        write_message_ledger(message_ledger)
                        print(f"[DB2_RECOVERY_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                        continue
                skipped_count += 1
                continue

            # 创建 Notion 页面 (数据库1)
            try:
                result = notion.create_page_with_all_blocks(
                    database_id=NOTION_DATABASE_ID,
                    properties=properties_db1,
                    children=content_blocks,
                    progress_callback=persist_progress
                )
            except NotionWriteError as exc:
                record = failure_record(
                    email,
                    phase=exc.phase,
                    error=exc.cause,
                    page_id=exc.page_id,
                    partial_page_created=exc.partial_page_created,
                )
                receipt["failures"].append(record)
                if exc.page_id and gmail_message_id:
                    # Preserve the page id so a later bounded retry cannot
                    # create a second page after an append failure.
                    message_ledger[gmail_message_id] = {
                        "page_id": exc.page_id,
                        "db1_state": "partial_page_created",
                        "db2_state": "pending" if db2_applicable else "not_applicable",
                        "blocks_appended": exc.blocks_appended,
                        "total_blocks": exc.total_blocks,
                        **_safe_subject(subject),
                        "date": date_str,
                        "url": validated_url or "",
                    }
                    write_message_ledger(message_ledger)
                print(f"[DB1_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
                continue

            if result.get("id"):
                print(f"[DB1] Synced: {subject[:50]}...")
                synced_count += 1
                existing_items.add(unique_id)
                if article_url_norm:
                    existing_urls.add(article_url_norm)
                if NOTION_GMAIL_MESSAGE_ID_PROPERTY:
                    existing_message_ids.add(gmail_message_id)
                message_ledger[gmail_message_id] = {
                    "page_id": result.get("id", ""),
                    "db1_state": "synced",
                    "db2_state": "pending" if db2_applicable else "not_applicable",
                    "blocks_appended": len(content_blocks),
                    "total_blocks": len(content_blocks),
                    **_safe_subject(subject),
                    "date": date_str,
                    "url": validated_url or "",
                }
                write_message_ledger(message_ledger)

                # 同步到数据库2 (Robs 仅同步到 DB1)
                if notion2 and sender_tag not in DB2_EXCLUDED_SOURCES:
                    try:
                        def persist_db2_progress(page_id, blocks_appended, total_blocks):
                            message_ledger[gmail_message_id].update({
                                "db2_state": "partial",
                                "db2_page_id": page_id,
                                "db2_blocks_appended": blocks_appended,
                                "db2_total_blocks": total_blocks,
                            })
                            write_message_ledger(message_ledger)
                        result2 = notion2.create_page_with_all_blocks(
                            database_id=NOTION_DATABASE_ID_2,
                            properties=properties_db2,
                            children=content_blocks,
                            progress_callback=persist_db2_progress,
                        )
                        if result2.get("id"):
                            print(f"[DB2] Synced: {subject[:50]}...")
                            message_ledger[gmail_message_id]["db2_state"] = "synced"
                            write_message_ledger(message_ledger)
                        else:
                            error_msg2 = result2.get('message', str(result2))
                            print(f"[DB2] Failed: {subject[:50]}... - {error_msg2}")
                            message_ledger[gmail_message_id]["db2_state"] = "failed"
                            write_message_ledger(message_ledger)
                            db2_record = failure_record(email, phase="db2", error=RuntimeError(error_msg2))
                            receipt["db2_failures"].append(db2_record)
                            receipt["failures"].append(db2_record)
                    except Exception as e2:
                        db2_page_id = getattr(e2, "page_id", "")
                        db2_record = failure_record(email, phase="db2", error=e2, page_id=db2_page_id, partial_page_created=bool(db2_page_id))
                        receipt["db2_failures"].append(db2_record)
                        receipt["failures"].append(db2_record)
                        message_ledger[gmail_message_id]["db2_state"] = "failed"
                        write_message_ledger(message_ledger)
                        print(f"[DB2_FAILED] {json.dumps(db2_record, ensure_ascii=False, sort_keys=True)}")
            else:
                record = failure_record(
                    email,
                    phase="create_page",
                    error=RuntimeError(result.get('message', str(result))),
                )
                receipt["failures"].append(record)
                print(f"[DB1_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")

        except Exception as e:
            record = failure_record(email, phase="process_email", error=e)
            receipt["failures"].append(record)
            print(f"[EMAIL_FAILED] {json.dumps(record, ensure_ascii=False, sort_keys=True)}")
            continue

    receipt.update({
        "run_finished_at": datetime.now().isoformat(),
        "status": "FAIL" if receipt["failures"] else "PASS",
        "fetched_count": len(emails),
        "processed_count": processed_count,
        "synced_count": synced_count,
        "diagnosed_count": diagnosed_count,
        "skipped_count": skipped_count,
        "failure_count": len(receipt["failures"]),
        "db2_failure_count": len(receipt["db2_failures"]),
        "guard_stats": {
            "url_guard_calls": url_guard_calls,
            "url_guard_hits": url_guard_hits,
            "url_guard_fail_closed": url_guard_fail_closed,
        },
    })
    if TRANSLATION_DIAGNOSTIC_ONLY:
        print("[LEDGER] skipped write in translation diagnostic mode")
    else:
        write_message_ledger(message_ledger)
    write_sync_receipt(receipt)
    print(f"=" * 60)
    print(f"Sync completed! Added {synced_count} new articles")
    print(
        f"Guard stats: url_guard_calls={url_guard_calls}, url_guard_hits={url_guard_hits}, "
        f"url_guard_fail_closed={url_guard_fail_closed}"
    )
    print(f"=" * 60)
    if receipt["failures"]:
        print(f"ERROR: {len(receipt['failures'])} write/processing failure(s); failing closed")
        raise SystemExit(1)


if __name__ == "__main__":
    def bootstrap_failure_receipt(phase, message):
        receipt = {
            "schema_version": "substack_sync_receipt_v2",
            "run_started_at": datetime.now().isoformat(),
            "run_finished_at": datetime.now().isoformat(),
            "status": "FAIL",
            "phase": phase,
            "error": str(message)[:500],
            "failures": [{"phase": phase, "error_type": "RuntimeError", "error": str(message)[:500]}],
            "failure_count": 1,
            "db2_failure_count": 0,
        }
        write_sync_receipt(receipt)

    # 检查必需的环境变量
    if not NOTION_API_TOKEN:
        print("Error: NOTION_API_TOKEN environment variable not set")
        bootstrap_failure_receipt("bootstrap_env", "NOTION_API_TOKEN environment variable not set")
        raise SystemExit(1)
    if not NOTION_DATABASE_ID:
        print("Error: NOTION_DATABASE_ID environment variable not set")
        bootstrap_failure_receipt("bootstrap_env", "NOTION_DATABASE_ID environment variable not set")
        raise SystemExit(1)
    if not os.environ.get("GMAIL_TOKEN"):
        print("Error: GMAIL_TOKEN environment variable not set")
        bootstrap_failure_receipt("bootstrap_env", "GMAIL_TOKEN environment variable not set")
        raise SystemExit(1)

    sync_gmail_to_notion()
