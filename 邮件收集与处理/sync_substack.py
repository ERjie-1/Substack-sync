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
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from email.utils import parsedate_to_datetime

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

# 翻译开关
ENABLE_TRANSLATION = os.environ.get("ENABLE_TRANSLATION", "true").lower() == "true"

# Gmail API
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

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
    citrini@substack.com
) -"sign in to substack" -"upgrade to a paid subscription" -"your payment receipt from"'''

# 发件人显示名称映射
SOURCE_MAPPING = {
    'lobwedge@substack.com': 'LW Research',
    'robonomics@substack.com': 'Robonomics',
    'purpledrink@substack.com': 'Purple Drinks',
    'nathanbancroft@substack.com': 'Nathan',
    'jamesbulltard@substack.com': 'Bulltrad',
    'globalsemiresearch@substack.com': 'GlobalSemiresearch',
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
}

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


def generate_unique_id(subject: str, sender: str, date_str: str) -> str:
    """生成唯一 ID 用于去重"""
    date_only = date_str[:10] if date_str else ""
    content = f"{subject}|{sender}|{date_only}"
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


def call_deepseek_api(texts: List[str], timeout: int = 60) -> Optional[str]:
    """调用 DeepSeek API"""
    if not DEEPSEEK_API_KEY:
        return None

    marked_input = "\n".join([f"[P{i+1}] {t}" for i, t in enumerate(texts)])

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
            print(f"    DeepSeek error: {response.status_code}")
            return None

        return response.json()["choices"][0]["message"]["content"]

    except requests.exceptions.Timeout:
        print(f"    DeepSeek timeout")
        return None
    except Exception as e:
        print(f"    DeepSeek exception: {e}")
        return None


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


def get_emails(service, query: str, max_results: int = 50) -> List[Dict]:
    """获取邮件列表"""
    emails = []
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

    def query_database(self, database_id: str, start_cursor: str = None) -> Dict:
        url = f"{self.BASE_URL}/databases/{database_id}/query"
        body = {}
        if start_cursor:
            body["start_cursor"] = start_cursor
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()

    def create_page(self, database_id: str, properties: Dict, children: List[Dict] = None) -> Dict:
        url = f"{self.BASE_URL}/pages"
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if children:
            body["children"] = children[:100]
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()

    def append_blocks(self, page_id: str, children: List[Dict]) -> Dict:
        url = f"{self.BASE_URL}/blocks/{page_id}/children"
        body = {"children": children[:100]}
        response = requests.patch(url, headers=self.headers, json=body)
        return response.json()

    def create_page_with_all_blocks(self, database_id: str, properties: Dict, children: List[Dict]) -> Dict:
        if not children:
            children = []

        result = self.create_page(database_id, properties, children[:100])

        if not result.get("id"):
            return result

        page_id = result["id"]
        remaining = children[100:]
        while remaining:
            batch = remaining[:100]
            remaining = remaining[100:]
            self.append_blocks(page_id, batch)

        return result


def sanitize_blocks_for_notion(blocks: List[Dict]) -> List[Dict]:
    """清理 blocks 中的无效链接"""
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
                    if "link" in rt.get("text", {}):
                        link_url = rt["text"]["link"].get("url", "")
                        validated = validate_and_fix_url(link_url)
                        if validated:
                            rt["text"]["link"]["url"] = validated
                        else:
                            del rt["text"]["link"]
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


# ============ 主同步函数 ============
def sync_gmail_to_notion():
    """主同步函数"""
    print(f"=" * 60)
    print(f"Substack to Notion Sync - {datetime.now().isoformat()}")
    print(f"=" * 60)
    print(f"Translation: {'Enabled (DeepSeek)' if ENABLE_TRANSLATION and DEEPSEEK_API_KEY else 'Disabled'}")

    max_results = int(os.environ.get("MAX_EMAIL_LIMIT", "50"))
    print(f"Max emails to fetch: {max_results}")

    # 初始化 Notion API
    notion = NotionAPI(NOTION_API_TOKEN)
    notion2 = NotionAPI(NOTION_API_TOKEN_2) if NOTION_API_TOKEN_2 and NOTION_DATABASE_ID_2 else None
    if notion2:
        print("DB2: Enabled")
    else:
        print("DB2: Disabled (missing NOTION_API_TOKEN_2 or NOTION_DATABASE_ID_2)")

    # 获取已存在的文章 (用于去重)
    existing_items = set()
    try:
        has_more = True
        start_cursor = None

        while has_more:
            result = notion.query_database(NOTION_DATABASE_ID, start_cursor=start_cursor)

            for page in result.get("results", []):
                props = page.get("properties", {})
                title_prop = props.get("Name", {}).get("title", [])
                sender_prop = props.get("发件人", {}).get("select", {})
                date_prop = props.get("Date", {}).get("date", {})

                title = title_prop[0].get("text", {}).get("content", "") if title_prop else ""
                sender_name = sender_prop.get("name", "") if sender_prop else ""
                date_str = date_prop.get("start", "") if date_prop else ""

                if title and sender_name and date_str:
                    existing_items.add(generate_unique_id(title, sender_name, date_str))

            has_more = result.get("has_more", False)
            start_cursor = result.get("next_cursor")

    except Exception as e:
        print(f"Error fetching existing items: {e}")

    print(f"Existing articles in Notion: {len(existing_items)}")

    # 获取邮件
    try:
        gmail_service = get_gmail_service()
        emails = get_emails(gmail_service, GMAIL_QUERY, max_results=max_results)
        print(f"Fetched {len(emails)} emails from Gmail")
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return

    # 同步邮件
    synced_count = 0

    for email in emails:
        try:
            subject = email['subject']
            sender = email['from']
            body_html = email['body_html']
            body_text = email['body_text']
            sender_tag = extract_sender_tag(sender)

            # 跳过欢迎邮件
            if subject.lower().startswith('welcome to '):
                print(f"[SKIP] Welcome email: {subject[:50]}...")
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

            # 检查是否已存在
            unique_id = generate_unique_id(subject, sender_tag, date_str)
            if unique_id in existing_items:
                print(f"[SKIP] Duplicate: {subject[:50]}...")
                continue

            # 提取文章 URL
            article_url = extract_article_url(body_text) or extract_article_url(body_html)

            # 判断类型
            is_chat = 'new thread from' in subject.lower() or '/chat/' in (article_url or '')
            email_type = "Chat" if is_chat else "Article"

            # 转换为 Notion blocks
            content_blocks = html_to_notion_blocks(body_html) if body_html else []

            # 翻译
            if ENABLE_TRANSLATION and content_blocks:
                content_blocks = translate_blocks_deepseek(content_blocks)

            # 提取 Ticker
            tickers = extract_tickers(subject, body_html if body_html else "", sender_tag)

            # 构建基础属性
            properties = {
                "Name": {"title": [{"type": "text", "text": {"content": subject[:200]}}]},
                "Date": {"date": {"start": date_str}},
                "发件人": {"select": {"name": sender_tag[:100]}},
                "类型": {"select": {"name": email_type}},
            }

            if article_url:
                validated_url = validate_and_fix_url(article_url)
                if validated_url:
                    properties["URL"] = {"url": validated_url}

            if tickers:
                properties["提及公司"] = {
                    "multi_select": [{"name": t} for t in tickers[:10]]
                }

            # 数据库1：增加“状态=待处理”，数据库2不加
            properties_db1 = dict(properties)
            properties_db1["状态"] = {"select": {"name": "待处理"}}
            properties_db2 = properties

            # 清理无效链接
            content_blocks = sanitize_blocks_for_notion(content_blocks)

            # 创建 Notion 页面 (数据库1)
            result = notion.create_page_with_all_blocks(
                database_id=NOTION_DATABASE_ID,
                properties=properties_db1,
                children=content_blocks
            )

            if result.get("id"):
                print(f"[DB1] Synced: {subject[:50]}...")
                synced_count += 1
                existing_items.add(unique_id)

                # 同步到数据库2
                if notion2:
                    try:
                        result2 = notion2.create_page_with_all_blocks(
                            database_id=NOTION_DATABASE_ID_2,
                            properties=properties_db2,
                            children=content_blocks
                        )
                        if result2.get("id"):
                            print(f"[DB2] Synced: {subject[:50]}...")
                        else:
                            error_msg2 = result2.get('message', str(result2))
                            print(f"[DB2] Failed: {subject[:50]}... - {error_msg2}")
                    except Exception as e2:
                        print(f"[DB2] Failed: {subject[:50]}... - {e2}")
            else:
                error_msg = result.get('message', str(result))
                print(f"[DB1] Failed: {subject[:50]}... - {error_msg}")

        except Exception as e:
            print(f"Error processing email: {email.get('subject', 'unknown')[:30]}... - {e}")
            continue

    print(f"=" * 60)
    print(f"Sync completed! Added {synced_count} new articles")
    print(f"=" * 60)


if __name__ == "__main__":
    # 检查必需的环境变量
    if not NOTION_API_TOKEN:
        print("Error: NOTION_API_TOKEN environment variable not set")
        exit(1)
    if not NOTION_DATABASE_ID:
        print("Error: NOTION_DATABASE_ID environment variable not set")
        exit(1)
    if not os.environ.get("GMAIL_TOKEN"):
        print("Error: GMAIL_TOKEN environment variable not set")
        exit(1)

    sync_gmail_to_notion()
