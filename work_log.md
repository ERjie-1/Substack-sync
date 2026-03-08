# Work Log

## Project Context (Fixed Header — 2026-02-19 更新)

### 项目概述
Substack 邮件同步工具：通过 Gmail API 抓取订阅的 Substack newsletter 邮件，用 DeepSeek 翻译为中文，提取股票 Ticker，然后同步到两个 Notion 数据库。整个流程由 GitHub Actions 定时调度，无需本地运行。

### 架构
```
Gmail (Substack emails)
  ↓ Gmail API (OAuth)
sync_substack.py
  ├─ 解析邮件 → 提取标题/正文/发件人/日期/URL
  ├─ DeepSeek API → 聚合翻译（fallback: Google Translate）
  ├─ Ticker 提取 → 从正文匹配股票代码和公司名
  ├─ 去重（URL 匹配）
  ├─ → Notion DB1（主库，含"状态"字段，设为"待处理"）
  └─ → Notion DB2（备份库，部分发件人跳过）
```

### 关键文件
| 文件 | 作用 |
|------|------|
| `sync_substack.py` | 核心脚本，含全部逻辑（~1200行单文件） |
| `.github/workflows/sync.yml` | GitHub Actions 定时调度配置 |
| `requirements.txt` | Python 依赖（requests, google-auth, google-api） |
| `gmail_token_for_github.json` | Gmail OAuth token（本地用，不入 git） |
| `refresh_token.py` | Gmail token 刷新辅助脚本 |

### 运行环境
- **Repo**: https://github.com/ERjie-1/Substack-sync
- **部署方式**: GitHub Actions（定时 schedule + 手动 workflow_dispatch）
- **调度频率**: 工作日早(07-09)晚(20-23)每30分钟，其余每5小时；周六仅 08:00/20:00
- **环境**: prod（定时）/ test（手动可选）
- **关键 Secrets**: `GMAIL_TOKEN_BASE64`, `NOTION_API_TOKEN`, `NOTION_DATABASE_ID`, `NOTION_API_TOKEN_2`, `NOTION_DATABASE_ID_2`, `DEEPSEEK_API_KEY`

### 踩坑备忘

- **Notion API 空响应导致去重失效**: Notion 分页查询偶尔返回空 body（JSON parse error），如果 except 只打印不退出，脚本会带着不完整的 existing_items 继续跑，导致大量重复写入。必须在查询失败时 exit(1)。
- **Notion API JSON 错误响应也会导致去重失效**: 即使 response.json() 不抛异常，502/429 等错误返回的 JSON（`{"object":"error",...}`）没有 results/has_more 字段，分页循环会静默提前退出。必须同时检查 HTTP 状态码和响应内容。

### 当前状态（2026-03-09 更新）
- **已完成**: 邮件抓取、DeepSeek 翻译、双 Notion 库同步、Ticker 提取、URL 去重、定时调度
- **待完成**: 可移除 debug 日志（`[DEBUG] from=...`，当前约第 1226 行），非紧急
- **已知问题**: prod smoke test 已通过；`test` 环境 secrets 仍配置异常（`GMAIL_TOKEN_BASE64` 无效、`NOTION_DATABASE_ID` 返回 404）
- **订阅源**: 17 个 Substack 作者（LW Research, Robonomics, SemiAnalysis, TMTB, LatentSpace 等）

---

## Change Log

### 2026-03-09 - prod smoke test 验证去重修复
- 变更类型: 验证 / 运维
- 动机: 在真实 prod secrets 下验证 2026-03-08 的去重与稳定性修复，避免直接合并后才发现回归
- 修改文件: `work_log.md`
- 行为影响: 无代码行为变更；补充真实验证记录
- 验证结果: GitHub Actions 手动触发 `workflow_dispatch`（branch=`codex/notion-sync-hardening`, environment=`prod`, `max_emails=1`），run `22824760438` 成功；脚本读取到 `Existing articles in Notion: 481`、`Existing URLs in Notion: 272`，抓取 1 封邮件后命中 `[SKIP] Duplicate (URL)`，最终 `Sync completed! Added 0 new articles`
- 后续待办: 修复 `test` 环境的 `GMAIL_TOKEN_BASE64` 和 `NOTION_DATABASE_ID`，以便后续改动先走测试环境

### 2026-03-09 - test 环境配置异常记录
- 变更类型: 调试记录
- 动机: 手动触发 `test` 环境验证时失败，需要记录失败原因避免重复排查
- 修改文件: `work_log.md`
- 行为影响: 无
- 验证结果: GitHub Actions run `22824692556` 在 `Run sync script` 阶段失败；日志显示 `base64: invalid input`，且 Notion 查询返回 `404 Client Error`
- 后续待办: 更新 `test` 环境 secrets 后再重跑 smoke test

### 2026-03-08 - 修复近期大量重复文章的根因
- 变更类型: Bug修复
- 动机: 复盘 2026-03-08 GitHub Actions 日志后发现，脚本会从 Gmail 抓最近 50 封邮件，但 Notion 去重只查最近 7 天；因此 `2/25`、`2/26` 这类旧邮件会在每次运行中持续被当成新文章重复写入
- 修改文件: `sync_substack.py`, `.github/workflows/sync.yml`
- 行为影响: Gmail 抓取窗口与 Notion 去重窗口统一为同一个 `SYNC_LOOKBACK_DAYS`（默认 21 天）；URL 去重从仅 `GlobalSemiResearch` 扩展到所有带文章 URL 的邮件；Notion 写入统一增加 HTTP 错误/超时校验；workflow 并发组改为按环境隔离，手动 `test` 不再取消 `prod`
- 验证结果: 本地代码检查通过；等待 GitHub Actions 下一轮调度验证
- 后续待办: 观察 `Sync completed! Added ...` 是否恢复为仅新增当日/近几日文章；如运行时长仍长期超过 30 分钟，再评估调度频率

### 2026-03-01 - 验证 LatentSpace sender_tag 映射
- 变更类型: 调试验证
- 动机: DB1 中 LatentSpace 文章的「发件人」显示为 "swyx" 而非 "LatentSpace"，怀疑映射未命中
- 修改文件: `sync_substack.py`（第 1209 行加 debug 日志）
- 行为影响: 无功能变更，仅增加日志输出
- 验证结果: debug 日志确认映射正常工作（`from='"Latent.Space" <swyx@substack.com>' -> sender_tag='LatentSpace'`）。之前 DB1 显示 "swyx" 是因为那条文章在加 SOURCE_MAPPING 之前就同步进去了
- 后续待办: 可清理 debug 日志；LatentSpace 的 Obsidian 归档需在「文章切分与上传」项目配置

### 2026-02-28 - 新增 LatentSpace 订阅源
- 变更类型: 新功能
- 动机: 用户希望订阅 swyx 的 LatentSpace newsletter
- 修改文件: `sync_substack.py`（GMAIL_QUERY、SOURCE_MAPPING、DB2 跳过逻辑）
- 行为影响: 新增 swyx@substack.com 和 swyx+ainews@substack.com 两个地址，统一映射为 LatentSpace，仅同步到 DB1（跳过 DB2）
- 验证结果: ✅ 已验证，映射正常

### 2026-02-27 - 空结果防护：去重查询返回 0 条时重试并中止

- 变更类型: 防护增强
- 动机: 即使 API 返回 200，结果也可能为空（临时性问题）；0 条记录意味着去重完全失效
- 修改文件: `sync_substack.py`（去重查询后加 len==0 检查 → 重试 → 仍为 0 则 exit(1)）
- 行为影响: 正常运行 7 天内必有文章，空结果触发重试；两次为空则中止脚本
- 验证结果: 已推送

### 2026-02-27 - 去重查询优化：全量 → 最近 7 天

- 变更类型: 性能优化
- 动机: 全量加载 ~1000 条记录需 ~10 次 API 请求，中途出错概率高（即两次重复问题的根因）；改为只查最近 7 天可降至 1-2 次
- 修改文件: `sync_substack.py`（加 timedelta import；去重查询加 Date on_or_after filter）
- 行为影响: 去重仅覆盖最近 7 天（远超 Gmail 取回的 2-3 天邮件范围），API 请求大幅减少
- 验证结果: 已推送，等待调度验证

### 2026-02-27 - 强化 Notion 去重防护（防 API 静默错误）

- 变更类型: Bug修复
- 动机: Notion 再次出现 10+ 重复文章。2/26 的 exit(1) 修复仅覆盖 response.json() 抛异常的情况，未覆盖 Notion 返回合法 JSON 错误响应（502/429）静默截断分页的场景
- 修改文件: `sync_substack.py`（query_database 加 raise_for_status；分页循环加 object=="error" 校验；unique_id 用 subject[:200]）, `.github/workflows/sync.yml`（加 concurrency group）
- 行为影响: Notion API 任何异常（HTTP 错误码 或 JSON 错误对象）都会触发 exit(1)；并发运行被阻止；subject 截断不再导致 hash 不匹配
- 验证结果: 已推送，等待调度验证
- 后续待办: 观察次日是否还有重复

### 2026-02-26 - Notion 去重查询失败时中止脚本

- 变更类型: Bug修复
- 动机: Notion API 分页查询返回空响应（`Expecting value` JSON error），导致 existing_items 仅加载 572/943 条，48 篇已有文章被重复写入
- 修改文件: `sync_substack.py`（第 1133 行，`except` 块内加 `exit(1)`）
- 行为影响: 去重查询失败时脚本直接退出，Actions 显示失败，不会写入重复数据
- 验证结果: 已推送至远端，等待下次调度验证
- 后续待办: 无

### 2026-02-19 - Gmail OAuth token 过期修复
- 变更类型: Bug修复
- 动机: Gmail OAuth token 于 ~2/14 过期，导致 Notion 停止更新但 Actions 仍显示 success（静默失败）
- 修改文件: `sync_substack.py`（第 1143 行 `return` → `exit(1)`）
- 行为影响: Gmail 认证失败时脚本以非零退出码退出，Actions 会显示失败状态
- 其他操作: 重新运行 `refresh_token.py` 获取新 token，更新 GitHub Secret `GMAIL_TOKEN_BASE64`；确认 Google Cloud OAuth 应用已处于 Production 模式
- 验证结果: 手动触发 workflow 成功同步 5 篇新文章
- 后续待办: 观察未来 7+ 天 token 是否稳定

### 2026-02-08 - Skip DB2 sync for Robs sender
- 变更类型: 功能调整
- 动机: Robs 的内容不需要同步到备份库
- 修改文件: `sync_substack.py`
- 行为影响: Robs 发件人的邮件仅同步到 DB1，跳过 DB2
- 验证结果: GitHub Actions 运行正常

### 2026-01-29 - 状态字段 + GSR 去重修复 + 调度更新
- 变更类型: Bug修复 + 功能增强
- 动机: DB1 需要"待处理"状态追踪；GlobalSemiResearch 出现重复；调度时间优化
- 修改文件: `sync_substack.py`, `.github/workflows/sync.yml`
- 行为影响: 新邮件同步到 DB1 时自动标记"待处理"；通过 URL 去重 GSR；调整 cron 时间
- 验证结果: 去重生效，调度正常

### 2026-01-22 - 项目初始化
- 变更类型: 新功能
- 动机: 从本地脚本迁移到 GitHub Actions 自动运行
- 修改文件: 全部文件（初始提交）
- 行为影响: 完整的 Substack → Gmail → Notion 同步管线上线
- 验证结果: prod 环境运行通过
