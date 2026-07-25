# Substack Sync

Minimal repo to sync Substack emails from Gmail into Notion.

## How it runs
- **Scheduled**: every 2 hours, runs in the **prod** environment.
- **Manual**: Actions → “Sync Substack to Notion” → choose `environment` (`test` or `prod`).

## Required Secrets (per environment)
- `GMAIL_TOKEN_BASE64`
- `NOTION_API_TOKEN`
- `NOTION_DATABASE_ID`

Optional:
- `NOTION_API_TOKEN_2`
- `NOTION_DATABASE_ID_2`
- `DEEPSEEK_API_KEY`

## Notion database fields
The script expects these property names (exact match):
- `Name` (title)
- `Date` (date)
- `发件人` (select)
- `类型` (select)
- `URL` (url)
- `提及公司` (multi-select)
- `状态` (select) — **only required in DB1** and used to set `待处理`

## Gmail token (base64)
Generate a single-line base64 string from your Gmail OAuth token JSON:

```bash
base64 -i gmail_token_for_github.json | tr -d '\n'
```

Paste the output into the `GMAIL_TOKEN_BASE64` secret.

## Bounded recovery runbook

Use only after the test gate and explicit owner authorization. In the Actions
manual dispatch, select `environment=test`, set `max_emails=1`, provide an
exact `sender_email`, set `sync_lookback_days` as required, and provide an
explicit comma-separated `message_ids` candidate list when available. Do not
use a production schedule for recovery. When `sender_email` is set, both the
Gmail query and per-message processing enforce the exact normalized address.

- If DB1 has a persisted `page_id` with `page_created`, `partial_page_created`,
  or `appending`, resume by appending only the missing blocks from the saved
  offset; never create another DB1 page.
- If DB1 is `synced` and DB2 is `pending`, a crash-safe recovery run enters the
  DB2-only path. If DB1 recovery discovers DB2 still pending, it records
  `db2_recovery_deferred`, changes DB2 to `failed`, exits nonzero, and requires
  a second bounded run.
- DB2-only recovery uses `db2_page_id` and `db2_blocks_appended` to append only
  missing blocks. It must not recreate either page or process an unbounded
  Gmail window.
- A production run requires the durable Notion Gmail message-id property via
  `NOTION_GMAIL_MESSAGE_ID_PROPERTY`; the local ledger is not cross-run durable
  on GitHub-hosted runners.
