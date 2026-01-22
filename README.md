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
