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
- `EXTRA_SUBSTACK_SOURCES`

## Add 1-2 more mail sources
Set `EXTRA_SUBSTACK_SOURCES` in the GitHub environment secret to append new senders without editing code.

Format:

```text
author1@substack.com=Source Name
author2@substack.com=Source Name 2
```

You can also use comma-separated entries:

```text
author1@substack.com=Source Name,author2@substack.com=Source Name 2
```

The script will:
- include these addresses in the Gmail search query
- map them to the provided display names in Notion `发件人`
- keep existing sources unchanged

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
