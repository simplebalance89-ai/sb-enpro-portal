# Render Deploy (V2.15 Rebuild)

## Goal
Deploy a Docker staging service from:
- Repo: `simplebalance89-ai/sb-enpro-portal`
- Branch: `v2.15-rebuild-from-2.13`

## Why staging first
- Keep current production untouched while validating conversational UX changes.
- Validate voice behavior and 3-pick cap in isolation.

## Service setup (Render UI)
1. Create `New +` → `Blueprint` and point to this repo/branch (uses `render.yaml`), or create `Web Service` manually as `Docker`.
2. Use branch `v2.15-rebuild-from-2.13`.
3. Plan: Starter.
4. Health check path: `/health`.

## Required env vars
Set these in Render before first real test:
- `SESSION_SECRET`
- `GLOBAL_PIN`
- `DATABASE_URL` (attach existing Postgres if you want auth+memory)
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_KEY`
- `AZURE_DEPLOYMENT_ROUTER`
- `AZURE_DEPLOYMENT_REASONING`
- `AZURE_BLOB_SAS`

If voice STT uses separate Whisper resource, also set:
- `AZURE_WHISPER_ENDPOINT`
- `AZURE_WHISPER_KEY`
- `AZURE_WHISPER_DEPLOYMENT` (default `whisper`)

Optional:
- `ADMIN_TOKEN`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `REPORT_EMAIL`

## Validation checklist
1. `/health` returns 200.
2. Natural language lookup works without command phrases.
3. Natural language compare works (`compare` capability, no command menu needed).
4. Pregame works from plain language prompt.
5. Voice input flows conversationally (no modal interception).
6. Responses show at most 3 picks and no trailing “Other options” dump.
