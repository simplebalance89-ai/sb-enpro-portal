# AGENTS.md — ENPRO FM Portal

## What This Repo Is
ENPRO FM Portal is an industrial supply platform with voice-enabled search. Users can search for products using natural voice commands, processed through Azure Whisper and GPT-4.1-mini.

## Tech Stack
- **Language**: Python 3.11+
- **Framework**: FastAPI
- **AI Pipeline**: Azure Whisper → GPT-4.1-mini → Fuzzy Matching
- **Matching**: RapidFuzz + Metaphone for phonetic matching
- **Deployment**: Render (Python runtime)

## Directory Structure
```
├── agent.py            # Voice search agent logic
├── server.py           # FastAPI server
├── static/
│   ├── css/
│   ├── index.html      # Main UI
│   └── js/
├── Dockerfile
├── render.yaml
└── requirements.txt
```

## How to Work Here

### Running Locally
```bash
pip install -r requirements.txt
python server.py
```

### Voice Search Pipeline
1. **Audio Input**: Browser mic capture
2. **Whisper**: Azure Speech-to-Text
3. **Extraction**: GPT-4.1-mini extracts search intent
4. **Resolution**: Fuzzy matching (RapidFuzz + Metaphone)
5. **Results**: Return matching products

### Key Conventions
- Voice-first UI design
- Keep product cards simple (extended description, minimal clutter)
- Fuzzy matching for typos and phonetic errors

### Environment Variables
```bash
AZURE_OPENAI_KEY=
AZURE_OPENAI_ENDPOINT=
AZURE_WHISPER_KEY=
AZURE_WHISPER_ENDPOINT=
```

## Current Priorities
- Voice search engine optimization
- Product catalog expansion
- UI simplification

## Deployment
- **Platform**: Render
- **URL**: Configured in render.yaml
- **Health Check**: `/health`

## Migration Note
This repo was migrated from `gcealyssa/enpro-fm-portal` to `simplebalance89-ai/sb-enpro-portal`.
