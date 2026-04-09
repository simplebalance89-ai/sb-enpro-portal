"""
Crowdsource the same UX research question to Grok, Kimi, DeepSeek, and Gemini
in parallel. Dump each answer to /tmp/llm_<name>.txt.
"""
import asyncio
import json
import os
import sys

import httpx

PROMPT = (
    "I am building a sales-rep-facing AI assistant on FastAPI + Azure OpenAI gpt-4.1 "
    "with SSE streaming. Users complain that responses still feel like a 'wall of text "
    "blob' even with structured JSON output (headline + ranked picks + reasons + "
    "follow-up question) and progressive SSE streaming. Give me the TOP 5 best "
    "practices to make AI chat responses feel scannable, conversational, and not "
    "blobby. Be concrete: what to render, what NOT to render, what visual hierarchy, "
    "what timing, what length. Under 400 words. No fluff. Real answers a senior AI "
    "engineer would give."
)


async def call_openai_compat(name: str, url: str, key: str, model: str) -> str:
    if not key:
        return f"[no key for {name}]"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": PROMPT}],
        "temperature": 0.5,
        "max_tokens": 800,
    }
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(url, json=payload, headers=headers)
            data = r.json()
            if r.status_code != 200:
                return f"[{name} HTTP {r.status_code}] {json.dumps(data)[:500]}"
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[{name} ERROR] {type(e).__name__}: {e}"


async def call_gemini(key: str) -> str:
    if not key:
        return "[no GOOGLE_API_KEY]"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key={key}"
    payload = {
        "contents": [{"parts": [{"text": PROMPT}]}],
        "generationConfig": {"temperature": 0.5, "maxOutputTokens": 800},
    }
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
            data = r.json()
            if r.status_code != 200:
                return f"[gemini HTTP {r.status_code}] {json.dumps(data)[:500]}"
            return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        return f"[gemini ERROR] {type(e).__name__}: {e}"


async def main():
    grok_key = os.environ.get("XAI_API_KEY", "")
    kimi_key = os.environ.get("MOONSHOT_API_KEY", "")
    ds_key = os.environ.get("DEEPSEEK_API_KEY", "")
    g_key = os.environ.get("GOOGLE_API_KEY", "")

    print(f"keys present: grok={bool(grok_key)} kimi={bool(kimi_key)} deepseek={bool(ds_key)} gemini={bool(g_key)}")

    results = await asyncio.gather(
        call_openai_compat("grok", "https://api.x.ai/v1/chat/completions", grok_key, "grok-4"),
        call_openai_compat("kimi", "https://api.moonshot.ai/v1/chat/completions", kimi_key, "kimi-k2-0905-preview"),
        call_openai_compat("deepseek", "https://api.deepseek.com/v1/chat/completions", ds_key, "deepseek-chat"),
        call_gemini(g_key),
    )
    names = ["grok", "kimi", "deepseek", "gemini"]
    for name, content in zip(names, results):
        with open(f"/tmp/llm_{name}.txt", "w", encoding="utf-8") as f:
            f.write(f"=== {name.upper()} ===\n\n{content}\n")
        print(f"\n{'='*70}\n{name.upper()}\n{'='*70}\n{content[:1200]}{'...[truncated]' if len(content) > 1200 else ''}")


if __name__ == "__main__":
    asyncio.run(main())
