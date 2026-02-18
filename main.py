from fastapi import FastAPI, Body
from pydantic import BaseModel
import httpx
from typing import List, Dict, Any
from datetime import datetime
from groq import Groq
import json
import os

app = FastAPI()

groq_client = Groq()

class PipelineRequest(BaseModel):
    email: str
    source: str

STORAGE_FILE = "processed_comments.json"
NOTIFICATION_LOG = "notification_log.txt"

async def fetch_comments() -> List[Dict[str, Any]]:
    url = "https://jsonplaceholder.typicode.com/comments?postId=1"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
            return data[:3]
    except Exception as e:
        raise Exception(f"Fetch failed: {str(e)}")

async def analyze_with_ai(text: str) -> Dict[str, str]:
    try:
        prompt = f"""Summarize this comment in 1-2 short sentences.
Classify sentiment as positive, negative or neutral.
Return ONLY JSON like this:
{{
  "summary": "your summary",
  "sentiment": "positive" or "negative" or "neutral"
}}

Comment: {text}"""

        completion = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=120,
            response_format={"type": "json_object"}
        )

        result = json.loads(completion.choices[0].message.content)
        return {
            "summary": result.get("summary", "No summary available"),
            "sentiment": result.get("sentiment", "neutral").lower()
        }
    except Exception as e:
        return {"summary": f"AI failed: {str(e)[:80]}", "sentiment": "neutral"}

async def store_result(item: Dict, source: str) -> bool:
    try:
        record = {**item, "source": source}
        data = []
        if os.path.exists(STORAGE_FILE):
            with open(STORAGE_FILE, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except:
                    data = []
        data.append(record)
        with open(STORAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except:
        return False

async def send_notification(email: str, success: bool) -> bool:
    msg = f"[{datetime.utcnow().isoformat()}Z] Pipeline done. Success: {success}. To: {email}\n"
    print(msg)
    try:
        with open(NOTIFICATION_LOG, "a", encoding="utf-8") as f:
            f.write(msg)
    except:
        pass
    return True

@app.post("/pipeline")
async def run_pipeline(data: PipelineRequest = Body(...)):
    errors = []
    items = []

    try:
        comments = await fetch_comments()
    except Exception as e:
        errors.append(str(e))
        comments = []

    for comment in comments:
        try:
            text = comment.get("body", "").strip()
            if not text:
                continue
            ai = await analyze_with_ai(text)
            item = {
                "original": text,
                "analysis": ai["summary"],
                "sentiment": ai["sentiment"],
                "stored": False,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            item["stored"] = await store_result(item, data.source)
            items.append(item)
        except Exception as e:
            errors.append(f"Comment error: {str(e)[:80]}")

    sent = await send_notification(data.email, len(items) > 0)

    return {
        "items": items,
        "notificationSent": sent,
        "processedAt": datetime.utcnow().isoformat() + "Z",
        "errors": errors
    }
