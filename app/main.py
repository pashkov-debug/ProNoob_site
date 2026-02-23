import os
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .db import ensure_schema, list_topics, list_tags, search_cards, get_meta

APP_TITLE = os.getenv("APP_TITLE", "Cheatsheets Search")

app = FastAPI(title=APP_TITLE)

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
app.mount("/web", StaticFiles(directory=WEB_DIR), name="web")

@app.on_event("startup")
def _startup():
  ensure_schema()

@app.get("/", response_class=HTMLResponse)
def index():
  with open(os.path.join(WEB_DIR, "index.html"), "r", encoding="utf-8") as f:
    return f.read().replace("{{APP_TITLE}}", APP_TITLE)

@app.get("/api/status")
def status():
  return {
    "last_sync_at": get_meta("last_sync_at", ""),
    "source_hash": get_meta("source_hash", ""),
  }

@app.get("/api/topics")
def api_topics():
  return {"items": list_topics()}

@app.get("/api/tags")
def api_tags():
  return {"items": list_tags()}

@app.get("/api/search")
def api_search(
  q: str = Query(default="", max_length=200),
  topics: str = Query(default="", max_length=800),   # CSV
  tags: str = Query(default="", max_length=800),     # CSV
  words: str = Query(default="", max_length=200),
  limit: int = Query(default=30, ge=1, le=50),
):
  topic_list = [t.strip() for t in (topics or "").split(",") if t.strip()]
  tag_list = [t.strip() for t in (tags or "").split(",") if t.strip()]
  return search_cards(q=q, topics=topic_list, tag_ids=tag_list, words=words, limit=limit)
