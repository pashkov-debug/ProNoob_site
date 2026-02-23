import hashlib
import os
from datetime import datetime, timezone

import httpx

from .db import rebuild_db, set_meta
from .importer_xlsx import parse_xlsx_bytes

def _now_iso() -> str:
  return datetime.now(timezone.utc).isoformat()

def _sha256(b: bytes) -> str:
  return hashlib.sha256(b).hexdigest()

def _looks_like_xlsx(b: bytes) -> bool:
  # XLSX — zip, начинается с 'PK'
  return len(b) > 4 and b[:2] == b"PK"

def fetch_xlsx(url: str) -> bytes:
  if not url:
    raise ValueError("SHEET_XLSX_URL пустой")
  with httpx.Client(timeout=30, follow_redirects=True) as client:
    r = client.get(url)
    r.raise_for_status()
    return r.content

def sync_once() -> dict:
  url = os.getenv("SHEET_XLSX_URL", "").strip()
  xlsx = fetch_xlsx(url)

  if not _looks_like_xlsx(xlsx):
    head = xlsx[:200].decode("utf-8", errors="replace")
    raise RuntimeError(f"Скачалось не XLSX (часто это HTML/401). Первые байты:\n{head}")

  h = _sha256(xlsx)
  payload = parse_xlsx_bytes(xlsx)
  caps = rebuild_db(payload)

  set_meta("last_sync_at", _now_iso())
  set_meta("source_hash", h)

  return {"changed": True, "source_hash": h, **caps}
