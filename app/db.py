import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "/data/app.db"))

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
  topic_id TEXT PRIMARY KEY,
  section TEXT,
  title_ru TEXT,
  title_en TEXT,
  ord INTEGER,
  status TEXT,
  updated_at TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS cards (
  card_id TEXT PRIMARY KEY,
  topic_id TEXT NOT NULL,
  concept TEXT,
  brief TEXT,
  example TEXT,
  when_use TEXT,
  pitfalls TEXT,
  keywords TEXT,
  status TEXT,
  updated_at TEXT,
  search_text TEXT,
  tags_text TEXT,
  aliases_text TEXT,
  FOREIGN KEY(topic_id) REFERENCES topics(topic_id)
);

CREATE TABLE IF NOT EXISTS tags (
  tag_id TEXT PRIMARY KEY,
  name_ru TEXT,
  name_en TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS card_tags (
  card_id TEXT NOT NULL,
  tag_id TEXT NOT NULL,
  notes TEXT,
  PRIMARY KEY(card_id, tag_id),
  FOREIGN KEY(card_id) REFERENCES cards(card_id),
  FOREIGN KEY(tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE IF NOT EXISTS aliases (
  alias TEXT PRIMARY KEY,
  card_id TEXT NOT NULL,
  weight REAL,
  notes TEXT,
  FOREIGN KEY(card_id) REFERENCES cards(card_id)
);

CREATE TABLE IF NOT EXISTS links (
  card_id TEXT NOT NULL,
  title TEXT,
  url TEXT,
  kind TEXT,
  notes TEXT,
  FOREIGN KEY(card_id) REFERENCES cards(card_id)
);
"""

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS cards_fts USING fts5(
  card_id UNINDEXED,
  topic_id UNINDEXED,
  concept,
  brief,
  example,
  when_use,
  pitfalls,
  keywords,
  search_text,
  tags_text,
  aliases_text
);
"""

def _connect() -> sqlite3.Connection:
  DB_PATH.parent.mkdir(parents=True, exist_ok=True)
  conn = sqlite3.connect(DB_PATH, check_same_thread=False)
  conn.row_factory = sqlite3.Row
  return conn

@contextmanager
def get_conn():
  conn = _connect()
  try:
    yield conn
    conn.commit()
  finally:
    conn.close()

def ensure_schema() -> dict:
  with get_conn() as conn:
    conn.executescript(SCHEMA)
    fts = True
    try:
      conn.executescript(FTS_SCHEMA)
    except sqlite3.OperationalError:
      fts = False
    return {"fts": fts}

def set_meta(key: str, value: str) -> None:
  with get_conn() as conn:
    conn.execute(
      "INSERT INTO meta(key, value) VALUES(?, ?) "
      "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      (key, value),
    )

def get_meta(key: str, default: str = "") -> str:
  with get_conn() as conn:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def rebuild_db(payload: dict) -> dict:
  """
  payload keys:
  - topics: list[dict]
  - cards: list[dict]
  - tags: list[dict]
  - card_tags: list[dict]
  - aliases: list[dict]
  - links: list[dict]
  """
  caps = ensure_schema()
  topics = payload.get("topics", [])
  cards = payload.get("cards", [])
  tags = payload.get("tags", [])
  card_tags = payload.get("card_tags", [])
  aliases = payload.get("aliases", [])
  links = payload.get("links", [])

  with get_conn() as conn:
    conn.execute("BEGIN")

    # очистка
    conn.execute("DELETE FROM cards_fts") if caps["fts"] else None
    conn.execute("DELETE FROM links")
    conn.execute("DELETE FROM aliases")
    conn.execute("DELETE FROM card_tags")
    conn.execute("DELETE FROM tags")
    conn.execute("DELETE FROM cards")
    conn.execute("DELETE FROM topics")

    conn.executemany(
      """INSERT INTO topics(topic_id, section, title_ru, title_en, ord, status, updated_at, notes)
         VALUES(:topic_id, :section, :title_ru, :title_en, :order, :status, :updated_at, :notes)""",
      topics,
    )

    conn.executemany(
      """INSERT INTO cards(card_id, topic_id, concept, brief, example, when_use, pitfalls, keywords, status, updated_at, search_text, tags_text, aliases_text)
         VALUES(:card_id, :topic_id, :concept, :brief, :example, :when_use, :pitfalls, :keywords, :status, :updated_at, :search_text, :tags_text, :aliases_text)""",
      cards,
    )

    if tags:
      conn.executemany(
        "INSERT INTO tags(tag_id, name_ru, name_en, notes) VALUES(:tag_id, :name_ru, :name_en, :notes)",
        tags,
      )

    if card_tags:
      conn.executemany(
        "INSERT INTO card_tags(card_id, tag_id, notes) VALUES(:card_id, :tag_id, :notes)",
        card_tags,
      )

    if aliases:
      conn.executemany(
        "INSERT INTO aliases(alias, card_id, weight, notes) VALUES(:alias, :card_id, :weight, :notes)",
        aliases,
      )

    if links:
      conn.executemany(
        "INSERT INTO links(card_id, title, url, kind, notes) VALUES(:card_id, :title, :url, :kind, :notes)",
        links,
      )

    if caps["fts"]:
      conn.execute(
        """
        INSERT INTO cards_fts(card_id, topic_id, concept, brief, example, when_use, pitfalls, keywords, search_text, tags_text, aliases_text)
        SELECT card_id, topic_id, concept, brief, example, when_use, pitfalls, keywords, search_text, tags_text, aliases_text
        FROM cards
        """
      )

    conn.execute("COMMIT")

  return {"fts": caps["fts"], "topics": len(topics), "cards": len(cards), "tags": len(tags)}

def list_topics() -> list[dict]:
  ensure_schema()
  with get_conn() as conn:
    rows = conn.execute(
      """
      SELECT topic_id, section, title_ru, title_en, ord
      FROM topics
      WHERE status='active'
      ORDER BY section, ord
      """
    ).fetchall()
  return [dict(r) for r in rows]

def list_tags() -> list[dict]:
  ensure_schema()
  with get_conn() as conn:
    rows = conn.execute(
      """
      SELECT tag_id, name_ru, name_en
      FROM tags
      ORDER BY COALESCE(name_ru, tag_id)
      """
    ).fetchall()
  return [dict(r) for r in rows]

def _tokenize(s: str) -> list[str]:
  s = (s or "").strip()
  if not s:
    return []
  parts = [p.strip() for p in s.replace(",", " ").split()]
  return [p for p in parts if p]

def _fts_escape_term(term: str) -> str:
  term = term.replace('"', '""').strip()
  return f'"{term}"' if term else ""

def search_cards(q: str, topics: list[str], tag_ids: list[str], words: str, limit: int = 30) -> dict:
  caps = ensure_schema()
  limit = max(1, min(int(limit), 50))
  topics = [t.strip() for t in (topics or []) if t and t.strip()]
  tag_ids = [t.strip() for t in (tag_ids or []) if t and t.strip()]

  terms = _tokenize(q) + _tokenize(words)

  with get_conn() as conn:
    base_select = """
      SELECT c.card_id, c.topic_id, c.concept, c.brief, c.example, c.when_use, c.pitfalls, c.keywords, c.tags_text,
             t.title_ru AS topic_title_ru, t.section
      FROM cards c
      JOIN topics t ON t.topic_id = c.topic_id
    """

    join_tags = ""
    where = ["c.status='active'"]
    params: list = []

    if topics:
      where.append("c.topic_id IN (" + ",".join(["?"] * len(topics)) + ")")
      params.extend(topics)

    if tag_ids:
      # карточка должна иметь ВСЕ выбранные теги (AND). Для OR — поменяем логику.
      join_tags = "JOIN card_tags ct ON ct.card_id = c.card_id"
      where.append("ct.tag_id IN (" + ",".join(["?"] * len(tag_ids)) + ")")
      params.extend(tag_ids)

    if caps["fts"] and terms:
      fts_query = " AND ".join(_fts_escape_term(t) for t in terms if t)
      sql = f"""
        SELECT x.*
        FROM (
          SELECT c.card_id, c.topic_id, c.concept, c.brief, c.example, c.when_use, c.pitfalls, c.keywords, c.tags_text,
                 t.title_ru AS topic_title_ru, t.section
          FROM cards_fts f
          JOIN cards c ON c.card_id = f.card_id
          JOIN topics t ON t.topic_id = c.topic_id
          {join_tags}
          WHERE f.cards_fts MATCH ?
        ) x
        WHERE {" AND ".join(where)}
        GROUP BY x.card_id
        HAVING COUNT(DISTINCT CASE WHEN ? THEN NULL END) IS NULL
        LIMIT ?
      """
      # HAVING выше “пустой” — держим GROUP BY для дедупликации при join_tags
      # params: [fts_query] + where_params + [limit] — но HAVING hack не нужен; проще собрать иначе:
      # Пересоберём без HAVING:

      params2 = [fts_query] + params + [limit]
      # Перепишем SQL проще:
      sql = f"""
        SELECT c.card_id, c.topic_id, c.concept, c.brief, c.example, c.when_use, c.pitfalls, c.keywords, c.tags_text,
               t.title_ru AS topic_title_ru, t.section
        FROM cards_fts f
        JOIN cards c ON c.card_id = f.card_id
        JOIN topics t ON t.topic_id = c.topic_id
        {join_tags}
        WHERE f.cards_fts MATCH ? AND {" AND ".join(where)}
        GROUP BY c.card_id
        LIMIT ?
      """
      rows = conn.execute(sql, params2).fetchall()
    else:
      # LIKE fallback по термам
      for t in terms:
        like = f"%{t}%"
        where.append(
          "(c.concept LIKE ? OR c.brief LIKE ? OR c.example LIKE ? OR c.when_use LIKE ? OR c.pitfalls LIKE ? OR c.keywords LIKE ? OR c.tags_text LIKE ? OR c.aliases_text LIKE ?)"
        )
        params.extend([like, like, like, like, like, like, like, like])

      sql = f"""
        {base_select}
        {join_tags}
        WHERE {" AND ".join(where)}
        GROUP BY c.card_id
        ORDER BY t.section, t.ord, c.card_id
        LIMIT ?
      """
      params2 = params + [limit]
      rows = conn.execute(sql, params2).fetchall()

  return {
    "fts": caps["fts"],
    "query": (q or "").strip(),
    "words": (words or "").strip(),
    "topics": topics,
    "tags": tag_ids,
    "total": len(rows),
    "items": [dict(r) for r in rows],
    "last_sync_at": get_meta("last_sync_at", ""),
    "source_hash": get_meta("source_hash", ""),
  }
