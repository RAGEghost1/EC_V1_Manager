# ec_manager/persist.py
from __future__ import annotations
import sqlite3, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

DEFAULT_DB = (Path(__file__).resolve().parents[1] / "logs" / "manager.db")

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at REAL NOT NULL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  ts REAL NOT NULL,
  role TEXT NOT NULL,          -- 'user' | 'assistant' | 'system'
  content TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  ts REAL NOT NULL,
  name TEXT NOT NULL,          -- e.g. 'self_check'
  result_json TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  ts REAL NOT NULL,
  action TEXT NOT NULL,        -- e.g. 'approve:<thing>'
  details_json TEXT,
  approved INTEGER NOT NULL,   -- 0/1
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  ts REAL NOT NULL,
  text TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
"""

class Persistence:
    def __init__(self, db_path: Path = DEFAULT_DB):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    # --- lifecycle ---
    def connect(self) -> None:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # --- sessions ---
    def new_session(self, note: str = "") -> int:
        self.connect()
        cur = self._conn.execute(
            "INSERT INTO sessions (started_at, note) VALUES (?, ?)",
            (time.time(), note),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def last_session(self) -> Optional[int]:
        self.connect()
        cur = self._conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return int(row[0]) if row else None

    # --- logging helpers ---
    def log_message(self, session_id: int, role: str, content: str) -> None:
        self.connect()
        self._conn.execute(
            "INSERT INTO messages(session_id, ts, role, content) VALUES (?, ?, ?, ?)",
            (session_id, time.time(), role, content),
        )
        self._conn.commit()

    def log_check(self, session_id: int, name: str, result: Dict[str, Any]) -> None:
        self.connect()
        self._conn.execute(
            "INSERT INTO checks(session_id, ts, name, result_json) VALUES (?, ?, ?, ?)",
            (session_id, time.time(), name, json.dumps(result, ensure_ascii=False)),
        )
        self._conn.commit()

    def log_action(self, session_id: int, action: str, details: Dict[str, Any], approved: bool) -> None:
        self.connect()
        self._conn.execute(
            "INSERT INTO actions(session_id, ts, action, details_json, approved) VALUES (?, ?, ?, ?, ?)",
            (session_id, time.time(), action, json.dumps(details or {}, ensure_ascii=False), 1 if approved else 0),
        )
        self._conn.commit()

    def log_suggestion(self, session_id: int, text: str) -> None:
        self.connect()
        self._conn.execute(
            "INSERT INTO suggestions(session_id, ts, text) VALUES (?, ?, ?)",
            (session_id, time.time(), text),
        )
        self._conn.commit()

    # --- readbacks ---
    def recent_messages(self, session_id: int, limit: int = 20) -> Iterable[Tuple[float, str, str]]:
        self.connect()
        cur = self._conn.execute(
            "SELECT ts, role, content FROM messages WHERE session_id=? ORDER BY id DESC LIMIT ?",
            (session_id, int(limit)),
        )
        return list(reversed(cur.fetchall()))

    def recent_suggestions(self, session_id: int, limit: int = 20) -> Iterable[str]:
        self.connect()
        cur = self._conn.execute(
            "SELECT text FROM suggestions WHERE session_id=? ORDER BY id DESC LIMIT ?",
            (session_id, int(limit)),
        )
        return [r[0] for r
