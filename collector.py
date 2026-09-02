#!/usr/bin/env python3
"""z6scope collector — read-only archiver for technocore.chat rooms.

Two-layer design (raw archive + selective publication):
  layer 1: every HTTP response body is stored verbatim (gzip) under raw/
  layer 2: parsed fields go into SQLite (messages / coverage / meta)
The publication layer (site JSON) is a separate tool and is NOT here.

Read-only by construction: this program only issues GET requests.
It never posts, never writes to the server, never creates keys.

Usage:
  python3 collector.py --once            # one polling cycle over all rooms
  python3 collector.py --loop            # poll every INTERVAL_SEC forever
  python3 collector.py --backfill lobby  # slowly drain what remains in one
                                         # room's ring buffer (oldest first)

Before first run, verify FIELD MAPPING and PAGINATION below against
https://technocore.chat/openapi.json and one real room response.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import re
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ----------------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------------
BASE_URL = "https://technocore.chat"
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "rooms.json"       # observation points live in config,
                                        # not in code (HTLC rooms get added here)
DB_PATH = ROOT / "data" / "z6scope.sqlite3"
RAW_DIR = ROOT / "raw"

INTERVAL_SEC = 300          # polling cycle (matches existing 5-min discipline)
PAGE_LIMIT = 100            # messages per request
BACKFILL_SLEEP_SEC = 3.0    # pause between backfill pages (deliberately slow;
                            # rate_read budget is 600/min, we use ~20/min)
POLL_SLEEP_BETWEEN_ROOMS = 2.0
HTTP_TIMEOUT = 20
RETRIES = 3
RETRY_SLEEP = 5.0
USER_AGENT = "z6scope-collector/0.1 (read-only archiver; github.com/shibainu-inu/z6scope)"

# --- FIELD MAPPING (VERIFY against a real response before first run) --------
# Expected room response shape: {"messages": [ {...}, ... ], ...}
# Per-message keys tried in order; first present key wins.
KEYS_SEQ = ("seq",)
KEYS_DID = ("did", "from", "sender")
KEYS_TS = ("ts", "time", "timestamp")
KEYS_BODY = ("text", "body", "message")
MESSAGES_KEY_CANDIDATES = ("messages", "items", "msgs")

# --- PAGINATION (VERIFY against /openapi.json) ------------------------------
# Parameter name used to request messages OLDER than a given seq during
# backfill. Set to None to disable backfill paging until verified.
BEFORE_PARAM = "before_seq"   # TODO verify; candidates: before_seq / before / max_seq

RE_REPLY = re.compile(r"^\s*re:\s*(\d+)", re.IGNORECASE)

log = logging.getLogger("z6scope")

# ----------------------------------------------------------------------------
# storage
# ----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  room      TEXT NOT NULL,
  seq       INTEGER NOT NULL,
  did       TEXT,
  ts        TEXT,
  body      TEXT,
  body_hash TEXT,
  body_len  INTEGER,
  reply_to  INTEGER,
  fetched_at INTEGER NOT NULL,
  PRIMARY KEY (room, seq)
);
CREATE INDEX IF NOT EXISTS idx_messages_did ON messages(did);
CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room, ts);

-- seq ranges we know we can never recover (evicted from the ring buffer
-- before we could read them). Honest coverage accounting.
CREATE TABLE IF NOT EXISTS coverage_gaps (
  room       TEXT NOT NULL,
  gap_start  INTEGER NOT NULL,
  gap_end    INTEGER NOT NULL,
  detected_at INTEGER NOT NULL,
  PRIMARY KEY (room, gap_start)
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS parse_failures (
  room       TEXT NOT NULL,
  fetched_at INTEGER NOT NULL,
  raw_file   TEXT,
  reason     TEXT
);
"""


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


# ----------------------------------------------------------------------------
# fetch (GET only)
# ----------------------------------------------------------------------------
def http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 503):
                wait = RETRY_SLEEP * attempt
                log.warning("HTTP %s on %s — backing off %.0fs", e.code, url, wait)
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            log.warning("network error on %s (attempt %d/%d): %s", url, attempt, RETRIES, e)
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"giving up on {url}: {last_err}")


def room_url(room: str, before_seq: int | None = None) -> str:
    params = {"format": "json", "limit": str(PAGE_LIMIT)}
    if before_seq is not None:
        if BEFORE_PARAM is None:
            raise RuntimeError("BEFORE_PARAM not verified; backfill paging disabled")
        params[BEFORE_PARAM] = str(before_seq)
    return f"{BASE_URL}/r/{urllib.parse.quote(room)}?{urllib.parse.urlencode(params)}"


def save_raw(room: str, payload: bytes, fetched_at: int) -> str:
    day = time.strftime("%Y-%m-%d", time.gmtime(fetched_at))
    d = RAW_DIR / day
    d.mkdir(parents=True, exist_ok=True)
    name = f"{room.replace('/', '_')}-{fetched_at}.json.gz"
    path = d / name
    with gzip.open(path, "wb") as f:
        f.write(payload)
    return str(path.relative_to(ROOT))


# ----------------------------------------------------------------------------
# parse — untrusted data: bytes in, plain values out, nothing executed
# ----------------------------------------------------------------------------
def first_key(d: dict, keys: tuple) -> object:
    for k in keys:
        if k in d:
            return d[k]
    return None


def extract_messages(payload: bytes) -> list[dict]:
    doc = json.loads(payload)
    if isinstance(doc, list):
        return [m for m in doc if isinstance(m, dict)]
    if isinstance(doc, dict):
        for k in MESSAGES_KEY_CANDIDATES:
            v = doc.get(k)
            if isinstance(v, list):
                return [m for m in v if isinstance(m, dict)]
    raise ValueError("no message array found (adjust MESSAGES_KEY_CANDIDATES)")


def normalize(msg: dict) -> dict | None:
    seq = first_key(msg, KEYS_SEQ)
    if seq is None:
        return None
    body = first_key(msg, KEYS_BODY)
    body = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
    m = RE_REPLY.match(body or "")
    return {
        "seq": int(seq),
        "did": first_key(msg, KEYS_DID),
        "ts": str(first_key(msg, KEYS_TS)),
        "body": body,
        "body_hash": hashlib.sha256((body or "").encode("utf-8")).hexdigest(),
        "body_len": len(body or ""),
        "reply_to": int(m.group(1)) if m else None,
    }


# ----------------------------------------------------------------------------
# ingest + coverage accounting
# ----------------------------------------------------------------------------
def ingest(con: sqlite3.Connection, room: str, payload: bytes,
           fetched_at: int, raw_file: str) -> tuple[int, int, int]:
    """Returns (parsed, inserted, min_seq_in_page)."""
    try:
        raw_msgs = extract_messages(payload)
    except (ValueError, json.JSONDecodeError) as e:
        con.execute(
            "INSERT INTO parse_failures(room, fetched_at, raw_file, reason) VALUES (?,?,?,?)",
            (room, fetched_at, raw_file, str(e)))
        con.commit()
        log.error("[%s] parse failure (%s) — raw kept at %s", room, e, raw_file)
        return (0, 0, -1)

    rows, min_seq = [], -1
    for m in raw_msgs:
        n = normalize(m)
        if n is None:
            continue
        min_seq = n["seq"] if min_seq < 0 else min(min_seq, n["seq"])
        rows.append((room, n["seq"], n["did"], n["ts"], n["body"],
                     n["body_hash"], n["body_len"], n["reply_to"], fetched_at))
    cur = con.executemany(
        "INSERT OR IGNORE INTO messages"
        "(room, seq, did, ts, body, body_hash, body_len, reply_to, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return (len(rows), cur.rowcount, min_seq)


def record_gap_if_any(con: sqlite3.Connection, room: str, oldest_available: int) -> None:
    """If the oldest seq still on the server is newer than (our max stored seq
    at last poll + 1), the range in between was evicted before we read it."""
    row = con.execute(
        "SELECT MAX(seq) FROM messages WHERE room=?", (room,)).fetchone()
    have_max = row[0]
    if have_max is None or oldest_available <= have_max + 1:
        return
    gap = (have_max + 1, oldest_available - 1)
    con.execute(
        "INSERT OR IGNORE INTO coverage_gaps(room, gap_start, gap_end, detected_at)"
        " VALUES (?,?,?,?)", (room, gap[0], gap[1], int(time.time())))
    con.commit()
    log.warning("[%s] coverage gap recorded: seq %d..%d (evicted before read)",
                room, gap[0], gap[1])


# ----------------------------------------------------------------------------
# modes
# ----------------------------------------------------------------------------
def poll_room(con: sqlite3.Connection, room: str) -> None:
    fetched_at = int(time.time())
    payload = http_get(room_url(room))
    raw_file = save_raw(room, payload, fetched_at)
    parsed, inserted, min_seq = ingest(con, room, payload, fetched_at, raw_file)
    if min_seq >= 0:
        record_gap_if_any(con, room, oldest_available=min_seq)
    log.info("[%s] parsed=%d new=%d", room, parsed, inserted)


def backfill_room(con: sqlite3.Connection, room: str) -> None:
    """Drain what still exists in the ring buffer, paging oldest-ward slowly."""
    log.info("[%s] backfill start (sleep %.1fs/page)", room, BACKFILL_SLEEP_SEC)
    fetched_at = int(time.time())
    payload = http_get(room_url(room))
    raw_file = save_raw(room, payload, fetched_at)
    _, inserted, min_seq = ingest(con, room, payload, fetched_at, raw_file)
    total = inserted
    while min_seq > 0:
        time.sleep(BACKFILL_SLEEP_SEC)
        fetched_at = int(time.time())
        try:
            payload = http_get(room_url(room, before_seq=min_seq))
        except RuntimeError as e:
            log.error("[%s] backfill stopped: %s", room, e)
            break
        raw_file = save_raw(room, payload, fetched_at)
        parsed, inserted, page_min = ingest(con, room, payload, fetched_at, raw_file)
        total += inserted
        if parsed == 0 or page_min < 0 or page_min >= min_seq:
            break  # reached the oldest surviving message
        min_seq = page_min
    log.info("[%s] backfill done, %d messages stored; oldest surviving seq=%d",
             room, total, max(min_seq, 0))


def load_rooms() -> list[str]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    rooms = cfg.get("rooms", [])
    if not rooms:
        raise SystemExit("rooms.json has no rooms")
    return rooms


_stop = False


def _sig(_n, _f):
    global _stop
    _stop = True


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--once", action="store_true")
    g.add_argument("--loop", action="store_true")
    g.add_argument("--backfill", metavar="ROOM")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    con = open_db()

    if args.backfill:
        backfill_room(con, args.backfill)
        return

    while True:
        for room in load_rooms():  # reloaded every cycle: HTLC rooms can be
            if _stop:              # added to rooms.json without a restart
                return
            try:
                poll_room(con, room)
            except Exception as e:  # noqa: BLE001 — keep the loop alive
                log.error("[%s] poll failed: %s", room, e)
            time.sleep(POLL_SLEEP_BETWEEN_ROOMS)
        if args.once or _stop:
            return
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
