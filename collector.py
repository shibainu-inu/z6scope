#!/usr/bin/env python3
"""z6scope collector — read-only archiver for technocore.chat rooms.

Two-layer design (raw archive + selective publication):
  layer 1: every HTTP response body is stored verbatim (gzip) under raw/
  layer 2: parsed fields go into SQLite (messages / coverage / meta)
The publication layer (site JSON) is a separate tool and is NOT here.

Read-only by construction: this program only issues GET requests.
It never posts, never writes to the server, never creates keys.

API facts verified against /openapi.json (2026-09-02):
  - GET /r/{room}?format=json&limit=N&since=SEQ  (forward paging only;
    `since` returns messages with a GREATER seq — there is no backward
    paging parameter)
  - `n` query param: ignored by the server, varies the URL past a cache
    (used here as a cache-buster on every request)
  - GET /r/{room}/export exists (used for --backfill; format verified at
    first run — raw is archived even if parsing is not yet wired)

Usage:
  python3 collector.py --once            # one polling cycle over all rooms
  python3 collector.py --loop            # poll every INTERVAL_SEC forever
  python3 collector.py --backfill lobby  # one-shot: archive /export of a room
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

INTERVAL_SEC = 300          # polling cycle (5-min discipline)
PAGE_LIMIT = 100            # messages per request
MAX_PAGES_PER_CYCLE = 60    # safety cap per room per cycle (6,000 msgs);
                            # at ~300 msgs/min in /r/technocore a 5-min cycle
                            # needs ~16 pages, so 60 leaves headroom
PAGE_SLEEP_SEC = 0.5        # pause between catch-up pages
POLL_SLEEP_BETWEEN_ROOMS = 2.0
HTTP_TIMEOUT = 20
EXPORT_TIMEOUT = 90   # /export generates the whole ring server-side; be patient
RETRIES = 3
RETRY_SLEEP = 5.0
USER_AGENT = "z6scope-collector/0.2 (read-only archiver; github.com/shibainu-inu/z6scope)"

# --- FIELD MAPPING (verified against live /r/technocore, 2026-09-02) --------
KEYS_SEQ = ("seq",)
KEYS_DID = ("from", "did", "sender")
KEYS_TS = ("ts", "time", "timestamp")
KEYS_BODY = ("text", "body", "message")
KEYS_NONCE = ("nonce",)
KEYS_SIG = ("sig",)
MESSAGES_KEY_CANDIDATES = ("messages", "items", "msgs")

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
  nonce     TEXT,
  sig       TEXT,
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

MIGRATIONS = (
    "ALTER TABLE messages ADD COLUMN nonce TEXT",
    "ALTER TABLE messages ADD COLUMN sig TEXT",
)


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    for stmt in MIGRATIONS:          # no-ops on a fresh DB, safe on an old one
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass
    con.commit()
    return con


def meta_get(con: sqlite3.Connection, key: str) -> str | None:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?,?)", (key, value))
    con.commit()


# ----------------------------------------------------------------------------
# fetch (GET only)
# ----------------------------------------------------------------------------
def http_get(url: str, timeout: int = HTTP_TIMEOUT) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
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


def room_url(room: str, since: int | None = None) -> str:
    params = {"format": "json", "limit": str(PAGE_LIMIT), "n": str(int(time.time() * 1000))}
    if since is not None:
        params["since"] = str(since)
    return f"{BASE_URL}/r/{urllib.parse.quote(room)}?{urllib.parse.urlencode(params)}"


def export_url(room: str) -> str:
    return f"{BASE_URL}/r/{urllib.parse.quote(room)}/export"


def save_raw(room: str, payload: bytes, fetched_at: int, kind: str = "poll") -> str:
    day = time.strftime("%Y-%m-%d", time.gmtime(fetched_at))
    d = RAW_DIR / day
    d.mkdir(parents=True, exist_ok=True)
    name = f"{room.replace('/', '_')}-{kind}-{fetched_at}-{int(time.monotonic()*1000)%100000}.json.gz"
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


def extract(payload: bytes) -> tuple[dict, list[dict]]:
    """Returns (top_level_doc, message_dicts).

    Handles both response shapes observed live (2026-09-02):
      - room polling: one JSON object with a "messages" array
      - /export: JSONL — one JSON object per line ("Extra data" on whole-body
        parse is the signature of this format)
    """
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError as e:
        if "Extra data" not in str(e):
            raise
        return ({}, _extract_jsonl(payload))
    if isinstance(doc, list):
        return ({}, [m for m in doc if isinstance(m, dict)])
    if isinstance(doc, dict):
        for k in MESSAGES_KEY_CANDIDATES:
            v = doc.get(k)
            if isinstance(v, list):
                return (doc, [m for m in v if isinstance(m, dict)])
    raise ValueError("no message array found (adjust MESSAGES_KEY_CANDIDATES)")


def _extract_jsonl(payload: bytes) -> list[dict]:
    msgs: list[dict] = []
    bad = 0
    for line in payload.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            bad += 1
            continue
        if not isinstance(obj, dict):
            continue
        picked = False
        for k in MESSAGES_KEY_CANDIDATES:
            v = obj.get(k)
            if isinstance(v, list):
                msgs.extend(m for m in v if isinstance(m, dict))
                picked = True
                break
        if not picked:
            msgs.append(obj)   # the line itself is a message object
    if bad:
        log.warning("jsonl: %d unparseable lines skipped (raw retains them)", bad)
    if not msgs:
        raise ValueError("jsonl detected but no message objects found")
    return msgs


def normalize(msg: dict) -> dict | None:
    seq = first_key(msg, KEYS_SEQ)
    if seq is None:
        return None
    body = first_key(msg, KEYS_BODY)
    body = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
    m = RE_REPLY.match(body or "")
    nonce = first_key(msg, KEYS_NONCE)
    return {
        "seq": int(seq),
        "did": first_key(msg, KEYS_DID),
        "ts": str(first_key(msg, KEYS_TS)),
        "body": body,
        "body_hash": hashlib.sha256((body or "").encode("utf-8")).hexdigest(),
        "body_len": len(body or ""),
        "reply_to": int(m.group(1)) if m else None,
        "nonce": None if nonce is None else str(nonce),
        "sig": first_key(msg, KEYS_SIG),
    }


# ----------------------------------------------------------------------------
# ingest
# ----------------------------------------------------------------------------
def ingest(con: sqlite3.Connection, room: str, payload: bytes,
           fetched_at: int, raw_file: str) -> dict:
    """Returns {'parsed','inserted','min_seq','max_seq','first_seq','last_seq'}
    (seq fields are -1 when absent)."""
    out = {"parsed": 0, "inserted": 0, "min_seq": -1, "max_seq": -1,
           "first_seq": -1, "last_seq": -1}
    try:
        doc, raw_msgs = extract(payload)
    except (ValueError, json.JSONDecodeError) as e:
        con.execute(
            "INSERT INTO parse_failures(room, fetched_at, raw_file, reason) VALUES (?,?,?,?)",
            (room, fetched_at, raw_file, str(e)))
        con.commit()
        log.error("[%s] parse failure (%s) — raw kept at %s", room, e, raw_file)
        return out

    if isinstance(doc.get("first_seq"), int):
        out["first_seq"] = doc["first_seq"]
    if isinstance(doc.get("last_seq"), int):
        out["last_seq"] = doc["last_seq"]

    rows = []
    for m in raw_msgs:
        n = normalize(m)
        if n is None:
            continue
        out["min_seq"] = n["seq"] if out["min_seq"] < 0 else min(out["min_seq"], n["seq"])
        out["max_seq"] = max(out["max_seq"], n["seq"])
        rows.append((room, n["seq"], n["did"], n["ts"], n["body"], n["body_hash"],
                     n["body_len"], n["reply_to"], n["nonce"], n["sig"], fetched_at))
    cur = con.executemany(
        "INSERT OR IGNORE INTO messages"
        "(room, seq, did, ts, body, body_hash, body_len, reply_to, nonce, sig, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    out["parsed"], out["inserted"] = len(rows), cur.rowcount
    return out


def record_gap(con: sqlite3.Connection, room: str, start: int, end: int) -> None:
    if end < start:
        return
    con.execute(
        "INSERT OR IGNORE INTO coverage_gaps(room, gap_start, gap_end, detected_at)"
        " VALUES (?,?,?,?)", (room, start, end, int(time.time())))
    con.commit()
    log.warning("[%s] coverage gap recorded: seq %d..%d (evicted before read)",
                room, start, end)


# ----------------------------------------------------------------------------
# modes
# ----------------------------------------------------------------------------
def stored_max_seq(con: sqlite3.Connection, room: str) -> int | None:
    row = con.execute("SELECT MAX(seq) FROM messages WHERE room=?", (room,)).fetchone()
    return row[0]


def poll_room(con: sqlite3.Connection, room: str) -> None:
    """Catch up from our stored position using forward paging (`since`).

    Gap rule: when we ask for `since=cursor` and the server's oldest returned
    seq is > cursor+1 on the FIRST page, the range in between was evicted
    before we could read it — recorded as a coverage gap.
    """
    cursor = stored_max_seq(con, room)
    total_new = 0
    for page in range(MAX_PAGES_PER_CYCLE):
        fetched_at = int(time.time())
        payload = http_get(room_url(room, since=cursor))
        raw_file = save_raw(room, payload, fetched_at)
        r = ingest(con, room, payload, fetched_at, raw_file)
        if r["parsed"] == 0:
            break
        if page == 0:
            if cursor is None:
                meta_set(con, f"first_observed:{room}", str(r["min_seq"]))
                log.info("[%s] observation starts at seq %d "
                         "(older history is unreachable via polling; use --backfill)",
                         room, r["min_seq"])
            elif r["min_seq"] > cursor + 1:
                record_gap(con, room, cursor + 1, r["min_seq"] - 1)
        total_new += r["inserted"]
        cursor = r["max_seq"]
        if r["parsed"] < PAGE_LIMIT:
            break  # caught up
        time.sleep(PAGE_SLEEP_SEC)
    else:
        log.warning("[%s] page cap (%d) hit — will continue next cycle",
                    room, MAX_PAGES_PER_CYCLE)
    log.info("[%s] new=%d cursor=%s", room, total_new, cursor)


def backfill_room(con: sqlite3.Connection, room: str) -> None:
    """One-shot archive of GET /r/{room}/export.

    The export format has not been observed yet: the raw body is always
    archived first; JSON parsing is attempted on top. If parsing fails the
    data is safe in raw/ and a parser can be added later.
    """
    fetched_at = int(time.time())
    payload = http_get(export_url(room), timeout=EXPORT_TIMEOUT)
    raw_file = save_raw(room, payload, fetched_at, kind="export")
    log.info("[%s] export archived: %s (%d bytes)", room, raw_file, len(payload))
    r = ingest(con, room, payload, fetched_at, raw_file)
    if r["parsed"]:
        log.info("[%s] export parsed: %d messages, %d new, seq %d..%d",
                 room, r["parsed"], r["inserted"], r["min_seq"], r["max_seq"])
    else:
        log.warning("[%s] export not parsed as JSON messages — raw archived, "
                    "send the first bytes of %s for a parser patch", room, raw_file)


def reparse_raw(con: sqlite3.Connection, room: str, raw_path: str) -> None:
    """Ingest an already-archived raw file without touching the server."""
    path = (ROOT / raw_path) if not Path(raw_path).is_absolute() else Path(raw_path)
    with gzip.open(path, "rb") as f:
        payload = f.read()
    fetched_at = int(time.time())
    r = ingest(con, room, payload, fetched_at, str(raw_path))
    log.info("[%s] reparse %s: parsed=%d new=%d seq %d..%d",
             room, path.name, r["parsed"], r["inserted"], r["min_seq"], r["max_seq"])


def load_rooms() -> list[str]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    rooms = cfg.get("rooms", [])
    if not rooms:
        raise SystemExit("rooms.json has no rooms")
    return rooms


_stop = False


def _sig_handler(_n, _f):
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
    g.add_argument("--reparse", nargs=2, metavar=("ROOM", "RAWFILE"))
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)
    con = open_db()

    if args.backfill:
        backfill_room(con, args.backfill)
        return
    if args.reparse:
        reparse_raw(con, args.reparse[0], args.reparse[1])
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
