#!/usr/bin/env python3
"""Independent re-derivation of docs/coverage.sql (A) — CLAUDE.md rule 7 —
plus the raw/ residual check that (A) cannot do on its own.

Set arithmetic only (no window functions, no LAG): for each room, the seqs
present at/below the boundary vs. the dense range [MIN(seq), boundary].
Read-only; opens the DB with mode=ro and never writes anything.

  python3 docs/coverage_check.py                 # boundaries from meta.last_export_max,
                                                 # cross-checked against coverage.sql (A)
  python3 docs/coverage_check.py kibble=1012034  # pin boundaries to reproduce a snapshot
                                                 # (cross-check only if pins == meta)
  python3 docs/coverage_check.py --raw-residual  # seqs that exist in raw/ but not in
                                                 # `messages` at/below the boundary
                                                 # (recoverable via --reparse, so NOT
                                                 # "permanently lost"); also counts
                                                 # unparseable JSONL lines. Must be 0.
  python3 docs/coverage_check.py --selftest      # synthetic DB incl. a hole straddling
                                                 # the boundary and a room without a
                                                 # boundary; runs the REAL (A) statement
                                                 # from docs/coverage.sql against it
Exit code 1 on any mismatch or non-zero residual.
"""
import gzip, json, pathlib, sqlite3, sys, time

ROOT = pathlib.Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "z6scope.sqlite3"
SQL = ROOT / "docs" / "coverage.sql"
RAW = ROOT / "raw"


def bounds_from_meta(con):
    return {k[len("last_export_max:"):]: int(v) for k, v in
            con.execute("SELECT key, value FROM meta WHERE key LIKE 'last_export_max:%'")}


def rooms_in_messages(con):
    return {r for (r,) in con.execute("SELECT DISTINCT room FROM messages")}


def derive(con, bounds):
    out = {}
    for room, bound in sorted(bounds.items()):
        seqs = {s for (s,) in con.execute(
            "SELECT seq FROM messages WHERE room=? AND seq<=?", (room, bound))}
        if not seqs:
            out[room] = (bound, None, 0, None)
            continue
        lo = min(seqs)
        span = bound - lo + 1
        out[room] = (bound, span, len(seqs), span - len(seqs))
    return out


def statement_a():
    """Statement (A) of docs/coverage.sql, anchored on its '-- (A)' marker."""
    text = SQL.read_text()
    start = text.index("-- (A)")
    end = text.index(";\n\n", start)   # statements are separated by a blank line
    return text[start:end]


def query_a(con):
    cur = con.execute(statement_a())
    cols = [d[0] for d in cur.description]
    res = {}
    for row in cur:
        r = dict(zip(cols, row))
        res[r["room"]] = (r["boundary_last_export_max"], r["span_at_boundary"],
                          r["present_at_boundary"], r["lost_permanent"])
    return res


def cross_check(con, py):
    sql = query_a(con)
    missing_rooms = rooms_in_messages(con) - set(py)
    same = set(sql) == set(py) | missing_rooms and all(sql.get(r) == v for r, v in py.items())
    for r in sorted(missing_rooms):
        print(f"WARNING room {r!r} has no last_export_max (never exported) — flagged by (A), not derivable here")
    print("matches docs/coverage.sql (A):", "YES" if same else
          "NO -> " + str({r: (sql.get(r), py.get(r)) for r in set(sql) | set(py) if sql.get(r) != py.get(r)}))
    return same


def raw_residual(con, bounds):
    """Scan every raw/*.json.gz: seqs parsed from raw that are absent from
    `messages` at/below the boundary, and JSONL lines that do not parse."""
    seen = {r: set() for r in bounds}
    bad_lines = {r: 0 for r in bounds}
    files = 0
    for path in sorted(RAW.glob("*/*.json.gz")):
        room = path.name.rsplit("-", 3)[0]      # <room>-<kind>-<ts>-<rand>.json.gz
        if room not in bounds:
            continue
        files += 1
        with gzip.open(path, "rb") as f:
            body = f.read()
        try:
            doc = json.loads(body)
            msgs = doc.get("messages", []) if isinstance(doc, dict) else doc
        except json.JSONDecodeError:
            msgs = []
            for line in body.splitlines():
                if not line.strip():
                    continue
                try:
                    msgs.append(json.loads(line))
                except json.JSONDecodeError:
                    bad_lines[room] += 1
        for m in msgs:
            s = m.get("seq") if isinstance(m, dict) else None
            if isinstance(s, int) and s <= bounds[room]:
                seen[room].add(s)
    print(f"raw residual @ {time.strftime('%Y-%m-%d %H:%M:%S %Z')}  files scanned={files}")
    print(f"{'room':18}{'in_raw<=b':>11}{'in_raw_not_in_db':>18}{'unparseable_lines':>19}")
    ok = True
    for room in sorted(bounds):
        have = {s for (s,) in con.execute("SELECT seq FROM messages WHERE room=? AND seq<=?", (room, bounds[room]))}
        resid = seen[room] - have
        print(f"{room:18}{len(seen[room]):>11}{len(resid):>18}{bad_lines[room]:>19}")
        ok &= not resid and bad_lines[room] == 0
    print("raw residual: " + ("NONE — every seq in raw/ at/below the boundary is in messages; no unparseable lines" if ok else "PRESENT — run --reparse before calling these ranges lost"))
    return ok


def selftest():
    con = sqlite3.connect(":memory:")
    con.executescript("""
      CREATE TABLE messages (room TEXT, seq INTEGER, PRIMARY KEY (room, seq));
      CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
      CREATE TABLE coverage_gaps (room TEXT, gap_start INTEGER, gap_end INTEGER, detected_at INTEGER);
    """)
    # room x: present 100..200, hole 201..249, present 250..300, hole 301..399 STRADDLING
    # boundary 350, present 400..410, hole 411..419 above boundary, present 420..430
    present = list(range(100, 201)) + list(range(250, 301)) + list(range(400, 411)) + list(range(420, 431))
    con.executemany("INSERT INTO messages VALUES ('x', ?)", [(s,) for s in present])
    con.execute("INSERT INTO meta VALUES ('last_export_max:x', '350')")
    # room y: no boundary at all -> must appear flagged in (A) and be reported here, not vanish
    con.executemany("INSERT INTO messages VALUES ('y', ?)", [(s,) for s in (1, 2, 3)])
    got = query_a(con)
    exp_lost = 49 + (350 - 301 + 1)          # 201..249 fully, 301..350 straddle part
    assert got["x"] == (350, 350 - 100 + 1, sum(1 for s in present if s <= 350), exp_lost), got["x"]
    py = derive(con, bounds_from_meta(con))
    assert py["x"] == got["x"]
    assert set(got) == {"x", "y"} and got["y"][0] is None, got.get("y")
    assert cross_check(con, py)
    cur = con.execute(statement_a()); cols = [d[0] for d in cur.description]
    x = dict(zip(cols, next(r for r in cur if r[0] == "x")))
    assert x["pending_unknown"] == (399 - 351 + 1) + 9, x["pending_unknown"]
    assert x["sanity_boundary_in_span"] == 1 and x["sanity_no_boundary"] == 0
    print("selftest OK: straddling hole split lost=%d pending=%d; room without boundary flagged on both sides" %
          (exp_lost, x["pending_unknown"]))


def main(argv):
    if "--selftest" in argv:
        selftest(); return 0
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    meta_bounds = bounds_from_meta(con)
    if "--raw-residual" in argv:
        return 0 if raw_residual(con, meta_bounds) else 1
    bounds = dict(meta_bounds)
    pinned = False
    for arg in argv:
        room, _, b = arg.partition("=")
        if b:
            bounds[room] = int(b); pinned = True
    py = derive(con, bounds)
    print("python derivation @", time.strftime("%Y-%m-%d %H:%M:%S %Z"))
    print(f"{'room':18}{'boundary':>10}{'span@b':>9}{'present@b':>11}{'lost':>9}")
    for room, (bound, span, present, lost) in py.items():
        print(f"{room:18}{bound:>10}{str(span):>9}{present:>11}{str(lost):>9}")
    if pinned and bounds != meta_bounds:
        print("NO CROSS-CHECK: pinned boundaries differ from meta.last_export_max; compare the numbers "
              "above with the snapshot doc by eye (docs/coverage-YYYY-MM-DD.md)")
        return 0
    return 0 if cross_check(con, py) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
