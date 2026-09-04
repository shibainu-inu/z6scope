-- docs/coverage.sql — read-only coverage accounting for z6scope.
-- Usage:  sqlite3 -readonly -column -header data/z6scope.sqlite3 < docs/coverage.sql
-- Second, independent derivation (rule 7):  python3 docs/coverage_check.py
--
-- Derives coverage from the seqs actually present in `messages`, NOT from
-- coverage_gaps: rows recorded before 2026-09-03 16:45 JST over-record (they
-- were polling skips, mostly filled later) AND some losses were never
-- recorded at all — see (B) and (B2).
--
-- Boundary per room = meta.last_export_max (top of the latest /export).
-- PREMISE (measured 2026-09-02/03; re-verify before relying on it, rule 7):
-- an /export returns one contiguous ring and server seqs are dense. Under
-- that premise a seq missing at or below the boundary lies below the ring's
-- oldest seq and cannot be fetched again. Residual to name when publishing:
-- a seq can be absent from `messages` yet still exist in raw/ as a JSONL
-- line that failed to parse (skipped with only a log warning, no
-- parse_failures row) — grep the collector log for "unparseable" first.
-- Missing seqs above the boundary were only head-polled and may still be
-- filled by the next export.
--
-- How to read (CLAUDE.md rule 5):
--   present_at_boundary = exact count held at/below the boundary -> "at least"
--   lost_permanent      = exact only under the premise above -> publish WITH it
--   pending_unknown     = upper bound on what may still arrive (time-varying)
--   sanity_* columns    = the two checks that CAN fail (boundary inside the
--                         span; boundary exists)
-- Note: present + lost + pending == span holds by construction (PRIMARY KEY
-- (room, seq) + the LAG partition) and so would any duplicate-seq check;
-- neither is evidence. The independent evidence is docs/coverage_check.py
-- agreeing on the same numbers, and its --raw-residual scan of raw/.

-- (A) per-room summary
WITH b AS (
  SELECT substr(key, 17) AS room, CAST(value AS INTEGER) AS bound
  FROM meta WHERE key LIKE 'last_export_max:%'
),
rooms AS (SELECT DISTINCT room FROM messages),
holes AS (
  SELECT room,
         LAG(seq) OVER (PARTITION BY room ORDER BY seq) + 1 AS h_start,
         seq - 1 AS h_end
  FROM messages
),
h AS (
  SELECT holes.room, h_start, h_end, b.bound
  FROM holes JOIN b USING (room)
  WHERE h_start IS NOT NULL AND h_end >= h_start
),
split AS (
  SELECT room,
         SUM(MAX(0, MIN(h_end, bound) - h_start + 1))     AS lost,     -- part of hole <= bound
         SUM(MAX(0, h_end - MAX(h_start, bound + 1) + 1)) AS pending   -- part of hole  > bound
  FROM h GROUP BY room
)
SELECT r.room,
       b.bound                                      AS boundary_last_export_max,
       MIN(m.seq)                                   AS span_start,
       MAX(m.seq)                                   AS span_end,
       MAX(m.seq) - MIN(m.seq) + 1                  AS span,
       COUNT(m.seq)                                 AS present,
       COALESCE(s.lost, 0)                          AS lost_permanent,
       COALESCE(s.pending, 0)                       AS pending_unknown,
       SUM(m.seq <= b.bound)                        AS present_at_boundary,   -- reproducible
       b.bound - MIN(m.seq) + 1                     AS span_at_boundary,      -- reproducible
       (b.bound BETWEEN MIN(m.seq) AND MAX(m.seq))  AS sanity_boundary_in_span, -- must be 1
       (b.bound IS NULL)                            AS sanity_no_boundary     -- must be 0 (1 = room never exported)
FROM rooms r
LEFT JOIN b        USING (room)   -- LEFT: a room without a boundary shows up flagged, not silently dropped
LEFT JOIN messages m USING (room)
LEFT JOIN split s  USING (room)
GROUP BY r.room;

-- (B0) preconditions for (B)/(B2): must both be empty / zero
SELECT 'overlapping_gap_pairs' AS check_name, COUNT(*) AS n   -- must be 0
FROM coverage_gaps a JOIN coverage_gaps c USING (room)
WHERE a.gap_start < c.gap_start AND c.gap_start <= a.gap_end;
SELECT 'adjacent_gap_pairs' AS check_name, COUNT(*) AS n      -- informational (merge treats as separate)
FROM coverage_gaps a JOIN coverage_gaps c USING (room)
WHERE c.gap_start = a.gap_end + 1;
SELECT 'gap_rows_above_boundary' AS check_name, COUNT(*) AS n -- must be 0 for (B) to equal (A).lost
FROM coverage_gaps c JOIN (SELECT substr(key, 17) AS room, CAST(value AS INTEGER) AS bound
                           FROM meta WHERE key LIKE 'last_export_max:%') b USING (room)
WHERE c.gap_end > b.bound;

-- (B) what coverage_gaps recorded, per regime, vs. what is really missing
--     old regime (detected_at < 1788421510 = 2026-09-03 16:45:10 JST): polling
--     skips recorded every cycle, later mostly filled by exports -> over-records.
--     new regime: only export-confirmed permanent losses.
--     Rows above the boundary are excluded so the numbers stay comparable to
--     (A).lost_permanent. Islands split on adjacency as well as separation, so
--     merged_intervals counts overlap-free pieces, not contiguous regions;
--     the sums are unaffected. Verified 2026-09-04: no overlapping and no
--     adjacent pairs exist, i.e. the merge is a no-op on this data.
WITH b AS (
  SELECT substr(key, 17) AS room, CAST(value AS INTEGER) AS bound
  FROM meta WHERE key LIKE 'last_export_max:%'
),
g AS (
  SELECT c.room, gap_start, gap_end,
         CASE WHEN detected_at < 1788421510 THEN 'old' ELSE 'new' END AS regime
  FROM coverage_gaps c JOIN b USING (room)
  WHERE gap_end <= b.bound
),
ord AS (
  SELECT *, MAX(gap_end) OVER (PARTITION BY room, regime ORDER BY gap_start
                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
  FROM g
),
isl AS (
  SELECT *, SUM(CASE WHEN prev_max IS NULL OR gap_start > prev_max THEN 1 ELSE 0 END)
              OVER (PARTITION BY room, regime ORDER BY gap_start) AS island
  FROM ord
),
merged AS (
  SELECT room, regime, island, MIN(gap_start) AS s, MAX(gap_end) AS e
  FROM isl GROUP BY room, regime, island
)
SELECT room, regime,
       COUNT(*)       AS merged_intervals,
       SUM(e - s + 1) AS recorded_seqs_merged,
       SUM(e - s + 1) - SUM((SELECT COUNT(*) FROM messages m
                             WHERE m.room = merged.room AND m.seq BETWEEN s AND e))
                      AS still_missing_in_recorded
FROM merged GROUP BY room, regime ORDER BY room, regime DESC;

-- (B2) losses never recorded in coverage_gaps — computed from a single,
--      regime-agnostic union so it is correct even if old/new rows overlapped
WITH b AS (
  SELECT substr(key, 17) AS room, CAST(value AS INTEGER) AS bound
  FROM meta WHERE key LIKE 'last_export_max:%'
),
ord AS (
  SELECT c.room, gap_start, gap_end,
         MAX(gap_end) OVER (PARTITION BY c.room ORDER BY gap_start
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
  FROM coverage_gaps c JOIN b USING (room)
  WHERE gap_end <= b.bound
),
isl AS (
  SELECT *, SUM(CASE WHEN prev_max IS NULL OR gap_start > prev_max THEN 1 ELSE 0 END)
              OVER (PARTITION BY room ORDER BY gap_start) AS island
  FROM ord
),
merged AS (SELECT room, island, MIN(gap_start) AS s, MAX(gap_end) AS e FROM isl GROUP BY room, island),
rec AS (
  SELECT room, SUM(e - s + 1) - SUM((SELECT COUNT(*) FROM messages m
                                     WHERE m.room = merged.room AND m.seq BETWEEN s AND e))
               AS missing_in_any_record
  FROM merged GROUP BY room
),
holes AS (
  SELECT room, LAG(seq) OVER (PARTITION BY room ORDER BY seq) + 1 AS h_start, seq - 1 AS h_end
  FROM messages
),
lost AS (
  SELECT holes.room, SUM(MAX(0, MIN(h_end, b.bound) - h_start + 1)) AS lost
  FROM holes JOIN b USING (room) WHERE h_start IS NOT NULL AND h_end >= h_start
  GROUP BY holes.room
)
SELECT lost.room,
       lost.lost                                        AS lost_permanent,
       COALESCE(rec.missing_in_any_record, 0)           AS lost_covered_by_any_gap_record,
       lost.lost - COALESCE(rec.missing_in_any_record, 0) AS lost_never_recorded
FROM lost LEFT JOIN rec USING (room) ORDER BY lost.room;

-- (C) permanently lost ranges (for publication footnotes)
WITH b AS (
  SELECT substr(key, 17) AS room, CAST(value AS INTEGER) AS bound
  FROM meta WHERE key LIKE 'last_export_max:%'
),
holes AS (
  SELECT room, LAG(seq) OVER (PARTITION BY room ORDER BY seq) + 1 AS h_start, seq - 1 AS h_end
  FROM messages
)
SELECT holes.room, h_start AS lost_start, MIN(h_end, bound) AS lost_end,
       MIN(h_end, bound) - h_start + 1 AS n
FROM holes JOIN b USING (room)
WHERE h_start IS NOT NULL AND h_end >= h_start AND h_start <= bound
ORDER BY holes.room, h_start;
