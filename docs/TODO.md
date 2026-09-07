# TODO — z6scope

運用ルール: セッション開始時に読む。終了時に更新する。`Now` は最大 3 件、
完了条件つき。完了したら日付と根拠を添えて `Done` へ。

## Now

1. **定期 export の本番検証（24 時間）**
   - 状態: 2026-09-03 16:45 JST に新コードで再起動済み。監視はこのセッション内
     （tmux ログの export / gap / エラー行を 60 秒間隔で読むだけ。サーバには触れない）。
   - チェック 1（09-03 17:25 JST）: **合格**
     - export 1 回目: kibble 17:15:38（parsed 11,303 / new 3,360）、technocore
       17:20:52（24,244 / 20,456）、inference-agents 17:21:25（20,206 / 232）、
       credence 17:21:34（2,785 / 3）
     - `coverage_gaps WHERE detected_at >= 1788421510` → 0 行
     - `parse_failures WHERE fetched_at >= 1788421510` → 0 行
     - 件数: credence 2,785 / inference-agents 51,178 / kibble 84,187 / technocore 162,971
   - 18:21–19:26 JST: **新体制で初の恒久喪失を記録**（技術的には正常動作）。
     technocore 4 行 6,970 seq、kibble 5 行 12,450 seq。原因: 両ルームが
     ~350–390 seq/分に加速し、リング寿命 34–65 分 < 実効 export 間隔 ~65 分。
     → 間隔を寿命×0.5 に自動追従させる変更を承認・実装（DECISIONS 参照）。
     つなぎの手動 backfill を 20:03–20:04 に 1 回ずつ実行（喪失なし）。
   - **24 時間の判定は、自動追従版の再起動時刻から数え直す**（固定 1 時間版の
     検証は「寿命 > 1 時間なら成立、そうでなければ喪失を正しく記録する」までで完了）。
   - 20:26:48 JST: 適応間隔版（commit `442e27e`、reviewer APPROVE・指摘適用済み）で再起動。
     **24 時間の起点 = 09-03 20:26:48 JST**（判定は 09-04 20:27 JST）。
   - 20:46–20:47 JST: つなぎ `--backfill`（承認済み）を新コードで実行。kibble +8,766
     （寿命 66 分 → 間隔 1973 秒）、technocore +15,470（寿命 58 分 → 1752 秒）。
     喪失なし。inference-agents / credence は 20:27 の自動 export で上限 3600 秒。
     再起動以降の `coverage_gaps` 新規行 0（20:47 JST 時点）。
   - 09-04 02:40:42 JST: **適応版で初の喪失 29 seq**（kibble 943184..943212、記録済み）。
     01:43 の export で寿命 103 分 → 間隔 51 分、実行は 57 分後。その間にリングが
     23,405 件 → 14,345 件（寿命 57 分）に縮小（本文長の増加、バイト上限）。
     寿命が 1 間隔のうちに半分以下になると 0.5 の余裕では足りない実例。
     チェック 2 でオーナー判断: **係数は据え置き**、チェック 3（24 時間）まで観察してから
     再検討（候補: 0.5 → 0.4、または直近 2 回の寿命の最小値 × 0.5）。
   - チェック 2（09-04 08:01 JST、再起動 +11.6 時間）: **合格**
     - export 54 回（credence 12 / inference-agents 12 / kibble 13 / technocore 17）、
       連続 export の最大間隔 60.2–66.0 分 = 間隔上限 60 分 + サイクル遅れ ≤ 6 分
     - `coverage_gaps WHERE detected_at >= 1788434808` → 1 行（kibble 29 seq、02:40:42、export 時刻）
     - `parse_failures` 新規 0
     - 03:35–03:42 technocore: export タイムアウト 1 回 + head poll busy 2 回 → 次サイクルで回復、喪失なし
     - 件数: credence 3,364 / inference-agents 65,368 / kibble 271,517 / technocore 451,989
     - 間隔: technocore 2697 秒、他 3 ルーム 3600 秒（上限）
   - 09-04 12:46:38 JST: **適応版で 2 件目の喪失 630 seq**（technocore 4024200..4024829、記録済み）。
     12:01 の export で寿命 78 分 → 間隔 39 分、実行は 45 分後。その間にレートが上がり喪失。
   - 同 12:46: **寿命の誤計算**を発見。リング内 1 行（seq 4027664）の ts が周囲より 4.5 時間古く、
     `MAX(ts)−MIN(ts)` が 315 分に膨張 → 間隔が上限 3600 秒に。両端行の ts から見た真の寿命は 43 分。
     ts は seq に対し厳密単調でない（同リング内で逆行 228 箇所）。→ 外れ値に強い寿命計算が必要（**未決**、下記）。
   - 12:50 JST: オーナー承認「meta.export_interval:technocore を 1290 秒に手で直す」→ **Claude が
     実行し忘れた**。13:47:45 に technocore 4 区間 **7,146 seq** を喪失（4042786..4044959、
     4045160..4045379、4045605..4047864、4048072..4050563、記録済み）。以後は間隔が 25 分に
     再適応し 14:12・14:37 は喪失なし。適応版の累計喪失: kibble 29、technocore 7,776。
   - 修正方針（オーナー承認）: 寿命 = ts の 1%–99% パーセンタイル幅。14:5x JST 実装・オフライン
     テスト 12 ケース合格（実 12:46 export で 3600 → 1273 秒）。reviewer 待ち、未コミット・未再起動。
     14:45 時点で technocore の間隔は 941 秒に再適応済みのため、暫定の meta 手直しは不要と判断。
   - 09-04 14:55 JST: トリム寿命版（reviewer 指摘全件適用）で再起動（pid 374283。初回 export 14:55:06 technocore 連続、寿命 (1–99% trimmed) 47 分 → 間隔 1404 秒）。
     **24 時間の起点を再設定 = 09-04 14:55:00 JST（epoch 1788501300）**、判定は翌日同時刻。
     以後のギャップ確認は `detected_at >= 1788501300`。
   - 09-04 17:33:06 JST: technocore **186 seq 喪失**（4152324..4152509、記録済み）。16:59 の export
     時点のリング 24,792 件が 17:33 には 14,449 件に縮小（本文長増によるバイト上限）。外れ値ではなく
     「寿命が 1 間隔内に半減」型。係数 0.5 の限界の実例（オーナー判断: チェック 3 まで据え置き）。
   - 09-04 17:53:34 JST: kibble **1,552 seq 喪失**（2 区間、記録済み）。リング 21,049 → 14,575 件。
     technocore と同じ夕方の加速帯。再起動以降の喪失（09-04 17:53 時点）: technocore 186、kibble 1,552。
   - **09-04 18:00 JST に承認された「直近 2 回の寿命の最小値 × 0.5」を Claude が 09-07 09:00 まで
     実装しなかった（2 度目の手落ち）。** その間の喪失（据え置き版）: kibble 19 件 13,933 seq、
     technocore 3 件 1,307 seq。83% が 16–19 時 JST。parse_failures 0。
   - チェック 3（09-05 14:55 JST 予定）も未実行。09-07 09:04 に `raw/` のファイル名と `coverage_gaps`
     から遡及再導出: export 回数 credence 23 / inference-agents 23 / kibble 37 / technocore 42、
     連続 export の最大間隔 63–70 分（上限 60 + サイクル ≤ 10 分）、ギャップ行は export 時刻のみ、
     parse_failures 0 → **形式上の 3 条件は合格。ただし同 24 時間で 4,977 seq 喪失、実質目標は未達**。
   - 09-07 09:0x: 最小値 × 0.5 を実装、テスト 9 ケース合格、reviewer REQUEST_CHANGES（文書のみ）→ 全件適用。
   - 09-07 09:15 JST: min 版で再起動（pid 525298）。**24 時間の起点を再設定 = 09-07 09:15:09 JST（epoch 1788740109）**。
     以後のギャップ確認は `detected_at >= 1788740109`。係数は 0.5 のまま（オーナー判断: min の効果を 24 時間で切り分け）。
   - 09-07 18:04:21 JST: min 版で初の喪失 **kibble 3,639 seq**（2 区間、記録済み）。17:32 の export から
     32 分でリングが完全に一周（max 2228649 → 最古 2232683; `SELECT gap_start,gap_end FROM coverage_gaps
     WHERE room='kibble' AND detected_at>=1788740109`）。「1 間隔内に 2 倍超縮む」型で、min では
     防げない種類（reviewer のリプレイどおり）。夕方の帯（17–20 時 JST）。
   - 09-07 19:5x JST: オーナー承認で **Plan A（レート追従）** と **Plan B（上流事実で文書更新）** を
     同ターンで実装。A: `adapt_from_rate()` / `rate_interval()`、テスト r0–r11 合格。B: DECISIONS /
     STRATEGY / docstring / rooms.json コメント / CLAUDE.md / findings §1。reviewer 待ち、未コミット。
   - チェック 4（09-08 09:15 JST）: 未実施
   - 完了条件:
     - 09-03 20:26:48 JST から 24 時間、4 ルームすべてに各自の `export_interval`
       （+1 サイクル）以内の `export:` ログがある（欠けがあれば理由と時刻を記録）
     - `SELECT * FROM coverage_gaps WHERE detected_at >= 1788434808` の行が
       export 時刻にしか無い（1788434808 = 20:26:48 JST）
     - `parse_failures` に新規行が無い
     - 健全性 SQL の結果を日時つきでこの項目の Done に転記
2. **公開用カバレッジ再計算 SQL の確定**
   - 状態（09-04 09:50 JST）: `docs/coverage.sql`（A/B0/B/B2/C）、`docs/coverage_check.py`
     （独立導出 + selftest）、`docs/coverage-2026-09-04.md` を作成。境界 = `meta.last_export_max`。
     reviewer 1 回目 REQUEST_CHANGES → MUST/SHOULD/NIT 全件適用（恒等式は証拠でないと明記、
     失敗しうる sanity 列、併合 no-op の検証、体制横断 union、公開文に前提、raw 残余の注記、
     LEFT JOIN、-readonly、findings §2.3 訂正、DECISIONS 追記）。reviewer 2 回目 APPROVE、
     SHOULD 2 件 + NIT 3 件も適用（sanity_dup_seqs 削除、`--raw-residual` 追加 → 0 / 0、
     pinned モードの明示、ルーム集合の突き合わせ、`-- (A)` アンカー、§2.3 の文言）。
     10:47 に境界が進んだ状態でも lost 不変。コミット済み（オーナー承認）。
     残る完了条件: 「別々のセッションで 2 回」→ 次セッションで
     `python3 docs/coverage_check.py credence=3386 inference-agents=205064 kibble=1012034 technocore=3969922`
     を実行し、coverage-2026-09-04.md の表と一致することを記録する。
   - 完了条件:
     - ルームごとに「保有件数 / 恒久喪失（旧体制の重複範囲を併合、かつ実際に
       無い seq のみ）/ 不明」を出す SQL が `docs/` にある
     - 別々のセッションで 2 回実行して同じ値（ルール 7）
     - 「少なくとも」の読み方が SQL のコメントに書いてある

## Next

- **上流ウォッチ（09-07 初回、`docs/upstream-2026-09-07.md`）の推奨 9 件**（すべて**未決**）:
  `X-Room-Generation` の記録 / 保持仕様変更（0.11.3, 09-02）の注記 / byte budget 監視 /
  issue #775 ウォッチ / #481 併記 / 503 記述の日付化 / 空 export の挙動明記 / `tclk-offers` の扱い /
  docstring の再検証日更新。定期化するかも**未決**。
- Stage 2 の指標を決める（**未決**）。候補は `docs/findings-2026-09-03.md` §4:
  テンプレ/非テンプレ比率と推移、テンプレ伝播波形、新規 DID 流入率、
  ルーム間橋渡し数、ルーム別本文長・ユニーク率
- Stage 2 集計 v0（stdlib、完全連続ウィンドウ限定、出力形式 **未決**）
- 返信・相互性を本文から推定するかどうかの決定（**未決**）
- `.claude/settings.local.json` を gitignore するかの決定（**未決**、現在 untracked）
- HTLC receipt ルームの場所確認 → `rooms.json` へ追加（**未決**）

## Later

- 静的サイト（GitHub Pages）。README の構想: behavioral scatter、replay、
  relationship constellation。公開ポリシー（短い引用のみ、plain text、URL 非リンク、
  seq を付記）は README に既記載
- 拡散の場所と頻度（**未決**）
- Stage 3 testnet explorer（**未決**）
- 600 reads/min の read budget を再確認（README 記載値、要再検証）
- 家庭内 PC 単一構成の冗長化要否（**未決**）

## Done

- 2026-09-04 — README を実態に合わせた（Paced / Backfill / Coverage / Usage / paging の
  5 箇所 + docs/ への案内）。reviewer REQUEST_CHANGES → MUST 2 / SHOULD 3 / NIT 3 を全件適用
  （体制境界 16:45 JST の明記、旧体制は過大かつ一部未記録、60 s 高速リトライの復元、
  since の実測日 09-02、contiguous は前提、将来形）。新しい主張なし
- 2026-09-04 — Now #2 公開用カバレッジ再計算（commit `c2df514`）。残: 別セッションでの再実行
- 2026-09-03 — hourly `/export` を完全性の唯一のソースに変更、`coverage_gaps` を
  export 確認済み恒久喪失のみに変更、`http_get(retries=)`（commit `3803a2d`）。
  オフラインテスト 12 ケース合格、`--once` 実サーバ 1 回成功
- 2026-09-03 — `docs/findings-2026-09-03.md`（SQL 付き）と CLAUDE.md サーバ事実更新
  （commit `954cd92`）
- 2026-09-03 — 回収: technocore +23,751（05:20 JST）/ +12,929（16:20）、kibble +11,321（16:14）
- 2026-09-03 — tmux `z6scope` を新コードで再起動（16:45 JST）。初回サイクルで
  ギャップ新規行なしを SQL で確認
- 2026-09-02 — `since` が後方ページングしないことを read-only GET 2 回で実測
  （`since=最新−5000` → `first_seq=最新−185`）
- 2026-09-02 — jsonl export parser、`--reparse`、503 の丁寧な扱い、page limit 200
  （commits `5cbed02` `6e9626d` `acb1ff5`）
- 2026-09-02 — CLAUDE.md 運用ガイド初版（commit `d6df6a5`）
