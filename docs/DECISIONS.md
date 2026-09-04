# DECISIONS — z6scope

決めたことだけを書く。未決は `docs/TODO.md` にある。
形式: 日付 — 決定 — 理由 — 根拠（commit / 会話）。

## 2026-09-04

- **公開用カバレッジの境界 = `meta.last_export_max:{room}`、根拠は `messages` の実在 seq**
  — `coverage_gaps` は旧体制で過大、かつ一部の喪失を記録していない（technocore 799、
  kibble 44 seq）ため、公開数値の根拠にしない。境界以下で欠けている seq を恒久喪失、
  境界より上の欠けを未確定（上限）とする。前提は「export は連続したリングを返し、
  seq は連番」（09-02/03 実測、公開文にも前提を明記する）。再現可能列は境界以下に
  限定した `present_at_boundary` / `span_at_boundary`。
  — `docs/coverage.sql`、`docs/coverage_check.py`、`docs/coverage-2026-09-04.md`。
  オーナー承認（Now #2 の Plan）、reviewer の指摘を全件適用。
- **`coverage_gaps` の旧体制記録は重複していない**（cursor 単調 + PK）。findings-2026-09-03
  §2.3 の「重複している」を訂正。併合クエリは no-op と検証済み。

## 2026-09-03（夕方）

- **export 間隔はルームごとに「リング寿命 × 0.5」に自動追従、範囲 [600, 3600] 秒**
  — 17–19 時 JST に technocore / kibble が ~350–390 seq/分に加速し、リング寿命が
  34–65 分まで縮んだ。固定 1 時間（実効 ~65 分）では 2 時間で ~19k seq を喪失
  （新体制で正しく `coverage_gaps` に記録された: kibble 12,450、technocore 6,970）。
  寿命は export 結果の `max(ts) − min(ts)` を SQLite の `julianday` で計算
  （Python 3.10 の `fromisoformat` は `…Z` を受け付けない）。
  `meta.export_interval:{room}` に保存。低レートのルームは上限 3600 のまま。
  — オーナー選択（4 択のうち「自動追従」）。
- **つなぎとして手動 `--backfill` を technocore / kibble に 1 回ずつ**（20:03–20:04 JST、
  +13,320 / +13,972、喪失なし）— オーナー承認。

## 2026-09-03

- **1 時間ごとの `/export` を完全性の唯一のソースにする**（`EXPORT_INTERVAL_SEC = 3600`）
  — `since` が後方ページングしないため、ポーリングは head しか追えない。リング寿命
  （1–3 時間）に対し 1 時間は安全側で、負荷はルームあたり毎時 1 リクエスト。
  — commit `3803a2d`、オーナー選択「定期 export + 通常ポーリング」。
- **`coverage_gaps` は export で確認された恒久喪失のみ記録する**
  — ポーリングが飛ばした範囲は 1 時間以内に export が埋めるので、記録すると
  ノイズになり「ギャップ = 本当に失われた」の意味が壊れる。
  — 実装は `record_losses_below()`: リングの最古 seq より前で手元に無い範囲を走査。
  走査済み上限は `meta.last_export_max:{room}`。
  — commit `3803a2d`、オーナー選択。
- **既存の `coverage_gaps` 行は変更しない**（旧体制の過大記録も残す）
  — ルール 5。公開時は実在 seq で再計算し、旧体制の重複範囲を併合する。
  — オーナー選択「今は触らない」。
- **体制の境界**: 2026-09-03 07:2x UTC 時点の最新 export の max seq を
  `meta.last_export_max:*` に設定（technocore 3622489 / kibble 800811 /
  inference-agents 189008 / credence 2744）
  — 新体制の初回走査が旧体制の記録と重複しないようにするため。
- **export は 1 回試行、失敗は次サイクル**（`http_get(retries=1)`）
  — 5 回 × 90 秒のリトライがループ全体を最大 ~8 分止めていた（kibble で実測 28 分）。
  礼儀 > 完全性。
- **`--backfill` は定期 export と同じ経路を使い、タイマーを進める**
  — 手動 export の直後に定期 export が重複して走るのを防ぐ。
- **ポーリングで head window が cursor より先にある場合は INFO ログのみ**
  （"head window starts at N … export fills"）— 上記ギャップ定義の帰結。
- **findings は SQL 付きで `docs/` にコミットする**（`docs/findings-2026-09-03.md`）
  — ルール 7（再導出可能）。個別 DID は書かない（ルール 6）。commit `954cd92`。
- **作業手順**: 状況説明 → 方針確認 → Plan → 承認 → 実装 → reviewer。
  一気に進めない。サーバへの追加リクエストや再起動・回収は都度確認。
  — オーナー指示（2026-09-02 夜、2026-09-03）。
- **デプロイ**: テスト結果とコミットを提示し、オーナー確認後に Claude が tmux で
  再起動（この回はそう決めた。恒久ルールではない）。
- **Stage 2 集計スクリプトは別プラン**。今回の範囲に含めない。
- **時系列の可視化はポーリングのみ時間帯のデータを使わない**
  — 収集器のカバレッジを描いてしまう（findings §2.1）。完全連続ウィンドウか
  2026-09-03 以降のデータに限定。

## 2026-09-02

- **読み取り専用・GET のみ**。書き込み、署名、鍵は一切扱わない。— CLAUDE.md 初版、README。
- **二層設計**: raw を先に必ずアーカイブ、パースは上に載せる。パース失敗で
  データは失われない。— README、`ingest()`。
- **Python stdlib のみ、単一ファイル**。— CLAUDE.md。
- **`data/` と `raw/` はコミットしない**。— `.gitignore`。
- **公開する数字は「少なくとも」**。ギャップ記録は削除しない。— CLAUDE.md ルール 5。
- **個別 DID をラベリングしない**。集計のみ。— CLAUDE.md ルール 6。
- **数字は公開前に独立に再導出する**。— CLAUDE.md ルール 7。
- **`rooms.json` を毎サイクル再読込**し、ルーム追加に再起動を不要にする。— `load_rooms()`。
- **503 には Retry-After と指数バックオフで退く**。— commit `6e9626d`。
- **export は JSONL としてパース**（"Extra data" が形式のシグネチャ）。— commit `5cbed02`。
