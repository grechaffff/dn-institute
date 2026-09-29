"""End-to-end tests: CLI, output files, exit codes, idempotent loading."""

import csv
import json
import sqlite3
from pathlib import Path

from pipeline import EXIT_BAD_INPUT, EXIT_DEAD_LETTER, EXIT_IO, EXIT_OK, main, run_pipeline

SAMPLE = Path(__file__).resolve().parent.parent / "sample_feed.csv"


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_cli_writes_all_outputs(tmp_path):
    assert main([str(SAMPLE), "--out", str(tmp_path)]) == EXIT_OK

    clean = read_csv(tmp_path / "clean_trades.csv")
    dead = read_csv(tmp_path / "dead_letter.csv")
    dups = read_csv(tmp_path / "duplicates.csv")
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))

    assert [r["event_id"] for r in clean] == ["evt_001", "evt_002", "evt_004", "evt_006", "evt_008"]
    assert clean[-1]["dq_flags"] == "INGESTION_TIME_REGRESSION;INGESTED_BEFORE_BLOCK_TIME"
    assert [(r["event_id"], r["reasons"], r["retryable"]) for r in dead] == [
        ("evt_005", "MISSING_BLOCK_TIME", "true")
    ]
    assert [(r["event_id"], r["duplicate_of"]) for r in dups] == [
        ("evt_003", "evt_002"),
        ("evt_007", "evt_006"),
    ]
    assert report["rows_in"] == len(clean) + len(dead) + len(dups) == 8


def test_original_values_are_preserved_for_lineage(tmp_path):
    main([str(SAMPLE), "--out", str(tmp_path)])
    dead = read_csv(tmp_path / "dead_letter.csv")[0]
    assert dead["block_time"] == "null"
    assert dead["wallet"] == "0xE5…"  # raw casing kept; normalisation is internal


def test_clean_output_is_canonical_but_keeps_source_values(tmp_path):
    # a feed where side and amount need normalising
    feed = tmp_path / "feed.csv"
    feed.write_text(
        "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
        "e1,0xAB,2026-01-01T00:00:00Z,0xWW, buy ,120000.0,2026-01-01T00:00:01Z\n",
        encoding="utf-8",
    )
    assert main([str(feed), "--out", str(tmp_path / "out")]) == EXIT_OK
    clean = read_csv(tmp_path / "out" / "clean_trades.csv")[0]
    assert clean["side"] == "BUY" and clean["amount"] == "120000"  # canonical for analytics
    assert clean["wallet"] == "0xww"  # hex lowercased
    assert clean["src_side"] == " buy " and clean["src_amount"] == "120000.0"  # lineage


def test_report_json_includes_sqlite_stats(tmp_path):
    db = tmp_path / "trades.sqlite"
    assert main([str(SAMPLE), "--out", str(tmp_path / "out"), "--sqlite", str(db)]) == EXIT_OK
    report = json.loads((tmp_path / "out" / "report.json").read_text(encoding="utf-8"))
    assert report["sqlite_rows_inserted"] == 5  # present on disk, not only in the console


def test_sqlite_conflict_is_reported_not_silently_overwritten(tmp_path):
    db = tmp_path / "trades.sqlite"
    first = tmp_path / "a.csv"
    first.write_text(
        "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
        "e1,0xAB,2026-01-01T00:00:00Z,0xWW,BUY,10,2026-01-01T00:00:01Z\n",
        encoding="utf-8",
    )
    run_pipeline(first, sqlite_path=db)

    # same trade key, but a different block_time (e.g. a reorg)
    second = tmp_path / "b.csv"
    second.write_text(
        "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
        "e2,0xAB,2026-01-01T06:00:00Z,0xWW,BUY,10,2026-01-01T06:00:01Z\n",
        encoding="utf-8",
    )
    run = run_pipeline(second, sqlite_path=db)
    assert run.report["sqlite_rows_inserted"] == 0
    assert len(run.report["sqlite_conflicts"]) == 1

    with sqlite3.connect(str(db)) as conn:
        stored = conn.execute("SELECT block_time FROM trades").fetchall()
    assert stored == [("2026-01-01T00:00:00Z",)]  # original value kept, not overwritten


def test_output_write_failure_uses_its_own_exit_code(tmp_path):
    # point --out at a path blocked by an existing file so mkdir fails
    blocker = tmp_path / "blocked"
    blocker.write_text("i am a file, not a directory", encoding="utf-8")
    assert main([str(SAMPLE), "--out", str(blocker / "sub")]) == EXIT_IO


def test_fail_on_dead_letter_exit_code(tmp_path):
    assert main([str(SAMPLE), "--out", str(tmp_path), "--fail-on-dead-letter"]) == EXIT_DEAD_LETTER


def test_missing_column_is_rejected(tmp_path, capsys):
    bad = tmp_path / "bad.csv"
    bad.write_text("event_id,tx_hash,wallet,side,amount,ingested_at\n", encoding="utf-8")
    assert main([str(bad), "--out", str(tmp_path / "out")]) == EXIT_BAD_INPUT
    assert "block_time" in capsys.readouterr().err


def test_missing_file_is_rejected(tmp_path):
    assert main([str(tmp_path / "nope.csv"), "--out", str(tmp_path)]) == EXIT_BAD_INPUT


def test_sqlite_sink_is_idempotent_across_batches(tmp_path):
    db = tmp_path / "trades.sqlite"
    args = [str(SAMPLE), "--out", str(tmp_path / "out"), "--sqlite", str(db)]
    assert main(args) == EXIT_OK
    assert main(args) == EXIT_OK  # the whole batch replayed

    replay = tmp_path / "replay.csv"  # evt_002 re-emitted later under a new id
    replay.write_text(
        "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
        "evt_099,0xAA2,09:41:20,0xD4…,BUY,120000.0,11:00:00\n",
        encoding="utf-8",
    )
    assert main([str(replay), "--out", str(tmp_path / "out2"), "--sqlite", str(db)]) == EXIT_OK

    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute("SELECT event_id FROM trades ORDER BY block_time").fetchall()
    assert [r[0] for r in rows] == ["evt_001", "evt_002", "evt_004", "evt_006", "evt_008"]
