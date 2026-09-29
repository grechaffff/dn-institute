"""End-to-end tests: CLI, output files, exit codes, idempotent loading."""

import csv
import json
import sqlite3
from pathlib import Path

from pipeline import EXIT_BAD_INPUT, EXIT_DEAD_LETTER, EXIT_OK, main

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
