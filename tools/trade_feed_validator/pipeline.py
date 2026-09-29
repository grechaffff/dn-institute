"""Trade feed pipeline: read -> validate -> route -> write.

Replaces the original

    for event in feed_stream:
        row = parse(event)
        insert_into(analytics_table, row)

with a pipeline in which every row is validated, duplicates are removed,
untrusted rows go to a dead-letter file, and the analytics sink is idempotent.

Usage:
    python pipeline.py sample_feed.csv --out output
    python pipeline.py --help
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Sequence

from metrics import volume_summary
from validator import (
    REQUIRED_COLUMNS,
    RETRYABLE_CODES,
    BlockTimeResolver,
    Severity,
    TradeEvent,
    ValidationResult,
    ValidatorConfig,
    canonical_amount,
    format_timestamp,
    validate_feed,
)

EXIT_OK = 0
EXIT_DEAD_LETTER = 1
EXIT_BAD_INPUT = 2


class SchemaError(Exception):
    """The input file does not match the expected feed contract."""


@dataclass
class PipelineRun:
    result: ValidationResult
    columns: List[str]
    report: dict


def read_feed(path: Path) -> tuple:
    """Return (columns, rows). Rows keep arrival order."""
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        columns = list(reader.fieldnames or [])
        missing = [c for c in REQUIRED_COLUMNS if c not in columns]
        if missing:
            raise SchemaError(f"{path}: missing required column(s): {', '.join(missing)}")
        return columns, list(reader)


def _write_csv(path: Path, columns: Sequence[str], rows: List[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _is_retryable(event: TradeEvent) -> bool:
    errors = {i.code for i in event.issues if i.severity is Severity.ERROR}
    return bool(errors) and errors <= RETRYABLE_CODES


def write_outputs(run: PipelineRun, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    result, columns = run.result, run.columns

    _write_csv(
        out_dir / "clean_trades.csv",
        columns + ["dq_flags"],
        [{**e.raw, "dq_flags": ";".join(e.flags)} for e in result.accepted],
    )
    _write_csv(
        out_dir / "dead_letter.csv",
        columns + ["reasons", "retryable", "details"],
        [
            {
                **e.raw,
                "reasons": ";".join(i.code.value for i in e.issues if i.severity is Severity.ERROR),
                "retryable": str(_is_retryable(e)).lower(),
                "details": " | ".join(i.message for i in e.issues if i.severity is Severity.ERROR),
            }
            for e in result.dead_letter
        ],
    )
    _write_csv(
        out_dir / "duplicates.csv",
        columns + ["duplicate_of", "reason"],
        [
            {
                **e.raw,
                "duplicate_of": e.duplicate_of or "",
                "reason": " | ".join(i.message for i in e.issues if i.severity is Severity.DUPLICATE),
            }
            for e in result.duplicates
        ],
    )
    with (out_dir / "report.json").open("w", encoding="utf-8") as fh:
        json.dump(run.report, fh, indent=2, ensure_ascii=False)


def build_report(result: ValidationResult) -> dict:
    return {
        "rows_in": len(result.events),
        "accepted": len(result.accepted),
        "duplicates": len(result.duplicates),
        "dead_letter": len(result.dead_letter),
        "issues": [i.to_dict() for i in result.issues],
        "metrics": {
            "naive": volume_summary(result.events),
            "validated": volume_summary(result.accepted),
        },
    }


def load_sqlite(result: ValidationResult, db_path: Path) -> int:
    """Idempotently load accepted trades into SQLite; return rows inserted.

    The primary key is the trade identity (not event_id), so replays that
    arrive in a later batch are ignored instead of double-counted. With a
    log_index in the feed the key would be (tx_hash, log_index).
    """
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                tx_hash     TEXT NOT NULL,
                wallet      TEXT NOT NULL,
                side        TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
                amount      TEXT NOT NULL,
                block_time  TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                ingested_at TEXT,
                dq_flags    TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (tx_hash, wallet, side, amount)
            )
            """
        )
        before = conn.total_changes
        conn.executemany(
            "INSERT OR IGNORE INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    e.tx_hash,
                    e.wallet,
                    e.side,
                    canonical_amount(e.amount),
                    format_timestamp(e.block_time),
                    e.event_id,
                    format_timestamp(e.ingested_at) if e.ingested_at else None,
                    ";".join(e.flags),
                )
                for e in result.accepted
            ],
        )
        conn.commit()
        return conn.total_changes - before
    finally:
        conn.close()


def run_pipeline(
    input_path: Path,
    out_dir: Optional[Path] = None,
    config: Optional[ValidatorConfig] = None,
    resolver: Optional[BlockTimeResolver] = None,
    sqlite_path: Optional[Path] = None,
) -> PipelineRun:
    columns, rows = read_feed(input_path)
    result = validate_feed(rows, config, resolver)
    run = PipelineRun(result=result, columns=columns, report=build_report(result))
    if out_dir is not None:
        write_outputs(run, out_dir)
    if sqlite_path is not None:
        run.report["sqlite_rows_inserted"] = load_sqlite(result, sqlite_path)
    return run


def print_summary(run: PipelineRun, out: Optional[Path]) -> None:
    r = run.report
    print(
        f"rows in: {r['rows_in']}  accepted: {r['accepted']}  "
        f"duplicates: {r['duplicates']}  dead-letter: {r['dead_letter']}"
    )
    for issue in run.result.issues:
        print(f"  [{issue.severity.value:9}] {issue.event_id or 'row ' + str(issue.row):8} "
              f"{issue.code.value}: {issue.message}")
    naive, valid = r["metrics"]["naive"], r["metrics"]["validated"]
    print(f"total volume: naive {naive['total_volume']} -> validated {valid['total_volume']}")
    print(f"trade count:  naive {naive['trade_count']} -> validated {valid['trade_count']}")
    if out is not None:
        print(f"outputs written to {out}/")
    if "sqlite_rows_inserted" in r:
        print(f"sqlite rows inserted: {r['sqlite_rows_inserted']}")


def parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate a trade event feed before analytics.")
    p.add_argument("input", type=Path, help="CSV feed in arrival order")
    p.add_argument("--out", type=Path, default=Path("output"), help="output directory (default: output)")
    p.add_argument("--feed-date", type=date.fromisoformat, default=date(1970, 1, 1),
                   help="date for time-only timestamps, YYYY-MM-DD")
    p.add_argument("--clock-skew-tolerance", type=float, default=30.0,
                   help="seconds of allowed clock disagreement (default: 30)")
    p.add_argument("--max-lag", type=float, default=120.0,
                   help="seconds after the block before an event is flagged late (default: 120)")
    p.add_argument("--quarantine-clock-skew", action="store_true",
                   help="dead-letter events ingested before their block instead of flagging them")
    p.add_argument("--strict-ids", action="store_true",
                   help="require full EVM-format wallets and tx hashes")
    p.add_argument("--sqlite", type=Path, help="also load accepted trades into this SQLite file (idempotent)")
    p.add_argument("--fail-on-dead-letter", action="store_true",
                   help="exit with code 1 if any event is dead-lettered (for CI / orchestration)")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config = ValidatorConfig(
        feed_date=args.feed_date,
        clock_skew_tolerance=timedelta(seconds=args.clock_skew_tolerance),
        max_ingestion_lag=timedelta(seconds=args.max_lag),
        quarantine_clock_skew=args.quarantine_clock_skew,
        strict_identifiers=args.strict_ids,
    )
    try:
        run = run_pipeline(args.input, args.out, config, sqlite_path=args.sqlite)
    except (SchemaError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BAD_INPUT
    print_summary(run, args.out)
    if args.fail_on_dead_letter and run.result.dead_letter:
        return EXIT_DEAD_LETTER
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
