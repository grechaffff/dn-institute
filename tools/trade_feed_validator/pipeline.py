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
EXIT_IO = 3  # writing outputs or loading the SQLite sink failed

# Canonical (normalised) columns emitted for analytics. Every original input
# column is also emitted alongside these under a "src_" prefix (built per feed),
# so the accepted file keeps the full raw record for lineage.
CANONICAL_COLUMNS = [
    "event_id", "tx_hash", "block_time", "wallet", "side", "amount", "ingested_at", "dq_flags",
]


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


def _clean_row(e: TradeEvent, raw_cols: Sequence[str]) -> dict:
    """Analytics row: canonical (normalised) fields, plus every original source
    value under ``src_*`` so the full raw record is preserved for lineage."""
    canonical = {
        "event_id": e.event_id,
        "tx_hash": e.tx_hash,
        "block_time": format_timestamp(e.block_time),
        "wallet": e.wallet,
        "side": e.side,
        "amount": canonical_amount(e.amount),
        "ingested_at": format_timestamp(e.ingested_at) if e.ingested_at else "",
        "dq_flags": ";".join(e.flags),
    }
    canonical.update({f"src_{c}": e.raw.get(c, "") for c in raw_cols})
    return canonical


def write_outputs(run: PipelineRun, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    result, columns = run.result, run.columns

    accepted_raw_cols = list(columns)
    if any("_overflow" in e.raw for e in result.accepted):
        accepted_raw_cols.append("_overflow")
    _write_csv(
        out_dir / "clean_trades.csv",
        CANONICAL_COLUMNS + [f"src_{c}" for c in accepted_raw_cols],
        [_clean_row(e, accepted_raw_cols) for e in result.accepted],
    )
    _write_csv(
        out_dir / "dead_letter.csv",
        columns + ["reasons", "retryable", "details", "_overflow"],
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


def load_sqlite(result: ValidationResult, db_path: Path) -> tuple:
    """Idempotently load accepted trades into SQLite.

    Returns ``(rows_inserted, conflicts)``. The primary key is the trade
    identity (not event_id), so an exact replay arriving in a later batch is
    ignored instead of double-counted. A row whose key already exists but whose
    ``block_time`` disagrees is **not** overwritten and is reported as a
    conflict, so a reorg or indexer bug cannot silently replace analytics data.
    With a log_index in the feed the key would be (tx_hash, log_index).
    """
    conn = sqlite3.connect(str(db_path))
    inserted = 0
    conflicts: List[dict] = []
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
        for e in result.accepted:
            key = (e.tx_hash, e.wallet, e.side, canonical_amount(e.amount))
            block_time = format_timestamp(e.block_time)
            existing = conn.execute(
                "SELECT block_time, event_id FROM trades "
                "WHERE tx_hash = ? AND wallet = ? AND side = ? AND amount = ?",
                key,
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        *key,
                        block_time,
                        e.event_id,
                        format_timestamp(e.ingested_at) if e.ingested_at else None,
                        ";".join(e.flags),
                    ),
                )
                inserted += 1
            elif existing[0] != block_time:
                conflicts.append(
                    {
                        "trade": list(key),
                        "stored_block_time": existing[0],
                        "incoming_block_time": block_time,
                        "stored_event_id": existing[1],
                        "incoming_event_id": e.event_id,
                    }
                )
            # else: same key, same block_time -> idempotent replay, ignored.
        conn.commit()
        return inserted, conflicts
    finally:
        conn.close()


def _prepare(
    input_path: Path,
    config: Optional[ValidatorConfig] = None,
    resolver: Optional[BlockTimeResolver] = None,
) -> PipelineRun:
    """Read + validate. Raises SchemaError/OSError on bad input."""
    columns, rows = read_feed(input_path)
    result = validate_feed(rows, config, resolver)
    return PipelineRun(result=result, columns=columns, report=build_report(result))


def _emit(run: PipelineRun, out_dir: Optional[Path], sqlite_path: Optional[Path]) -> None:
    """Load the sink and write outputs. Raises sqlite3.Error/OSError on failure.

    The sink runs first so the SQLite statistics are present in every
    representation of the report, including the ``report.json`` on disk.
    """
    if sqlite_path is not None:
        inserted, conflicts = load_sqlite(run.result, sqlite_path)
        run.report["sqlite_rows_inserted"] = inserted
        if conflicts:
            run.report["sqlite_conflicts"] = conflicts
    if out_dir is not None:
        write_outputs(run, out_dir)


def run_pipeline(
    input_path: Path,
    out_dir: Optional[Path] = None,
    config: Optional[ValidatorConfig] = None,
    resolver: Optional[BlockTimeResolver] = None,
    sqlite_path: Optional[Path] = None,
) -> PipelineRun:
    run = _prepare(input_path, config, resolver)
    _emit(run, out_dir, sqlite_path)
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
    if r.get("sqlite_conflicts"):
        print(f"sqlite conflicts (same trade, different block_time): {len(r['sqlite_conflicts'])}")


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
        run = _prepare(args.input, config)
    except (SchemaError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BAD_INPUT
    try:
        _emit(run, args.out, args.sqlite)
    except (sqlite3.Error, OSError) as exc:
        print(f"error writing outputs: {exc}", file=sys.stderr)
        return EXIT_IO
    print_summary(run, args.out)
    if args.fail_on_dead_letter and run.result.dead_letter:
        return EXIT_DEAD_LETTER
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
