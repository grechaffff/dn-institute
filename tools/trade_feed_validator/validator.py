"""Trade feed validator.

Takes raw trade events in arrival order and routes every event to exactly one
of three destinations:

* ACCEPTED     - safe for analytics (possibly carrying warning flags);
* DUPLICATE    - a copy of an event that was already accepted, dropped;
* DEAD_LETTER  - cannot be trusted as-is, kept with reasons for replay/repair.

Nothing is dropped silently: every decision is recorded as an ``Issue``.

Stages (see ``validate_feed``):
    1. parse + per-row schema checks
    2. event_id uniqueness
    3. block_time consistency inside a transaction
    4. block_time backfill (from a sibling event of the same tx, or a resolver)
    5. trade-level deduplication
    6. timeliness checks (clock skew, ingestion regressions, lateness, ordering)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple

REQUIRED_COLUMNS = (
    "event_id",
    "tx_hash",
    "block_time",
    "wallet",
    "side",
    "amount",
    "ingested_at",
)
NULL_TOKENS = {"", "null", "none"}
VALID_SIDES = {"BUY", "SELL"}

EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
EVM_TX_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
TIME_ONLY_RE = re.compile(r"^\d{2}:\d{2}:\d{2}(\.\d{1,6})?$")
EPOCH_RE = re.compile(r"^\d{9,13}(\.\d+)?$")

# Resolver signature: tx_hash -> block time (UTC) or None if unknown.
BlockTimeResolver = Callable[[str], Optional[datetime]]


class Severity(str, Enum):
    ERROR = "error"  # event goes to the dead-letter queue
    DUPLICATE = "duplicate"  # event is dropped as a copy of another event
    WARNING = "warning"  # event is kept and flagged
    INFO = "info"  # event is kept; informational note


class Code(str, Enum):
    MALFORMED_ROW = "MALFORMED_ROW"
    MISSING_FIELD = "MISSING_FIELD"
    MISSING_BLOCK_TIME = "MISSING_BLOCK_TIME"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_SIDE = "INVALID_SIDE"
    INVALID_AMOUNT = "INVALID_AMOUNT"
    INVALID_IDENTIFIER = "INVALID_IDENTIFIER"
    DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
    EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
    BLOCK_TIME_CONFLICT = "BLOCK_TIME_CONFLICT"
    BLOCK_TIME_BACKFILLED = "BLOCK_TIME_BACKFILLED"
    DUPLICATE_TRADE = "DUPLICATE_TRADE"
    INGESTED_BEFORE_BLOCK_TIME = "INGESTED_BEFORE_BLOCK_TIME"
    INGESTION_TIME_REGRESSION = "INGESTION_TIME_REGRESSION"
    MISSING_INGESTED_AT = "MISSING_INGESTED_AT"
    LATE_ARRIVAL = "LATE_ARRIVAL"
    OUT_OF_ORDER_ARRIVAL = "OUT_OF_ORDER_ARRIVAL"


# Dead-letter reasons that can be fixed automatically by a later retry
# (e.g. once a node lookup succeeds). Everything else needs a human.
RETRYABLE_CODES = {Code.MISSING_BLOCK_TIME}


class Status(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class Issue:
    code: Code
    severity: Severity
    row: int
    event_id: Optional[str]
    message: str
    related: Tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "code": self.code.value,
            "severity": self.severity.value,
            "row": self.row,
            "event_id": self.event_id,
            "message": self.message,
            "related": list(self.related),
        }


@dataclass
class TradeEvent:
    row: int  # 1-based position in arrival order
    raw: Dict[str, str]  # original values, used for output and lineage
    event_id: Optional[str] = None
    tx_hash: Optional[str] = None
    wallet: Optional[str] = None
    side: Optional[str] = None
    amount: Optional[Decimal] = None
    block_time: Optional[datetime] = None
    ingested_at: Optional[datetime] = None
    status: Status = Status.ACCEPTED
    issues: List[Issue] = field(default_factory=list)
    duplicate_of: Optional[str] = None

    @property
    def label(self) -> str:
        return self.event_id or f"row {self.row}"

    @property
    def fingerprint(self) -> Tuple:
        """Identity of the underlying trade, independent of event_id.

        block_time is deliberately excluded: it is a property of the
        transaction, so two copies of one trade that disagree on it are a
        conflict to investigate, not two different trades.
        """
        return (self.tx_hash, self.wallet, self.side, self.amount)

    @property
    def flags(self) -> List[str]:
        return [
            i.code.value
            for i in self.issues
            if i.severity in (Severity.WARNING, Severity.INFO)
        ]

    def add_issue(
        self,
        code: Code,
        severity: Severity,
        message: str,
        related: Iterable[str] = (),
    ) -> Issue:
        issue = Issue(code, severity, self.row, self.event_id, message, tuple(related))
        self.issues.append(issue)
        if severity is Severity.ERROR:
            self.status = Status.DEAD_LETTER
        elif severity is Severity.DUPLICATE and self.status is Status.ACCEPTED:
            self.status = Status.DUPLICATE
        return issue


@dataclass(frozen=True)
class ValidatorConfig:
    # Date used for time-only values such as "09:14:02" (the sample feed).
    feed_date: date = date(1970, 1, 1)
    # Allowed disagreement between the chain clock and our ingestion clock.
    clock_skew_tolerance: timedelta = timedelta(seconds=30)
    # Events that arrive later than this after their block are flagged late.
    max_ingestion_lag: timedelta = timedelta(minutes=2)
    # If True, events ingested "before" their block go to the dead-letter
    # queue instead of being accepted with a warning.
    quarantine_clock_skew: bool = False
    # If True, wallets and tx hashes must be full EVM-format identifiers.
    strict_identifiers: bool = False


@dataclass
class ValidationResult:
    events: List[TradeEvent]  # arrival order, one entry per input row

    @property
    def accepted(self) -> List[TradeEvent]:
        """Accepted events in chain order (not arrival order)."""
        rows = [e for e in self.events if e.status is Status.ACCEPTED]
        return sorted(rows, key=lambda e: (e.block_time, e.tx_hash, e.event_id, e.row))

    @property
    def duplicates(self) -> List[TradeEvent]:
        return [e for e in self.events if e.status is Status.DUPLICATE]

    @property
    def dead_letter(self) -> List[TradeEvent]:
        return [e for e in self.events if e.status is Status.DEAD_LETTER]

    @property
    def issues(self) -> List[Issue]:
        return [i for e in self.events for i in e.issues]

    def get(self, event_id: str) -> TradeEvent:
        for e in self.events:
            if e.event_id == event_id:
                return e
        raise KeyError(event_id)


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------


def is_null(value: Optional[str]) -> bool:
    return value is None or value.strip().lower() in NULL_TOKENS


def normalize_identifier(value: str) -> str:
    """EVM hex identifiers are case-insensitive, so lowercase them.

    Other formats (e.g. base58 on Solana) are case-sensitive and are only
    stripped of surrounding whitespace.
    """
    value = value.strip()
    if value.lower().startswith("0x"):
        return value.lower()
    return value


def parse_timestamp(value: str, feed_date: date) -> datetime:
    """Parse a timestamp into an aware UTC datetime.

    Accepts ISO-8601 (``Z`` or an offset; naive values are taken as UTC),
    time-only ``HH:MM:SS`` (combined with ``feed_date``) and Unix epoch
    seconds or milliseconds. Raises ValueError on anything else.
    """
    v = value.strip()
    if TIME_ONLY_RE.match(v):
        return datetime.combine(feed_date, time.fromisoformat(v), tzinfo=timezone.utc)
    if EPOCH_RE.match(v):
        seconds = float(v)
        if seconds > 1e11:  # milliseconds
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_amount(amount: Decimal) -> str:
    """'120000', '120000.0' and '1.2E+5' all map to '120000'."""
    return format(amount.normalize(), "f")


def _seconds(delta: timedelta) -> str:
    return f"{abs(delta.total_seconds()):g}s"


def parse_row(row_num: int, raw: Mapping[Optional[str], object], config: ValidatorConfig) -> TradeEvent:
    """Parse one raw row and run the per-row (schema) checks."""
    extra_values = raw.get(None)  # csv.DictReader puts surplus fields here
    clean_raw = {k: ("" if v is None else str(v)) for k, v in raw.items() if k is not None}
    event = TradeEvent(row=row_num, raw=clean_raw)

    if not is_null(clean_raw.get("event_id")):
        event.event_id = clean_raw["event_id"].strip()

    if extra_values:
        event.add_issue(
            Code.MALFORMED_ROW,
            Severity.ERROR,
            f"row has {len(extra_values)} more field(s) than the header; columns may be shifted",
        )

    for name in ("event_id", "tx_hash", "wallet", "side", "amount"):
        if is_null(clean_raw.get(name)):
            event.add_issue(Code.MISSING_FIELD, Severity.ERROR, f"required field '{name}' is missing")

    # Identifiers
    for name in ("tx_hash", "wallet"):
        value = clean_raw.get(name)
        if is_null(value):
            continue
        normalized = normalize_identifier(value)
        setattr(event, name, normalized)
        if config.strict_identifiers:
            pattern = EVM_TX_HASH_RE if name == "tx_hash" else EVM_ADDRESS_RE
            if not pattern.match(normalized):
                event.add_issue(
                    Code.INVALID_IDENTIFIER,
                    Severity.ERROR,
                    f"{name} '{value}' is not a full EVM-format identifier",
                )

    # Side
    side = clean_raw.get("side")
    if not is_null(side):
        normalized_side = side.strip().upper()
        if normalized_side in VALID_SIDES:
            event.side = normalized_side
        else:
            event.add_issue(Code.INVALID_SIDE, Severity.ERROR, f"side '{side}' is not BUY or SELL")

    # Amount
    amount = clean_raw.get("amount")
    if not is_null(amount):
        try:
            value = Decimal(amount.strip())
        except InvalidOperation:
            event.add_issue(Code.INVALID_AMOUNT, Severity.ERROR, f"amount '{amount}' is not a number")
        else:
            if not value.is_finite() or value <= 0:
                event.add_issue(
                    Code.INVALID_AMOUNT,
                    Severity.ERROR,
                    f"amount '{amount}' must be a finite number greater than zero",
                )
            else:
                event.amount = value

    # block_time: a null value is handled later (it may be backfillable)
    block_time = clean_raw.get("block_time")
    if not is_null(block_time):
        try:
            event.block_time = parse_timestamp(block_time, config.feed_date)
        except ValueError:
            event.add_issue(
                Code.INVALID_TIMESTAMP, Severity.ERROR, f"block_time '{block_time}' cannot be parsed"
            )

    # ingested_at is our own metadata: without it the trade is still valid,
    # we just cannot run the timeliness checks on it.
    ingested_at = clean_raw.get("ingested_at")
    if is_null(ingested_at):
        event.add_issue(
            Code.MISSING_INGESTED_AT,
            Severity.WARNING,
            "ingested_at is missing; timeliness checks skipped for this event",
        )
    else:
        try:
            event.ingested_at = parse_timestamp(ingested_at, config.feed_date)
        except ValueError:
            event.add_issue(
                Code.MISSING_INGESTED_AT,
                Severity.WARNING,
                f"ingested_at '{ingested_at}' cannot be parsed; timeliness checks skipped",
            )

    return event


# --------------------------------------------------------------------------
# Feed-level stages
# --------------------------------------------------------------------------


def _payload(event: TradeEvent) -> Tuple:
    """Business content of an event, ignoring our own ingestion timestamp."""
    return (event.tx_hash, event.wallet, event.side, event.amount, event.block_time)


def check_event_ids(events: List[TradeEvent]) -> None:
    first_seen: Dict[str, TradeEvent] = {}
    for event in events:
        if event.event_id is None:
            continue
        original = first_seen.get(event.event_id)
        if original is None:
            first_seen[event.event_id] = event
            continue
        if _payload(original) == _payload(event):
            event.duplicate_of = original.event_id
            event.add_issue(
                Code.DUPLICATE_EVENT_ID,
                Severity.DUPLICATE,
                f"event_id re-delivered with identical content (first seen at row {original.row})",
                [original.label],
            )
        else:
            message = (
                f"event_id '{event.event_id}' is used by rows {original.row} and {event.row} "
                "with different content"
            )
            for e in (original, event):
                e.add_issue(Code.EVENT_ID_CONFLICT, Severity.ERROR, message, [original.label, event.label])


def check_block_time_consistency(events: List[TradeEvent]) -> None:
    """All events of one transaction come from one block and share its time."""
    by_tx: Dict[str, List[TradeEvent]] = {}
    for event in events:
        if event.tx_hash is not None and event.status is not Status.DUPLICATE:
            by_tx.setdefault(event.tx_hash, []).append(event)
    for tx_hash, group in by_tx.items():
        times = {e.block_time for e in group if e.block_time is not None}
        if len(times) > 1:
            labels = [e.label for e in group]
            message = (
                f"tx {tx_hash} has {len(times)} different block_time values; "
                "possible chain reorganisation or indexer bug"
            )
            for e in group:
                e.add_issue(Code.BLOCK_TIME_CONFLICT, Severity.ERROR, message, labels)


def backfill_block_times(
    events: List[TradeEvent], resolver: Optional[BlockTimeResolver] = None
) -> None:
    known: Dict[str, TradeEvent] = {
        e.tx_hash: e
        for e in events
        if e.status is Status.ACCEPTED and e.block_time is not None and e.tx_hash is not None
    }
    for event in events:
        # Only events whose block_time is genuinely null (not unparseable).
        if event.block_time is not None or not is_null(event.raw.get("block_time")):
            continue
        if event.status is Status.DUPLICATE:
            continue
        if event.status is Status.DEAD_LETTER or event.tx_hash is None:
            # Already dead-lettered for another reason: record this one too.
            event.add_issue(Code.MISSING_BLOCK_TIME, Severity.ERROR, "block_time is missing")
            continue

        sibling = known.get(event.tx_hash)
        if sibling is not None:
            event.block_time = sibling.block_time
            event.raw["block_time"] = sibling.raw.get("block_time", format_timestamp(sibling.block_time))
            event.add_issue(
                Code.BLOCK_TIME_BACKFILLED,
                Severity.INFO,
                f"block_time copied from {sibling.label} (same transaction)",
                [sibling.label],
            )
            continue

        resolved: Optional[datetime] = None
        failure = ""
        if resolver is not None:
            try:
                resolved = resolver(event.tx_hash)
            except Exception as exc:  # a lookup failure must not stop the batch
                failure = f"; resolver failed: {exc}"
        if resolved is not None:
            if resolved.tzinfo is None:
                resolved = resolved.replace(tzinfo=timezone.utc)
            event.block_time = resolved.astimezone(timezone.utc)
            event.raw["block_time"] = format_timestamp(event.block_time)
            event.add_issue(
                Code.BLOCK_TIME_BACKFILLED,
                Severity.INFO,
                f"block_time resolved from tx {event.tx_hash}",
            )
        else:
            event.add_issue(
                Code.MISSING_BLOCK_TIME,
                Severity.ERROR,
                "block_time is missing and could not be recovered; retryable via a "
                f"node or indexer lookup of tx {event.tx_hash}{failure}",
            )


def deduplicate_trades(events: List[TradeEvent]) -> None:
    """Drop repeated copies of the same trade that carry different event_ids."""
    first_seen: Dict[Tuple, TradeEvent] = {}
    for event in events:
        if event.status is not Status.ACCEPTED:
            continue
        original = first_seen.get(event.fingerprint)
        if original is None:
            first_seen[event.fingerprint] = event
            continue
        if original.ingested_at and event.ingested_at:
            delay = event.ingested_at - original.ingested_at
            if delay == timedelta(0):
                how = "delivered twice with the same ingestion time (double emit)"
            else:
                how = f"re-delivered {_seconds(delay)} after the original (replay)"
        else:
            how = "repeated delivery"
        event.duplicate_of = original.event_id
        event.add_issue(
            Code.DUPLICATE_TRADE,
            Severity.DUPLICATE,
            f"same trade as {original.label} (tx {event.tx_hash}, same wallet, side and amount), "
            f"new event_id; {how}",
            [original.label],
        )


def check_timeliness(events: List[TradeEvent], config: ValidatorConfig) -> None:
    tolerance = config.clock_skew_tolerance

    # 1) Our ingestion clock should never go backwards in arrival order.
    latest: Optional[TradeEvent] = None
    for event in events:
        if event.ingested_at is None:
            continue
        if latest is not None and event.ingested_at < latest.ingested_at - tolerance:
            if event.status is not Status.DUPLICATE:
                event.add_issue(
                    Code.INGESTION_TIME_REGRESSION,
                    Severity.WARNING,
                    f"ingested_at is {_seconds(latest.ingested_at - event.ingested_at)} earlier than "
                    f"{latest.label}, which arrived before it; ingestion clock is unreliable",
                    [latest.label],
                )
        if latest is None or event.ingested_at > latest.ingested_at:
            latest = event

    # 2) Per-event lag between the chain clock and our clock.
    newest_block: Optional[TradeEvent] = None
    for event in events:
        if event.status is not Status.ACCEPTED or event.block_time is None:
            continue
        if event.ingested_at is not None:
            lag = event.ingested_at - event.block_time
            if lag < -tolerance:
                event.add_issue(
                    Code.INGESTED_BEFORE_BLOCK_TIME,
                    Severity.ERROR if config.quarantine_clock_skew else Severity.WARNING,
                    f"ingested_at is {_seconds(lag)} earlier than block_time, which is impossible; "
                    "one of the two clocks is wrong",
                )
            elif lag > config.max_ingestion_lag:
                event.add_issue(
                    Code.LATE_ARRIVAL,
                    Severity.WARNING,
                    f"arrived {_seconds(lag)} after its block (limit {_seconds(config.max_ingestion_lag)}); "
                    "time windows that were already closed must be recomputed",
                )
        # 3) Arrival order vs chain order (only among events still accepted).
        if event.status is not Status.ACCEPTED:
            continue
        if newest_block is not None and event.block_time < newest_block.block_time:
            event.add_issue(
                Code.OUT_OF_ORDER_ARRIVAL,
                Severity.INFO,
                f"block_time is earlier than {newest_block.label}, which arrived before it",
                [newest_block.label],
            )
        if newest_block is None or event.block_time > newest_block.block_time:
            newest_block = event


def validate_feed(
    rows: Iterable[Mapping[Optional[str], object]],
    config: Optional[ValidatorConfig] = None,
    resolver: Optional[BlockTimeResolver] = None,
) -> ValidationResult:
    """Validate raw rows given in arrival order."""
    config = config or ValidatorConfig()
    events = [parse_row(i, row, config) for i, row in enumerate(rows, start=1)]
    check_event_ids(events)
    check_block_time_consistency(events)
    backfill_block_times(events, resolver)
    deduplicate_trades(events)
    check_timeliness(events, config)
    return ValidationResult(events)
