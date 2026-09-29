"""Edge cases beyond the sample: each rule tested on small synthetic feeds."""

from datetime import date, datetime, timezone

import pytest

from validator import (
    Code,
    Status,
    ValidatorConfig,
    parse_timestamp,
    validate_feed,
)

WALLET = "0x" + "ab" * 20
TX = "0x" + "11" * 32


def row(**overrides):
    base = {
        "event_id": "e1",
        "tx_hash": TX,
        "block_time": "2026-09-28T10:00:00Z",
        "wallet": WALLET,
        "side": "BUY",
        "amount": "100",
        "ingested_at": "2026-09-28T10:00:03Z",
    }
    base.update(overrides)
    return base


def codes(event):
    return [i.code for i in event.issues]


def test_empty_feed():
    result = validate_feed([])
    assert result.events == [] and result.accepted == []


def test_clean_row_is_accepted_without_issues():
    result = validate_feed([row()])
    assert result.events[0].status is Status.ACCEPTED
    assert result.events[0].issues == []


# --- deduplication -------------------------------------------------------

def test_distinct_trades_in_one_transaction_are_not_merged():
    # multi-leg swaps emit several trades per tx; they must all survive
    result = validate_feed([
        row(event_id="e1"),
        row(event_id="e2", amount="250"),
        row(event_id="e3", wallet="0x" + "cd" * 20),
        row(event_id="e4", side="SELL"),
    ])
    assert len(result.accepted) == 4


def test_duplicate_detected_across_address_case():
    # EVM addresses are case-insensitive (EIP-55 checksum casing)
    result = validate_feed([
        row(event_id="e1", wallet=WALLET),
        row(event_id="e2", wallet=WALLET.upper().replace("0X", "0x")),
    ])
    assert result.events[1].status is Status.DUPLICATE


def test_duplicate_detected_across_amount_formatting():
    result = validate_feed([row(event_id="e1", amount="100"), row(event_id="e2", amount="100.00")])
    assert result.events[1].status is Status.DUPLICATE


def test_non_hex_identifiers_keep_their_case():
    # base58 (e.g. Solana) is case-sensitive: these are two different wallets
    result = validate_feed([
        row(event_id="e1", wallet="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"),
        row(event_id="e2", wallet="7xkxtg2cw87d97txjsdpbd5jbkhetqa83tzrujosgasu"),
    ])
    assert len(result.accepted) == 2


def test_same_event_id_same_payload_is_a_duplicate():
    result = validate_feed([row(), row(ingested_at="2026-09-28T10:05:00Z")])
    assert codes(result.events[1]) == [Code.DUPLICATE_EVENT_ID]
    assert result.events[1].status is Status.DUPLICATE


def test_same_event_id_different_payload_quarantines_both():
    result = validate_feed([row(amount="100"), row(amount="999")])
    assert all(e.status is Status.DEAD_LETTER for e in result.events)
    assert all(Code.EVENT_ID_CONFLICT in codes(e) for e in result.events)


# --- block_time ----------------------------------------------------------

def test_conflicting_block_times_in_one_tx_are_quarantined():
    # e.g. a reorg re-included the tx in a later block
    result = validate_feed([
        row(event_id="e1"),
        row(event_id="e2", block_time="2026-09-28T10:00:12Z", ingested_at="2026-09-28T10:00:15Z"),
    ])
    assert all(e.status is Status.DEAD_LETTER for e in result.events)
    assert all(Code.BLOCK_TIME_CONFLICT in codes(e) for e in result.events)


def test_missing_block_time_is_backfilled_from_a_sibling_in_the_same_tx():
    result = validate_feed([row(event_id="e1"), row(event_id="e2", amount="7", block_time="null")])
    evt = result.events[1]
    assert evt.status is Status.ACCEPTED
    assert evt.block_time == result.events[0].block_time
    assert Code.BLOCK_TIME_BACKFILLED in codes(evt)


def test_incomplete_copy_of_a_trade_is_backfilled_then_deduplicated():
    result = validate_feed([row(event_id="e1"), row(event_id="e2", block_time="")])
    assert result.events[1].status is Status.DUPLICATE


@pytest.mark.parametrize("resolver", [lambda tx: None, lambda tx: 1 / 0], ids=["returns_none", "raises"])
def test_unresolvable_block_time_stays_in_dead_letter(resolver):
    result = validate_feed([row(block_time="null")], resolver=resolver)
    evt = result.events[0]
    assert evt.status is Status.DEAD_LETTER
    assert codes(evt) == [Code.MISSING_BLOCK_TIME]


def test_unparseable_block_time_is_not_treated_as_missing():
    result = validate_feed([row(block_time="yesterday")], resolver=lambda tx: datetime.now(timezone.utc))
    evt = result.events[0]
    assert evt.status is Status.DEAD_LETTER
    assert codes(evt) == [Code.INVALID_TIMESTAMP]


# --- field validation ----------------------------------------------------

@pytest.mark.parametrize("amount", ["abc", "0", "-5", "NaN", "Infinity", "1e"])
def test_invalid_amounts_are_quarantined(amount):
    evt = validate_feed([row(amount=amount)]).events[0]
    assert evt.status is Status.DEAD_LETTER
    assert Code.INVALID_AMOUNT in codes(evt)


def test_side_is_normalised():
    evt = validate_feed([row(side=" buy ")]).events[0]
    assert evt.status is Status.ACCEPTED and evt.side == "BUY"


def test_unknown_side_is_quarantined():
    evt = validate_feed([row(side="HOLD")]).events[0]
    assert codes(evt) == [Code.INVALID_SIDE]


@pytest.mark.parametrize("field", ["event_id", "tx_hash", "wallet", "side", "amount"])
@pytest.mark.parametrize("value", ["", "null", "NULL", None], ids=["empty", "null", "NULL", "None"])
def test_missing_required_fields_are_quarantined(field, value):
    evt = validate_feed([row(**{field: value})]).events[0]
    assert evt.status is Status.DEAD_LETTER
    assert Code.MISSING_FIELD in codes(evt)


def test_missing_ingested_at_keeps_the_trade_with_a_warning():
    evt = validate_feed([row(ingested_at="null")]).events[0]
    assert evt.status is Status.ACCEPTED
    assert codes(evt) == [Code.MISSING_INGESTED_AT]


def test_row_with_extra_fields_is_quarantined():
    raw = row()
    raw[None] = ["unexpected"]  # how csv.DictReader reports surplus columns
    evt = validate_feed([raw]).events[0]
    assert Code.MALFORMED_ROW in codes(evt)
    assert evt.status is Status.DEAD_LETTER


def test_strict_identifiers():
    config = ValidatorConfig(strict_identifiers=True)
    good = validate_feed([row()], config).events[0]
    bad = validate_feed([row(wallet="0xD4…", tx_hash="0xaa1")], config).events[0]
    assert good.status is Status.ACCEPTED
    assert codes(bad).count(Code.INVALID_IDENTIFIER) == 2


# --- timeliness ----------------------------------------------------------

def test_small_clock_skew_within_tolerance_is_not_flagged():
    evt = validate_feed([row(ingested_at="2026-09-28T09:59:50Z")]).events[0]  # 10 s early
    assert evt.issues == []


def test_late_arrival_is_flagged():
    evt = validate_feed([row(ingested_at="2026-09-28T10:30:00Z")]).events[0]
    assert Code.LATE_ARRIVAL in evt.flags
    assert evt.status is Status.ACCEPTED


def test_out_of_order_arrival_is_noted_and_output_is_sorted():
    result = validate_feed([
        row(event_id="e1", tx_hash="0x" + "22" * 32,
            block_time="2026-09-28T10:05:00Z", ingested_at="2026-09-28T10:05:02Z"),
        row(event_id="e2", ingested_at="2026-09-28T10:05:03Z"),  # block 10:00:00, arrives later
    ])
    assert Code.OUT_OF_ORDER_ARRIVAL in result.events[1].flags
    assert [e.event_id for e in result.accepted] == ["e2", "e1"]


# --- timestamp parsing ---------------------------------------------------

@pytest.mark.parametrize("value", [
    "2026-09-28T10:00:00Z",
    "2026-09-28T13:00:00+03:00",
    "2026-09-28 10:00:00",
    "1790589600",
    "1790589600000",
])
def test_timestamp_formats(value):
    assert parse_timestamp(value, date(2026, 9, 28)) == datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc)


def test_time_only_uses_feed_date():
    assert parse_timestamp("09:14:02", date(2026, 9, 28)) == datetime(
        2026, 9, 28, 9, 14, 2, tzinfo=timezone.utc
    )
