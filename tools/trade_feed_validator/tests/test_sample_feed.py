"""One test per data-quality issue found in the challenge sample feed."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline import run_pipeline
from validator import Code, Severity, Status, ValidatorConfig

SAMPLE = Path(__file__).resolve().parent.parent / "sample_feed.csv"


def codes(event):
    return [i.code for i in event.issues]


@pytest.fixture(scope="module")
def run():
    return run_pipeline(SAMPLE)


@pytest.fixture(scope="module")
def result(run):
    return run.result


def test_sample_has_eight_rows_in_arrival_order(result):
    assert [e.event_id for e in result.events] == [f"evt_00{i}" for i in range(1, 9)]


def test_every_row_is_routed_exactly_once(result):
    routed = result.accepted + result.duplicates + result.dead_letter
    assert sorted(e.row for e in routed) == list(range(1, 9))


# Issue 1: evt_003 is a replay of evt_002 with a new event_id
def test_evt003_replay_is_dropped_as_duplicate_of_evt002(result):
    evt = result.get("evt_003")
    assert evt.status is Status.DUPLICATE
    assert evt.duplicate_of == "evt_002"
    assert Code.DUPLICATE_TRADE in codes(evt)
    assert "158s" in evt.issues[0].message  # re-delivered 2m38s later
    assert result.get("evt_002").status is Status.ACCEPTED


# Issue 2: evt_007 is a double emit of evt_006 (same ingestion time)
def test_evt007_double_emit_is_dropped_as_duplicate_of_evt006(result):
    evt = result.get("evt_007")
    assert evt.status is Status.DUPLICATE
    assert evt.duplicate_of == "evt_006"
    assert "double emit" in evt.issues[0].message
    assert result.get("evt_006").status is Status.ACCEPTED


def test_event_id_alone_would_not_catch_the_duplicates(result):
    ids = [e.event_id for e in result.events]
    assert len(ids) == len(set(ids))  # all ids are unique ...
    assert len(result.duplicates) == 2  # ... yet two events are copies


# Issue 3: evt_005 has no block_time
def test_evt005_missing_block_time_goes_to_dead_letter_as_retryable(run, result):
    evt = result.get("evt_005")
    assert evt.status is Status.DEAD_LETTER
    assert codes(evt) == [Code.MISSING_BLOCK_TIME]
    assert "0xaa4" in evt.issues[0].message


def test_evt005_is_backfilled_when_a_resolver_knows_the_tx():
    def resolver(tx_hash):
        assert tx_hash == "0xaa4"
        return datetime(1970, 1, 1, 9, 58, 26, tzinfo=timezone.utc)

    result = run_pipeline(SAMPLE, resolver=resolver).result
    evt = result.get("evt_005")
    assert evt.status is Status.ACCEPTED
    assert Code.BLOCK_TIME_BACKFILLED in codes(evt)
    assert [e.event_id for e in result.accepted] == [
        "evt_001", "evt_002", "evt_004", "evt_005", "evt_006", "evt_008"
    ]


# Issue 4: evt_008 was "ingested" 610 s before its block was produced
def test_evt008_ingested_before_block_time_is_flagged_and_kept(result):
    evt = result.get("evt_008")
    assert evt.status is Status.ACCEPTED
    assert Code.INGESTED_BEFORE_BLOCK_TIME in evt.flags
    issue = next(i for i in evt.issues if i.code is Code.INGESTED_BEFORE_BLOCK_TIME)
    assert issue.severity is Severity.WARNING
    assert "610s" in issue.message


def test_evt008_ingestion_clock_went_backwards(result):
    evt = result.get("evt_008")
    assert Code.INGESTION_TIME_REGRESSION in evt.flags


def test_evt008_can_be_quarantined_by_policy():
    config = ValidatorConfig(quarantine_clock_skew=True)
    evt = run_pipeline(SAMPLE, config=config).result.get("evt_008")
    assert evt.status is Status.DEAD_LETTER


# Issue 5: the original pipeline trusts arrival order; output is chain order
def test_accepted_trades_are_exactly_the_clean_set_in_chain_order(result):
    accepted = result.accepted
    assert [e.event_id for e in accepted] == ["evt_001", "evt_002", "evt_004", "evt_006", "evt_008"]
    times = [e.block_time for e in accepted]
    assert times == sorted(times)


def test_rows_without_problems_carry_no_flags(result):
    for event_id in ("evt_001", "evt_002", "evt_004", "evt_006"):
        assert result.get(event_id).issues == []


# Downstream impact, quantified
def test_naive_pipeline_overstates_volume_and_activity(run):
    naive = run.report["metrics"]["naive"]
    valid = run.report["metrics"]["validated"]
    assert (naive["total_volume"], valid["total_volume"]) == ("705000", "465000")
    assert (naive["buy_volume"], valid["buy_volume"]) == ("540000", "330000")
    assert (naive["trade_count"], valid["trade_count"]) == (8, 5)
    # wallet 0xF6 looks like it accumulated 90k; in reality it round-tripped to 0
    assert naive["wallets"]["0xf6…"]["net_position"] == "90000"
    assert valid["wallets"]["0xf6…"]["net_position"] == "0"
    assert naive["wallets"]["0xd4…"]["trades"] == 3
    assert valid["wallets"]["0xd4…"]["trades"] == 2
