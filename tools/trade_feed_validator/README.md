# Trade Feed Validator

A small, reusable validator that sits between a trade event feed (blockchain indexer or exchange) and the analytics table. Every incoming event is routed to exactly one place:

| Destination | Meaning | File |
|---|---|---|
| accepted | safe for analytics, may carry warning flags | `output/clean_trades.csv` |
| duplicate | copy of an event already accepted, dropped | `output/duplicates.csv` |
| dead letter | cannot be trusted as-is, kept with reasons for repair or replay | `output/dead_letter.csv` |

Nothing is dropped silently. Every decision is recorded with a reason code in `output/report.json`, and every output row keeps the original raw values for lineage.

The validator and pipeline use only the Python standard library. `pytest` is needed for the tests only.

## Quick start

Tested with Python 3.12.

```bash
cd tools/trade_feed_validator
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# run the pipeline on the sample feed
python pipeline.py sample_feed.csv --out output

# run the tests
python -m pytest -v
```

Output of the pipeline run:

```
rows in: 8  accepted: 5  duplicates: 2  dead-letter: 1
  [duplicate] evt_003  DUPLICATE_TRADE: same trade as evt_002 (tx 0xaa2, same wallet, side and amount), new event_id; re-delivered 158s after the original (replay)
  [error    ] evt_005  MISSING_BLOCK_TIME: block_time is missing and could not be recovered; retryable via a node or indexer lookup of tx 0xaa4
  [duplicate] evt_007  DUPLICATE_TRADE: same trade as evt_006 (tx 0xaa5, same wallet, side and amount), new event_id; delivered twice with the same ingestion time (double emit)
  [warning  ] evt_008  INGESTION_TIME_REGRESSION: ingested_at is 205s earlier than evt_006, which arrived before it; ingestion clock is unreliable
  [warning  ] evt_008  INGESTED_BEFORE_BLOCK_TIME: ingested_at is 610s earlier than block_time, which is impossible; one of the two clocks is wrong
total volume: naive 705000 -> validated 465000
trade count:  naive 8 -> validated 5
outputs written to output/
```

## Data-quality issues found in the sample

| # | Issue | Affected rows | Reason code | Handling |
|---|---|---|---|---|
| 1 | Replayed event: the same trade delivered again 2 min 38 s later under a new `event_id` | evt_003 (copy of evt_002) | `DUPLICATE_TRADE` | dropped, logged in `duplicates.csv` |
| 2 | Double-emitted event: the same trade delivered twice in the same second under a new `event_id` | evt_007 (copy of evt_006) | `DUPLICATE_TRADE` | dropped, logged in `duplicates.csv` |
| 3 | Missing `block_time` | evt_005 | `MISSING_BLOCK_TIME` | dead letter, marked retryable (see below) |
| 4 | Impossible timestamps: `ingested_at` is 10 min 10 s *before* `block_time`, and 3 min 25 s earlier than evt_006, which arrived before it | evt_008 | `INGESTED_BEFORE_BLOCK_TIME`, `INGESTION_TIME_REGRESSION` | accepted with flags; can be quarantined with `--quarantine-clock-skew` |
| 5 | Pipeline defects: no validation, no idempotency, rows stored in arrival order, `event_id` treated as identity | whole feed | n/a | fixed by this tool |

Issues 1 and 2 have the same effect but different signatures, which point to different causes. evt_003 arrived minutes later, which looks like a replay (for example, a consumer re-reading after a restart or an indexer re-processing a range). evt_007 arrived in the same second as evt_006, which looks like a double emit at the producer. Both are caught by the same rule, but their messages differ so the upstream owner can tell them apart.

Why `event_id` cannot be used for deduplication: all eight `event_id` values are unique, yet two events are copies. In this feed `event_id` identifies a *delivery*, not a *trade*. The validator therefore identifies a trade by `(tx_hash, wallet, side, amount)` after normalisation (lowercased hex identifiers, `120000` = `120000.0`). `block_time` is deliberately not part of the key: it belongs to the transaction, so two copies that disagree on it are a conflict to investigate, not two trades.

Which clock is wrong in evt_008: it arrived after evt_006 and evt_007, whose `ingested_at` is 10:03:15, so it cannot have been ingested at 09:59:50. Its `block_time` (10:10:00) is consistent with that arrival order. The evidence points at the ingestion timestamp, so the trade is kept at its chain time and flagged. If the block time were the wrong one, the trade would be placed about 10 minutes off; consumers who cannot accept that risk can quarantine such rows with `--quarantine-clock-skew`, and the same resolver used for evt_005 can confirm the block time from the chain.

The arrival order in this sample happens to match chain order for all rows that have a `block_time`, so the out-of-order check (`OUT_OF_ORDER_ARRIVAL`) does not fire here. The pipeline still sorts accepted trades by `block_time` rather than relying on arrival order, and the check is covered by tests.

### Schema gaps worth raising with the feed owner

These are not row-level errors, but they limit what can be computed or guaranteed:

- **No price field.** VWAP, which the brief lists as a target metric, cannot be computed from this feed at all. Only volume and wallet activity can.
- **No `log_index`.** One transaction can legitimately contain several trades. Without a per-trade index inside the transaction, deduplication has to fall back to comparing content. Two genuinely identical fills in the same transaction (same wallet, side and amount) would be merged. With `log_index`, the key becomes `(tx_hash, log_index)` and this ambiguity disappears.
- **No token pair and no unit for `amount`.** It is not clear whether `amount` is in base token units, quote units or USD, or whether it is already scaled by token decimals. Summing amounts across pairs would be meaningless.

## What each issue corrupts downstream

| Issue | What breaks |
|---|---|
| Duplicates (evt_003, evt_007) | Volume is overstated. Trade counts and wallet activity are inflated. Wallet net positions are wrong: 0xF6… looks like it accumulated 90,000, while it actually bought 90,000 and sold the same amount about 7 minutes later. VWAP would give duplicated trades double weight. Wallet clustering and behaviour models see activity that did not happen. |
| Missing `block_time` (evt_005) | The trade drops out of every time-window query (`WHERE block_time BETWEEN ...`) but stays in totals without a time filter, so hourly and daily figures stop reconciling. Databases sort nulls differently, so "first/last trade" logic becomes engine-dependent. 0xE5…'s sell volume is understated by 30,000. |
| Bad `ingested_at` (evt_008) | An incremental load that uses `ingested_at` as a watermark (`WHERE ingested_at > last_run`) and has already advanced to 10:03:15 will never pick this row up, silently losing a 90,000 SELL. Ingestion lag and freshness metrics become negative or meaningless. |
| Arrival-order storage | Anything that assumes table order equals time order (running totals, first-seen wallet, sequence features) is wrong whenever events arrive late. |

Measured on the sample (from `output/report.json`):

| Metric | Original pipeline | Validated |
|---|---|---|
| Trades | 8 | 5, plus 1 in the dead-letter queue |
| Total volume | 705,000 | 465,000, plus 30,000 pending |
| Buy volume | 540,000 | 330,000 |
| Sell volume | 165,000 | 135,000, plus 30,000 pending |
| 0xD4… trades | 3 | 2 |
| 0xF6… net position | +90,000 | 0 |

Total volume in the original pipeline is overstated by about half compared with the validated figure.

## How evt_005 is handled

**Decision: dead-letter queue, after an automatic backfill attempt.**

evt_005 is a real trade: it has a transaction hash, wallet, side and amount. Only its time is missing. The block time of a transaction is not lost information; it can be recovered from the chain using `tx_hash`. So the pipeline:

1. copies `block_time` from another event of the same transaction if one exists (no external call needed);
2. otherwise calls an optional `resolver(tx_hash)`, for example a node or indexer lookup;
3. if both fail, sends the row to `dead_letter.csv` with `retryable=true`, the raw values unchanged, so it can be replayed once the time is known.

With no resolver configured, as in the sample run, evt_005 ends up in the dead-letter queue. The test `test_evt005_is_backfilled_when_a_resolver_knows_the_tx` shows the same row being accepted when a resolver returns a time.

Why not the alternatives:

- **Drop:** loses a real 30,000 SELL and hides the fact that anything was lost.
- **Backfill with `ingested_at`:** invents a chain timestamp from our own clock. evt_008 shows that clock is not reliable, and even when it is, it places the trade in the window in which we happened to receive it.

What would change the answer:

- `tx_hash` also missing or malformed: the time cannot be recovered, so the row stays in the dead-letter queue as non-retryable for manual review, and is dropped with an audit record after the retention period.
- The transaction is not found on chain (for example, dropped or removed by a reorg): the trade did not happen, so it should be dropped.
- A resolver is available in production: backfill inline and use the dead-letter queue only for lookup failures.
- Metrics must be published in real time: publish the affected windows marked as incomplete and restate them once the row is backfilled.
- Dead-letter volume becomes large (a systematic indexer fault): stop publishing metrics instead of publishing partial ones.

## General practice (150 words or fewer)

Treat the feed as a contract that is enforced at ingestion, not discovered in dashboards.

1. **Validation on every event:** types, nulls, allowed values, ranges and timestamp sanity, versioned in code and run in CI against fixture feeds like this sample.
2. **Idempotent writes:** a natural trade key (`tx_hash` + `log_index`) with insert-or-ignore at the sink, so at-least-once delivery cannot double-count.
3. **Dead-letter queue** with reason codes, automatic retry for recoverable errors, and an alert on the dead-letter rate.
4. **Reconciliation:** scheduled comparison of per-block trade counts and volume against an independent source, such as a node or a second indexer.
5. **Monitoring:** freshness, ingestion lag distribution, duplicate rate and clock skew, with thresholds that alert the pipeline owner.

Every rejected or modified row keeps its raw form and reason, so any metric can be traced back to its inputs.

## Design

`validator.py` runs the stages in this order:

1. **Parse and schema checks** per row: required fields, `side` in {BUY, SELL} (case-insensitive), `amount` a finite number above zero, parseable timestamps, surplus fields that indicate shifted columns.
2. **`event_id` uniqueness:** the same id with the same content is a duplicate; the same id with different content sends both rows to the dead-letter queue (`EVENT_ID_CONFLICT`).
3. **Block time consistency:** all events of one transaction must share one `block_time`. Disagreement, for example after a reorg, sends the whole transaction to the dead-letter queue (`BLOCK_TIME_CONFLICT`).
4. **Backfill** of missing `block_time` (see evt_005 above).
5. **Trade deduplication** by trade identity, keeping the first copy in arrival order.
6. **Timeliness:** ingestion clock going backwards, ingestion before the block, late arrival beyond `--max-lag`, out-of-order arrival.

Accepted trades are returned sorted by `block_time`. `pipeline.py` reads the CSV, checks the header against the required columns, runs the validator and writes the outputs.

### Beyond the sample

Checks that the sample does not trigger but real feeds will:

- Hex identifiers (`0x...`) are lowercased before comparison, because EVM addresses are case-insensitive. Other formats, such as base58, are case-sensitive and kept as-is.
- `120000`, `120000.0` and `1.2E+5` are treated as the same amount.
- Timestamps are accepted as ISO-8601 (with `Z`, an offset, or naive, taken as UTC), Unix seconds or milliseconds, or time-only with `--feed-date`.
- `--strict-ids` enforces full-length EVM addresses and transaction hashes. It is off by default because the sample uses shortened identifiers.
- `--sqlite trades.sqlite` loads accepted trades into a table whose primary key is the trade identity, so re-running a batch or receiving a replay in a later batch does not add rows. `test_sqlite_sink_is_idempotent_across_batches` covers this.
- `--fail-on-dead-letter` exits with code 1 when anything was dead-lettered, so an orchestrator or CI job can stop downstream steps. A missing required column exits with code 2.

### Options

```
python pipeline.py INPUT.csv [--out DIR] [--feed-date YYYY-MM-DD]
                   [--clock-skew-tolerance SECONDS]   # default 30
                   [--max-lag SECONDS]                # default 120
                   [--quarantine-clock-skew] [--strict-ids]
                   [--sqlite FILE] [--fail-on-dead-letter]
```

The default tolerances are starting points. In production they should be set from the observed distribution of ingestion lag for each source.

### Limitations

- The validator works on one batch in memory. In a streaming setup, deduplication across batches needs either a keyed state store with a TTL or a unique key at the sink, as the SQLite example shows.
- Without `log_index`, two genuinely identical fills in one transaction would be merged (see Schema gaps).
- Time-only timestamps are assumed to fall on one UTC day, as the sample notes state.

### Observed but out of scope

After deduplication, wallet 0xF6… shows a buy and a sell of the same size about 7 minutes apart. That is a market-behaviour pattern, not a data-quality defect, so the validator does not flag it. Clean input is what makes such patterns visible: with the duplicate left in, the same wallet looks like a net buyer.

## Files

```
tools/trade_feed_validator/
├── README.md
├── sample_feed.csv          # the challenge sample, arrival order
├── validator.py             # rules and routing
├── pipeline.py              # CLI: read, validate, write, optional SQLite sink
├── metrics.py               # naive vs validated volume summary
├── requirements.txt         # pytest (tests only)
├── pytest.ini
└── tests/
    ├── test_sample_feed.py  # one test per issue in the sample
    ├── test_rules.py        # edge cases for each rule
    └── test_pipeline.py     # CLI, outputs, exit codes, idempotent loading
```
