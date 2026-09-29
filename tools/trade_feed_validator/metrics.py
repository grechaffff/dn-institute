"""Volume metrics used to show the effect of validation.

``naive`` = what the original pipeline (parse + insert every row) would
report; ``validated`` = what the analytics table contains after this tool.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Iterable

from validator import TradeEvent, canonical_amount


def volume_summary(events: Iterable[TradeEvent]) -> Dict:
    total = Decimal(0)
    by_side = {"BUY": Decimal(0), "SELL": Decimal(0)}
    wallets: Dict[str, Dict] = {}
    count = 0
    for e in events:
        if e.amount is None or e.side is None or e.wallet is None:
            continue  # the naive pipeline would fail or insert garbage here
        count += 1
        total += e.amount
        by_side[e.side] += e.amount
        w = wallets.setdefault(e.wallet, {"trades": 0, "BUY": Decimal(0), "SELL": Decimal(0)})
        w["trades"] += 1
        w[e.side] += e.amount

    return {
        "trade_count": count,
        "total_volume": canonical_amount(total),
        "buy_volume": canonical_amount(by_side["BUY"]),
        "sell_volume": canonical_amount(by_side["SELL"]),
        "wallets": {
            wallet: {
                "trades": w["trades"],
                "buy_volume": canonical_amount(w["BUY"]),
                "sell_volume": canonical_amount(w["SELL"]),
                "net_position": canonical_amount(w["BUY"] - w["SELL"]),
            }
            for wallet, w in sorted(wallets.items())
        },
    }
