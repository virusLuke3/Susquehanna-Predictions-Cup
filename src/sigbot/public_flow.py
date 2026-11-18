"""Verified public trade windows: market activity, never an own-fill model."""

from decimal import Decimal

from sigbot.domain import ZERO, utc_time
from sigbot.errors import SchemaMismatch


def trade_window(result, market, exchange, start, end, received):
    complete = bool(
        result["pagination_complete"]
        and result["coverage"]
        and all(c["complete"] for c in result["coverage"])
        and result["projection_checkpoint_present"]
    )
    seconds = Decimal(str((end - start).total_seconds()))
    if seconds <= 0:
        raise SchemaMismatch("PUBLIC_FLOW_WINDOW_INVALID")
    rows, keys = result["trades"], set()
    for row in rows:
        key = row["id"], row["createdAt"]
        if key in keys or not start <= utc_time(row["createdAt"]) < end:
            raise SchemaMismatch("PUBLIC_FLOW_DUPLICATE_OR_OUTSIDE_WINDOW")
        keys.add(key)
    shares = sum((Decimal(str(r["size"])) for r in rows), ZERO)
    priced = [r for r in rows if r["price"] is not None]
    priced_shares = sum((Decimal(str(r["size"])) for r in priced), ZERO)
    return {
        "market_id": market,
        "exchange_id": exchange,
        "status": "VERIFIED_PUBLIC_WINDOW" if complete else "PARTIAL_PUBLIC_WINDOW",
        "from": start.isoformat(),
        "to": end.isoformat(),
        "received_at": received.isoformat(),
        "window_seconds": str(seconds),
        "trade_count": len(rows),
        "shares": str(shares),
        "public_events_per_second": str(Decimal(len(rows)) / seconds) if complete else None,
        "public_shares_per_second": str(shares / seconds) if complete else None,
        "yes_price_vwap": str(
            sum((Decimal(str(r["price"])) * Decimal(str(r["size"])) for r in priced), ZERO)
            / priced_shares
        )
        if priced_shares
        else None,
        "coverage": result["coverage"],
        "aggressor_direction": "UNKNOWN",
        "queue_position": "UNKNOWN",
        "own_fill_probability": None,
        "a_per_second": None,
        "k_inverse_price": None,
        "trade_events_are_market_orders": "NOT_VERIFIED",
        "own_fill_events_used": False,
        "synthetic_data_used": str(market).startswith("SIM-"),
    }
