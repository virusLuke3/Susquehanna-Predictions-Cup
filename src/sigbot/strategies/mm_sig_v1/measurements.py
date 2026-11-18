"""All quotes stay in the sample, including no-fill and unresolved inventory."""

import json
from decimal import Decimal

from sqlalchemy import select

from sigbot.domain import ZERO, utc_time
from sigbot.storage import Audit, FillRow, IntentRow, Order, Position, StrategyEvent
from sigbot.strategies.mm_sig_v1.core import check_book
from sigbot.strategies.mm_sig_v1.models import Book, MmConfig


def reference(store, clock, account_id, instrument_id, book: Book, cfg: MmConfig):
    bid, ask = check_book(book, clock.now, cfg)
    with store.transaction() as s:
        store.lock_account(s, account_id)
        store.audit(
            s,
            account_id,
            "MM_REFERENCE",
            {
                "instrument_id": instrument_id,
                "sequence": book.sequence,
                "mid": str((bid + ask) / 2),
                "source_at": book.source_at.isoformat(),
            },
            clock.now,
        )


def summarize(store, clock, account_id):
    with store.transaction() as s:
        orders = list(
            s.scalars(
                select(Order)
                .join(IntentRow, Order.intent_id == IntentRow.id)
                .where(Order.account_id == account_id)
            )
        )
        owned = {
            o.id: o
            for o in orders
            if (
                s.get(IntentRow, o.intent_id).payload.get("strategy_id"),
                s.get(IntentRow, o.intent_id).payload.get("strategy_version"),
            )
            == ("mm_sig", "1.0")
        }
        fills = [
            f
            for f in s.scalars(select(FillRow).where(FillRow.account_id == account_id))
            if f.order_id in owned
        ]
        events = list(
            s.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account_id,
                    StrategyEvent.strategy_id == "mm_sig",
                    StrategyEvent.strategy_version == "1.0",
                )
            )
        )
        quote_records = {
            key: value for e in events for key, value in e.data.get("quotes", {}).items()
        }
        refs = list(
            s.scalars(
                select(Audit)
                .where(Audit.account_id == account_id, Audit.kind == "MM_REFERENCE")
                .order_by(Audit.at)
            )
        )
        reserved = {
            r.data["order_id"]: r.data
            for r in s.scalars(
                select(Audit).where(
                    Audit.account_id == account_id, Audit.kind == "MM_QUOTE_RESERVED"
                )
            )
        }
        buy_cost = sum(
            (
                Decimal(f.payload["quantity"]) * Decimal(f.payload["price"])
                + Decimal(f.payload["fee"])
                for f in fills
                if f.payload["side"] == "BUY"
            ),
            ZERO,
        )
        sell_net = sum(
            (
                Decimal(f.payload["quantity"]) * Decimal(f.payload["price"])
                - Decimal(f.payload["fee"])
                for f in fills
                if f.payload["side"] == "SELL"
            ),
            ZERO,
        )
        ids = {o.instrument_id for o in owned.values()}
        positions = [
            p
            for p in s.scalars(select(Position).where(Position.account_id == account_id))
            if p.instrument_id in ids
        ]
        remaining_cost = sum((p.cost or ZERO for p in positions), ZERO)
        samples = []
        for oid, order in owned.items():
            intent = s.get(IntentRow, order.intent_id)
            record = quote_records.get(oid, {})
            ack = utc_time(record["acknowledged_at"]) if record.get("acknowledged_at") else None
            terminal = utc_time(record["terminal_at"]) if record.get("terminal_at") else None
            own_fills = [f for f in fills if f.order_id == oid]
            first = min((utc_time(f.payload["filled_at"]) for f in own_fills), default=None)
            samples.append(
                {
                    "order_id": oid,
                    "event_key": reserved[oid]["event_key"],
                    "outcome": reserved[oid]["outcome"],
                    "quote_distance": reserved[oid]["quote_distance"],
                    "book_sequence": reserved[oid]["sequence"],
                    "side": order.side,
                    "quantity": str(order.quantity),
                    "price": str(order.price),
                    "filled": str(order.filled),
                    "uncompleted_quantity": str(order.quantity - order.filled),
                    "acknowledged_at": ack,
                    "terminal_at": terminal,
                    "observed_exposure_seconds": (
                        min(clock.now, terminal or clock.now) - ack
                    ).total_seconds()
                    if ack
                    else 0
                    if record.get("never_sent")
                    else None,
                    "first_fill_wait_seconds": (first - ack).total_seconds()
                    if first and ack
                    else None,
                    "censored": terminal is None,
                    "state": order.state,
                    "cancel_state": order.cancel_state,
                    "round_id": json.loads(intent.payload["note"])["round_id"],
                    "rules_hash": intent.payload["rules_hash"],
                    "mapping_hash": intent.payload["mapping_hash"],
                    "execution_classification": "SYNTHETIC_EXPLICIT_FILL"
                    if own_fills
                    else "NO_FILL",
                }
            )
        markouts = []
        for fill in fills:
            price = Decimal(fill.payload["price"])
            at = utc_time(fill.payload["filled_at"])
            for seconds in (60, 300, 1800):
                eligible = [
                    r
                    for r in refs
                    if r.data["instrument_id"] == fill.payload["instrument_id"]
                    and seconds <= (utc_time(r.at) - at).total_seconds() <= seconds + 5
                ]
                mid = Decimal(eligible[0].data["mid"]) if eligible else None
                markouts.append(
                    {
                        "fill_id": fill.id,
                        "horizon_seconds": seconds,
                        "status": "OBSERVED" if mid is not None else "NOT_OBSERVED",
                        "per_share": (mid - price) * (1 if fill.payload["side"] == "BUY" else -1)
                        if mid is not None
                        else None,
                    }
                )
        return {
            "quotes": samples,
            "quote_count": len(samples),
            "no_fill_quotes": sum(not o.filled for o in owned.values()),
            "censored_quotes": sum(q["censored"] for q in samples),
            "fill_count": len(fills),
            "realized_pnl": str(sell_net - buy_cost + remaining_cost),
            "remaining_inventory": {
                p.instrument_id: {"quantity": str(p.quantity), "cost": str(p.cost)}
                for p in positions
                if p.quantity
            },
            "peak_inventory": max(
                (e.data.get("peak_inventory", "0") for e in events), key=Decimal, default="0"
            ),
            "peak_inventory_cost": max(
                (e.data.get("peak_inventory_cost", "0") for e in events), key=Decimal, default="0"
            ),
            "markouts": markouts,
            "markouts_are_realized_pnl": False,
            "quote_activation_time": "LOCAL_ACK_OBSERVATION; REMOTE_ACTIVATION_UNKNOWN",
            "queue_position": "UNKNOWN",
            "profit_verified_on_sig": False,
        }
