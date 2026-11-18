"""Bounded v2 execution; the historical alpha and frozen plan stay unchanged.

Two ticks are an operator experiment (one uncertainty, one desired profit),
not a fitted edge. Ordinary limit orders may rest; no IOC/queue claims.
"""

from decimal import Decimal as D

from sigbot.domain import TERMINAL, ZERO, utc_time
from sigbot.errors import RiskRejected
from sigbot.strategies.grid_sig import floor_tick

VERSION = "BOUNDED_HYBRID_V2"
SUPPORTED_VERSIONS = {VERSION, "BOUNDED_HYBRID_V1"}
PASSIVE_SECONDS = 900
ACTIVE_SECONDS = 300


def entry_choice(book, plan, slot, tick):
    if not book:
        return {"action": "SKIP", "reason": "FRESH_BOOK_REQUIRED"}
    bids, asks = book
    bid, ask = bids[0][0], asks[0][0]
    level = D(plan["levels"][slot - 1])
    ceiling = min(level, floor_tick(D(plan["target"]) - 2 * tick, tick))
    # History defines a region, never a stale trade-price trigger. Do not
    # leave a distant low bid when the current market has already recovered.
    if ask < D(plan["stop"]) + tick or bid > ceiling or bid >= D(plan["mu"]):
        return {"action": "SKIP", "reason": "CURRENT_PRICE_OUTSIDE_ENTRY_REGION"}
    active = ask <= ceiling
    price = ceiling if active else min(bid, ceiling)
    if price < D(plan["stop"]) + tick:
        return {"action": "SKIP", "reason": "ENTRY_PRICE_BELOW_STOP_REGION"}
    return {
        "action": "ACTIVE_LIMIT" if active else "PASSIVE_LIMIT",
        "version": VERSION,
        "price": str(price),
        "ceiling": str(ceiling),
        "uncertainty_ticks": 1,
        "minimum_profit_ticks": 1,
        "fees": "0",
        "order_seconds": ACTIVE_SECONDS if active else PASSIVE_SECONDS,
    }


def check_entry_execution(inst, plan, slot, execution, price, at, ttl):
    from sigbot.strategies.grid_v2_runtime import quote

    book = quote(inst, at, ttl)
    choice = entry_choice(book, plan, slot, inst.tick)
    if execution.get("version") not in SUPPORTED_VERSIONS or execution.get("action") not in {
        "ACTIVE_LIMIT",
        "PASSIVE_LIMIT",
    }:
        raise RiskRejected("GRID_EXECUTION_CONTRACT_INVALID")
    if choice["action"] == "SKIP" or price > D(choice["ceiling"]):
        raise RiskRejected("GRID_EXECUTION_PRICE_REGION_CHANGED")
    if execution.get("ceiling") != choice["ceiling"] or price != D(execution["price"]):
        raise RiskRejected("GRID_EXECUTION_FROZEN_LIMIT_CHANGED")
    if book is None:
        raise RiskRejected("GRID_EXECUTION_FRESH_BOOK_REQUIRED")
    if execution["action"] == "ACTIVE_LIMIT" and book[1][0][0] > price:
        raise RiskRejected("GRID_EXECUTION_ASK_MOVED_ABOVE_LIMIT")
    if execution["action"] == "PASSIVE_LIMIT" and book[1][0][0] <= price:
        raise RiskRejected("GRID_EXECUTION_PASSIVE_WOULD_CROSS")
    return book


def order_seconds(note, default):
    execution = note.get("execution")
    if execution is None:
        return default  # Persisted pre-upgrade requests retain their native TTL.
    action = execution.get("action")
    expected = PASSIVE_SECONDS if action == "PASSIVE_LIMIT" else ACTIVE_SECONDS
    if (
        execution.get("version") not in SUPPORTED_VERSIONS
        or action not in {"ACTIVE_LIMIT", "PASSIVE_LIMIT", "TARGET_LIMIT"}
        or execution.get("order_seconds") != expected
    ):
        raise RiskRejected("GRID_EXECUTION_EXPIRATION_INVALID")
    return expected


def replacement_state(order, pending, at):
    """A replacement can spend only after the exact zero-fill parent is resolved."""
    if order is None or order.id != pending["source_order_id"]:
        raise RiskRejected("GRID_REPLACEMENT_SOURCE_NOT_FOUND")
    if order.filled > ZERO or order.observed_filled > ZERO:
        return "ABORT_FILLED"
    if at >= utc_time(pending["expires_at"]):
        return "EXPIRED"
    if (
        order.state not in TERMINAL
        or order.request_state != "RESOLVED"
        or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
        or order.reserve_cash > ZERO
        or order.reserve_quantity > ZERO
    ):
        return "WAIT_CANCEL"
    return "READY"


def buy_review(order, note, profile, plan, book, at, tick, default_seconds):
    import json

    meta = json.loads(note.get("note") or "{}")
    if (
        (at - utc_time(note["created_at"])).total_seconds() >= order_seconds(meta, default_seconds)
        or meta.get("replacement_deadline")
        and at >= utc_time(meta["replacement_deadline"])
    ):
        return "MAX_ORDER_AGE"
    if not profile.get("eligible"):
        return "HISTORY_INELIGIBLE"
    if D(profile.get("drift") or "0") <= D("-.01"):
        return "ADVERSE_DRIFT"
    if not book:
        return "FRESH_BOOK_REQUIRED"
    bid = book[0][0][0]
    if bid <= D(plan["stop"]) or bid >= D(plan["target"]):
        return "STOP_OR_RECOVERY_REGION"
    if meta.get("execution"):
        choice = entry_choice(book, plan, meta["slot"], tick)
        if choice["action"] == "SKIP":
            return choice["reason"]
        if (
            order is not None
            and choice["action"] == "ACTIVE_LIMIT"
            and D(choice["price"]) > order.price
        ):
            # The runner cancels first; common OMS reservation remains until
            # terminal confirmation. A later cycle sizes only remaining risk.
            return "REPRICE_TO_BOUNDED_ACTIVE_WAIT_CANCEL"
    if D(profile["mu"]) < D(plan["mu"]) - max(2 * D(plan["width"]), 2 * tick):
        return "CENTER_INVALIDATED"
    return "KEEP"


def executable_exit(book, quantity, target):
    """Return full-quantity bid VWAP/size; a public target print is insufficient."""
    if not book or quantity <= ZERO:
        return None
    left, value = quantity, ZERO
    for price, available in book[0]:
        # A target limit cannot consume below-target depth even if VWAP passes.
        if price < target:
            break
        take = min(left, available)
        left -= take
        value += take * price
        if left == ZERO:
            return {
                "quantity": str(quantity),
                "bid_vwap": str(value / quantity),
                "limit": str(target),
            }
    return None
