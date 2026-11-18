"""Keep safe queue positions; safety changes override optimization debounce."""

import json
from decimal import Decimal

from sigbot.domain import utc_time

RECOVERABLE_DATA_CANCELS = {
    "MM13_DATA_UNCERTAIN",
    "MM13_BOOK_CONTINUITY_OR_REST_CHECK_UNKNOWN",
    "MM13_BOOK_RECONCILE_REQUIRED",
    "MM_OWN_ORDER_NEWER_THAN_BOOK",
    "MM_OWN_ORDER_BOOK_MISMATCH",
    "MM_OWN_ORDERS_OUTSIDE_VISIBLE_DEPTH",
}


def visible_queue(book, order):
    """Visible competing volume is an upper bound, never a FIFO position or fill."""
    levels = book.bids if order.side == "BUY" else book.asks
    volume = sum(
        (
            level.quantity - level.own_quantity
            for level in levels
            if (level.price >= order.price if order.side == "BUY" else level.price <= order.price)
        ),
        Decimal(0),
    )
    return {
        "status": "VISIBLE_COMPETING_VOLUME_UPPER_BOUND",
        "visible_ahead_upper": str(volume),
        "actual_queue_position": None,
        "priority_rule_verified": False,
        "cancellation_counted_as_fill": False,
        "remaining_quantity": str(order.quantity - order.filled),
    }


def replacement_value(order, price, total, quote, settings):
    """Compare supported first-partial-fill value after measured cancel+submit delay."""
    delay = quote.get("replacement_latency_seconds")
    adverse = quote.get("own_fill_buy_AS" if order.side == "BUY" else "own_fill_sell_AS")
    if delay is None or adverse is None:
        return None
    models = quote.get("standard_model", {}).get("local_parameters", {}).get(order.side, {})
    mid = (Decimal(quote["external_bid"]) + Decimal(quote["external_ask"])) / 2
    horizon = Decimal(settings.normal_reprice_min_seconds)
    reference = Decimal(
        quote.get(
            "reservation_buy_value" if order.side == "BUY" else "reservation_sell_value", str(mid)
        )
    )
    fee = Decimal(
        quote.get("buy_fee_per_share" if order.side == "BUY" else "sell_fee_per_share", "0")
    )

    def value(p, original_quantity, remaining, seconds):
        data = models.get("quantity_models", {}).get(str(int(original_quantity)), models)
        distance = mid - p if order.side == "BUY" else p - mid
        point = next(
            (
                r
                for r in data.get("distance_models", [])
                if Decimal(r["distance"]) == distance
                and Decimal(r["order_quantity"]) == original_quantity
                and r["status"] == "READY_DISTANCE_RATE"
            ),
            None,
        )
        if point is None:
            return None
        probability = (
            1 - (-Decimal(point["observed_events_per_second"]) * max(Decimal(0), seconds)).exp()
        )
        shares = min(remaining, Decimal(point["mean_partial_quantity"]))
        edge = reference - p if order.side == "BUY" else p - reference
        return probability * shares * (edge - fee - Decimal(adverse))

    old = value(order.price, order.quantity, order.quantity - order.filled, horizon)
    new = value(price, total, total, horizon - Decimal(delay))
    if old is None or new is None:
        return None
    return old, new


def keep_order(order, row, price, total, quote, settings, timestamp):
    directive = quote.get("buy_directive" if order.side == "BUY" else "sell_directive")
    if order.cancel_state:
        return True  # Already queued/sent cancellation is resolved by the OMS.
    if directive == "WITHDRAW":
        return False  # Explicit withdrawal must also survive a pending acknowledgement.
    if order.request_state != "ACKNOWLEDGED":
        return True  # No new order and no cancellation derived from WAIT_OWN_ACK.
    if (
        order.side == "SELL"
        and quote.get("exit_claim")
        and json.loads(row.payload.get("note") or "{}").get("kind") != "INVENTORY_EXIT"
    ):
        return False  # A normal TP quote must not delay a latched risk exit.
    remaining = order.quantity - order.filled
    safety_total = Decimal(
        quote.get("maximum_buy_total" if order.side == "BUY" else "maximum_sell_total", str(total))
    )
    if remaining > safety_total:
        return False
    if quote.get("sell_kind") == "INVENTORY_EXIT" and order.side == "SELL":
        if remaining > total or order.price < Decimal(quote["ask"]):
            return False
        age = (timestamp - utc_time(row.payload["created_at"])).total_seconds()
        return (
            order.price <= Decimal(quote["external_bid"])
            or order.price == price
            or age < settings.normal_reprice_min_seconds
        )
    if order.side == "BUY":
        safe = order.price <= Decimal(quote["buy_guard"]) and order.price < Decimal(
            quote["external_ask"]
        )
    else:
        safe = order.price >= Decimal(quote["sell_guard"]) and order.price > Decimal(
            quote["external_bid"]
        )
    if not safe:
        return False
    if directive == "DEFER" or not quote.get("allow_new_orders", True):
        return True
    if total <= 0:
        return False
    economic = replacement_value(order, price, total, quote, settings)
    if economic is not None:
        return economic[0] >= economic[1]
    small_change = (
        abs(order.price - price)
        < settings.normal_reprice_distance_ticks * Decimal(quote.get("tick", ".005"))
        and abs(total - remaining) / max(total, remaining) < settings.normal_resize_fraction
        and remaining >= order.quantity * settings.keep_remaining_fraction
    )
    age = (timestamp - utc_time(row.payload["created_at"])).total_seconds()
    if remaining < order.quantity * settings.keep_remaining_fraction:
        return False
    if (
        quote.get("mode") in {"REAL_PASSIVE_CALIBRATION", "BASIC_PASSIVE_MM"}
        and order.price != price
    ):
        # Return old retreat orders to the competitive price after debounce.
        return age < settings.normal_reprice_min_seconds
    return small_change or not order.filled and age < settings.normal_reprice_min_seconds


def revalidate_cancel(session, order, command, timestamp):
    """Under the existing account/order claim, immediately before any DELETE or release."""
    from sigbot.domain import TERMINAL, content_hash
    from sigbot.storage import Account, Instrument, IntentRow
    from sigbot.strategies.mm_sig_v1.live.models import Settings

    payload = command.payload
    if payload.get("strategy_revision") != "1.3.1":
        return True  # Older/other-strategy cancellation is not silently revoked.
    if order.cancel_state:
        return False  # Already sent/unknown: the OMS continues querying it.
    reason = None
    if payload["order_id"] != order.id or payload.get("order_generation") != order.intent_id:
        reason = "CANCEL_WRONG_ORDER_GENERATION"
    elif payload.get("venue_order_id") not in {None, order.remote_id}:
        reason = "CANCEL_VENUE_ID_CHANGED"
    elif order.state in TERMINAL and order.request_state == "RESOLVED":
        reason = "CANCEL_TERMINAL_RECONCILE_ONLY"
    elif payload.get("risk_latched") and payload.get("reason") not in RECOVERABLE_DATA_CANCELS:
        return True
    else:
        inst = session.get(Instrument, order.instrument_id)
        latest = inst.mapping_evidence.get("mm131_latest", {})
        if latest and payload.get("reason") in RECOVERABLE_DATA_CANCELS:
            from sigbot.strategies.mm_sig_v1.live.execution import current_verified_quote

            recovered = current_verified_quote(session, order, inst, latest, timestamp)
            if recovered is not None:
                latest = {**latest, "at": timestamp.isoformat(), "quote": recovered}
        if not latest or not 0 <= (timestamp - utc_time(latest["at"])).total_seconds() <= 5:
            if payload.get("risk_latched"):
                return True  # No fresh proof: retain the original safety cancellation.
            reason = "SOFT_CANCEL_REQUIRES_CURRENT_DECISION"
        else:
            quote = latest["quote"]
            key = "buy" if order.side == "BUY" else "sell"
            if payload.get("risk_latched"):
                account = session.get(Account, order.account_id)
                settings = Settings.model_validate(latest["settings"])
                if (
                    account.lifecycle != "HEALTHY"
                    or not account.reconciled_at
                    or not 0
                    <= (timestamp - account.reconciled_at).total_seconds()
                    <= settings.account_ttl_seconds
                    or not inst.reviewed
                    or (quote.get("rules_hash"), quote.get("mapping_hash"))
                    != (inst.rules_hash, inst.mapping_hash)
                ):
                    return True
            if quote.get("blocking_reasons") or quote.get(key + "_directive") == "WITHDRAW":
                command.payload = {
                    **payload,
                    "risk_latched": True,
                    "reason": "REVALIDATED_WITHDRAWAL",
                }
                command.digest = content_hash(command.payload)
                return True
            row = session.get(IntentRow, order.intent_id)
            if keep_order(
                order,
                row,
                Decimal(quote["bid" if order.side == "BUY" else "ask"]),
                Decimal(quote[key + "_quantity"]),
                quote,
                Settings.model_validate(latest["settings"]),
                timestamp,
            ):
                reason = "SOFT_CANCEL_SUPERSEDED_BY_CURRENT_TARGET"
            else:
                return True
    command.state, command.result = "SUPERSEDED_BEFORE_SEND", {"code": reason, "cancel_sent": False}
    return False
