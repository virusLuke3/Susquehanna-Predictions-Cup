"""Order-owned uncertainty with fully retained exposure, not an account-wide veto."""

from decimal import Decimal, InvalidOperation

from sqlalchemy import select

from sigbot.domain import ZERO, content_hash
from sigbot.errors import RiskRejected
from sigbot.storage import Audit, Instrument, IntentRow, LiveWire, Order, Position


def unresolved(order):
    return order.request_state in {"UNKNOWN", "SENDING"} or order.cancel_state == "CANCEL_UNKNOWN"


def order_scope(session, account, order):
    """Only recorded ownership and a fully reserved immutable request can isolate."""
    row = session.get(IntentRow, order.intent_id)
    inst = session.get(Instrument, order.instrument_id)
    origin = row.payload if row else {}
    result = {
        "order_id": order.id,
        "remote_id": order.remote_id,
        "strategy_id": origin.get("strategy_id"),
        "strategy_version": origin.get("strategy_version"),
        "instrument_id": order.instrument_id,
        "exchange_id": inst.exchange_id if inst else None,
        "event_group": inst.event_group if inst else None,
        "request_state": order.request_state,
        "classification": "SUBMISSION_IN_FLIGHT"
        if order.request_state == "SENDING"
        else "SUBMIT_RESULT_UNKNOWN"
        if order.request_state == "UNKNOWN"
        else "CANCEL_RESULT_UNKNOWN",
        "cancel_state": order.cancel_state,
        "reserve_cash": str(order.reserve_cash),
        "reserve_quantity": str(order.reserve_quantity),
        "isolatable": False,
    }
    remaining = order.quantity - order.filled
    if (
        not row
        or not inst
        or not origin.get("strategy_id")
        or not origin.get("strategy_version")
        or (row.account_id, order.account_id, origin.get("account_id")) != (account.id,) * 3
        or origin.get("instrument_id") != inst.id
        or origin.get("tournament_id") != account.tournament
        or inst.tournament != account.tournament
        or not inst.exchange_id
        or not inst.event_group
        or order.request_state == "SENDING"  # An active request is not a lost acknowledgement.
        or order.observed_filled != order.filled
        or remaining <= ZERO
    ):
        return result
    if order.side == "BUY":
        covered = order.reserve_cash >= remaining * (order.price + order.fee_bound / order.quantity)
    elif order.side == "SELL":
        position = session.get(Position, (account.id, inst.id))
        covered = bool(
            position
            and position.cost is not None
            and position.quantity >= remaining
            and order.reserve_quantity >= remaining
        )
    else:
        covered = False
    if not covered:
        return result
    if account.namespace == "live":
        wire = session.get(LiveWire, order.id)
        if wire is None or wire.digest != content_hash(wire.payload):
            return result
        payload = wire.payload
        try:
            exact_amounts = (
                Decimal(str(payload.get("quantity", 0))) == order.quantity
                and Decimal(str(payload.get("price", -1))) == order.price
            )
        except (InvalidOperation, ValueError, TypeError):
            return result
        if (
            payload.get("idempotencyKey") != order.id
            or payload.get("exchangeId") != inst.exchange_id
            or payload.get("tournamentId") != account.tournament
            or payload.get("side") != inst.outcome.lower()
            or payload.get("action") != order.side.lower()
            or not exact_amounts
            or not wire.approval_id
            or not wire.approval_digest
        ):
            return result
        if (
            order.request_state == "UNKNOWN"
            and not order.remote_id
            and not session.scalar(
                select(Audit.id)
                .where(
                    Audit.account_id == account.id,
                    Audit.kind == "HTTP_TRANSMISSION_STARTED",
                    Audit.data["order"].as_string() == order.id,
                )
                .limit(1)
            )
        ):
            return result
        result["recovery_status"] = (
            "REMOTE_QUERY_AVAILABLE"
            if order.remote_id
            else "EXPIRED_REQUEST_RECOVERY"
            if payload.get("expirationDate")
            else "GTC_RECEIPT_UNCONFIRMED_NO_SAFE_LOOKUP"
        )
    result["isolatable"] = True
    return result


def scopes(session, account, orders=None):
    if orders is None:
        orders = session.scalars(select(Order).where(Order.account_id == account.id))
    return [order_scope(session, account, order) for order in orders if unresolved(order)]


def scope_blockers(pending, target, parallel=True):
    """One policy for strategy inputs, reference sampling and actual allocation."""
    related = False
    for item in pending:
        if not parallel or not item["isolatable"]:
            return ["UNRESOLVED_REQUEST"]
        if item["exchange_id"] == target.exchange_id or item["event_group"] == target.event_group:
            related = True
    return ["UNRESOLVED_REQUEST_IN_MARKET"] if related else []


def check_scope(session, account, target, parallel, *, exclude_order=None):
    """Keep same exchange/event blocked; all reservations still enter shared risk."""
    pending = [item for item in scopes(session, account) if item["order_id"] != exclude_order]
    blockers = scope_blockers(pending, target, parallel)
    if blockers:
        raise RiskRejected(blockers[0])
    from sigbot.observed_orders import protect_observed_scope

    protect_observed_scope(session, account, target)
