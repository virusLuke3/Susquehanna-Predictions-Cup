"""Import a proven native unfilled BUY without assigning it to a strategy.

This is a read observation in the COMMON OMS, not a reconstructed submission.
Original UNKNOWN requests remain untouched, even if amounts happen to match.
"""

import json
from datetime import timedelta
from decimal import Decimal as D

from sqlalchemy import select

from sigbot.domain import ZERO, Intent, check_grid, content_hash, utc_time
from sigbot.errors import RiskRejected, SchemaMismatch
from sigbot.storage import FillRow, Instrument, IntentRow, LiveWire, Order, Outbox, PositionLot

IDENTITY = ("external_observed", "1.0")


def import_unfilled_buy(store, session, account, native, proof, at, fee):
    """Caller holds the common account operation lease and account row lock.

    Only an unfilled BUY is supported: independent complete per-order fills
    prove zero, not nullable quantityFilled or a price/time/size match.
    """
    remote = str(native["id"])
    if store.namespace == "live" and not remote.isdecimal():
        raise SchemaMismatch("EXTERNAL_NATIVE_ORDER_ID_REQUIRED")
    if (native["tournamentId"], proof.get("account_id")) != (account.tournament, account.id):
        raise SchemaMismatch("EXTERNAL_ORDER_ACCOUNT_SCOPE_MISMATCH")
    existing = session.scalar(
        select(Order).where(Order.account_id == account.id, Order.remote_id == remote)
    )
    if existing:
        return existing.id  # Never overwrite an existing strategy's ownership.
    page = proof.get("fills", {})
    if (
        proof.get("status") != "PASS"
        or not proof.get("checkpoint")
        or not 0 <= (at - utc_time(proof["received_at"])).total_seconds() <= 30
        or str(page.get("orderId")) != remote
        or page.get("exchangeId") != native["exchangeId"]
        or page.get("tournamentId") != account.tournament
        or not page.get("coverage", {}).get("complete")
        or page.get("pagination", {}).get("hasMore") is not False
        or page.get("pagination", {}).get("nextCursor") is not None
        or D(str(page.get("totalQuantityFilled", "-1"))) != ZERO
        or page.get("data") != []
        or native.get("action") != "buy"
        or not native.get("open")
        or native.get("side") not in {"yes", "no"}
        or native.get("priceLimit") is None
        or native.get("quantityFilled") not in {None, 0, "0", ZERO}
        or fee < ZERO
    ):
        raise SchemaMismatch("EXTERNAL_UNFILLED_BUY_PROOF_REQUIRED")
    instruments = list(
        session.scalars(
            select(Instrument).where(
                Instrument.tournament == account.tournament,
                Instrument.exchange_id == native["exchangeId"],
                Instrument.outcome == native["side"].upper(),
            )
        )
    )
    if len(instruments) != 1:
        raise SchemaMismatch("EXTERNAL_ORDER_INSTRUMENT_MAPPING_AMBIGUOUS")
    inst = instruments[0]
    q, price = D(str(native["quantity"])), D(str(native["priceLimit"]))
    if q <= ZERO or not ZERO < price < 1:
        raise SchemaMismatch("EXTERNAL_ORDER_AMOUNT_INVALID")
    check_grid(price, q, inst.tick, inst.step)
    reserve = q * (price + fee)
    total = sum(
        (
            o.reserve_cash
            for o in session.scalars(select(Order).where(Order.account_id == account.id))
        ),
        ZERO,
    )
    if total + reserve > account.cash:
        raise RiskRejected("EXTERNAL_FULL_CASH_RESERVATION_UNAVAILABLE")
    key = (
        ("SIM-" if account.id.startswith("SIM-") else "SIG-")
        + "OBSERVED-"
        + content_hash([account.id, account.tournament, remote])
    )
    note = {
        "source": "OFFICIAL_NATIVE_ORDER_OBSERVATION_ONLY",
        "native": native,
        "proof": proof,
        "ownership": "UNATTRIBUTED",
        "venue_write_authorized": False,
    }
    # Official money values arrive as Decimal. Keep their exact spelling in
    # JSON evidence as strings, including the audit's nested native response.
    note = json.loads(json.dumps(note, default=str))
    # Schema-compatible observational envelope, already expired and unregistered.
    # It MUST NOT be presented as an original client trading instruction.
    envelope = Intent(
        schema_version="1.0",
        intent_id=key,
        synthetic=account.id.startswith("SIM-"),
        account_id=account.id,
        tournament_id=account.tournament,
        instrument_id=inst.id,
        outcome_side=inst.outcome,
        action="INCREASE_TO_TARGET",
        target_quantity=q,
        limit_price=price,
        max_order_notional=q * price,
        created_at=at - timedelta(microseconds=1),
        expires_at=at,
        rules_hash=inst.rules_hash,
        mapping_hash=inst.mapping_hash,
        strategy_id=IDENTITY[0],
        strategy_version=IDENTITY[1],
        note=json.dumps(note, default=str),
    )
    session.add(
        IntentRow(
            id=key,
            account_id=account.id,
            instrument_id=inst.id,
            digest=envelope.digest,
            payload=envelope.model_dump(mode="json"),
            active=True,
        )
    )
    session.flush()
    session.add(
        Order(
            id=key,
            intent_id=key,
            account_id=account.id,
            instrument_id=inst.id,
            remote_id=remote,
            side="BUY",
            quantity=q,
            price=price,
            fee_bound=q * fee,
            state="OPEN",
            request_state="ACKNOWLEDGED",
            reserve_cash=reserve,
            reserve_quantity=ZERO,
        )
    )
    session.flush()
    observed = {
        "observed_only": True,
        "exchangeId": inst.exchange_id,
        "tournamentId": account.tournament,
        "side": native["side"],
        "action": "buy",
        "quantity": str(q),
        "price": str(price),
    }
    # There is NO idempotencyKey, invented submit receipt or trading approval.
    session.add(
        LiveWire(
            order_id=key,
            approval_id="OBSERVATION_ONLY_NO_WRITE_AUTHORITY",
            approval_digest=content_hash(note),
            payload=observed,
            digest=content_hash(observed),
            receipt=None,
        )
    )
    session.add(Outbox(order_id=key, state="OBSERVATION_ONLY_NEVER_SEND"))
    store.audit(
        session,
        account.id,
        "EXTERNAL_NATIVE_ORDER_OBSERVED",
        {
            "order_id": key,
            "remote_id": remote,
            "ownership": "UNATTRIBUTED",
            "reserve_cash": str(reserve),
            "original_unknown_requests_changed": False,
            "evidence": note,
        },
        at,
    )
    return key


def protect_observed_scope(session, account, target):
    """An observed foreign order/lot is not an UNKNOWN submission result."""
    for order, inst in session.execute(
        select(Order, Instrument)
        .join(Instrument, Order.instrument_id == Instrument.id)
        .join(IntentRow, Order.intent_id == IntentRow.id)
        .where(
            Order.account_id == account.id,
            IntentRow.payload["strategy_id"].as_string() == IDENTITY[0],
        )
    ):
        if inst.exchange_id != target.exchange_id and inst.event_group != target.event_group:
            continue
        held = session.scalar(
            select(PositionLot.id)
            .join(
                FillRow,
                (FillRow.account_id == PositionLot.account_id)
                & (PositionLot.economic_id == "fill:" + FillRow.id),
            )
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                PositionLot.account_id == account.id,
                PositionLot.instrument_id == inst.id,
                PositionLot.quantity > ZERO,
                IntentRow.payload["strategy_id"].as_string() == IDENTITY[0],
            )
            .limit(1)
        )
        if order.reserve_cash or order.reserve_quantity or held is not None:
            raise RiskRejected("EXTERNAL_OBSERVED_ORDER_OR_INVENTORY_IN_MARKET")
