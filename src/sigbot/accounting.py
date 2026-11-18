from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from sigbot.domain import ZERO, Fill, content_hash
from sigbot.errors import Conflict, SchemaMismatch
from sigbot.storage import (
    Account,
    EconomicEvent,
    FillRow,
    Instrument,
    IntentRow,
    Journal,
    Order,
    Position,
    PositionLot,
    Store,
    StrategyEvent,
)


def binary_partner(s, inst):
    """Only the two reviewed outcomes of the very same official binary exchange."""
    if (
        inst is None
        or inst.outcome not in {"YES", "NO"}
        or not inst.reviewed
        or inst.mapping_evidence.get("review", {}).get("status")
        != "VERIFIED_OFFICIAL_BINARY_SCOPE_AND_PREDICATE"
    ):
        raise SchemaMismatch("BINARY_NETTING_MAPPING_UNVERIFIED")
    partners = list(
        s.scalars(
            select(Instrument).where(
                Instrument.exchange_id == inst.exchange_id,
                Instrument.tournament == inst.tournament,
                Instrument.outcome == ("NO" if inst.outcome == "YES" else "YES"),
            )
        )
    )
    if (
        len(partners) != 1
        or not partners[0].reviewed
        or (partners[0].rules_hash, partners[0].mapping_hash)
        != (inst.rules_hash, inst.mapping_hash)
    ):
        raise SchemaMismatch("BINARY_NETTING_MAPPING_UNVERIFIED")
    return partners[0]


def complementary_mm(row):
    return bool(
        row
        and row.payload.get("strategy_id") == "mm_sig"
        and row.payload.get("strategy_version") == "1.3"
        and json.loads(row.payload.get("note") or "{}")
        .get("settings", {})
        .get("complementary_quotes_enabled")
    )


def lot_origins(session, account_id, instrument_ids=None):
    """Remaining physical lots with their exact persisted fill/intent ownership."""
    query = (
        select(PositionLot, IntentRow.payload, FillRow.payload)
        .outerjoin(FillRow, (FillRow.account_id == PositionLot.account_id)
                   & (PositionLot.economic_id == "fill:" + FillRow.id))
        .outerjoin(Order, FillRow.order_id == Order.id)
        .outerjoin(IntentRow, Order.intent_id == IntentRow.id)
        .where(PositionLot.account_id == account_id, PositionLot.quantity > ZERO)
        .order_by(PositionLot.id)
    )
    if instrument_ids is not None:
        query = query.where(PositionLot.instrument_id.in_(instrument_ids))
    return [(lot, origin or {}, fill or {}) for lot, origin, fill in session.execute(query)]


def cash_adjustments(session, account_id, strategy_id):
    """Explicit settlement/correction/allocation ownership; no guessed cash flow."""
    cash, external, valid = ZERO, ZERO, True
    for event in session.scalars(select(EconomicEvent).where(
        EconomicEvent.account_id == account_id,
        EconomicEvent.kind.in_(["PAIR", "SETTLEMENT", "CASH_CORRECTION", "STRATEGY_TRANSFER"]),
    )):
        allocations = event.payload.get("strategy_cash_allocations", {})
        amount = Decimal(allocations.get(strategy_id, "0"))
        cash += amount
        if event.kind == "STRATEGY_TRANSFER":
            external += amount
        if event.payload.get("ownership_conflict") and strategy_id in event.payload.get("owners", []):
            valid = False
    return cash, external, valid


def strategy_finances(session, account_id, strategy_id, principal, *, exclude_order=None):
    """Project an allocated cash pool from the existing ledger, across owned versions."""
    own = IntentRow.payload["strategy_id"].as_string() == strategy_id
    fills = list(
        session.scalars(
            select(FillRow)
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(FillRow.account_id == account_id, own)
        )
    )
    cash = principal
    for fill in fills:
        data = fill.payload
        if data["side"] not in {"BUY", "SELL"}:
            raise SchemaMismatch("STRATEGY_FILL_SIDE_UNKNOWN")
        value = Decimal(data["quantity"]) * Decimal(data["price"])
        cash += (value if data["side"] == "SELL" else -value) - Decimal(data["fee"])
    adjustments, external_flow, ownership_valid = cash_adjustments(session, account_id, strategy_id)
    cash += adjustments
    # Old same-strategy pairs predate explicit attribution; never credit a mixed
    # pair simply because this strategy's fill happened to complete it.
    if fills:
        for pair in session.scalars(select(EconomicEvent).where(
            EconomicEvent.account_id == account_id,
            EconomicEvent.id.in_(["pair:" + f.id for f in fills]),
        )):
            if "strategy_cash_allocations" not in pair.payload:
                ownership_valid = False
    lots = list(
        session.scalars(
            select(PositionLot).where(
                PositionLot.account_id == account_id,
                PositionLot.quantity > ZERO,
                PositionLot.economic_id.in_(["fill:" + f.id for f in fills]),
            )
        )
    )
    if any(lot.cost is None for lot in lots):
        raise SchemaMismatch("STRATEGY_COST_BASIS_UNKNOWN")
    cost = sum((lot.cost for lot in lots if lot.cost is not None), ZERO)
    reserves = sum(session.scalars(
        select(Order.reserve_cash)
        .join(IntentRow, Order.intent_id == IntentRow.id)
        .where(Order.account_id == account_id, own, Order.id != (exclude_order or ""))
    ), ZERO)
    future_fees = sum(session.scalars(
        select(StrategyEvent.future_fee_reserve).where(
            StrategyEvent.account_id == account_id, StrategyEvent.strategy_id == strategy_id
        )
    ), ZERO)
    return {
        "principal": principal,
        "cash": cash,
        "cost": cost,
        "reserved_cash": reserves,
        "future_fee_reserve": future_fees,
        "available": max(ZERO, min(cash, principal - cost) - reserves - future_fees) if ownership_valid else ZERO,
        "equity_at_fifo_cost": cash + cost,
        "realized_pnl": cash + cost - principal - external_flow,
        "external_flow": external_flow,
        "ownership_valid": ownership_valid,
        "pnl_status": "ATTRIBUTED" if ownership_valid else "OWNERSHIP_UNRESOLVED",
    }


class Ledger:
    def __init__(self, store: Store):
        self.store = store

    def cash_ownership(self, session, account_id, quantities, payout):
        sources, owners = [], set()
        for iid, quantity in quantities.items():
            remaining = quantity
            for lot, origin, _ in lot_origins(session, account_id, [iid]):
                take = min(remaining, lot.quantity)
                if not take:
                    continue
                owner = origin.get("strategy_id", "__UNATTRIBUTED__")
                owners.add(owner)
                sources.append({"lot_id": lot.id, "economic_id": lot.economic_id,
                    "instrument_id": iid, "quantity": str(take), "strategy_id": owner,
                    "cost": str(lot.cost * take / lot.quantity) if lot.cost is not None else None})
                remaining -= take
                if not remaining:
                    break
            if remaining:
                owners.add("__UNATTRIBUTED__")
        unambiguous = len(owners) == 1 and "__UNATTRIBUTED__" not in owners
        return {"owners": sorted(owners), "consumed_lot_ownership": sources,
                "strategy_cash_allocations": {next(iter(owners)): str(payout)} if unambiguous else {},
                "ownership_conflict": not unambiguous}

    def transfer_strategy_cash(self, account_id, event_id, source, target, amount, principals, proof):
        """Explicit internal allocation only; physical account cash never changes."""
        if not amount.is_finite() or amount <= ZERO or source == target or not proof or source not in principals or target not in principals:
            raise SchemaMismatch("STRATEGY_TRANSFER_NOT_VERIFIED")
        payload={"source":source,"target":target,"amount":str(amount),"proof":proof,
                 "economic_cash_flow":False,
                 "strategy_cash_allocations":{source:str(-amount),target:str(amount)}}
        with self.store.transaction() as session:
            account=self.store.lock_account(session,account_id)
            available=strategy_finances(session,account.id,source,principals[source])["available"]
            if not self.event(session,account,event_id,"STRATEGY_TRANSFER",payload):
                return False
            if available < amount:
                raise SchemaMismatch("STRATEGY_TRANSFER_AVAILABLE_CASH_EXCEEDED")
            return True

    def event(self, s: Session, account: Account, key: str, kind: str, payload: dict) -> bool:
        digest = content_hash(payload)
        old = s.get(EconomicEvent, (account.id, key))
        if old:
            if old.digest != digest or old.kind != kind:
                raise Conflict("ECONOMIC_EVENT_CONTENT_CONFLICT")
            return False
        s.add(
            EconomicEvent(account_id=account.id, id=key, kind=kind, digest=digest, payload=payload)
        )
        s.flush()
        return True

    def movement(
        self,
        s: Session,
        account: Account,
        key: str,
        asset: str,
        amount: Decimal,
        other: str = "EXCHANGE",
    ) -> None:
        # Each asset is balanced independently; cash and shares are never summed together.
        s.add_all(
            [
                Journal(
                    account_id=account.id,
                    economic_id=key,
                    asset=asset,
                    bucket="ACCOUNT",
                    amount=amount,
                ),
                Journal(
                    account_id=account.id,
                    economic_id=key,
                    asset=asset,
                    bucket=other,
                    amount=-amount,
                ),
            ]
        )

    def position(self, s: Session, account_id: str, instrument_id: str) -> Position:
        position = s.get(Position, (account_id, instrument_id))
        if position is None:
            position = Position(
                account_id=account_id, instrument_id=instrument_id, quantity=ZERO, cost=ZERO
            )
            s.add(position)
            s.flush()
        return position

    def fill(self, s: Session, account: Account, order: Order, fill: Fill, fault=None) -> bool:
        payload = {k: str(v) for k, v in asdict(fill).items()}
        if (fill.order_id, fill.instrument_id, fill.side) != (
            order.remote_id,
            order.instrument_id,
            order.side,
        ):
            raise SchemaMismatch("UNEXPECTED_ORDER_NORMALIZATION")
        if fill.quantity <= 0 or not 0 < fill.price < 1 or fill.fee < 0:
            raise SchemaMismatch("INVALID_FILL")
        if (fill.side == "BUY" and fill.price > order.price) or (
            fill.side == "SELL" and fill.price < order.price
        ):
            raise SchemaMismatch("LIMIT_VIOLATION")
        if fill.fee > order.fee_bound * fill.quantity / order.quantity:
            raise SchemaMismatch("FEE_BOUND_EXCEEDED")
        key = "fill:" + fill.fill_id
        if not self.event(s, account, key, "FILL", payload):
            return False
        if order.filled + fill.quantity > order.quantity:
            raise SchemaMismatch("OVERFILL")
        s.add(FillRow(account_id=account.id, id=fill.fill_id, order_id=order.id, payload=payload))
        s.flush()
        if fault:
            fault("after_fill_insert")
        pos = self.position(s, account.id, order.instrument_id)
        if pos.cost is None:
            raise SchemaMismatch("UNKNOWN_COST_BASIS")
        amount = fill.quantity * fill.price
        if fill.side == "BUY":
            delta_cash, delta_qty = -amount - fill.fee, fill.quantity
            pos.cost += amount + fill.fee
            s.add(
                PositionLot(
                    account_id=account.id,
                    instrument_id=pos.instrument_id,
                    economic_id=key,
                    quantity=fill.quantity,
                    cost=amount + fill.fee,
                )
            )
        elif fill.side == "SELL":
            if fill.quantity > pos.quantity:
                raise SchemaMismatch("UNCOVERED_SELL")
            delta_cash, delta_qty = amount - fill.fee, -fill.quantity
            pos.cost -= self.consume_fifo(s, account.id, pos.instrument_id, fill.quantity)
        else:
            raise SchemaMismatch("UNKNOWN_SIDE")
        if account.cash + delta_cash < 0:
            raise SchemaMismatch("NEGATIVE_CASH")
        pos.quantity += delta_qty
        account.cash += delta_cash
        order.filled += fill.quantity
        self.movement(s, account, key, "CASH", delta_cash)
        self.movement(s, account, key, pos.instrument_id, delta_qty)
        if fill.side == "BUY" and complementary_mm(s.get(IntentRow, order.intent_id)):
            partner = binary_partner(s, s.get(Instrument, order.instrument_id))
            opposite = self.position(s, account.id, partner.id)
            paired = min(pos.quantity, opposite.quantity)
            if paired:
                self.pair_locked(
                    s,
                    account,
                    "pair:" + fill.fill_id,
                    pos.instrument_id,
                    partner.id,
                    paired,
                    paired,
                    "OFFICIAL_SAME_BINARY:"
                    + content_hash([partner.exchange_id, partner.rules_hash, partner.mapping_hash]),
                )
        return True

    def consume_fifo(
        self, s: Session, account_id: str, instrument_id: str, quantity: Decimal
    ) -> Decimal:
        lots = list(
            s.scalars(
                select(PositionLot)
                .where(
                    PositionLot.account_id == account_id, PositionLot.instrument_id == instrument_id
                )
                .order_by(PositionLot.id)
            )
        )
        if sum((lot.quantity for lot in lots), ZERO) < quantity or any(
            lot.cost is None for lot in lots if lot.quantity
        ):
            raise SchemaMismatch("FIFO_LOTS_UNKNOWN")
        remaining, consumed = quantity, ZERO
        for lot in lots:
            if not remaining:
                break
            if not lot.quantity:
                continue
            assert lot.cost is not None
            take = min(lot.quantity, remaining)
            cost = lot.cost if take == lot.quantity else lot.cost * take / lot.quantity
            lot.quantity -= take
            lot.cost -= cost
            remaining -= take
            consumed += cost
        return consumed

    def link_transaction(self, account_id: str, canonical_id: str, payload: dict) -> None:
        # Explicit source linkage, never amount/time similarity. No second cash mutation.
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            event = s.get(EconomicEvent, (account_id, canonical_id))
            if event is None:
                raise SchemaMismatch("UNLINKED_ECONOMIC_TRANSACTION")
            if event.kind == "FILL" and event.payload != {k: str(v) for k, v in payload.items()}:
                raise SchemaMismatch("TRANSACTION_FILL_MISMATCH")
            self.store.audit(
                s,
                account.id,
                "TRANSACTION_LINKED",
                {"canonical_id": canonical_id},
                datetime.now(timezone.utc),
            )

    def settlement(
        self,
        account_id: str,
        event_id: str,
        instrument_id: str,
        quantity: Decimal,
        payout: Decimal,
        cost: Decimal | None,
    ) -> bool:
        if quantity < 0 or payout < 0 or cost is None:
            raise SchemaMismatch("UNKNOWN_SETTLEMENT_COST")
        payload = {
            "instrument": instrument_id,
            "quantity": str(quantity),
            "payout": str(payout),
            "cost": str(cost),
        }
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            old = s.get(EconomicEvent, (account_id, event_id))
            attribution = (old.payload if old else self.cash_ownership(s, account_id, {instrument_id: quantity}, payout))
            payload.update({key: attribution[key] for key in ["owners", "consumed_lot_ownership", "strategy_cash_allocations", "ownership_conflict"] if key in attribution})
            if not self.event(s, account, event_id, "SETTLEMENT", payload):
                return False
            pos = self.position(s, account_id, instrument_id)
            if (pos.quantity, pos.cost) != (quantity, cost):
                raise SchemaMismatch("SETTLEMENT_POSITION_MISMATCH")
            if any(
                o.reserve_cash or o.reserve_quantity
                for o in s.scalars(
                    select(Order).where(
                        Order.account_id == account_id, Order.instrument_id == instrument_id
                    )
                )
            ):
                raise SchemaMismatch("SETTLEMENT_WITH_ACTIVE_ORDER")
            account.cash += payout
            pos.quantity, pos.cost = ZERO, ZERO
            for lot in s.scalars(
                select(PositionLot).where(
                    PositionLot.account_id == account_id, PositionLot.instrument_id == instrument_id
                )
            ):
                lot.quantity, lot.cost = ZERO, ZERO
            self.movement(s, account, event_id, "CASH", payout, "SETTLEMENT")
            self.movement(s, account, event_id, instrument_id, -quantity, "SETTLEMENT")
        return True

    def pair(
        self,
        account_id: str,
        event_id: str,
        left: str,
        right: str,
        quantity: Decimal,
        payout: Decimal,
        proof: str,
    ) -> bool:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            return self.pair_locked(s, account, event_id, left, right, quantity, payout, proof)

    def pair_locked(self, s, account, event_id, left, right, quantity, payout, proof):
        """Atomic with the causing fill; never a second snapshot cash adjustment."""
        if left == right or quantity <= 0 or payout < 0 or not proof:
            raise SchemaMismatch("PAIR_NOT_VERIFIED")
        payload = {
            "left": left,
            "right": right,
            "quantity": str(quantity),
            "payout": str(payout),
            "proof": proof,
        }
        old = s.get(EconomicEvent, (account.id, event_id))
        attribution = (old.payload if old else self.cash_ownership(
            s, account.id, {left: quantity, right: quantity}, payout))
        payload.update({key: attribution[key] for key in ["owners", "consumed_lot_ownership", "strategy_cash_allocations", "ownership_conflict"] if key in attribution})
        if not self.event(s, account, event_id, "PAIR", payload):
            return False
        for instrument in (left, right):
            pos = self.position(s, account.id, instrument)
            if pos.quantity < quantity or pos.cost is None:
                raise SchemaMismatch("PAIR_POSITION_MISMATCH")
            pos.cost -= self.consume_fifo(s, account.id, instrument, quantity)
            pos.quantity -= quantity
            self.movement(s, account, event_id, instrument, -quantity, "PAIR")
        account.cash += payout
        self.movement(s, account, event_id, "CASH", payout, "PAIR")
        return True

    def correction(
        self, account_id: str, event_id: str, original_id: str, cash_delta: Decimal, reason: str
    ) -> bool:
        with self.store.transaction() as s:
            account = self.store.lock_account(s, account_id)
            if not reason or s.get(EconomicEvent, (account_id, original_id)) is None:
                raise SchemaMismatch("CORRECTION_WITHOUT_ORIGINAL")
            original = s.get(EconomicEvent, (account_id, original_id))
            owners = original.payload.get("owners", [])
            if original.kind == "FILL":
                fill = s.get(FillRow, (account_id, original_id.removeprefix("fill:")))
                order = s.get(Order, fill.order_id) if fill else None
                row = s.get(IntentRow, order.intent_id) if order else None
                owners = [row.payload.get("strategy_id")] if row else []
            known = len(owners) == 1 and bool(owners[0]) and owners[0] != "__UNATTRIBUTED__"
            payload = {"original": original_id, "cash_delta": str(cash_delta), "reason": reason,
                       "owners": owners, "ownership_conflict": not known,
                       "strategy_cash_allocations": {owners[0]: str(cash_delta)} if known else {}}
            old = s.get(EconomicEvent, (account_id,event_id))
            if old:
                # Preserve the immutable digest of legacy corrections on replay.
                payload = {k:v for k,v in payload.items() if k in old.payload}
            if not self.event(s, account, event_id, "CASH_CORRECTION", payload):
                return False
            if account.cash + cash_delta < 0:
                raise SchemaMismatch("NEGATIVE_CASH")
            account.cash += cash_delta
            self.movement(s, account, event_id, "CASH", cash_delta, "CORRECTION")
        return True
