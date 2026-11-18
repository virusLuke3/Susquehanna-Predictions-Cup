from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from typing import Protocol

from sigbot.domain import ZERO, Clock, Fill, Observation, OrderRequest, check_grid, content_hash
from sigbot.errors import Conflict, SchemaMismatch, TransportUnknown


@dataclass
class AccountSnapshot:
    account_id: str
    tournament_id: str
    cash: Decimal
    positions: dict[str, tuple[Decimal, Decimal | None]]
    orders: list[Observation]
    checkpoint: str
    coherent: bool = True
    fills: list[Fill] = field(default_factory=list)
    transactions: list[dict] = field(default_factory=list)
    position_lots: dict[str, list[dict]] = field(default_factory=dict)


class Exchange(Protocol):
    synthetic: bool

    def submit_limit(self, request: OrderRequest) -> Observation: ...
    def cancel_order(self, remote_id: str) -> Observation: ...
    def get_order(self, logical_id: str) -> Observation | None: ...
    def snapshot(self) -> AccountSnapshot: ...


class FakeExchange:
    """D protocol only. Its fields and zero-fee default are NOT SIG semantics."""

    synthetic = True

    def __init__(
        self,
        clock: Clock,
        cash: Decimal = Decimal("1000"),
        account_id: str = "SIM-ACCOUNT",
        tournament_id: str = "SIM-CUP",
        *,
        price_tick: Decimal = Decimal("0.01"),
    ):
        if not account_id.startswith("SIM-") or not tournament_id.startswith("SIM-"):
            raise SchemaMismatch("FAKE_SCOPE_MUST_BE_SIM")
        self.clock, self.cash = clock, cash
        self.account_id, self.tournament_id = account_id, tournament_id
        if price_tick <= 0:
            raise ValueError("Positive FakeExchange price tick required")
        self.price_tick = price_tick
        self.orders: dict[str, Observation] = {}
        self.requests: dict[str, OrderRequest] = {}
        self.positions: dict[str, tuple[Decimal, Decimal | None]] = {}
        self.lots: dict[str, list[list[Decimal]]] = {}
        self.transactions: list[dict] = []
        self.faults: list[str] = []
        self.submit_calls = 0
        self.cancel_calls = 0
        self.revision = 0

    def submit_limit(self, request: OrderRequest) -> Observation:
        self.submit_calls += 1
        if not request.synthetic or not request.instrument_id.startswith("SIM-"):
            raise SchemaMismatch("FAKE_SCOPE_MUST_BE_SIM")
        if (request.account_id, request.tournament_id) != (self.account_id, self.tournament_id):
            raise SchemaMismatch("IDENTITY_MISMATCH")
        check_grid(request.price, request.quantity, self.price_tick, Decimal("1"))
        if request.logical_id in self.requests:
            if self.requests[request.logical_id] != request:
                raise Conflict("IDEMPOTENCY_PAYLOAD_CONFLICT")
            return self.orders[request.logical_id]
        fault = self.faults.pop(0) if self.faults else ""
        if fault == "before_accept":
            raise TransportUnknown("FAKE_TRANSPORT_UNKNOWN")
        rejected = fault == "reject"
        self.revision += 1
        obs = Observation(
            f"SIM-ORDER-{len(self.orders) + 1}",
            request.logical_id,
            request.instrument_id,
            request.side,
            request.quantity,
            request.price,
            ZERO,
            "REJECTED" if rejected else "OPEN",
            self.revision,
        )
        self.orders[request.logical_id], self.requests[request.logical_id] = obs, request
        if fault == "after_accept":
            raise TransportUnknown("FAKE_ACK_LOST")
        return obs

    def get_order(self, logical_id: str) -> Observation | None:
        # Absence is NOT proof of non-acceptance, even in the execution engine.
        return self.orders.get(logical_id)

    def fill(
        self, logical_id: str, fill_id: str, quantity: Decimal, price: Decimal, fee: Decimal = ZERO
    ) -> Observation:
        obs = self.orders[logical_id]
        old = next((f for f in obs.fills if f.fill_id == fill_id), None)
        fill = Fill(
            fill_id,
            obs.order_id,
            obs.instrument_id,
            obs.side,
            quantity,
            price,
            fee,
            old.filled_at if old else self.clock.now,
        )
        if old:
            if old != fill:
                raise Conflict("FILL_ID_CONFLICT")
            return obs
        if (
            obs.state not in ("OPEN", "PARTIAL")
            or quantity <= 0
            or obs.filled + quantity > obs.quantity
        ):
            raise SchemaMismatch("INVALID_FILL")
        if not 0 < price < 1 or fee < 0:
            raise SchemaMismatch("INVALID_FILL")
        if (obs.side == "BUY" and price > obs.limit_price) or (
            obs.side == "SELL" and price < obs.limit_price
        ):
            raise SchemaMismatch("LIMIT_VIOLATION")
        qty, cost = self.positions.get(obs.instrument_id, (ZERO, ZERO))
        if cost is None:
            raise SchemaMismatch("UNKNOWN_COST_BASIS")
        if obs.side == "BUY":
            if self.cash < quantity * price + fee:
                raise SchemaMismatch("FAKE_INSUFFICIENT_CASH")
            self.cash -= quantity * price + fee
            qty, cost = qty + quantity, cost + quantity * price + fee
            self.lots.setdefault(obs.instrument_id, []).append([quantity, quantity * price + fee])
        elif obs.side == "SELL":
            if quantity > qty:
                raise SchemaMismatch("UNCOVERED_SELL")
            remaining, removed_cost = quantity, ZERO
            for lot in self.lots.get(obs.instrument_id, []):
                if not remaining:
                    break
                if not lot[0]:
                    continue
                take = min(remaining, lot[0])
                removed = lot[1] if take == lot[0] else lot[1] * take / lot[0]
                lot[0] -= take
                lot[1] -= removed
                remaining -= take
                removed_cost += removed
            if remaining:
                raise SchemaMismatch("FAKE_FIFO_LOTS_UNKNOWN")
            cost -= removed_cost
            qty -= quantity
            self.cash += quantity * price - fee
        else:
            raise SchemaMismatch("UNKNOWN_SIDE")
        self.positions[obs.instrument_id] = (qty, cost)
        self.revision += 1
        new = replace(
            obs,
            filled=obs.filled + quantity,
            revision=self.revision,
            state="FILLED" if obs.filled + quantity == obs.quantity else "PARTIAL",
            fills=obs.fills + (fill,),
        )
        self.orders[logical_id] = new
        self.transactions.append(
            {
                "id": "tx:" + fill_id,
                "canonical_id": "fill:" + fill_id,
                "kind": "FILL_REFERENCE",
                "payload": asdict(fill),
            }
        )
        return new

    def cancel_order(self, remote_id: str) -> Observation:
        self.cancel_calls += 1
        logical_id = next(k for k, v in self.orders.items() if v.order_id == remote_id)
        if self.faults and self.faults[0] == "cancel_timeout":
            self.faults.pop(0)
            raise TransportUnknown("FAKE_CANCEL_UNKNOWN")
        obs = self.orders[logical_id]
        if obs.state not in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
            self.revision += 1
            obs = replace(obs, state="CANCELED", revision=self.revision)
            self.orders[logical_id] = obs
        return obs

    def snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            self.account_id,
            self.tournament_id,
            self.cash,
            copy.deepcopy(self.positions),
            list(self.orders.values()),
            "SIM-WATERMARK-" + str(self.revision),
            fills=[f for o in self.orders.values() for f in o.fills],
            transactions=copy.deepcopy(self.transactions),
        )

    def settle(
        self,
        instrument_id: str,
        event_id: str,
        outcome: str,
        refundable_cost: Decimal | None = None,
    ) -> dict:
        qty, cost = self.positions.get(instrument_id, (ZERO, ZERO))
        if outcome == "PENDING":
            return {"status": "PENDING"}
        if outcome not in ("YES", "NO", "N/A"):
            raise SchemaMismatch("UNKNOWN_SETTLEMENT")
        signature = content_hash([instrument_id, outcome, refundable_cost])
        prior = next((t for t in self.transactions if t["id"] == event_id), None)
        if prior:
            if prior["signature"] != signature:
                raise Conflict("SETTLEMENT_ID_CONFLICT")
            return prior
        if outcome == "N/A" and refundable_cost is None:
            raise SchemaMismatch("UNKNOWN_REFUND_COST")
        payout = qty if outcome == "YES" else (refundable_cost if outcome == "N/A" else ZERO)
        assert payout is not None
        record = {
            "id": event_id,
            "kind": "SETTLEMENT",
            "instrument_id": instrument_id,
            "quantity": str(qty),
            "cash": str(payout),
            "cost": str(cost),
            "signature": signature,
        }
        self.cash += payout
        self.positions[instrument_id] = (ZERO, ZERO)
        self.transactions.append(record)
        self.revision += 1
        return record
