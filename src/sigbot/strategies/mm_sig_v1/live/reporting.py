"""FIFO attribution and conservative executable valuation; markouts are never added to PnL."""

import json
from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select

from sigbot.domain import TERMINAL, ZERO, utc_time
from sigbot.storage import (
    Account,
    Audit,
    EconomicEvent,
    ExecutionCommand,
    FillRow,
    Instrument,
    IntentRow,
    Order,
    Position,
)
from sigbot.strategies.mm_sig_v1.live.analytics import owned


def side_metrics(orders, observations, fills):
    """Unknown orders and gaps are excluded from confirmed zero-fill/exposure counts."""
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for order in orders:
        key = order.instrument_id
        if key not in result:
            result[key] = {
                side: {
                    "order_count": 0,
                    "accepted_order_count": 0,
                    "actual_filled_shares": ZERO,
                    "confirmed_terminal_no_fill_orders": 0,
                    "confirmed_terminal_partial_fill_orders": 0,
                    "currently_partial_orders": 0,
                    "unresolved_orders": 0,
                    "valid_passive_quote_seconds": ZERO,
                    "filled_shares_on_valid_passive_quotes": ZERO,
                }
                for side in ("BUY", "SELL")
            }
        row = result[key][order.side]
        row["order_count"] += 1
        row["accepted_order_count"] += int(order.remote_id is not None)
        row["actual_filled_shares"] += sum(
            (Decimal(f.payload["quantity"]) for f, o, _ in fills if o.id == order.id), ZERO
        )
        resolved = (
            order.remote_id is not None
            and order.state in TERMINAL
            and order.request_state == "RESOLVED"
        )
        row["confirmed_terminal_no_fill_orders"] += int(resolved and order.filled == ZERO)
        row["confirmed_terminal_partial_fill_orders"] += int(
            resolved and ZERO < order.filled < order.quantity
        )
        row["currently_partial_orders"] += int(order.state == "PARTIAL")
        row["unresolved_orders"] += int(
            order.request_state not in {"ACKNOWLEDGED", "RESOLVED"}
            or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
        )
    grouped = defaultdict(list)
    for obs in observations:
        grouped[obs.data["instrument_id"]].append(obs)
    for instrument, samples in grouped.items():
        if instrument not in result:
            continue
        for before, after in zip(samples, samples[1:], strict=False):
            seconds = Decimal(str((after.at - before.at).total_seconds()))
            if (
                not before.data.get("order_exposure_usable", before.data.get("reference_usable"))
                or not after.data.get("order_exposure_usable", after.data.get("reference_usable"))
                or not ZERO < seconds <= 5
            ):
                continue
            for side, row in result[instrument].items():
                eligible = before.data.get("eligible_orders", {}).get(side, [])
                if not eligible:
                    continue
                row["valid_passive_quote_seconds"] += seconds
                row["filled_shares_on_valid_passive_quotes"] += sum(
                    (
                        Decimal(fill.payload["quantity"])
                        for fill, order, kind in fills
                        if order.id in eligible
                        and order.side == side
                        and kind == "MM_REAL_PASSIVE"
                        and before.at < utc_time(fill.payload["filled_at"]) <= after.at
                    ),
                    ZERO,
                )
    for sides in result.values():
        for row in sides.values():
            seconds = row["valid_passive_quote_seconds"]
            row["observed_filled_shares_per_valid_minute"] = (
                str(row["filled_shares_on_valid_passive_quotes"] * 60 / seconds)
                if seconds
                else None
            )
            for field in (
                "valid_passive_quote_seconds",
                "filled_shares_on_valid_passive_quotes",
                "actual_filled_shares",
            ):
                row[field] = str(row[field])
    return result


def summarize(
    store, account_id, timestamp, books, *, principal=None, reporting_timezone="Asia/Shanghai"
):
    with store.transaction() as session:
        account = session.get(Account, account_id)
        fills = []
        for fill, order, row in session.execute(
            select(FillRow, Order, IntentRow)
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(FillRow.account_id == account_id)
        ):
            if row is not None and owned(row):
                fills.append(
                    (
                        fill,
                        order,
                        json.loads(row.payload.get("note") or "{}").get("kind", "UNKNOWN"),
                    )
                )
        fills.sort(key=lambda item: (utc_time(item[0].payload["filled_at"]), item[0].id))
        own_orders = [
            order
            for order, row in session.execute(
                select(Order, IntentRow)
                .join(IntentRow, Order.intent_id == IntentRow.id)
                .where(Order.account_id == account_id)
            )
            if owned(row)
        ]
        observations = list(
            session.scalars(
                select(Audit)
                .where(
                    Audit.account_id == account_id,
                    Audit.kind == "MM13_OBSERVATION",
                    Audit.data["instrument_id"]
                    .as_string()
                    .in_({o.instrument_id for o in own_orders}),
                    Audit.at >= timestamp - timedelta(minutes=65),
                    Audit.at <= timestamp,
                )
                .order_by(Audit.at)
            )
        )
        lots: dict[str, deque[list[Any]]] = defaultdict(deque)
        realized: dict[str, Decimal] = defaultdict(lambda: ZERO)
        realized_by_day: dict[str, Decimal] = defaultdict(lambda: ZERO)
        sale_pnl: dict[str, Decimal] = defaultdict(lambda: ZERO)
        pair_pnl: dict[str, Decimal] = defaultdict(lambda: ZERO)
        exits = []
        incomplete = set()
        classifications: dict[str, int] = defaultdict(int)
        economic_sides: dict[str, dict[str, Decimal]] = {}
        for fill, order, _ in fills:
            inst = session.get(Instrument, order.instrument_id)
            side = "YES_BID" if (inst.outcome == "YES") == (order.side == "BUY") else "YES_ASK"
            economic_sides.setdefault(inst.exchange_id, {"YES_BID": ZERO, "YES_ASK": ZERO})[
                side
            ] += Decimal(fill.payload["quantity"])
        pairs = list(
            session.scalars(
                select(EconomicEvent).where(
                    EconomicEvent.account_id == account_id,
                    EconomicEvent.kind == "PAIR",
                )
            )
        )
        pair_instruments = {
            iid for event in pairs for iid in (event.payload["left"], event.payload["right"])
        }
        paired_quantity = ZERO

        def consume(iid, quantity, at):
            remainder, consumed = quantity, ZERO
            while remainder and lots[iid]:
                lot = lots[iid][0]
                take = min(remainder, lot[0])
                allocated_cost = lot[1] if take == lot[0] else lot[1] * take / lot[0]
                lot[0] -= take
                lot[1] -= allocated_cost
                remainder -= take
                consumed += allocated_cost
                exits.append((at - lot[2]).total_seconds())
                if not lot[0]:
                    lots[iid].popleft()
            if remainder:
                incomplete.add(iid)
            return consumed

        for fill, order, kind in fills:
            data = fill.payload
            at = utc_time(data["filled_at"])
            quantity = Decimal(data["quantity"])
            cost = quantity * Decimal(data["price"])
            classifications[kind] += 1
            if order.side == "BUY":
                lots[order.instrument_id].append([quantity, cost + Decimal(data["fee"]), at, kind])
                inst = session.get(Instrument, order.instrument_id)
                if order.instrument_id in pair_instruments:
                    for other in session.scalars(
                        select(Instrument).where(
                            Instrument.tournament == inst.tournament,
                            Instrument.exchange_id == inst.exchange_id,
                            Instrument.outcome != inst.outcome,
                        )
                    ):
                        paired = min(
                            sum((lot[0] for lot in lots[inst.id]), ZERO),
                            sum((lot[0] for lot in lots[other.id]), ZERO),
                        )
                        if paired:
                            net = (
                                paired
                                - consume(inst.id, paired, at)
                                - consume(other.id, paired, at)
                            )
                            realized["BINARY_PAIR_NETTING"] += net
                            realized_by_day[
                                at.astimezone(ZoneInfo(reporting_timezone)).date().isoformat()
                            ] += net
                            pair_pnl[inst.exchange_id] += net
                            paired_quantity += paired
                continue
            consumed = consume(order.instrument_id, quantity, at)
            net = cost - Decimal(data["fee"]) - consumed
            realized[kind] += net
            realized_by_day[at.astimezone(ZoneInfo(reporting_timezone)).date().isoformat()] += net
            sale_pnl[order.instrument_id] += net
        if paired_quantity != sum((Decimal(event.payload["quantity"]) for event in pairs), ZERO):
            incomplete.update(pair_instruments)
        own_ids = {o.id for o in own_orders}
        decisions = list(
            session.scalars(
                select(ExecutionCommand).where(
                    ExecutionCommand.account_id == account_id,
                    ExecutionCommand.strategy_id == "mm_sig",
                    ExecutionCommand.action == "CANCEL",
                    or_(
                        ExecutionCommand.order_id.in_(own_ids),
                        ExecutionCommand.payload["order_id"].as_string().in_(own_ids),
                    ),
                )
            )
        )
        cancels: dict[str, int] = defaultdict(int)
        revoked = 0
        for command in decisions:
            cancels[command.payload.get("reason", "UNKNOWN")] += 1
            revoked += int(command.state == "SUPERSEDED_BEFORE_SEND")
        market_sides = side_metrics(own_orders, observations, fills)
        by_id = {order.id: order for order in own_orders}
        for iid, sides in market_sides.items():
            for side, metrics in sides.items():
                commands = [
                    c
                    for c in decisions
                    if (o := by_id.get(c.order_id or c.payload.get("order_id")))
                    and (o.instrument_id, o.side) == (iid, side)
                ]
                metrics["cancel_proposals_by_reason"] = {
                    reason: sum(c.payload.get("reason", "UNKNOWN") == reason for c in commands)
                    for reason in {c.payload.get("reason", "UNKNOWN") for c in commands}
                }
                metrics["cancel_proposals_revoked_before_send"] = sum(
                    c.state == "SUPERSEDED_BEFORE_SEND" for c in commands
                )
                metrics["confirmed_canceled_orders"] = sum(
                    o.instrument_id == iid
                    and o.side == side
                    and o.cancel_state == "CANCEL_CONFIRMED"
                    for o in own_orders
                )
                metrics["fifo_realized_sale_pnl_after_fees"] = (
                    str(sale_pnl[iid]) if side == "SELL" and iid not in incomplete else None
                )
        observed_id = Audit.data["order"].as_string()

        def first_observed(states):
            return dict(
                session.execute(
                    select(observed_id, func.min(Audit.at))
                    .where(
                        Audit.account_id == account_id,
                        Audit.kind == "ORDER_OBSERVED",
                        observed_id.in_(own_ids),
                        Audit.data["state"].as_string().in_(states),
                    )
                    .group_by(observed_id)
                ).all()
            )

        opened, closed = first_observed(["OPEN", "PARTIAL"]), first_observed(list(TERMINAL))
        lifetimes = [
            max(0, (closed[oid] - at).total_seconds())
            for oid, at in opened.items()
            if oid in closed
        ]
        markouts = []
        for fill, order, kind in fills:
            at = utc_time(fill.payload["filled_at"])
            for horizon in (5, 30, 60, 300):
                future = next(
                    (
                        o
                        for o in observations
                        if o.data["instrument_id"] == order.instrument_id
                        and o.data.get("price_reference_usable")
                        and o.data.get("mid") is not None
                        and horizon <= (o.at - at).total_seconds() <= horizon + 5
                    ),
                    None,
                )
                markouts.append(
                    {
                        "fill_id": fill.id,
                        "instrument_id": order.instrument_id,
                        "side": order.side,
                        "kind": kind,
                        "horizon_seconds": horizon,
                        "status": "OBSERVED" if future else "NOT_OBSERVED",
                        "net_markout_per_share": str(
                            (Decimal(future.data["mid"]) - Decimal(fill.payload["price"]))
                            * (1 if order.side == "BUY" else -1)
                            - Decimal(fill.payload["fee"]) / Decimal(fill.payload["quantity"])
                        )
                        if future
                        else None,
                    }
                )
        positions = list(session.scalars(select(Position).where(Position.account_id == account_id)))
        funds = None
        if principal is not None:
            from sigbot.accounting import strategy_finances

            funds = strategy_finances(session, account_id, "mm_sig", principal)
            positions = [p for p in positions if any(lot[0] for lot in lots[p.instrument_id])]
        inventory, unvalued, priced = {}, {}, ZERO
        for position in positions:
            owned_lots: Any = lots.get(position.instrument_id, [])
            quantity = (
                sum((lot[0] for lot in owned_lots), ZERO)
                if funds is not None
                else position.quantity
            )
            cost = sum((lot[1] for lot in owned_lots), ZERO) if funds is not None else position.cost
            if not quantity:
                continue
            book = books.get(position.instrument_id)
            instrument = session.get(Instrument, position.instrument_id)
            remaining, value = quantity, ZERO
            if (
                book
                and instrument.fee_per_share is not None
                and book.continuity_verified
                and book.validated_at
                and 0 <= (timestamp - book.validated_at).total_seconds() <= 5
            ):
                for level in sorted(book.bids, key=lambda level: level.price, reverse=True):
                    take = min(remaining, level.quantity - level.own_quantity)
                    value += take * max(ZERO, level.price - instrument.fee_per_share)
                    remaining -= take
            priced += value
            if remaining:
                unvalued[position.instrument_id] = str(remaining)
            if owned_lots and (
                sum((lot[0] for lot in owned_lots), ZERO) != quantity
                or sum((lot[1] for lot in owned_lots), ZERO) != cost
                or quantity > position.quantity
            ):
                incomplete.add(position.instrument_id)
            inventory[position.instrument_id] = {
                "quantity": str(quantity),
                "fifo_cost": str(cost),
                "priced_quantity": str(quantity - remaining),
                "visible_executable_value_after_fee_bound": str(value),
                "priced_inventory_revaluation_vs_allocated_fifo_cost": str(
                    value - cost * (quantity - remaining) / quantity
                )
                if cost is not None
                else None,
                "oldest_owned_lot_seconds": max(
                    (timestamp - lot[2]).total_seconds() for lot in owned_lots
                )
                if owned_lots
                else None,
            }
        return {
            "layer": "REAL_READ" if store.namespace == "live" else "MOCK",
            "as_of": timestamp.isoformat(),
            "cash": str(funds["cash"] if funds is not None else account.cash),
            "account_cash": str(account.cash),
            "strategy_finances": {k: str(v) for k, v in funds.items()}
            if funds is not None
            else None,
            "cash_plus_priced_inventory_after_fee_bound": str(
                (funds["cash"] if funds is not None else account.cash) + priced
            ),
            "unvalued_shares": unvalued,
            "inventory": inventory,
            "fifo_realized_pnl_by_execution_intent": {
                k: str(v) if not incomplete else None for k, v in realized.items()
            },
            "fifo_realized_pnl_by_reporting_day": {
                k: str(v) if not incomplete else None for k, v in realized_by_day.items()
            },
            "fifo_reporting_timezone": reporting_timezone,
            "pnl_status": "PARTIAL" if incomplete else "RECONSTRUCTED_FROM_OWN_FILLS",
            "fifo_attribution_unresolved": sorted(incomplete),
            "fill_count_by_intent": dict(classifications),
            "per_market_side_execution": market_sides,
            "binary_pair_net_pnl_after_fees_by_exchange": {
                eid: str(net) if not incomplete else None for eid, net in pair_pnl.items()
            },
            "binary_pair_pnl_side_attribution": "JOINT_YES_NO_CYCLE_NOT_ASSIGNED_TO_ONE_SIDE",
            "filled_shares_by_yes_economic_side": {
                eid: {side: str(quantity) for side, quantity in sides.items()}
                for eid, sides in economic_sides.items()
            },
            "cancel_proposals_by_reason": dict(cancels),
            "cancel_proposals_revoked_before_send": revoked,
            "cancel_proposals_are_not_venue_cancel_receipts": True,
            "confirmed_open_to_terminal_observation_seconds": {
                "completed_orders": len(lifetimes),
                "median": median(lifetimes) if lifetimes else None,
                "minimum": min(lifetimes) if lifetimes else None,
                "maximum": max(lifetimes) if lifetimes else None,
                "basis": "LOCAL_FIRST_OPEN_AND_FIRST_TERMINAL_RECEIPTS_NOT_EXACT_VENUE_LIFETIME",
            },
            "actual_fill_markouts": markouts,
            "binary_pair_redemption_cash": str(
                sum((Decimal(e.payload["payout"]) for e in pairs), ZERO)
            ),
            "binary_pair_quantity": str(paired_quantity),
            "binary_inventory": {
                eid: {
                    "signed_yes_shares": str(
                        sum(
                            (
                                p.quantity
                                * (
                                    1
                                    if session.get(Instrument, p.instrument_id).outcome == "YES"
                                    else -1
                                )
                                for p in positions
                                if session.get(Instrument, p.instrument_id).exchange_id == eid
                            ),
                            ZERO,
                        )
                    ),
                    "remaining_cost": str(
                        sum(
                            (
                                p.cost or ZERO
                                for p in positions
                                if session.get(Instrument, p.instrument_id).exchange_id == eid
                            ),
                            ZERO,
                        )
                    ),
                }
                for eid in {session.get(Instrument, p.instrument_id).exchange_id for p in positions}
            },
            "valid_quote_statistics_window_seconds": 3900,
            "order_counts_include_all_owned_versions": True,
            "maker_closed_pnl": "UNKNOWN_UNLESS_BOTH_FILL_ROLES_CONFIRMED",
            "execution_role_for_unconfirmed_passive_intents": "TAKER_OR_ROLE_UNKNOWN",
            "median_observed_fifo_exit_seconds": median(exits) if exits else None,
            "p95_observed_fifo_exit_seconds": sorted(exits)[(95 * len(exits) + 99) // 100 - 1]
            if exits
            else None,
            "longest_observed_fifo_exit_seconds": max(exits) if exits else None,
            "markouts_added_to_realized_pnl": False,
            "settlement_inventory_is_cash": False,
            "settlement_pnl": "NOT_DERIVED_FROM_OFFICIAL_SETTLEMENT",
            "profit_improvement_verified": False,
        }
