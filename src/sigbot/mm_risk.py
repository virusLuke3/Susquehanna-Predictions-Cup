"""MM-only limits and exit arbitration over the existing physical ledger/OMS.

Callers hold the account transaction lock. No exchange adapter or cash ledger is
introduced here. One parent/episode spans a binary instrument's complete cycle,
including both outcomes, replaces, delayed fills and unresolved requests.
"""

import json
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import inspect, select
from sqlalchemy.orm import load_only

from sigbot.accounting import lot_origins, strategy_finances
from sigbot.domain import TERMINAL, ZERO, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, RiskRejected
from sigbot.mm_risk_policy import MmRiskPolicy
from sigbot.storage import (
    EconomicEvent,
    FillRow,
    Instrument,
    IntentRow,
    MmRiskEpisode,
    MmRiskOrder,
    MmRiskState,
    Order,
)


def decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise RiskRejected("MM_RISK_NONFINITE_INPUT")
    return result


def floor(value, step):
    return (max(ZERO, value) / step).to_integral_value(rounding=ROUND_FLOOR) * step


def encoded(value):
    if isinstance(value, dict):
        return {str(k): encoded(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encoded(v) for v in value]
    return str(value) if isinstance(value, Decimal) else value


def own_orders(session, account_id):
    return list(
        session.scalars(
            select(Order)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                Order.account_id == account_id,
                IntentRow.payload["strategy_id"].as_string() == "mm_sig",
            )
        )
    )


def possible(order):
    return bool(
        order.reserve_cash
        or order.reserve_quantity
        or order.state not in TERMINAL
        or order.request_state != "RESOLVED"
    )


def checked_policy(policy, live=False):
    if policy.missing():
        raise RiskRejected("MM_RISK_PARAMETERS_REQUIRED:" + ",".join(policy.missing()))
    if live and not policy.live_authorized:
        raise AuthorizationRequired("MM_RISK_CANDIDATE_NOT_LIVE_AUTHORIZED")
    if live and policy.new_orders_until is None:
        raise AuthorizationRequired("MM_RISK_VERIFIED_ORDER_DEADLINE_REQUIRED")


def installed_state(session, account_id):
    # Feature-off code may precede the additive migration. Never cache absence:
    # once installed, removing configuration must still block MM new risk.
    if not inspect(session.connection()).has_table(MmRiskState.__tablename__):
        return None
    return session.get(MmRiskState, account_id)


def deadline(ends_at, policy):
    return min(ends_at, policy.new_orders_until) if policy.new_orders_until else ends_at


def levels(inst, now, *, stress=False, policy=None):
    """No last/mid valuation. Return external price/quantity and coverage quality."""
    verified = (
        utc_time(inst.book["validated_at"])
        if inst.book.get("continuity_verified") and inst.book.get("validated_at")
        else inst.book_at
    )
    valid = (
        inst.quality == "HEALTHY"
        and inst.book_at is not None
        and verified is not None
        and utc_time(inst.book_at) <= utc_time(verified)
        and 0 <= (now - utc_time(verified)).total_seconds() <= 5
        and inst.book.get("own_orders_known") is True
        and inst.fee_per_share is not None
    )
    if not valid:
        return [], False
    if stress and policy.stress_mode == "FULL_PAID_COST_FALLBACK":
        return [], True  # Explicit hypothetical stress proceeds, never an actual NAV mark/fill.
    result = []
    for row in inst.book.get("bids", []):
        quantity = decimal(row["quantity"])
        own = decimal(row.get("own_quantity", quantity if row.get("own") else ZERO))
        price = decimal(row["price"])
        if quantity < own or own < ZERO or price <= ZERO or price >= 1:
            raise RiskRejected("MM_RISK_INVALID_BOOK")
        if stress:
            quantity = floor((quantity - own) * policy.stress_depth_fraction, inst.step)
            price = max(ZERO, price - inst.tick * policy.stress_price_ticks)
        else:
            quantity -= own
        if quantity:
            result.append((price, quantity))
    return sorted(result, reverse=True), True


def liquidate(inst, quantity, now, policy, *, stress=False):
    rows, current = levels(inst, now, stress=stress, policy=policy)
    remaining, value = quantity, ZERO
    for price, available in rows:
        take = min(remaining, available)
        value += take * max(ZERO, price - inst.fee_per_share)
        remaining -= take
        if not remaining:
            break
    return value, remaining, current


def portfolio(session, account, policy, now, *, exclude_order=None):
    funds = strategy_finances(
        session, account.id, "mm_sig", policy.principal, exclude_order=exclude_order
    )
    instruments = {
        i.id: i
        for i in session.scalars(
            select(Instrument).where(Instrument.tournament == account.tournament).options(
                load_only(
                    Instrument.id, Instrument.exchange_id, Instrument.event_group,
                    Instrument.fee_per_share, Instrument.step, Instrument.tick,
                )
            )
        )
    }
    if exclude_order:
        excluded = session.get(Order, exclude_order)
        if excluded and excluded.side == "BUY":
            fee = instruments[excluded.instrument_id].fee_per_share
            if fee is not None:
                funds["available"] += min(
                    funds["future_fee_reserve"], (excluded.quantity - excluded.filled) * fee
                )
    lots = [
        (lot, origin, fill)
        for lot, origin, fill in lot_origins(session, account.id)
        if origin.get("strategy_id") == "mm_sig"
    ]
    owned = defaultdict(lambda: {"quantity": ZERO, "cost": ZERO, "oldest_at": now})
    for lot, _, fill in lots:
        if lot.cost is None or not fill.get("filled_at"):
            raise RiskRejected("MM_RISK_LOT_BASIS_UNKNOWN")
        data = owned[lot.instrument_id]
        data["quantity"] += lot.quantity
        data["cost"] += lot.cost
        data["oldest_at"] = min(data["oldest_at"], utc_time(fill["filled_at"]))
    pending = [o for o in own_orders(session, account.id) if o.id != exclude_order and possible(o)]
    exposures = {}
    events = defaultdict(lambda: {"cost": ZERO, "stress": ZERO, "full_loss": ZERO})
    value = ZERO
    complete = funds["ownership_valid"]
    for iid in set(owned) | {o.instrument_id for o in pending if o.side == "BUY"}:
        inst = instruments[iid]
        if inst.fee_per_share is None:
            raise RiskRejected("MM_RISK_EXPOSURE_FEE_UNKNOWN")
        data = owned[iid]
        buy = [o for o in pending if o.instrument_id == iid and o.side == "BUY"]
        if any(
            o.reserve_cash < (o.quantity - o.filled) * (o.price + o.fee_bound / o.quantity)
            for o in buy
        ):
            raise RiskRejected("MM_RISK_UNCOVERED_PENDING_BUY")
        pending_quantity = sum((o.quantity - o.filled for o in buy), ZERO)
        cash = sum((o.reserve_cash for o in buy), ZERO)
        net, unpriced, current = liquidate(inst, data["quantity"], now, policy)
        exposure_policy = effective_policy(policy, find_episode(session, account.id, inst))
        stress_value, stress_unpriced, _ = liquidate(
            inst, data["quantity"] + pending_quantity, now, exposure_policy, stress=True
        )
        complete = complete and not unpriced
        total_cost = data["cost"] + cash
        item = {
            **data,
            "pending_quantity": pending_quantity,
            "pending_cash": cash,
            "net_value": net,
            "unpriced_quantity": unpriced,
            "book_current": current,
            "cost_with_pending": total_cost,
            "stress": max(ZERO, total_cost - stress_value) + stress_unpriced * inst.fee_per_share,
            "full_loss": total_cost + (data["quantity"] + pending_quantity) * inst.fee_per_share,
        }
        exposures[iid] = item
        value += net
        for key, source in [
            ("cost", "cost_with_pending"),
            ("stress", "stress"),
            ("full_loss", "full_loss"),
        ]:
            events[inst.event_group][key] += item[source]
    realized = defaultdict(lambda: ZERO)
    for fill, event_group in session.execute(
        select(FillRow, Instrument.event_group)
        .join(Order, FillRow.order_id == Order.id)
        .join(IntentRow, Order.intent_id == IntentRow.id)
        .join(Instrument, Order.instrument_id == Instrument.id)
        .where(
            FillRow.account_id == account.id,
            IntentRow.payload["strategy_id"].as_string() == "mm_sig",
        )
    ):
        p = fill.payload
        value_at_fill = decimal(p["quantity"]) * decimal(p["price"])
        realized[event_group] += (
            value_at_fill if p["side"] == "SELL" else -value_at_fill
        ) - decimal(p["fee"])
    for iid, data in owned.items():
        realized[instruments[iid].event_group] += data["cost"]
    for event in session.scalars(
        select(EconomicEvent).where(
            EconomicEvent.account_id == account.id,
            EconomicEvent.kind.in_(["PAIR", "SETTLEMENT", "CASH_CORRECTION"]),
        )
    ):
        amount = decimal(event.payload.get("strategy_cash_allocations", {}).get("mm_sig", "0"))
        if not amount:
            continue
        iid = event.payload.get("left", event.payload.get("instrument"))
        if iid not in instruments and event.kind == "CASH_CORRECTION":
            original = session.get(EconomicEvent, (account.id, event.payload["original"]))
            if original and original.kind == "FILL":
                iid = original.payload.get("instrument_id")
        if iid not in instruments:
            complete = False
        else:
            realized[instruments[iid].event_group] += amount
    for key, value_realized in realized.items():
        events[key]["realized_pnl"] = value_realized
        events[key]["stress"] += max(ZERO, -value_realized)
        events[key]["full_loss"] += max(ZERO, -value_realized)
    # Uncovered quantity is explicit, never reported as an actual zero-price fill.
    return {
        "funds": funds,
        "instruments": instruments,
        "owned": owned,
        "pending": pending,
        "exposures": exposures,
        "events": events,
        "gross_cost": sum((v["cost_with_pending"] for v in exposures.values()), ZERO),
        "stress": sum((v["stress"] for v in events.values()), ZERO),
        "full_loss": sum((v["full_loss"] for v in events.values()), ZERO),
        "economic_equity_lower_bound": funds["cash"] + value,
        "valuation_complete": complete,
    }


def pool_state(session, account, policy, now, book, *, persist=True):
    day = now.astimezone(ZoneInfo(policy.reporting_timezone)).date().isoformat()
    row = session.get(MmRiskState, account.id)
    equity = book["economic_equity_lower_bound"] - book["funds"]["external_flow"]
    if row is None:
        if not policy.migration_basis_approved or policy.migration_hwm is None:
            raise RiskRejected("MM_RISK_MIGRATION_BASELINE_REQUIRED")
        if not book["valuation_complete"]:
            raise RiskRejected("MM_RISK_MIGRATION_NAV_INCOMPLETE")
        base = (
            policy.migration_day_equity
            if policy.migration_day == day and policy.migration_day_equity is not None
            else equity
        )
        historical = session.scalar(
            select(FillRow.id)
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                FillRow.account_id == account.id,
                IntentRow.payload["strategy_id"].as_string() == "mm_sig",
            )
            .limit(1)
        )
        if policy.migration_day_method == "DEPLOYMENT_NAV_WITH_CARRIED_REALIZED_LOSS":
            if policy.migration_carried_realized_loss is None or policy.migration_day != day:
                raise RiskRejected("MM_RISK_CARRIED_REALIZED_LOSS_REQUIRED")
            base = equity + policy.migration_carried_realized_loss
        elif historical and (policy.migration_day != day or policy.migration_day_equity is None):
            raise RiskRejected("MM_RISK_DAY_MIGRATION_BASELINE_REQUIRED")
        data = {
            "policy_hash": policy.digest,
            "principal": str(policy.principal),
            "hwm": str(max(policy.migration_hwm, equity)),
            "day": day,
            "day_equity": str(base),
            "external_flow": "0",
            "latched": False,
            "migration": {
                "hwm": str(policy.migration_hwm),
                "day_equity": str(base),
                "basis_explicitly_approved": True,
                "day_method": policy.migration_day_method,
                "calendar_day_opening_nav_known": policy.migration_day_method
                == "VERIFIED_DAY_OPEN",
            },
            "events": {},
        }
        row = MmRiskState(account_id=account.id, updated_at=now, data=data)
        if persist:
            session.add(row)
    data = dict(row.data)
    if decimal(data["principal"]) != policy.principal:
        raise RiskRejected("MM_RISK_PRINCIPAL_CHANGE_REQUIRES_MIGRATION")
    if data["day"] != day:
        # Day rolls only after an actual complete valuation; HWM/latch never roll.
        if not book["valuation_complete"]:
            raise RiskRejected("MM_RISK_DAY_NAV_INCOMPLETE")
        last_complete_at = utc_time(data.get("last_complete_at", row.updated_at.isoformat()))
        gap = (now - last_complete_at).total_seconds()
        data.update(
            day=day,
            day_equity=data.get("last_complete_equity", str(equity)),
            day_baseline_pending=gap > 45,
            day_baseline_observed_at=last_complete_at.isoformat(),
            day_baseline_method="LAST_COMPLETE_PRE_BOUNDARY_OBSERVATION",
        )
    if book["valuation_complete"]:
        data["hwm"] = str(max(decimal(data["hwm"]), equity))
        daily = decimal(data["day_equity"]) - equity
        drawdown = decimal(data["hwm"]) - equity
        reason = (
            "MM_DAILY_LOSS"
            if daily >= policy.daily_loss
            else "MM_HWM_DRAWDOWN"
            if drawdown >= policy.drawdown
            else None
        )
        if reason and not data.get("latched"):
            data.update(latched=True, reason=reason, latched_at=now.isoformat())
        data.update(daily_loss=str(daily), drawdown=str(drawdown))
        data["last_complete_equity"] = str(equity)
        data["last_complete_at"] = now.isoformat()
    event_states = dict(data.get("events", {}))
    for key, values in book["events"].items():
        current = dict(event_states.get(key, {}))
        event_exposures = [
            v for iid, v in book["exposures"].items() if book["instruments"][iid].event_group == key
        ]
        loss = -values.get("realized_pnl", ZERO) + sum(
            (v["cost"] - v["net_value"] for v in event_exposures), ZERO
        )
        if (
            all(not v["unpriced_quantity"] for v in event_exposures)
            and loss >= policy.event_economic_loss
        ):
            current.update(latched=True, reason="EVENT_ECONOMIC_LOSS", at=now.isoformat())
        if current:
            event_states[key] = current
    data["events"] = event_states
    data.update(
        valuation_complete=book["valuation_complete"],
        economic_equity_lower_bound=str(equity),
        policy_hash=policy.digest,
    )
    if persist:
        row.data, row.updated_at = encoded(data), now
    return data


def find_episode(session, account_id, inst):
    rows = list(
        session.scalars(
            select(MmRiskEpisode).where(
                MmRiskEpisode.account_id == account_id,
                MmRiskEpisode.closed_at.is_(None),
                MmRiskEpisode.data["exchange_id"].as_string() == inst.exchange_id,
            )
        )
    )
    if len(rows) > 1:
        raise RiskRejected("MM_RISK_DUPLICATE_EPISODE")
    return rows[0] if rows else None


def episode_orders(session, episode):
    ids = set(episode.data.get("legacy_order_ids", []))
    ids.update(
        session.scalars(select(MmRiskOrder.order_id).where(MmRiskOrder.episode_id == episode.id))
    )
    return ids


def episode_cash(session, account_id, episode, owned_cost):
    ids = episode_orders(session, episode)
    spent, net = ZERO, ZERO
    fills = list(
        session.scalars(
            select(FillRow).where(FillRow.account_id == account_id, FillRow.order_id.in_(ids))
        )
    )
    for fill in fills:
        p = fill.payload
        amount = decimal(p["quantity"]) * decimal(p["price"])
        fee = decimal(p["fee"])
        if p["side"] == "BUY":
            spent += amount + fee
            net -= amount + fee
        else:
            net += amount - fee
            spent += fee
    if fills:
        for pair in session.scalars(
            select(EconomicEvent).where(
                EconomicEvent.account_id == account_id,
                EconomicEvent.id.in_(["pair:" + f.id for f in fills]),
            )
        ):
            # A real automatic pair is not proof that its cash belongs to MM.
            # Mixed/unattributed pairs remain cash suspense in strategy_finances.
            net += decimal(pair.payload.get("strategy_cash_allocations", {}).get("mm_sig", "0"))
    return spent, net + owned_cost


def ensure_episode(session, account, inst, policy, now, book):
    episode = find_episode(session, account.id, inst)
    ids = {i.id for i in book["instruments"].values() if i.exchange_id == inst.exchange_id}
    if episode is None:
        original_ids = set(
            session.scalars(
                select(FillRow.order_id).where(
                    FillRow.account_id == account.id,
                    FillRow.id.in_(
                        [
                            lot.economic_id.removeprefix("fill:")
                            for lot, origin, _ in lot_origins(session, account.id, ids)
                            if origin.get("strategy_id") == "mm_sig"
                        ]
                    ),
                )
            )
        )
        original_ids.update(o.id for o in book["pending"] if o.instrument_id in ids)
        stamps = [
            v["oldest_at"] for iid, v in book["owned"].items() if iid in ids and v["quantity"]
        ]
        start = min(stamps) if stamps else now
        original_stamps = [
            utc_time(fill.payload["filled_at"])
            for fill in session.scalars(
                select(FillRow).where(
                    FillRow.account_id == account.id, FillRow.order_id.in_(original_ids)
                )
            )
        ]
        original_stamps += [
            utc_time(session.get(IntentRow, o.intent_id).payload["created_at"])
            for o in book["pending"]
            if o.id in original_ids
        ]
        if original_stamps:
            start = min(start, *original_stamps)
        # Include actual historical sales in the same still-open legacy cycle.
        for fill in session.scalars(
            select(FillRow)
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                FillRow.account_id == account.id,
                Order.instrument_id.in_(ids),
                IntentRow.payload["strategy_id"].as_string() == "mm_sig",
            )
        ):
            if utc_time(fill.payload["filled_at"]) >= start:
                original_ids.add(fill.order_id)
        key = ("SIM-MM-RISK-" if account.id.startswith("SIM-") else "SIG-MM-RISK-") + str(uuid4())
        episode = MmRiskEpisode(
            id=key,
            account_id=account.id,
            instrument_id=inst.id,
            event_key=inst.event_group,
            started_at=start,
            data={
                "parent_id": key,
                "exchange_id": inst.exchange_id,
                "instrument_ids": sorted(ids),
                "policy": policy.model_dump(mode="json"),
                "legacy_order_ids": sorted(original_ids),
                "exit_claims": {},
                "latched": False,
            },
        )
        session.add(episode)
        session.flush()
    return episode


def effective_policy(policy, episode):
    if episode is None:
        return policy
    frozen = MmRiskPolicy.model_validate(episode.data["policy"])
    changes = {
        name: min(getattr(policy, name), getattr(frozen, name))
        for name in [
            "parent_cash",
            "parent_equity_fraction",
            "event_gross_cost",
            "mm_gross_cost",
            "episode_planned_loss",
            "episode_stress_loss",
            "event_stress_loss",
            "mm_stress_loss",
            "event_economic_loss",
            "episode_full_loss",
            "event_full_loss",
            "mm_full_loss",
            "daily_loss",
            "drawdown",
            "entry_wait_seconds",
            "holding_seconds",
        ]
    }
    changes.update(
        target_profit_ticks=frozen.target_profit_ticks,
        no_new_seconds=max(policy.no_new_seconds, frozen.no_new_seconds),
        reduce_only_seconds=max(policy.reduce_only_seconds, frozen.reduce_only_seconds),
        urgent_seconds=max(policy.urgent_seconds, frozen.urgent_seconds),
    )
    if "FULL_PAID_COST_FALLBACK" in {policy.stress_mode, frozen.stress_mode}:
        changes.update(
            stress_mode="FULL_PAID_COST_FALLBACK",
            stress_depth_fraction=None,
            stress_price_ticks=None,
        )
    else:
        changes.update(
            stress_depth_fraction=min(policy.stress_depth_fraction, frozen.stress_depth_fraction),
            stress_price_ticks=max(policy.stress_price_ticks, frozen.stress_price_ticks),
        )
    if frozen.new_orders_until:
        changes["new_orders_until"] = deadline(frozen.new_orders_until, policy)
    return policy.model_copy(update=changes)


def exposure_for(book, ids):
    return {
        key: sum((v[source] for iid, v in book["exposures"].items() if iid in ids), ZERO)
        for key, source in [
            ("cost", "cost_with_pending"),
            ("stress", "stress"),
            ("full_loss", "full_loss"),
        ]
    }


def size(session, account, inst, policy, now, old_quantity, price, *, exclude_order=None):
    checked_policy(policy, account.namespace == "live")
    if inst.fee_per_share is None:
        raise RiskRejected("MM_RISK_TARGET_FEE_UNKNOWN")
    book = portfolio(session, account, policy, now, exclude_order=exclude_order)
    state = pool_state(session, account, policy, now, book)
    episode = find_episode(session, account.id, inst)
    binary_ids = {i.id for i in book["instruments"].values() if i.exchange_id == inst.exchange_id}
    if episode is None and (
        any(v["quantity"] for iid, v in book["owned"].items() if iid in binary_ids)
        or any(o.instrument_id in binary_ids for o in book["pending"])
    ):
        # Bootstrap legacy consumption BEFORE sizing, never after admitting an
        # upgrade's first child order. Prior sells do not refund parent cash.
        episode = ensure_episode(session, account, inst, policy, now, book)
    effective = effective_policy(policy, episode)
    exposure = exposure_for(book, binary_ids)
    held_cost = sum((v["cost"] for iid, v in book["owned"].items() if iid in binary_ids), ZERO)
    spent, realized = (
        episode_cash(session, account.id, episode, held_cost) if episode else (ZERO, ZERO)
    )
    if episode and book["valuation_complete"]:
        loss = -realized + sum(
            (
                v["cost"] - v["net_value"]
                for iid, v in book["exposures"].items()
                if iid in binary_ids
            ),
            ZERO,
        )
        if loss >= effective.episode_planned_loss:
            episode.data = {
                **episode.data,
                "latched": True,
                "latched_reason": "ECONOMIC_STOP",
                "latched_at": now.isoformat(),
            }
    episode_pending = sum(
        (
            o.reserve_cash
            for o in book["pending"]
            if o.instrument_id in binary_ids and o.side == "BUY"
        ),
        ZERO,
    )
    event = book["events"][inst.event_group]
    reasons = []
    if state.get("latched"):
        reasons.append(state.get("reason", "MM_RISK_LATCHED"))
    if episode and episode.data.get("latched"):
        reasons.append("MM_EPISODE_EXIT_LATCHED")
    if any(
        v["quantity"] and (now - v["oldest_at"]).total_seconds() >= 3600
        for iid, v in book["owned"].items()
        if iid in binary_ids
    ):
        reasons.append("MM_AGE_NO_ADD")
    if episode and now >= episode.started_at + timedelta(
        seconds=effective.entry_wait_seconds + effective.holding_seconds
    ):
        reasons.append("MM_PARENT_HOLD_DEADLINE")
    if not book["valuation_complete"]:
        reasons.append("MM_NAV_INCOMPLETE")
    if state.get("events", {}).get(inst.event_group, {}).get("latched"):
        reasons.append("MM_EVENT_LOSS_LATCHED")
    if state.get("day_baseline_pending"):
        reasons.append("MM_DAY_BOUNDARY_BASELINE_REVIEW_REQUIRED")
    if not levels(inst, now)[1]:
        reasons.append("MM_TARGET_BOOK_UNAVAILABLE")
    unit = price + inst.fee_per_share
    cash_unit = unit + inst.fee_per_share
    future_exit_fees = sum(
        (
            (v["quantity"] + v["pending_quantity"]) * book["instruments"][iid].fee_per_share
            for iid, v in book["exposures"].items()
            if iid in binary_ids and book["instruments"][iid].fee_per_share is not None
        ),
        ZERO,
    )
    equity_budget = min(policy.principal, book["economic_equity_lower_bound"])
    caps = {
        "OLD_QUANTITY": old_quantity,
        "CASH": book["funds"]["available"] / cash_unit,
        "PARENT_CASH": max(ZERO, effective.parent_cash - spent - episode_pending - future_exit_fees)
        / cash_unit,
        "PARENT_EQUITY": max(
            ZERO,
            effective.parent_equity_fraction * equity_budget
            - spent
            - episode_pending
            - future_exit_fees,
        )
        / cash_unit,
        "EVENT_GROSS": max(ZERO, effective.event_gross_cost - event["cost"]) / unit,
        "MM_GROSS": max(ZERO, effective.mm_gross_cost - book["gross_cost"]) / unit,
        "EPISODE_FULL_LOSS": max(
            ZERO, effective.episode_full_loss - exposure["full_loss"] + min(ZERO, realized)
        )
        / cash_unit,
        "EVENT_FULL_LOSS": max(ZERO, effective.event_full_loss - event["full_loss"]) / cash_unit,
        "MM_FULL_LOSS": max(ZERO, effective.mm_full_loss - book["full_loss"]) / cash_unit,
    }
    high = floor(min(caps.values()), inst.step)

    loss_budgets = {
        "EPISODE_PLANNED_LOSS": effective.episode_planned_loss,
        "EPISODE_STRESS_LOSS": effective.episode_stress_loss,
        "EVENT_STRESS_LOSS": effective.event_stress_loss,
        "MM_STRESS_LOSS": effective.mm_stress_loss,
    }

    def losses(quantity):
        existing = book["exposures"].get(
            inst.id,
            {
                "quantity": ZERO,
                "pending_quantity": ZERO,
                "cost": ZERO,
                "pending_cash": ZERO,
                "stress": ZERO,
            },
        )
        cost = existing["cost"] + existing["pending_cash"] + quantity * unit
        value, unpriced, _ = liquidate(
            inst,
            existing["quantity"] + existing["pending_quantity"] + quantity,
            now,
            effective,
            stress=True,
        )
        added = max(ZERO, cost - value) + unpriced * inst.fee_per_share - existing["stress"]
        normal_value, _, _ = liquidate(
            inst, existing["quantity"] + existing["pending_quantity"] + quantity, now, effective
        )
        normal_loss = cost - normal_value + max(ZERO, -realized)
        return {
            "EPISODE_PLANNED_LOSS": normal_loss,
            "EPISODE_STRESS_LOSS": exposure["stress"] + added + max(ZERO, -realized),
            "EVENT_STRESS_LOSS": event["stress"] + added,
            "MM_STRESS_LOSS": book["stress"] + added,
        }

    # Monotone joint-depth search, in actual quantity steps, with old quantity as ceiling.
    lo, hi = 0, int(high / inst.step)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if all(value <= loss_budgets[key] for key, value in losses(Decimal(mid) * inst.step).items()):
            lo = mid
        else:
            hi = mid - 1
    quantity = Decimal(lo) * inst.step if not reasons else ZERO
    if quantity < high:
        reasons.extend(
            key for key, value in losses(quantity + inst.step).items()
            if value > loss_budgets[key]
        )
    reasons += [name for name, cap in caps.items() if floor(cap, inst.step) == high]
    return quantity, encoded(
        {
            "q_old": old_quantity,
            "q_admitted": quantity,
            "caps": caps,
            "loss_budgets": loss_budgets,
            "joint_losses_at_admitted": losses(quantity),
            "binding_reasons": reasons,
            "parent_id": episode.id if episode else None,
            "policy_hash": policy.digest,
            "stress_mode": effective.stress_mode,
            "all_in_unit": cash_unit,
            "buy_debit_unit": unit,
            "cash_spent": spent,
            "pending_cash": episode_pending,
            "episode_existing": exposure,
            "event_existing": event,
            "mm_existing": {
                "cost": book["gross_cost"],
                "stress": book["stress"],
                "full_loss": book["full_loss"],
            },
            "source_at": inst.book.get("source_at"),
            "received_at": inst.book_at.isoformat() if inst.book_at else None,
            "book_hash": content_hash(inst.book),
            "economic_equity_lower_bound": book["economic_equity_lower_bound"],
        }
    )


def apply_plan(planner, session, account, intent, plan):
    if intent.strategy_id != "mm_sig":
        return plan
    policy = planner.config.mm_risk_policy
    installed = installed_state(session, account.id) if policy is None else None
    if policy is None:
        if installed is not None and plan.side == "BUY":
            raise RiskRejected("MM_RISK_POLICY_REMOVAL_BLOCKED")
        return plan
    checked_policy(policy, account.namespace == "live")
    inst = session.get(Instrument, intent.instrument_id)
    if planner.clock.now >= deadline(planner.ends_at, policy):
        return replace(
            plan,
            quantity=ZERO,
            reserve_cash=ZERO,
            reserve_quantity=ZERO,
            fee_bound=ZERO,
            risk_decision={"q_admitted": "0", "binding_reasons": ["MM_TRADING_DEADLINE_REACHED"]},
        )
    if plan.side != "BUY":
        if not plan.quantity:
            return plan
        basis = matched_cost(session, account.id, inst.id, plan.quantity)
        book = portfolio(session, account, policy, planner.clock.now)
        state = pool_state(session, account, policy, planner.clock.now, book)
        episode, claim = exit_claim(
            session, account, inst, policy, planner.clock.now, planner.ends_at, book, state
        )
        metadata = json.loads(intent.note or "{}")
        risk_exit = bool(claim or metadata.get("kind") == "INVENTORY_EXIT")
        target = (
            (basis / plan.quantity + inst.fee_per_share + policy.target_profit_ticks * inst.tick)
            / inst.tick
        ).to_integral_value(rounding=ROUND_CEILING) * inst.tick
        if not risk_exit and (target >= 1 or plan.price < target):
            raise RiskRejected("MM_NORMAL_TP_COST_OR_TARGET_UNREACHABLE")
        return replace(
            plan,
            risk_decision=encoded(
                {
                    "exit_claim": claim,
                    "normal_tp_floor": target,
                    "matched_fifo_cost": basis,
                    "q_admitted": plan.quantity,
                    "risk_exit": risk_exit,
                }
            ),
        )
    quantity, decision = size(
        session, account, inst, policy, planner.clock.now, plan.quantity, plan.price
    )
    # Remote waiting, holding, stressed exit and reconciliation all consume time.
    remaining = (deadline(planner.ends_at, policy) - planner.clock.now).total_seconds()
    need = max(
        policy.no_new_seconds,
        policy.entry_wait_seconds + policy.holding_seconds + policy.exit_and_reconcile_seconds,
    )
    if remaining <= need:
        quantity = ZERO
        decision["binding_reasons"].append("MM_ENTRY_CUTOFF_INCLUDING_WAIT")
    if plan.price + 2 * inst.fee_per_share + policy.target_profit_ticks * inst.tick >= 1:
        quantity = ZERO
        decision["binding_reasons"].append("MM_ENTRY_TP_UNREACHABLE")
    metadata = json.loads(intent.note or "{}") if (intent.note or "").startswith("{") else {}
    if (
        metadata.get("policy", {}).get("standard_core_execution_active")
        and quantity != plan.quantity
    ):
        quantity = ZERO
        decision["binding_reasons"].insert(0, "FORMAL_QUANTITY_SUPPORT_MISMATCH")
    fee = inst.fee_per_share * quantity
    decision["q_admitted"] = str(quantity)
    return replace(
        plan,
        quantity=quantity,
        reserve_cash=quantity * plan.price + fee,
        fee_bound=fee,
        risk_decision=decision,
    )


def reserved(session, account, intent, plan, order_id, policy, now):
    if intent.strategy_id != "mm_sig" or policy is None:
        return
    inst = session.get(Instrument, intent.instrument_id)
    book = portfolio(session, account, policy, now)
    episode = ensure_episode(session, account, inst, policy, now, book)
    decision = dict(plan.risk_decision or {})
    decision.update(parent_id=episode.id, episode_id=episode.id)
    session.add(
        MmRiskOrder(
            order_id=order_id,
            episode_id=episode.id,
            data={
                "sizing": decision,
                "policy_hash": policy.digest,
                "side": plan.side,
                "original_quantity": str(plan.quantity),
                "created_at": now.isoformat(),
            },
        )
    )


def matched_cost(session, account_id, instrument_id, quantity):
    remaining, result = quantity, ZERO
    for lot, origin, _ in lot_origins(session, account_id, [instrument_id]):
        if origin.get("strategy_id") != "mm_sig":
            raise RiskRejected("MM_EXIT_FOREIGN_INVENTORY")
        take = min(remaining, lot.quantity)
        if lot.cost is None:
            raise RiskRejected("MM_EXIT_COST_UNKNOWN")
        result += lot.cost * take / lot.quantity
        remaining -= take
        if not remaining:
            break
    if remaining:
        raise RiskRejected("MM_EXIT_OWNED_QUANTITY_EXCEEDED")
    return result


def exit_claim(session, account, inst, policy, now, ends_at, portfolio_book, state):
    owned = portfolio_book["owned"].get(inst.id)
    if not owned or not owned["quantity"]:
        return None, None
    episode = ensure_episode(session, account, inst, policy, now, portfolio_book)
    effective = effective_policy(policy, episode)
    ends_at = deadline(ends_at, effective)
    ids = set(episode.data["instrument_ids"])
    cost = sum((data["cost"] for iid, data in portfolio_book["owned"].items() if iid in ids), ZERO)
    _, realized = episode_cash(session, account.id, episode, cost)
    economic_loss = -realized + sum(
        (
            data["cost"] - portfolio_book["exposures"][iid]["net_value"]
            for iid, data in portfolio_book["owned"].items()
            if iid in ids and data["quantity"]
        ),
        ZERO,
    )
    complete = all(
        not portfolio_book["exposures"][iid]["unpriced_quantity"]
        for iid in ids
        if iid in portfolio_book["exposures"]
    )
    prior = dict(episode.data)
    claims = dict(prior.get("exit_claims", {}))
    current = claims.get(inst.id)
    remaining = (ends_at - now).total_seconds()
    candidates = []
    if state.get("latched"):
        candidates.append((100, state.get("reason", "MM_RISK_LATCHED")))
    if remaining <= effective.urgent_seconds:
        candidates.append((95, "URGENT_CUTOFF"))
    if state.get("events", {}).get(inst.event_group, {}).get("latched"):
        candidates.append((90, "EVENT_ECONOMIC_LOSS"))
    if complete and economic_loss >= effective.episode_planned_loss:
        candidates.append((85, "ECONOMIC_STOP"))
    if remaining <= effective.reduce_only_seconds:
        candidates.append((80, "CUTOFF_REDUCE_ONLY"))
    if (now - owned["oldest_at"]).total_seconds() >= effective.holding_seconds:
        candidates.append((70, "OLDEST_LOT_DEADLINE"))
    # A late fill inherits the frozen parent schedule rather than a fresh horizon.
    if now >= episode.started_at + timedelta(
        seconds=effective.entry_wait_seconds + effective.holding_seconds
    ):
        candidates.append((75, "PARENT_HOLD_DEADLINE"))
    if current:
        candidates.append((current["priority"], current["reason"]))
    if prior.get("latched_reason"):
        candidates.append((85, prior["latched_reason"]))
    if not candidates:
        return episode, None
    priority, reason = max(candidates)
    claim = {
        "id": current["id"] if current else episode.id + ":" + inst.id,
        "reason": reason,
        "priority": priority,
        "triggered_at": current["triggered_at"] if current else now.isoformat(),
        "economic_loss": str(economic_loss) if complete else None,
        "remaining_quantity": str(owned["quantity"]),
        "state": "EXIT_REQUIRED",
    }
    if complete and economic_loss > effective.episode_planned_loss:
        claim["loss_overrun"] = str(economic_loss - effective.episode_planned_loss)
    claims[inst.id] = claim
    episode.data = {**prior, "latched": True, "exit_claims": claims}
    return episode, claim


def overlay_quote(session, account, inst, settings, inputs, policy, now, ends_at, quote):
    """The original signal/model output is retained; only final risk/exit changes."""
    from sigbot.strategies.mm_sig_v1.live.policy import exit_batch
    from sigbot.strategies.mm_sig_v1.models import Book

    checked_policy(policy, account.namespace == "live")
    ends_at = deadline(ends_at, policy)
    result = dict(quote)
    old_buy = decimal(result["buy_quantity"])
    book = portfolio(session, account, policy, now)
    state = pool_state(session, account, policy, now, book)
    episode, claim = exit_claim(session, account, inst, policy, now, ends_at, book, state)
    if episode is not None and not claim and quote.get("sell_kind") == "INVENTORY_EXIT":
        claim = {
            "id": episode.id + ":" + inst.id,
            "reason": "INVENTORY_CAPACITY",
            "priority": 60,
            "triggered_at": now.isoformat(),
            "state": "EXIT_REQUIRED",
        }
        claims = dict(episode.data.get("exit_claims", {}))
        claims[inst.id] = claim
        episode.data = {**episode.data, "latched": True, "exit_claims": claims}
    effective = effective_policy(policy, episode)
    # Each source epoch owns one inventory reservation, regardless of exit cause.
    if claim:
        result.update(
            buy_quantity="0",
            maximum_buy_total="0",
            buy_directive="WITHDRAW",
            buy_directive_reason=claim["reason"],
            mode="RISK_REDUCE_ONLY",
            exit_claim=claim,
            allow_new_orders=True,
        )
        if not inputs.exit_authorized or not levels(inst, now)[1]:
            claim = {
                **claim,
                "state": "EXIT_BLOCKED",
                "blocker": "OWNERSHIP_ACCOUNT_OR_BOOK_UNAVAILABLE",
            }
            result.update(
                sell_quantity="0",
                maximum_sell_total="0",
                sell_directive="WITHDRAW",
                exit_blocker=claim["blocker"],
                exit_claim=claim,
            )
        else:
            actual = Book.model_validate(
                {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
            )
            bid = max(
                (level.price for level in actual.bids if level.quantity > level.own_quantity),
                default=None,
            )
            if bid is None or settings.inventory_exit_min_price is None:
                claim = {
                    **claim,
                    "state": "EXIT_BLOCKED",
                    "blocker": "NO_EXTERNAL_BID_OR_EXECUTION_AUTHORIZATION",
                }
                result.update(
                    sell_quantity="0",
                    maximum_sell_total="0",
                    sell_directive="WITHDRAW",
                    exit_blocker=claim["blocker"],
                    exit_claim=claim,
                )
            else:
                price, quantity, details = exit_batch(
                    actual, bid, inst.tick, book["owned"][inst.id]["quantity"], settings, inputs
                )
                result.update(
                    details,
                    ask=str(price),
                    sell_guard=str(price),
                    sell_quantity=str(quantity),
                    maximum_sell_total=str(quantity),
                    sell_kind="INVENTORY_EXIT",
                    sell_directive="QUOTE" if quantity else "WITHDRAW",
                    sell_directive_reason=claim["reason"],
                )
                if details.get("exit_blocker") or not quantity:
                    claim = {
                        **claim,
                        "state": "EXIT_BLOCKED",
                        "blocker": details.get("exit_blocker") or "NO_AVAILABLE_OWN_QUANTITY",
                    }
                    result["exit_claim"] = claim
        claims = dict(episode.data["exit_claims"])
        claims[inst.id] = result["exit_claim"]
        episode.data = {**episode.data, "exit_claims": claims}
    else:
        # Match the proposed SELL quantity against FIFO lots, never blended new cost.
        q = decimal(result["sell_quantity"])
        if q:
            basis = matched_cost(session, account.id, inst.id, q)
            target = (
                (basis / q + inst.fee_per_share + effective.target_profit_ticks * inst.tick)
                / inst.tick
            ).to_integral_value(rounding=ROUND_CEILING) * inst.tick
            result["economic_tp_floor"] = str(target)
            if target >= 1:
                result.update(
                    sell_quantity="0",
                    maximum_sell_total="0",
                    sell_directive="WITHDRAW",
                    sell_directive_reason="TP_UNREACHABLE",
                )
            else:
                result["ask"] = str(max(decimal(result["ask"]), target))
                result["sell_guard"] = str(max(decimal(result["sell_guard"]), target))
    if not claim and old_buy and result.get("bid") is not None:
        price = decimal(result["bid"])
        unreachable = (
            price + 2 * inst.fee_per_share + effective.target_profit_ticks * inst.tick >= 1
        )
        active = [o for o in book["pending"] if o.instrument_id == inst.id and o.side == "BUY"]
        quantity, decision = size(
            session,
            account,
            inst,
            policy,
            now,
            old_buy,
            price,
            exclude_order=active[0].id if len(active) == 1 else None,
        )
        if len(active) > 1:
            quantity = ZERO
            decision["binding_reasons"].insert(0, "MULTIPLE_OWN_QUOTE_PARENTS")
        need = max(
            effective.no_new_seconds,
            effective.entry_wait_seconds
            + effective.holding_seconds
            + effective.exit_and_reconcile_seconds,
        )
        if unreachable or (ends_at - now).total_seconds() <= need:
            quantity = ZERO
            decision["binding_reasons"].append(
                "TP_UNREACHABLE" if unreachable else "ENTRY_CUTOFF_INCLUDING_WAIT"
            )
        if result.get("standard_core_execution_active") and quantity != old_buy:
            quantity = ZERO
            decision["binding_reasons"].insert(0, "FORMAL_QUANTITY_SUPPORT_MISMATCH")
        decision["q_admitted"] = str(quantity)
        result.update(
            buy_quantity=str(quantity), maximum_buy_total=str(quantity), risk_sizing=decision
        )
        if not quantity:
            result.update(
                buy_directive="WITHDRAW", buy_directive_reason=decision["binding_reasons"][0]
            )
    result.update(
        original_buy_quantity=str(old_buy),
        risk_policy_hash=policy.digest,
        mm_risk_state=state,
        signal_and_pricing_unchanged=True,
    )
    return result


def check_dispatch(planner, session, account, intent, order):
    if intent.strategy_id != "mm_sig":
        return
    policy = planner.config.mm_risk_policy
    if policy is None:
        if installed_state(session, account.id) and order.side == "BUY":
            return {"binding_reasons": ["MM_RISK_POLICY_REMOVAL_BLOCKED"], "q_admitted": "0"}
        return
    checked_policy(policy, account.namespace == "live")
    if planner.clock.now >= deadline(planner.ends_at, policy):
        return {"binding_reasons": ["MM_TRADING_DEADLINE_REACHED"], "q_admitted": "0"}
    if order.side == "BUY":
        inst = session.get(Instrument, order.instrument_id)
        quantity, decision = size(
            session,
            account,
            inst,
            policy,
            planner.clock.now,
            order.quantity,
            order.price,
            exclude_order=order.id,
        )
        remaining = (deadline(planner.ends_at, policy) - planner.clock.now).total_seconds()
        need = max(
            policy.no_new_seconds,
            policy.entry_wait_seconds + policy.holding_seconds + policy.exit_and_reconcile_seconds,
        )
        if remaining <= need:
            quantity = ZERO
            decision["binding_reasons"].insert(0, "MM_ENTRY_CUTOFF_INCLUDING_WAIT")
        if quantity < order.quantity:
            decision["q_admitted"] = str(quantity)
            return decision
    return None


def observed(session, account, order, policy, now):
    if policy is None:
        return
    inst = session.get(Instrument, order.instrument_id)
    episode = find_episode(session, account.id, inst)
    if episode is None:
        return
    book = portfolio(session, account, policy, now)
    ids = set(episode.data["instrument_ids"])
    if not any(data["quantity"] for iid, data in book["owned"].items() if iid in ids) and not any(
        o.instrument_id in ids for o in book["pending"]
    ):
        episode.closed_at = now
        episode.data = {**episode.data, "closed_at": now.isoformat(), "state": "CONFIRMED_CLOSED"}
    pool_state(session, account, policy, now, book)


def supervise(runtime):
    """Run inside the existing gateway; reconciliation and other strategies stay on."""
    from sigbot.command_queue import CommandQueue
    from sigbot.domain import Intent
    from sigbot.errors import SigbotError
    from sigbot.storage import ExecutionCommand
    from sigbot.strategies.mm_sig_v1.live.execution import LivePlanner
    from sigbot.strategies.mm_sig_v1.live.lifecycle import keep_order
    from sigbot.strategies.mm_sig_v1.live.models import Settings

    identity = ("mm_sig", "1.3")
    authority = runtime.authorities.items.get(identity)
    if authority is None or authority.config.mm_risk_policy is None:
        return {"enabled": False, "status": "NOT_CONFIGURED"}
    policy = authority.config.mm_risk_policy
    checked_policy(policy, runtime.store.namespace == "live")
    executor = runtime.executor
    timestamp = runtime.clock.now
    cancels = []
    commands = []
    result = {
        "enabled": True,
        "at": timestamp.isoformat(),
        "policy_hash": policy.digest,
        "new_risk_only_latch_scope": "mm_sig",
        "exits": {},
        "entry_cancels": [],
        "allocated_exits": [],
    }
    with runtime.authorities.activate(executor, identity):
        planner = LivePlanner(
            runtime.store,
            executor.config,
            runtime.clock,
            executor.planner.starts_at,
            executor.planner.ends_at,
        )
        planner.ends_at = deadline(planner.ends_at, policy)
        with runtime.store.transaction() as session:
            account = runtime.store.lock_account(session, executor.account_id)
            if (
                account.lifecycle != "HEALTHY"
                or account.reconciled_at is None
                or not 0 <= (timestamp - account.reconciled_at).total_seconds()
                <= executor.config.account_ttl_seconds
            ):
                return {**result, "status": "ACCOUNT_CHECKPOINT_REQUIRED"}
            book = portfolio(session, account, policy, timestamp)
            state = pool_state(session, account, policy, timestamp, book)
            result["state"] = state
            for order in book["pending"]:
                if order.side != "BUY":
                    continue
                row = session.get(IntentRow, order.intent_id)
                created = utc_time(row.payload["created_at"])
                inst = book["instruments"][order.instrument_id]
                ep = ensure_episode(session, account, inst, policy, timestamp, book)
                effective = effective_policy(policy, ep)
                owned = book["owned"].get(inst.id)
                expired = timestamp >= created + timedelta(seconds=effective.entry_wait_seconds)
                aged = bool(
                    owned
                    and owned["quantity"]
                    and (timestamp - owned["oldest_at"]).total_seconds() >= 3600
                )
                cutoff = (planner.ends_at - timestamp).total_seconds() <= effective.no_new_seconds
                admitted, _ = size(
                    session,
                    account,
                    inst,
                    policy,
                    timestamp,
                    order.quantity - order.filled,
                    order.price,
                    exclude_order=order.id,
                )
                if (
                    expired
                    or aged
                    or cutoff
                    or state.get("latched")
                    or admitted < order.quantity - order.filled
                ):
                    reason = "MM_RISK_ENTRY_WAIT_EXPIRED" if expired else "MM_RISK_ENTRY_WITHDRAWAL"
                    if order.remote_id is None and order.request_state in {"SENDING", "UNKNOWN"}:
                        result["entry_cancels"].append(
                            {
                                "order_id": order.id,
                                "state": "UNCONFIRMED_REQUEST_RETAINED",
                                "cash_reserved": str(order.reserve_cash),
                            }
                        )
                    else:
                        cancels.append((order.id, row.payload["strategy_version"], reason))
            # Preallocation uses the same account lock, Order reservation and queue.
            # This also covers a producer restart or a model that currently cannot quote.
            for iid, data in list(book["owned"].items()):
                if not data["quantity"]:
                    continue
                inst = book["instruments"][iid]
                if timestamp >= planner.ends_at:
                    result["exits"][iid] = {
                        "state": "UNWOUND_INCOMPLETE",
                        "reason": "TRADING_DEADLINE_REACHED",
                        "remaining_quantity": str(data["quantity"]),
                        "remaining_cost": str(data["cost"]),
                    }
                    continue
                origins = [
                    origin
                    for _, origin, _ in lot_origins(session, account.id, {iid})
                    if origin.get("strategy_id") == "mm_sig"
                ]
                latest = max(origins, key=lambda p: utc_time(p["created_at"])) if origins else None
                if latest is None:
                    result["exits"][iid] = {
                        "state": "EXIT_BLOCKED",
                        "reason": "ORIGINAL_SETTINGS_MISSING",
                    }
                    continue
                try:
                    try:
                        metadata = json.loads(latest.get("note") or "{}")
                        if not isinstance(metadata, dict) or not isinstance(
                            metadata.get("settings"), dict
                        ):
                            raise RiskRejected("MM_RISK_ORIGINAL_SETTINGS_MISSING")
                        current = Settings.model_validate(
                            {
                                **metadata["settings"],
                                "strategy_version": "1.3",
                                "execution_route": "SHARED",
                                "risk_policy": policy.model_dump(mode="json"),
                            }
                        )
                    except (json.JSONDecodeError, ValidationError) as exc:
                        raise RiskRejected("MM_RISK_ORIGINAL_SETTINGS_INVALID") from exc
                    quote = planner.quote13(session, account, inst, current)
                    claim = quote.get("exit_claim")
                    if not claim:
                        continue
                    result["exits"][iid] = claim
                    orders = [
                        o for o in book["pending"] if o.instrument_id == iid and o.side == "SELL"
                    ]
                    if orders:
                        for order in orders:
                            origin = session.get(IntentRow, order.intent_id)
                            if not keep_order(
                                order,
                                origin,
                                decimal(quote["ask"]) if quote.get("ask") else None,
                                decimal(quote["sell_quantity"]),
                                quote,
                                current,
                                timestamp,
                            ):
                                cancels.append(
                                    (
                                        order.id,
                                        origin.payload["strategy_version"],
                                        "MM_RISK_EXIT_REPLACE",
                                    )
                                )
                        continue
                    if decimal(quote["sell_quantity"]) <= ZERO:
                        continue
                    pending_commands = session.scalars(
                        select(ExecutionCommand).where(
                            ExecutionCommand.account_id == account.id,
                            ExecutionCommand.strategy_id == "mm_sig",
                            ExecutionCommand.instrument_id == iid,
                            ExecutionCommand.action == "SUBMIT",
                            ExecutionCommand.state.in_(["WAITING_ALLOCATION", "OMS_PENDING"]),
                            ExecutionCommand.payload["action"].as_string() == "REDUCE_TO_TARGET",
                        )
                    )
                    if any(
                        command.order_id is None
                        or (
                            session.get(Order, command.order_id) is not None
                            and possible(session.get(Order, command.order_id))
                        )
                        for command in pending_commands
                    ):
                        continue
                    quantity = decimal(quote["sell_quantity"])
                    prefix = "SIM-MM-EXIT-" if account.id.startswith("SIM-") else "SIG-MM-EXIT-"
                    intent = Intent.model_validate(
                        {
                            "schema_version": "1.0",
                            "intent_id": prefix
                            + content_hash(
                                [claim["id"], timestamp.isoformat(), quote["ask"], str(quantity)]
                            ),
                            "synthetic": runtime.store.namespace in {"offline", "shadow"},
                            "account_id": account.id,
                            "tournament_id": account.tournament,
                            "instrument_id": iid,
                            "outcome_side": inst.outcome,
                            "action": "REDUCE_TO_TARGET",
                            "target_quantity": max(ZERO, data["quantity"] - quantity),
                            "limit_price": quote["ask"],
                            "max_order_notional": quantity
                            * (decimal(quote["ask"]) + inst.fee_per_share),
                            "created_at": timestamp,
                            "expires_at": min(timestamp + timedelta(seconds=30), planner.ends_at),
                            "rules_hash": inst.rules_hash,
                            "mapping_hash": inst.mapping_hash,
                            "strategy_id": "mm_sig",
                            "strategy_version": "1.3",
                            "note": json.dumps(
                                {
                                    "kind": "INVENTORY_EXIT",
                                    "settings": current.model_dump(mode="json"),
                                    "policy": quote,
                                }
                            ),
                        }
                    )
                    order_id = executor.planner.reserve_locked(session, account, intent)
                    if order_id:
                        commands.append(
                            CommandQueue(runtime.store, account.id, *identity).publish_preallocated(
                                session, intent, order_id
                            )
                        )
                        result["allocated_exits"].append(order_id)
                except SigbotError as exc:
                    result["exits"][iid] = {
                        "state": "EXIT_BLOCKED",
                        "reason": exc.code,
                        "remaining_quantity": str(data["quantity"]),
                    }
    for order_id, version, reason in cancels:
        if ("mm_sig", version) not in runtime.authorities.items:
            result["entry_cancels"].append(
                {"order_id": order_id, "state": "CANCEL_AUTHORITY_NOT_REGISTERED"}
            )
            continue
        CommandQueue(runtime.store, executor.account_id, "mm_sig", version).cancel(
            order_id, timestamp, reason=reason, safety=True
        )
        result["entry_cancels"].append(
            {"order_id": order_id, "state": "CANCEL_REQUESTED_NOT_CONFIRMED"}
        )
    result["status"] = "SUPERVISING"
    for command in commands:
        runtime.consumer.process(command)
    return result


def review_resume(session, account, policy, now, review_id, *, account_ttl_seconds=45):
    """Operator-only domain operation; callers must hold the account lock.

    A review clears the MM pool latch only. It cannot reset HWM, day basis,
    principal, episode/event limits, orders or reserves. No automatic caller.
    """
    from sigbot.storage import Audit

    if not review_id or not review_id.strip():
        raise AuthorizationRequired("MM_OPERATOR_REVIEW_REQUIRED")
    book = portfolio(session, account, policy, now)
    state = pool_state(session, account, policy, now, book)
    if (
        account.lifecycle != "HEALTHY"
        or account.reconciled_at is None
        or not 0 <= (now - account.reconciled_at).total_seconds() <= account_ttl_seconds
        or not book["valuation_complete"]
        or not book["funds"]["ownership_valid"]
        or state.get("day_baseline_pending")
        or decimal(state.get("daily_loss", "0")) >= policy.daily_loss
        or decimal(state.get("drawdown", "0")) >= policy.drawdown
    ):
        raise RiskRejected("MM_RESUME_CURRENT_RISK_CHECK_FAILED")
    row = session.get(MmRiskState, account.id)
    row.data = {
        **row.data,
        "latched": False,
        "resumed_at": now.isoformat(),
        "resume_review": review_id,
    }
    session.add(
        Audit(
            account_id=account.id,
            at=now,
            kind="MM_MANUAL_RISK_RESUME",
            data={"review": review_id, "before": state, "after": row.data},
        )
    )
    return row.data
