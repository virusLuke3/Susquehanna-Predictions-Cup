"""Persisted real observations and matured labels; missing evidence stays unknown."""

import json
from bisect import bisect_left, bisect_right
from contextlib import nullcontext
from datetime import timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from sigbot.domain import TERMINAL, ZERO, content_hash, utc_time
from sigbot.storage import (
    Audit,
    FillRow,
    Instrument,
    IntentRow,
    Order,
    Position,
    PositionLot,
    StrategyEvent,
)
from sigbot.strategies.mm_sig_v1.core import external_best
from sigbot.strategies.mm_sig_v1.live.policy import PolicyInputs

FAMILY = {("mm_sig", "1.1"), ("mm_sig", "1.2"), ("mm_sig", "1.3")}


def owned(row):
    return (
        row is not None
        and (row.payload.get("strategy_id"), row.payload.get("strategy_version")) in FAMILY
    )


def unresolved(order):
    return (
        order.state not in TERMINAL
        or order.request_state != "RESOLVED"
        or bool(order.reserve_cash or order.reserve_quantity)
    )


def statistics(session, account_id, instrument_id, timestamp, settings, *, tick):
    observations = [
        a
        for a in session.scalars(
            select(Audit)
            .where(
                Audit.account_id == account_id,
                Audit.kind == "MM13_OBSERVATION",
                Audit.data["instrument_id"].as_string() == instrument_id,
                Audit.at >= timestamp - timedelta(minutes=65),
                Audit.at <= timestamp,
            )
            .order_by(Audit.at)
        )
        if a.data["instrument_id"] == instrument_id
    ]
    refs = [a for a in observations if a.data.get("reference_usable")]
    times = [r.at for r in refs]
    inst = session.get(Instrument, instrument_id)
    binding = [inst.rules_hash, inst.mapping_hash] if inst else None
    price_refs = [
        a
        for a in observations
        if a.data.get("price_reference_usable", a.data.get("reference_usable"))
        and a.data.get("mid") is not None
        and a.data.get("signal_binding", binding) == binding
    ]
    price_times = [r.at for r in price_refs]
    latest = refs[-1].data if refs else {}

    def group(side, price, mid, state, activity):
        return (
            side,
            int((abs(price - mid) / tick).to_integral_value(rounding="ROUND_FLOOR")),
            state,
            activity,
        )

    target_groups = {
        side: group(
            side,
            Decimal(
                latest.get("quote", {}).get(
                    "bid" if side == "BUY" else "ask", latest.get("mid", "0")
                )
            ),
            Decimal(latest.get("mid", "0")),
            "OPEN",
            latest.get("activity", "UNKNOWN"),
        )
        for side in ("BUY", "SELL")
    }
    fills = []
    for fill, order, row in session.execute(
        select(FillRow, Order, IntentRow)
        .join(Order, FillRow.order_id == Order.id)
        .join(IntentRow, Order.intent_id == IntentRow.id)
        .where(FillRow.account_id == account_id, Order.instrument_id == instrument_id)
    ):
        if (
            order
            and row is not None
            and order.instrument_id == instrument_id
            and owned(row)
            and utc_time(fill.payload["filled_at"]) <= timestamp
        ):
            fills.append(
                (fill, order, json.loads(row.payload.get("note") or "{}").get("kind", "UNKNOWN"))
            )
    exposure = {"BUY": ZERO, "SELL": ZERO}
    eligible_fills = {"BUY": ZERO, "SELL": ZERO}
    labels: dict[str, dict[int, list[Decimal]]] = {
        side: {h: [] for h in (5, 30, 60, 300)} for side in ("BUY", "SELL")
    }
    for index, obs in enumerate(observations[:-1]):
        following = observations[index + 1]
        seconds = Decimal(str((following.at - obs.at).total_seconds()))
        if (
            not obs.data.get("reference_usable")
            or not following.data.get("reference_usable")
            or not 0 < seconds <= settings.book_ttl_seconds
        ):
            continue
        for side in exposure:
            ids = [
                oid
                for oid in obs.data.get("eligible_orders", {}).get(side, [])
                if tuple(obs.data.get("order_groups", {}).get(oid, target_groups[side]))
                == target_groups[side]
            ]
            if not ids:
                continue
            exposure[side] += seconds
            eligible_fills[side] += sum(
                (
                    Decimal(f.payload["quantity"])
                    for f, o, kind in fills
                    if o.side == side
                    and o.id in ids
                    and obs.at < utc_time(f.payload["filled_at"]) <= following.at
                    and kind == "MM_REAL_PASSIVE"
                ),
                ZERO,
            )
    markouts = []
    for fill, order, kind in fills:
        at = utc_time(fill.payload["filled_at"])
        index = bisect_right(times, at) - 1
        base = (
            refs[index]
            if index >= 0 and (at - refs[index].at).total_seconds() <= settings.book_ttl_seconds
            else None
        )
        rested = bool(base and order.id in base.data.get("eligible_orders", {}).get(order.side, []))
        price_index = bisect_right(price_times, at) - 1
        price_base = (
            price_refs[price_index]
            if price_index >= 0
            and (at - price_refs[price_index].at).total_seconds() <= settings.book_ttl_seconds
            else None
        )
        for horizon in (5, 30, 60, 300):
            future_index = bisect_left(price_times, at + timedelta(seconds=horizon))
            future = (
                price_refs[future_index]
                if future_index < len(price_refs)
                and (price_refs[future_index].at - at).total_seconds()
                <= horizon + settings.book_ttl_seconds
                else None
            )
            adverse = None
            if price_base and future:
                delta = Decimal(price_base.data["mid"]) - Decimal(future.data["mid"])
                adverse = delta if order.side == "BUY" else -delta
                if (
                    rested
                    and base
                    and kind == "MM_REAL_PASSIVE"
                    and at >= timestamp - timedelta(minutes=60)
                    and tuple(base.data.get("order_groups", {}).get(order.id, ()))
                    == target_groups[order.side]
                    and horizon in labels[order.side]
                ):
                    labels[order.side][horizon].append(adverse)
            markouts.append(
                {
                    "fill_id": fill.id,
                    "side": order.side,
                    "kind": kind,
                    "horizon_seconds": horizon,
                    "status": "MATURED" if adverse is not None else "NOT_OBSERVED",
                    "adverse_per_share": str(adverse) if adverse is not None else None,
                    "markout_per_share": str(
                        (Decimal(future.data["mid"]) - Decimal(fill.payload["price"]))
                        * (1 if order.side == "BUY" else -1)
                    )
                    if future is not None
                    else None,
                    "execution_role": "CONFIRMED_RESTING_BEFORE_FILL"
                    if rested
                    else "TAKER_OR_ROLE_UNKNOWN",
                }
            )
    adverse_estimates = {}
    conditional_adverse: dict[str, dict[str, dict[str, Any]]] = {}
    for side in labels:
        estimates = {}
        conditional_adverse[side] = {}
        for horizon, values in labels[side].items():
            conditional_adverse[side][str(horizon)] = {
                "actual_passive_fill_labels": len(values),
                "status": "INSUFFICIENT_MATURED_FILL_LABELS",
                "adverse_upper_proxy": None,
            }
            if len(values) >= settings.quality_min_fills:
                mean = sum(values, ZERO) / len(values)
                variance = sum(((v - mean) ** 2 for v in values), ZERO) / (len(values) - 1)
                allowance = max(tick / 2, (variance / len(values)).sqrt() * 2)
                estimates[horizon] = max(ZERO, mean + allowance).quantize(
                    Decimal(".0000000001"), rounding="ROUND_CEILING"
                )
                conditional_adverse[side][str(horizon)].update(
                    status="READY_EMPIRICAL_FILL_CONDITIONAL_RISK",
                    adverse_upper_proxy=str(estimates[horizon]),
                    statistical_guarantee=False,
                )
        ready = {5, 30} <= estimates.keys() or {60, 300} <= estimates.keys()
        adverse_estimates[side] = max(estimates.values()) if ready else None
    latency: dict[str, list[Decimal]] = {"submit_limit": [], "cancel_order": []}
    owned_ids = {order.id for _, order, _ in fills}
    owned_ids.update(
        oid
        for obs in observations
        for ids in obs.data.get("eligible_orders", {}).values()
        for oid in ids
    )
    if owned_ids:
        for receipt in session.scalars(
            select(Audit).where(
                Audit.account_id == account_id,
                Audit.kind == "LIVE_RECEIPT",
                Audit.at >= timestamp - timedelta(minutes=65),
                Audit.at <= timestamp,
                Audit.data["order"].as_string().in_(owned_ids),
            )
        ):
            capability = receipt.data.get("capability")
            elapsed = receipt.data.get("http_elapsed_seconds")
            if capability in latency and elapsed is not None:
                value = Decimal(elapsed)
                if value.is_finite() and value >= ZERO:
                    latency[capability].append(value)
    moves = []
    for ref in refs:
        index = bisect_left(times, ref.at + timedelta(seconds=60))
        future = (
            refs[index]
            if index < len(refs)
            and (refs[index].at - ref.at).total_seconds() <= 60 + settings.book_ttl_seconds
            else None
        )
        if future:
            moves.append(abs(Decimal(future.data["mid"]) - Decimal(ref.data["mid"])))
    # A full hour of continuous observations is required; silence is an observation, gaps are not.
    continuous_hour = (
        len(refs) > 1
        and (refs[-1].at - refs[0].at).total_seconds() >= 3600
        and all(
            0 < (b.at - a.at).total_seconds() <= settings.book_ttl_seconds
            for a, b in zip(refs, refs[1:], strict=False)
        )
    )
    movement = (
        sorted(moves)[max(0, (9 * len(moves) + 9) // 10 - 1)] if continuous_hour and moves else None
    )
    rates = {
        side: (eligible_fills[side] * 60 / seconds).quantize(
            Decimal(".0000000001"), rounding="ROUND_FLOOR"
        )
        if seconds >= settings.quality_min_quote_seconds
        else None
        for side, seconds in exposure.items()
    }
    conservative_rates = {
        side: (rate * settings.own_fill_rate_fraction).quantize(
            Decimal(".0000000001"), rounding="ROUND_FLOOR"
        )
        if rate is not None
        else None
        for side, rate in rates.items()
    }
    return {
        "buy_adverse": adverse_estimates["BUY"],
        "sell_adverse": adverse_estimates["SELL"],
        "sell_rate_per_minute": conservative_rates["SELL"],
        "buy_rate_per_minute": conservative_rates["BUY"],
        "empirical_fill_rates_per_minute": rates,
        "rate_fraction_is_calibrated_confidence_bound": False,
        "movement_scale": movement,
        "movement_risk_floor": max(moves, default=ZERO),
        "completed_movement_pairs": len(moves),
        "reference_observation_count": len(refs),
        "price_reference_observation_count": len(price_refs),
        "reference_gap_count": sum(
            (b.at - a.at).total_seconds() > settings.book_ttl_seconds
            for a, b in zip(refs, refs[1:], strict=False)
        ),
        "continuous_hour_verified": continuous_hour,
        "quote_seconds": exposure,
        "filled_quantity_on_valid_quotes": eligible_fills,
        "markouts": markouts,
        "fill_conditional_adverse": conditional_adverse,
        "replacement_latency_seconds": sum((max(v) for v in latency.values()), ZERO).quantize(
            Decimal(".000001"), rounding="ROUND_CEILING"
        )
        if all(latency.values())
        else None,
        "latency_support": {k: len(v) for k, v in latency.items()},
        "latency_method": "SUM_OBSERVED_CANCEL_AND_SUBMIT_MAXIMA; NOT_A_FUTURE_BOUND",
        "estimate_group": "INSTRUMENT_SIDE_DISTANCE_ORDER_STATE_ACTIVITY; sparse groups remain UNKNOWN",
        "current_groups": {k: list(v) for k, v in target_groups.items()},
        "uncertainty_is_statistical_guarantee": False,
    }


def policy_inputs(session, account, inst, book, settings, timestamp, ends_at, *, exploration=False):
    orders = list(session.scalars(select(Order).where(Order.account_id == account.id)))
    active = [o for o in orders if unresolved(o)]
    positions = list(session.scalars(select(Position).where(Position.account_id == account.id)))
    instruments = {
        i.id: i
        for i in session.execute(
            select(
                Instrument.id, Instrument.event_group, Instrument.exchange_id, Instrument.outcome
            ).where(Instrument.tournament == account.tournament)
        )
    }
    event_ids = {i.id for i in instruments.values() if i.event_group == inst.event_group}
    same_event = [o for o in active if o.instrument_id in event_ids]
    own_sells = [
        o
        for o in active
        if o.instrument_id == inst.id
        and o.side == "SELL"
        and owned(session.get(IntentRow, o.intent_id))
    ]
    pos = next((p for p in positions if p.instrument_id == inst.id), None)
    held = pos.quantity if pos else ZERO
    opposite_held = sum(
        (
            p.quantity
            for p in positions
            if p.instrument_id in instruments
            and instruments[p.instrument_id].exchange_id == inst.exchange_id
            and instruments[p.instrument_id].outcome != inst.outcome
        ),
        ZERO,
    )
    opposite_ids = {
        i.id
        for i in instruments.values()
        if i.exchange_id == inst.exchange_id and i.outcome != inst.outcome
    }
    from sigbot.accounting import lot_origins

    opposite_owned = sum((lot.quantity for lot, origin, _
                          in lot_origins(session, account.id, opposite_ids)
                          if (origin.get("strategy_id"), origin.get("strategy_version")) in FAMILY), ZERO)
    opposite_sales = sum(
        (
            o.reserve_quantity
            for o in active
            if o.instrument_id in opposite_ids and o.side == "SELL"
        ),
        ZERO,
    )
    own_buy_remaining = sum(
        (
            o.quantity - o.filled
            for o in active
            if o.instrument_id == inst.id
            and o.side == "BUY"
            and owned(session.get(IntentRow, o.intent_id))
        ),
        ZERO,
    )
    complement_reserve = sum(
        (
            o.quantity - o.filled
            for o in active
            if o.instrument_id in opposite_ids and o.side == "BUY" and o.reserve_cash
        ),
        ZERO,
    )
    lots = [
        lot
        for lot in session.scalars(
            select(PositionLot).where(
                PositionLot.account_id == account.id, PositionLot.instrument_id == inst.id
            )
        )
        if lot.quantity
    ]
    ages, origins, foreign = [], [], False
    owned_quantity = ZERO
    for lot in lots:
        fill = session.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
        origin = session.get(Order, fill.order_id) if fill else None
        row = session.get(IntentRow, origin.intent_id) if origin else None
        if not owned(row):
            foreign = True
        elif fill:
            owned_quantity += lot.quantity
            origins.append((utc_time(fill.payload["filled_at"]), lot.economic_id))
            ages.append(
                Decimal(
                    str(max(0, (timestamp - utc_time(fill.payload["filled_at"])).total_seconds()))
                )
            )
    marker = content_hash(
        [
            inst.rules_hash,
            inst.mapping_hash,
            [(o.id, str(o.filled)) for o in orders if o.instrument_id == inst.id and o.filled],
        ]
    )
    cached = inst.mapping_evidence.get("mm13_statistics_cache", {})
    if (
        cached.get("marker") == marker
        and cached.get("at")
        and 0 <= (timestamp - utc_time(cached["at"])).total_seconds() < 30
    ):
        stats = cached["statistics"]
    else:
        stats = statistics(session, account.id, inst.id, timestamp, settings, tick=inst.tick)
        inst.mapping_evidence = {
            **inst.mapping_evidence,
            "mm13_statistics_cache": {
                "at": timestamp.isoformat(),
                "marker": marker,
                "statistics": json.loads(json.dumps(stats, default=str)),
            },
        }
    capital = account.cash
    priced_value = ZERO
    # Unpriced or illiquid shares are separately reported, valued at zero for sizing.
    for p in positions:
        if p.instrument_id == inst.id:
            remain = min(p.quantity, owned_quantity) if settings.initial_capital_limit is not None else p.quantity
            value = ZERO
            for level in sorted(book.bids, key=lambda level: level.price, reverse=True):
                quantity = min(remain, level.quantity - level.own_quantity)
                value += quantity * max(ZERO, level.price - (inst.fee_per_share or ZERO))
                remain -= quantity
            capital += value
            priced_value += value
    funds = None
    if settings.initial_capital_limit is not None:
        from sigbot.accounting import strategy_finances

        funds = strategy_finances(session, account.id, "mm_sig", settings.initial_capital_limit)
        capital = max(ZERO, min(capital, funds["cash"] + priced_value))
    fees = sum(
        (
            e.future_fee_reserve
            for e in session.scalars(
                select(StrategyEvent).where(StrategyEvent.account_id == account.id)
            )
        ),
        ZERO,
    )
    cash_floor = max(
        (
            Decimal(e.data.get("cash_floor", "0"))
            for e in session.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account.id, StrategyEvent.state != "CLOSED"
                )
            )
        ),
        default=ZERO,
    )
    known = (
        account.lifecycle == "HEALTHY"
        and account.reconciled_at is not None
        and 0 <= (timestamp - account.reconciled_at).total_seconds() <= settings.account_ttl_seconds
    )
    from sigbot.uncertainty import scope_blockers, scopes

    scope_reasons = scope_blockers(scopes(session, account, active), inst)
    unknown = bool(scope_reasons)
    pending_own_ack = any(
        o.instrument_id == inst.id
        and owned(session.get(IntentRow, o.intent_id))
        and o.request_state in {"SEND_PENDING", "SENDING"}
        and not o.cancel_state
        for o in active
    )
    probe_authorized = False
    calibration_authorized = False
    if settings.calibration_sampling_enabled and settings.approval_file:
        from pathlib import Path

        approval = json.loads(Path(settings.approval_file).read_text())
        calibration_authorized = bool(
            approval.get("operator_approved")
            and approval.get("accept_mm_calibration")
            and approval.get("mm_calibration_until")
            and approval.get("account_id") == account.id
            and approval.get("tournament_id") == account.tournament
            and inst.id in approval.get("instrument_allowlist", [])
            and utc_time(approval["issued_at"])
            <= timestamp
            < utc_time(approval["new_orders_until"])
            and timestamp < utc_time(approval["mm_calibration_until"])
        )
    if settings.join_probe_enabled and settings.approval_file:
        from pathlib import Path

        approval = json.loads(Path(settings.approval_file).read_text())
        probe_authorized = bool(
            approval.get("operator_approved")
            and approval.get("accept_join_probe")
            and timestamp < utc_time(approval["new_orders_until"])
        )
    if inst.fee_per_share is None:
        from sigbot.errors import SchemaMismatch

        raise SchemaMismatch("MM13_FEE_UNKNOWN")
    metadata = inst.mapping_evidence.get("review", {})
    close = min(ends_at, utc_time(metadata["close_at"])) if metadata.get("close_at") else ends_at
    event = session.get(StrategyEvent, (account.id, "mm_sig", "1.3", inst.event_group))
    exit_data = {}
    if event and held:
        exits = event.data.get("mm13_exit_by_instrument", {})
        exit_data = exits.get(inst.id, {}) if exits else event.data.get("mm13_exit", {})
    if exit_data and exit_data.get("instrument_id", event.instrument_id) != inst.id:
        exit_data = {}
    epoch = exit_data.get("epoch") or (
        content_hash([account.id, inst.id, min(origins)[1]]) if origins else None
    )
    reference = Decimal(exit_data["reference_bid"]) if exit_data else None
    spent, reserved = ZERO, ZERO
    if reference is not None:
        for fill in session.scalars(select(FillRow).where(FillRow.account_id == account.id)):
            origin = session.get(Order, fill.order_id)
            row = session.get(IntentRow, origin.intent_id) if origin else None
            note = (
                json.loads(row.payload.get("note") or "{}")
                if row is not None and owned(row)
                else {}
            )
            if (
                origin
                and origin.instrument_id == inst.id
                and origin.side == "SELL"
                and note.get("kind") == "INVENTORY_EXIT"
                and note.get("policy", {}).get("exit_epoch") == epoch
            ):
                spent += max(
                    ZERO,
                    Decimal(fill.payload["quantity"]) * (reference - Decimal(fill.payload["price"]))
                    + Decimal(fill.payload["fee"]),
                )
        reserved = sum(
            (
                o.reserve_quantity * max(ZERO, reference - o.price + inst.fee_per_share)
                for o in own_sells
            ),
            ZERO,
        )
    inputs = PolicyInputs(
        capital=capital,
        inventory=held,
        event_inventory=sum((p.quantity for p in positions if p.instrument_id in event_ids), ZERO),
        event_cost=sum((p.cost or ZERO for p in positions if p.instrument_id in event_ids), ZERO),
        committed_buy_quantity=sum(
            (
                o.quantity - o.filled
                for o in same_event
                if o.side == "BUY" and o.instrument_id == inst.id
            ),
            ZERO,
        ),
        committed_buy_cost=sum(
            (o.reserve_cash for o in same_event if o.side == "BUY" and o.instrument_id == inst.id),
            ZERO,
        ),
        other_sell_reserved=sum(
            (
                o.reserve_quantity
                for o in active
                if o.instrument_id == inst.id and o.side == "SELL" and o not in own_sells
            ),
            ZERO,
        )
        + complement_reserve,
        cash_available=max(
            ZERO,
            min(
                account.cash - sum((o.reserve_cash for o in active), ZERO) - fees - cash_floor,
                funds["available"] if funds is not None else account.cash,
            ),
        ),
        buy_fee=inst.fee_per_share,
        sell_fee=inst.fee_per_share,
        lot_step=inst.step,
        sell_rate_per_minute=stats["sell_rate_per_minute"],
        buy_adverse=stats["buy_adverse"],
        sell_adverse=stats["sell_adverse"],
        movement_scale=stats["movement_scale"],
        movement_risk_floor=stats["movement_risk_floor"],
        oldest_lot_seconds=max(ages) if ages else None,
        exploration=exploration,
        new_risk_allowed=known
        and not unknown
        and not foreign
        and not opposite_held
        and (not held or bool(ages)),
        reduce_risk_allowed=known and not unknown and not foreign and bool(held and ages),
        pending_own_ack=pending_own_ack,
        account_scope_blockers=scope_reasons,
        replacement_latency_seconds=stats.get("replacement_latency_seconds"),
        join_probe_authorized=probe_authorized,
        calibration_authorized=calibration_authorized,
        complementary_reduce_quantity=min(
            own_buy_remaining, max(ZERO, opposite_owned - opposite_sales)
        )
        if settings.complementary_quotes_enabled and known and not unknown
        else ZERO,
        # Count all strategy inventory, including earlier revisions, and all
        # unresolved strategy BUY reservations. Unknown requests retain risk.
        calibration_committed_cost=sum(
            (
                p.cost or ZERO
                for p in positions
                if any(
                    o.instrument_id == p.instrument_id
                    and owned(session.get(IntentRow, o.intent_id))
                    for o in orders
                )
            ),
            ZERO,
        )
        + sum((o.reserve_cash for o in active if owned(session.get(IntentRow, o.intent_id))), ZERO)
        + fees,
        rules_current=inst.reviewed,
        seconds_to_close=Decimal(str((close - timestamp).total_seconds())),
        exit_authorized=settings.inventory_exit_min_price is not None
        and known
        and not unknown
        and not foreign,
        exit_epoch=epoch,
        exit_reference_bid=reference,
        exit_quantity_basis=Decimal(exit_data["quantity_basis"]) if exit_data else None,
        exit_slippage_spent=spent,
        exit_slippage_reserved=reserved,
        exit_slippage_budget=Decimal(exit_data["slippage_budget"]) if exit_data else None,
    )
    if settings.participation_model == "CDJ_BOOK_IMBALANCE":
        from sigbot.strategies.mm_sig_v1.live.imbalance import book_features, estimate

        features = book_features(book, inst.tick)
        if features:
            key = [features[k] for k in ("imbalance_state", "spread_ticks", "odds_bucket")]
            cache = inst.mapping_evidence.get("mm13_signal_cache", {})
            if (
                cache.get("key") == key
                and cache.get("binding") == [inst.rules_hash, inst.mapping_hash]
                and cache.get("at")
                and 0 <= (timestamp - utc_time(cache["at"])).total_seconds() < 30
            ):
                signal = {
                    **cache["signal"],
                    "feature": features,
                    "statistical_model_as_of": cache["at"],
                }
            else:
                signal = estimate(
                    session,
                    account.id,
                    inst.id,
                    timestamp,
                    settings,
                    inst.tick,
                    features,
                    (inst.rules_hash, inst.mapping_hash),
                )
                inst.mapping_evidence = {
                    **inst.mapping_evidence,
                    "mm13_signal_cache": {
                        "at": timestamp.isoformat(),
                        "key": key,
                        "binding": [inst.rules_hash, inst.mapping_hash],
                        "signal": signal,
                    },
                }
            inputs = inputs.model_copy(update={"public_signal": signal})
    if settings.pricing_model == "GUEANT_FINITE_GRID" or settings.model_comparisons:
        from sigbot.strategies.mm_sig_v1.live.calibration import estimate as standard_estimate

        parameters = inst.mapping_evidence.get("mm_standard_parameters")
        binding = [inst.rules_hash, inst.mapping_hash]
        if (
            not parameters
            or parameters.get("binding") != binding
            or not 0 <= (timestamp - utc_time(parameters["at"])).total_seconds() < 30
        ):
            parameters = standard_estimate(
                session, account.id, inst.id, timestamp, settings, binding
            )
            inst.mapping_evidence = {**inst.mapping_evidence, "mm_standard_parameters": parameters}
        inputs = inputs.model_copy(update={"standard_parameters": parameters})
        from sigbot.strategies.mm_sig_v1.live.standard import portfolio_bound

        held_ids = (
            {p.instrument_id for p in positions if p.quantity}
            | {o.instrument_id for o in active if o.side == "BUY" and o.reserve_cash}
            | {inst.id}
        )
        exposures = []
        held_map = {p.instrument_id: p.quantity for p in positions}
        # Only price-risk metadata for the actual risk set, not all cached books.
        for asset in session.scalars(select(Instrument).where(Instrument.id.in_(held_ids))):
            local = asset.mapping_evidence.get("mm_standard_parameters", {})
            risk = local.get("price_risk", {})
            usable = (
                local.get("binding") == [asset.rules_hash, asset.mapping_hash]
                and local.get("at")
                and 0 <= (timestamp - utc_time(local["at"])).total_seconds() < 30
                and risk.get("status") == "READY_LOCAL_VARIANCE"
            )
            exposures.append(
                {
                    "exchange_id": asset.exchange_id,
                    "outcome": asset.outcome,
                    "quantity": held_map.get(asset.id, ZERO),
                    "pending_quantity": sum(
                        (
                            o.quantity - o.filled
                            for o in active
                            if o.instrument_id == asset.id and o.side == "BUY" and o.reserve_cash
                        ),
                        ZERO,
                    ),
                    "variance": Decimal(risk["variance_price_squared_per_second"])
                    if usable
                    else None,
                }
            )
        inputs = inputs.model_copy(
            update={"portfolio_risk": portfolio_bound(exposures, inst.exchange_id)}
        )
    return inputs, stats


def record(
    store,
    account_id,
    instrument_id,
    book,
    quote,
    timestamp,
    *,
    reference_usable=True,
    price_reference_usable=None,
    quote_usable=None,
    order_exposure_usable=None,
    activity="UNKNOWN",
    session=None,
    orders=None,
):
    if not quote.get("reference_usable", True):
        with store.transaction() if session is None else nullcontext(session) as session:
            store.audit(
                session,
                account_id,
                "MM13_OBSERVATION",
                {
                    "instrument_id": instrument_id,
                    "reference_usable": False,
                    "eligible_orders": {},
                    "activity": activity,
                    "quote": {k: v for k, v in quote.items() if k != "fill_quality_samples"},
                    "layer": "REAL_READ" if store.namespace == "live" else "MOCK",
                },
                timestamp,
            )
        return
    bid, ask = external_best(book)
    from sigbot.strategies.mm_sig_v1.live.imbalance import book_features

    features = book_features(book, Decimal(quote.get("tick", "0.005")))
    eligible: dict[str, list[str]] = {"BUY": [], "SELL": []}
    groups, queues = {}, {}
    with store.transaction() if session is None else nullcontext(session) as session:
        rows = (
            orders
            if orders is not None
            else session.execute(
                select(Order, IntentRow)
                .join(IntentRow, Order.intent_id == IntentRow.id)
                .where(
                    Order.account_id == account_id,
                    Order.instrument_id == instrument_id,
                    Order.state.in_(["OPEN", "PARTIAL"]),
                    Order.request_state == "ACKNOWLEDGED",
                    Order.cancel_state.is_(None),
                )
            )
        )
        for order, row in rows:
            if (
                not owned(row)
                or order.state not in {"OPEN", "PARTIAL"}
                or order.request_state != "ACKNOWLEDGED"
                or order.cancel_state
            ):
                continue
            passive = order.price < ask if order.side == "BUY" else order.price > bid
            guard = (
                order.price <= Decimal(quote["buy_guard"])
                if order.side == "BUY"
                else order.price >= Decimal(quote["sell_guard"])
            )
            exposure_ok = (
                reference_usable
                and (reference_usable if quote_usable is None else quote_usable)
                and guard
                if order_exposure_usable is None
                else order_exposure_usable
            )
            if (
                exposure_ok
                and passive
                and json.loads(row.payload.get("note") or "{}").get("kind") == "MM_REAL_PASSIVE"
            ):
                eligible[order.side].append(order.id)
                from sigbot.strategies.mm_sig_v1.live.lifecycle import visible_queue

                queues[order.id] = visible_queue(book, order)
                groups[order.id] = [
                    order.side,
                    int(
                        (
                            abs(order.price - (bid + ask) / 2) / Decimal(quote["tick"])
                        ).to_integral_value(rounding="ROUND_FLOOR")
                    ),
                    order.state,
                    activity,
                ]
        store.audit(
            session,
            account_id,
            "MM13_OBSERVATION",
            {
                "instrument_id": instrument_id,
                "mid": str((bid + ask) / 2),
                "sequence": book.sequence,
                "book_features": features,
                "price_reference_usable": bool(features)
                and (
                    reference_usable if price_reference_usable is None else price_reference_usable
                ),
                "signal_binding": [quote.get("rules_hash"), quote.get("mapping_hash")],
                "source_at": book.source_at.isoformat(),
                "reference_usable": reference_usable,
                "quote_usable": reference_usable if quote_usable is None else quote_usable,
                "order_exposure_usable": (
                    reference_usable
                    and (reference_usable if quote_usable is None else quote_usable)
                    if order_exposure_usable is None
                    else order_exposure_usable
                ),
                "eligible_orders": eligible,
                "order_groups": groups,
                "queue_estimates": queues,
                "activity": activity,
                "quote": {k: v for k, v in quote.items() if k != "fill_quality_samples"},
                "layer": "REAL_READ" if store.namespace == "live" else "MOCK",
                "synthetic": store.namespace != "live",
            },
            timestamp,
        )
