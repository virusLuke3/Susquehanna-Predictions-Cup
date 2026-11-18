"""MM 1.3 deterministic Decimal policy; proposed parameters, never fitted alpha."""

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from pydantic import Field

from sigbot.domain import ZERO, Money, StrictModel
from sigbot.errors import SchemaMismatch
from sigbot.strategies.mm_sig_v1.core import external_best
from sigbot.strategies.mm_sig_v1.live.models import (
    event_cash_budget,
    policy_capital,
    quote_cash_budget,
)


class PolicyInputs(StrictModel):
    capital: Money = Field(ge=0)
    inventory: Money = Field(ge=0)
    event_inventory: Money = Field(ge=0)
    event_cost: Money = Field(ge=0)
    committed_buy_quantity: Money = Field(ge=0)
    committed_buy_cost: Money = Field(ge=0)
    other_sell_reserved: Money = Field(ge=0)
    cash_available: Money = Field(ge=0)
    buy_fee: Money = Field(ge=0)
    sell_fee: Money = Field(ge=0)
    lot_step: Money = Field(gt=0)
    sell_rate_per_minute: Money | None = Field(default=None, ge=0)
    buy_adverse: Money | None = Field(default=None, ge=0)
    sell_adverse: Money | None = Field(default=None, ge=0)
    movement_scale: Money | None = Field(default=None, ge=0)
    movement_risk_floor: Money = Field(default="0", ge=0, validate_default=True)
    oldest_lot_seconds: Money | None = Field(default=None, ge=0)
    exploration: bool = False
    new_risk_allowed: bool = True
    reduce_risk_allowed: bool | None = None
    pending_own_ack: bool = False
    join_probe_authorized: bool = False
    calibration_authorized: bool = False
    calibration_committed_cost: Money = Field(default="0", ge=0, validate_default=True)
    complementary_reduce_quantity: Money = Field(default="0", ge=0, validate_default=True)
    public_signal: dict | None = None
    standard_parameters: dict | None = None
    portfolio_risk: dict = Field(default_factory=dict)
    account_scope_blockers: list[str] = Field(default_factory=list)
    replacement_latency_seconds: Money | None = Field(default=None, ge=0)
    rules_current: bool = True
    seconds_to_close: Money
    exit_authorized: bool = False
    exit_epoch: str | None = None
    exit_reference_bid: Money | None = Field(default=None, gt=0, lt=1)
    exit_quantity_basis: Money | None = Field(default=None, gt=0)
    exit_slippage_spent: Money = Field(default="0", ge=0, validate_default=True)
    exit_slippage_reserved: Money = Field(default="0", ge=0, validate_default=True)
    exit_slippage_budget: Money | None = Field(default=None, ge=0)

    @property
    def can_reduce(self):
        return (
            self.new_risk_allowed or self.exit_authorized
            if self.reduce_risk_allowed is None
            else self.reduce_risk_allowed
        )


def floor_grid(value, step):
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_grid(value, step):
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def exit_batch(book, bid, tick, base, settings, inputs):
    """A total parent target; existing own reservations are not new batch allowance."""
    price = max(
        ceil_grid(settings.inventory_exit_min_price, tick),
        ceil_grid(bid * (1 - settings.exit_max_price_impact_fraction), tick),
        bid - settings.exit_depth_band_ticks * tick,
    )
    bids = [level for level in book.bids if level.quantity > level.own_quantity]
    depth = sum(
        (level.quantity - level.own_quantity for level in bids if level.price >= price), ZERO
    )
    band_depth = sum(
        (
            level.quantity - level.own_quantity
            for level in bids
            if level.price >= bid - settings.exit_depth_band_ticks * tick
        ),
        ZERO,
    )
    reference = inputs.exit_reference_bid or bid
    basis = inputs.exit_quantity_basis or inputs.inventory
    budget = reference * basis * settings.exit_total_slippage_fraction
    if inputs.exit_slippage_budget is not None:
        budget = min(budget, inputs.exit_slippage_budget)
    remaining = max(ZERO, budget - inputs.exit_slippage_spent)
    concession = max(ZERO, reference - price + inputs.sell_fee)
    quantity = min(
        max(ZERO, inputs.inventory - inputs.other_sell_reserved),
        base,
        depth,
        band_depth * settings.exit_depth_fraction,
    )
    if concession:
        quantity = min(quantity, remaining / concession)
    quantity = floor_grid(quantity, inputs.lot_step)
    blocker = (
        "EXIT_SLIPPAGE_BUDGET_EXHAUSTED"
        if concession and remaining < concession * inputs.lot_step
        else "EXIT_BATCH_DEPTH_OR_SIZE_UNAVAILABLE"
        if quantity == 0 and inputs.inventory > 0
        else None
    )
    return (
        price,
        quantity,
        {
            "exit_epoch": inputs.exit_epoch,
            "exit_reference_bid": str(reference),
            "exit_quantity_basis": str(basis),
            "exit_expected_worst_price": str(price),
            "exit_expected_worst_concession": str(quantity * concession),
            "exit_slippage_budget": str(budget),
            "exit_slippage_spent": str(inputs.exit_slippage_spent),
            "exit_slippage_reserved": str(inputs.exit_slippage_reserved),
            "exit_slippage_unreserved": str(max(ZERO, remaining - inputs.exit_slippage_reserved)),
            "exit_blocker": blocker,
            "exit_batch_size_is_total_parent_target": True,
        },
    )


def one_sided_exit(book, tick, settings, inputs):
    bids = [level for level in book.bids if level.quantity > level.own_quantity]
    bid = max((level.price for level in bids), default=None)
    depth = sum(
        (
            level.quantity - level.own_quantity
            for level in bids
            if bid is not None and level.price >= bid - settings.exit_depth_band_ticks * tick
        ),
        ZERO,
    )
    allowed = (
        inputs.exit_authorized
        and bid is not None
        and settings.inventory_exit_min_price is not None
        and bid >= settings.inventory_exit_min_price
        and inputs.seconds_to_close > 0
    )
    exit_price, quantity, details = bid, ZERO, {}
    if allowed:
        base = floor_grid(
            quote_cash_budget(settings, policy_capital(settings, inputs.capital))
            / (bid + inputs.sell_fee),
            inputs.lot_step,
        )
        exit_price, quantity, details = exit_batch(book, bid, tick, base, settings, inputs)
    price = str(exit_price) if exit_price is not None else None
    return {
        "bid": price,
        "ask": price,
        "external_bid": str(bid) if bid is not None else None,
        "external_ask": None,
        "buy_quantity": "0",
        "sell_quantity": str(quantity),
        "maximum_buy_total": "0",
        "maximum_sell_total": str(quantity),
        "buy_guard": "0",
        "sell_guard": str(settings.inventory_exit_min_price or ZERO),
        "mode": "REDUCE_ONLY" if inputs.seconds_to_close > 0 else "SETTLEMENT",
        "sell_kind": "INVENTORY_EXIT",
        "reference_usable": False,
        "quality_status": "UNKNOWN_NO_TWO_SIDED_REFERENCE",
        "movement_status": "UNKNOWN",
        "inventory_quantity": str(inputs.inventory),
        "inventory_target": "0",
        "inventory_capacity": "0",
        "exit_depth": str(depth),
        "exit_blocker": None if allowed else "ONE_SIDED_EXIT_REQUIRES_TRUSTED_BID_AND_AUTHORITY",
        "capital": str(policy_capital(settings, inputs.capital)),
        "tick": str(tick),
        "lot_step": str(inputs.lot_step),
        "oldest_lot_seconds": str(inputs.oldest_lot_seconds)
        if inputs.oldest_lot_seconds is not None
        else None,
        "price_source_at": book.source_at.isoformat(),
        "received_at": book.received_at.isoformat(),
        "sequence": book.sequence,
        "pricing_policy": "MM13_ONE_SIDED_COVERED_INVENTORY_EXIT",
        "parameters_return_calibrated": False,
        "net_spread": None,
        "quote_spread": None,
        "passive_before_submit": False,
        **details,
    }


def quote(book, tick, settings, inputs: PolicyInputs):
    if book.quality != "HEALTHY" or not book.own_orders_known:
        raise SchemaMismatch("MM13_BOOK_INDEPENDENCE_OR_CONTINUITY_UNKNOWN")
    if not tick.is_finite() or tick <= 0:
        raise SchemaMismatch("MM_TICK_UNVERIFIED")
    for levels in (book.bids, book.asks):
        for level in levels:
            if level.price % tick or level.quantity % inputs.lot_step:
                raise SchemaMismatch("MM_BOOK_GRID_INVALID")
    if not any(level.quantity > level.own_quantity for level in book.bids) or not any(
        level.quantity > level.own_quantity for level in book.asks
    ):
        return one_sided_exit(book, tick, settings, inputs)
    if settings.pricing_model == "GUEANT_FINITE_GRID":
        from sigbot.strategies.mm_sig_v1.live.standard_policy import quote as standard_quote

        return standard_quote(book, tick, settings, inputs)
    bid, ask = external_best(book)
    capital = policy_capital(settings, inputs.capital)
    cap_price = ask - tick
    if cap_price <= 0:
        raise SchemaMismatch("MM_NO_LEGAL_PASSIVE_BUY_PRICE")
    base = floor_grid(
        quote_cash_budget(settings, capital) / (cap_price + inputs.buy_fee), inputs.lot_step
    )
    depth = sum(
        (
            level.quantity - level.own_quantity
            for level in book.bids
            if level.price >= bid - tick * settings.exit_depth_band_ticks
        ),
        ZERO,
    )
    depth_cap = floor_grid(depth * settings.exit_depth_fraction, inputs.lot_step)
    rate = inputs.sell_rate_per_minute
    cdj = settings.participation_model == "CDJ_BOOK_IMBALANCE"
    signal = inputs.public_signal or {}
    signal_buy_ready = signal.get("BUY", {}).get("status") == "READY_EMPIRICAL_PROXY"
    signal_sell_ready = signal.get("SELL", {}).get("status") == "READY_EMPIRICAL_PROXY"
    capacity = (
        min(base, depth_cap)
        if rate is None
        else min(rate * settings.capacity_unwind_minutes, depth_cap)
    )
    movement = max(
        inputs.movement_scale if inputs.movement_scale is not None else tick,
        inputs.movement_risk_floor,
    )
    phi = tick / max(tick, movement)
    if rate is None:
        target = (
            phi * min(base / 2, capacity)
            if inputs.exploration or cdj and signal_buy_ready
            else ZERO
        )
    else:
        target = phi * min(base, capacity / 4, rate * settings.target_turnover_minutes)
    soft_age = (
        inputs.oldest_lot_seconds is not None
        and inputs.oldest_lot_seconds >= settings.soft_oldest_lot_minutes * 60
    )
    hard_age = (
        inputs.oldest_lot_seconds is not None
        and inputs.oldest_lot_seconds >= settings.hard_oldest_lot_minutes * 60
    )
    closing = inputs.seconds_to_close <= settings.no_new_inventory_hours_before_close * 3600
    if (
        soft_age
        or closing
        or not inputs.rules_current
        or rate == 0
        or not inputs.new_risk_allowed
        or inputs.event_inventory + inputs.committed_buy_quantity > capacity
    ):
        target = ZERO
    z = (
        max(Decimal(-1), min(Decimal(1), (inputs.inventory - target) / max(target, base)))
        if base
        else Decimal(1)
    )
    buy_total = floor_grid(base * max(ZERO, min(Decimal(1), 1 - z)), inputs.lot_step)
    sell_total = floor_grid(
        min(inputs.inventory, base * max(ZERO, min(Decimal(2), 1 + z))), inputs.lot_step
    )
    if target == 0 and (
        not (inputs.exploration or cdj and signal_buy_ready)
        or soft_age
        or closing
        or not inputs.new_risk_allowed
        or rate == 0
        or not inputs.rules_current
    ):
        buy_total = ZERO
    if inputs.movement_scale is None and not (inputs.exploration or cdj and signal_buy_ready):
        buy_total = ZERO
    mid = (bid + ask) / 2
    center = mid - tick * settings.inventory_skew_ticks * z
    half = (ask - bid) / 2
    eta = tick * settings.per_side_margin_ticks
    as_b = (
        inputs.buy_adverse
        if inputs.buy_adverse is not None
        else tick * settings.adverse_prior_ticks
    )
    as_a = (
        inputs.sell_adverse
        if inputs.sell_adverse is not None
        else tick * settings.adverse_prior_ticks
    )
    buy_guard = mid - inputs.buy_fee - as_b - eta
    sell_guard = mid + inputs.sell_fee + as_a + eta
    if cdj:
        # The new objective is positive side edge after data uncertainty, fees and inventory cost.
        # Own-fill AS remains separately UNKNOWN and measured negative evidence cannot be waived.
        if signal_buy_ready:
            forecast = Decimal(signal["BUY"]["conservative_change"])
            buy_guard = min(
                mid
                + forecast
                - inputs.buy_fee
                - tick * settings.inventory_skew_ticks * max(ZERO, z),
                mid - inputs.buy_fee - inputs.buy_adverse
                if inputs.buy_adverse is not None
                else Decimal(1),
            )
        if signal_sell_ready:
            forecast = Decimal(signal["SELL"]["conservative_change"])
            sell_guard = max(
                mid
                + forecast
                + inputs.sell_fee
                - tick * settings.inventory_skew_ticks * max(ZERO, z),
                mid + inputs.sell_fee + inputs.sell_adverse
                if inputs.sell_adverse is not None
                else ZERO,
            )
    buy = floor_grid(min(center - half, buy_guard, cap_price), tick)
    sell = ceil_grid(max(center + half, sell_guard, bid + tick), tick)
    raw_buy_ceiling, raw_sell_floor = buy_guard, sell_guard
    rounded_buy_ceiling, rounded_sell_floor = (
        floor_grid(buy_guard, tick),
        ceil_grid(sell_guard, tick),
    )
    probe = (
        settings.join_probe_enabled
        and inputs.join_probe_authorized
        and inputs.exploration
        and inputs.buy_adverse is None
    )
    buy_competitive, sell_competitive = False, False
    if inputs.buy_adverse is not None and not cdj or cdj and signal_buy_ready:
        candidates = [bid + tick, bid] if ask - bid >= 2 * tick and z <= 0 else [bid]
        for candidate in candidates:
            if candidate < ask and (candidate < buy_guard if cdj else candidate <= buy_guard):
                buy, buy_competitive = candidate, True
                break
    elif not cdj and probe and ask - bid - inputs.buy_fee - inputs.sell_fee > 0:
        buy, buy_guard, buy_competitive = bid, bid, True
        buy_total = min(buy_total, floor_grid(base * settings.join_probe_fraction, inputs.lot_step))
    if inputs.sell_adverse is not None and not cdj or cdj and signal_sell_ready:
        candidates = [ask - tick, ask] if ask - bid >= 2 * tick and z > 0 else [ask]
        for candidate in candidates:
            if candidate > bid and (candidate > sell_guard if cdj else candidate >= sell_guard):
                sell, sell_competitive = candidate, True
                break
    state = "REDUCE_ONLY" if target == 0 else "QUOTING"
    kind = "MM_REAL_PASSIVE"
    exit_reason = None
    exit_details = {}
    if (
        hard_age
        or inputs.inventory > capacity
        and (rate is not None or inputs.inventory > depth_cap)
        or inputs.seconds_to_close <= settings.exit_only_hours_before_close * 3600
    ):
        state = "REDUCE_ONLY"
        buy_total = ZERO
        if inputs.exit_authorized and settings.inventory_exit_min_price is not None:
            floor = ceil_grid(settings.inventory_exit_min_price, tick)
            if bid >= floor:
                kind = "INVENTORY_EXIT"
                sell, sell_total, exit_details = exit_batch(book, bid, tick, base, settings, inputs)
            else:
                exit_reason = "EXIT_PRICE_PROTECTION_BLOCKED"
        else:
            exit_reason = "ACTIVE_INVENTORY_EXIT_NOT_AUTHORIZED"
    buy_reasons, sell_reasons = [], []
    if not ZERO < buy < 1:
        buy_reasons.append("BUY_PRICE_OUT_OF_BOUNDS")
        buy_total = ZERO
    if not ZERO < sell < 1:
        sell_reasons.append("SELL_PRICE_OUT_OF_BOUNDS")
        sell_total = ZERO
    if inputs.seconds_to_close <= 0:
        state, buy_total, sell_total = "SETTLEMENT", ZERO, ZERO
    budget = event_cash_budget(settings, capital)
    remaining_cost = max(ZERO, budget - inputs.event_cost - inputs.committed_buy_cost)
    quantity_cap = max(ZERO, capacity - inputs.event_inventory - inputs.committed_buy_quantity)
    # Total target includes existing buys: no additional quantity is manufactured each cycle.
    affordable_total = (
        inputs.committed_buy_quantity
        + min(
            quantity_cap,
            depth_cap,
            floor_grid(
                min(remaining_cost, inputs.cash_available) / (buy + inputs.buy_fee), inputs.lot_step
            ),
        )
        if buy > 0
        else ZERO
    )
    buy_total = floor_grid(min(buy_total, affordable_total), inputs.lot_step)
    if (
        inputs.event_inventory + inputs.committed_buy_quantity > capacity
        or inputs.event_cost + inputs.committed_buy_cost > budget
    ):
        buy_total = ZERO
        buy_reasons.append("INVENTORY_OR_EVENT_COST_CAPACITY_EXCEEDED")
    sell_total = floor_grid(
        min(sell_total, max(ZERO, inputs.inventory - inputs.other_sell_reserved)), inputs.lot_step
    )
    if (
        kind == "MM_REAL_PASSIVE"
        and buy_total > 0
        and sell_total > 0
        and sell - buy - inputs.buy_fee - inputs.sell_fee <= 0
    ):
        buy_total, sell_total = ZERO, ZERO
        buy_reasons.append("NONPOSITIVE_PAIRED_NET_SPREAD")
        sell_reasons.append("NONPOSITIVE_PAIRED_NET_SPREAD")
    buy_decision = "QUOTE" if buy_total > 0 else "WITHDRAW"
    sell_decision = "QUOTE" if sell_total > 0 else "WITHDRAW"
    buy_reason, sell_reason = "VALID_TOTAL_TARGET", "VALID_TOTAL_TARGET"
    known_unsafe = not buy_competitive and (
        inputs.buy_adverse is not None or cdj and signal_buy_ready
    )
    if inputs.pending_own_ack and not buy_reasons and not known_unsafe:
        buy_decision, buy_reason = "DEFER", "WAIT_OWN_ACK"
    elif not buy_competitive:
        buy_total, buy_decision = ZERO, "WITHDRAW"
        buy_reason = (
            ("CDJ_MODEL_WARMUP" if not signal_buy_ready else "CDJ_ADVERSE_BUY_STATE")
            if cdj
            else ("NO_QUOTE_UNCALIBRATED" if inputs.buy_adverse is None else "NO_COMPETITIVE_QUOTE")
        )
    if cdj and signal_sell_ready and not sell_competitive and kind == "MM_REAL_PASSIVE":
        sell_total, sell_decision, sell_reason = ZERO, "WITHDRAW", "CDJ_ADVERSE_SELL_STATE"
    if kind == "MM_REAL_PASSIVE" and not sell_competitive and inputs.inventory > 0:
        # Inventory may still be offered passively at its protective floor. Label it explicitly.
        sell_reason = (
            "PASSIVE_INVENTORY_OFFER_UNCALIBRATED"
            if inputs.sell_adverse is None
            else "PASSIVE_INVENTORY_OFFER"
        )
    if ask - bid - inputs.buy_fee - inputs.sell_fee <= 0:
        buy_total, buy_decision, buy_reason = ZERO, "WITHDRAW", "NO_QUOTE_COST"
    if buy_reasons:
        buy_reason = buy_reasons[0]
    if sell_reasons:
        sell_reason = sell_reasons[0]
    if buy_total == 0 and buy_decision != "DEFER" and buy_reason == "VALID_TOTAL_TARGET":
        buy_reason = buy_reason if buy_reasons else "INVENTORY_TARGET_OR_CAPACITY_ZERO"
    if sell_total == 0 and sell_reason == "VALID_TOTAL_TARGET":
        sell_reason = sell_reason if sell_reasons else "NO_AVAILABLE_INVENTORY"
    if buy_total == 0 and sell_total == 0 and buy_decision != "DEFER" and inputs.inventory == 0:
        state = "NO_COMPETITIVE_QUOTE"
    elif inputs.inventory == 0:
        state = (
            "PASSIVE_ACCUMULATION"
            if inputs.buy_adverse is not None or cdj and signal_buy_ready
            else "WARMUP"
        )
    elif buy_total > 0 and sell_total > 0:
        state = "TWO_SIDED"
    return {
        "implementation_revision": "1.3.2" if cdj else "1.3.1",
        "participation_model": settings.participation_model,
        "pricing_risk_source": "PUBLIC_BOOK_DEPLETION_PROXY_WITH_OWN_FILL_CHECK"
        if cdj
        else "OWN_FILL_AS_OR_EXPLICIT_PROBE",
        "public_book_signal": signal if cdj else None,
        "buy_edge_proxy": str(buy_guard - buy) if cdj and signal_buy_ready else None,
        "sell_edge_proxy": str(sell - sell_guard) if cdj and signal_sell_ready else None,
        "own_fill_buy_AS": str(inputs.buy_adverse) if inputs.buy_adverse is not None else None,
        "own_fill_sell_AS": str(inputs.sell_adverse) if inputs.sell_adverse is not None else None,
        "buy_directive": buy_decision,
        "sell_directive": sell_decision,
        "buy_directive_reason": buy_reason,
        "sell_directive_reason": sell_reason,
        "allow_new_orders": inputs.new_risk_allowed and not inputs.pending_own_ack,
        "target_buy_leaves": None if buy_decision == "DEFER" else str(buy_total),
        "target_sell_leaves": str(sell_total),
        "additional_buy_quantity": str(max(ZERO, buy_total - inputs.committed_buy_quantity)),
        "raw_buy_ceiling": str(raw_buy_ceiling),
        "rounded_buy_ceiling": str(rounded_buy_ceiling),
        "raw_sell_floor": str(raw_sell_floor),
        "rounded_sell_floor": str(rounded_sell_floor),
        "buy_competitive": buy_competitive,
        "sell_competitive": sell_competitive,
        "buy_adverse_status": "ESTIMATED" if inputs.buy_adverse is not None else "PRIOR_ONLY",
        "sell_adverse_status": "ESTIMATED" if inputs.sell_adverse is not None else "PRIOR_ONLY",
        "join_probe": probe,
        "probe_positive_expectation_proven": False,
        "bid": str(buy),
        "ask": str(sell),
        "external_bid": str(bid),
        "external_ask": str(ask),
        "quote_spread": str(sell - buy),
        "net_spread": str(sell - buy - inputs.buy_fee - inputs.sell_fee),
        "buy_quantity": str(buy_total),
        "sell_quantity": str(sell_total),
        "maximum_buy_total": str(affordable_total if not buy_reasons else ZERO),
        "maximum_sell_total": str(max(ZERO, inputs.inventory - inputs.other_sell_reserved)),
        "inventory_quantity": str(inputs.inventory),
        "base_quantity": str(base),
        "inventory_target": str(target),
        "inventory_capacity": str(capacity),
        "inventory_z": str(z),
        "exit_depth": str(depth),
        "buy_guard": str(buy_guard),
        "sell_guard": str(sell_guard),
        "buy_adverse": str(inputs.buy_adverse)
        if cdj and inputs.buy_adverse is not None
        else None
        if cdj
        else str(as_b),
        "sell_adverse": str(inputs.sell_adverse)
        if cdj and inputs.sell_adverse is not None
        else None
        if cdj
        else str(as_a),
        "quality_status": "ESTIMATED"
        if inputs.buy_adverse is not None and inputs.sell_adverse is not None
        else "UNKNOWN_USING_MARKED_PRIOR",
        "movement_status": "OBSERVED"
        if inputs.movement_scale is not None
        else "WARMUP_WITH_PARTIAL_CONSERVATIVE_FLOOR"
        if inputs.movement_risk_floor > tick
        else "WARMUP_USING_ONE_TICK_PRIOR",
        "movement_risk_floor": str(inputs.movement_risk_floor),
        "movement_floor_is_tail_bound": False,
        "mode": state,
        "sell_kind": kind,
        "exit_priority": 2
        if inputs.seconds_to_close <= settings.priority_exit_hours_before_close * 3600
        else 1
        if kind == "INVENTORY_EXIT"
        else 0,
        "exit_blocker": exit_reason,
        "buy_blocking_reasons": buy_reasons,
        "sell_blocking_reasons": sell_reasons,
        "oldest_lot_seconds": str(inputs.oldest_lot_seconds)
        if inputs.oldest_lot_seconds is not None
        else None,
        "price_source_at": book.source_at.isoformat(),
        "received_at": book.received_at.isoformat(),
        "sequence": book.sequence,
        "capital": str(capital),
        "lot_step": str(inputs.lot_step),
        "pricing_policy": "MM13_SIDE_QUALITY_INVENTORY_AND_EXIT_CAPACITY",
        "roundtrip_fee_stress_net_spreads": {
            str(t): str(sell - buy - tick * t) for t in (ZERO, Decimal(".5"), Decimal(1))
        },
        "stress_is_actual_fee": False,
        "parameters_return_calibrated": False,
        "passive_before_submit": kind != "INVENTORY_EXIT",
        **exit_details,
    }
