"""Standard model integration; execution and accounting remain in the shared OMS."""

from decimal import Decimal
from functools import lru_cache

from sigbot.domain import ZERO
from sigbot.strategies.mm_sig_v1.core import external_best
from sigbot.strategies.mm_sig_v1.live.models import (
    event_cash_budget,
    policy_capital,
    quote_cash_budget,
)
from sigbot.strategies.mm_sig_v1.live.policy import ceil_grid, floor_grid
from sigbot.strategies.mm_sig_v1.live.standard import (
    Intensity,
    as_baseline,
    finite_grid,
    liquidation,
)


@lru_cache(maxsize=32)
def solve_cached(scalars, costs, buy, sell, cross_penalty=ZERO):
    keys = (
        "bid",
        "ask",
        "tick",
        "inventory",
        "capacity",
        "gamma",
        "variance_per_second",
        "horizon_seconds",
        "buy_fee",
        "sell_fee",
    )
    return finite_grid(
        **dict(zip(keys, scalars, strict=True)),
        terminal_costs=dict(enumerate(costs)),
        buy=buy,
        sell=sell,
        cross_penalty_per_second=cross_penalty,
    )


def parameters_ready(parameters, inventory=ZERO):
    """A flat account needs a BUY estimate; an inventory-bearing core needs both."""
    return bool(
        parameters.get("price_risk", {}).get("status") == "READY_LOCAL_VARIANCE"
        and parameters.get("BUY", {}).get("status") == "READY_OWN_ORDER_INTENSITY"
        and (
            not inventory or parameters.get("SELL", {}).get("status") == "READY_OWN_ORDER_INTENSITY"
        )
    )


def sampling(result, bid, ask, tick, settings, inputs):
    """Bounded baseline quoting while the separate own-fill model learns."""
    reason = "CALIBRATION_AUTHORIZATION_REQUIRED"
    result.update(
        selected_quote_core="BASIC_PASSIVE_MM",
        standard_core_execution_active=False,
        calibration_sampling=True,
        parameters_return_calibrated=False,
        account_scope_blockers=inputs.account_scope_blockers,
    )
    if inputs.account_scope_blockers:
        reason = inputs.account_scope_blockers[0]
        result.update(
            buy_directive_reason=reason, sell_directive_reason=reason, mode="ACCOUNT_SCOPE_BLOCKED"
        )
        return result
    if not inputs.calibration_authorized:
        result.update(buy_directive_reason=reason, sell_directive_reason=reason, mode=reason)
        return result
    capital = policy_capital(settings, inputs.capital)
    budget = quote_cash_budget(settings, capital, calibration=True)
    limit = (
        settings.initial_capital_limit * settings.calibration_exposure_fraction
        if settings.calibration_exposure_fraction is not None
        else inputs.cash_available + inputs.calibration_committed_cost
    )
    # A one-tick book has discrete participation choices. Do not damage queue
    # position just to create a second support for an unidentifiable k fit.
    b, a = bid, ask
    parameters = inputs.standard_parameters or {}
    size = int(floor_grid(budget / (b + inputs.buy_fee), inputs.lot_step))
    if settings.complementary_quotes_enabled:
        # Equal shares on both binary sides; fund the dearer side independently.
        size = min(
            size,
            int(
                floor_grid(
                    budget
                    / max(
                        ask + inputs.buy_fee,
                        1 - bid + inputs.sell_fee,
                    ),
                    inputs.lot_step,
                )
            ),
        )
    # Existing reservations are already counted; the proposed target replaces
    # this event's current BUY target. Shared allocation rechecks this bound.
    remaining = max(ZERO, limit - inputs.calibration_committed_cost + inputs.committed_buy_cost)
    # The caller supplies its independently derived unwind capacity.
    capacity = Decimal(result["inventory_capacity"])
    # A full entry reaches the working centre. Size comes from actual capacity,
    # not the minimum lot or a fill model measured for a different quantity.
    size = min(size, int(floor_grid(capacity / 2, inputs.lot_step)))
    # A quote lot is an order-size ceiling, not the inventory target or risk
    # limit. One full entry reaches the working center instead of disabling BUY.
    movement = max(tick, inputs.movement_scale or tick, inputs.movement_risk_floor)
    working_target = floor_grid(min(Decimal(size) * tick / movement, capacity / 2), inputs.lot_step)
    working_upper = min(capacity, 2 * working_target)
    if ask - bid - tick - inputs.buy_fee - inputs.sell_fee > 0:
        if inputs.inventory > working_target:
            a = ask - tick
        elif inputs.inventory < working_target:
            b = bid + tick
    risk = parameters.get("price_risk", {})
    variance = (
        Decimal(risk["variance_price_squared_per_second"])
        if risk.get("status") == "READY_LOCAL_VARIANCE"
        else None
    )
    gamma = settings.model_risk_aversion
    if gamma is None:
        gamma = 1 / quote_cash_budget(settings, settings.initial_capital_limit)
    horizon = min(
        Decimal(settings.capacity_unwind_minutes * 60), max(ZERO, inputs.seconds_to_close)
    )
    if inputs.oldest_lot_seconds is not None and inputs.inventory:
        horizon = min(
            horizon,
            max(ZERO, Decimal(settings.hard_oldest_lot_minutes * 60) - inputs.oldest_lot_seconds),
        )
    shift = (
        gamma * variance * horizon * (inputs.event_inventory - working_target)
        if variance is not None
        else None
    )
    cross = inputs.portfolio_risk.get("target_cross_variance_penalty")
    cross_shift = gamma * horizon * Decimal(cross) if cross is not None else ZERO
    alphas = {}
    for side in ("BUY", "SELL"):
        signal = (inputs.public_signal or {}).get(side, {})
        alphas[side] = (
            Decimal(signal["conservative_change"])
            if signal.get("status") == "READY_EMPIRICAL_PROXY"
            else None
        )
    if shift is not None or any(v is not None for v in alphas.values()) or cross_shift:
        b = floor_grid(
            min(ask - tick, bid + (alphas["BUY"] or ZERO) - (shift or ZERO) - cross_shift), tick
        )
        a = ceil_grid(max(bid + tick, ask + (alphas["SELL"] or ZERO) - (shift or ZERO)), tick)
    mid = (bid + ask) / 2
    result.update(
        reservation_buy_value=str(mid + (alphas["BUY"] or ZERO) - (shift or ZERO) - cross_shift),
        reservation_sell_value=str(mid + (alphas["SELL"] or ZERO) - (shift or ZERO)),
        reservation_price_diagnostics={
            "inventory_shift": str(shift) if shift is not None else None,
            "inventory_shift_source": "LOCAL_VARIANCE_AND_WORKING_INVENTORY"
            if shift is not None
            else "DISCRETE_INVENTORY_FALLBACK",
            "buy_alpha": str(alphas["BUY"]) if alphas["BUY"] is not None else None,
            "sell_alpha": str(alphas["SELL"]) if alphas["SELL"] is not None else None,
            "cross_market_buy_penalty": str(cross_shift) if cross is not None else None,
            "unknown_alpha_imputed": False,
        },
        portfolio_risk=inputs.portfolio_risk,
        sizing_policy="SHARED_CASH_LIQUIDATION_CAPACITY_AND_WORKING_INVENTORY",
        minimum_lot_forced=False,
        quantity_probability_extrapolated=False,
        replacement_latency_seconds=str(inputs.replacement_latency_seconds)
        if inputs.replacement_latency_seconds is not None
        else None,
        buy_fee_per_share=str(inputs.buy_fee),
        sell_fee_per_share=str(inputs.sell_fee),
    )
    if not ZERO < b < a < 1:
        result.update(
            mode="RESERVATION_OUTSIDE_PRICE_GRID",
            buy_directive_reason="RESERVATION_OUTSIDE_PRICE_GRID",
            sell_directive_reason="RESERVATION_OUTSIDE_PRICE_GRID",
        )
        return result
    buy_limit = min(
        remaining / (b + inputs.buy_fee),
        (inputs.cash_available + inputs.committed_buy_cost) / (b + inputs.buy_fee),
        max(ZERO, capacity - inputs.event_inventory),
        max(ZERO, event_cash_budget(settings, capital) - inputs.event_cost) / (b + inputs.buy_fee),
    )
    desired_buy = floor_grid(
        min(
            Decimal(size),
            max(ZERO, working_upper - inputs.event_inventory),
            buy_limit,
            budget / (b + inputs.buy_fee),
        ),
        inputs.lot_step,
    )
    if inputs.complementary_reduce_quantity:
        desired_buy = min(desired_buy, inputs.complementary_reduce_quantity)
    can_buy = (
        (inputs.new_risk_allowed or inputs.complementary_reduce_quantity > 0)
        and inputs.rules_current
        and inputs.seconds_to_close > settings.no_new_inventory_hours_before_close * 3600
        and (
            inputs.oldest_lot_seconds is None
            or inputs.oldest_lot_seconds < settings.soft_oldest_lot_minutes * 60
        )
        and a - b - inputs.buy_fee - inputs.sell_fee > 0
        and size > 0
        and desired_buy > 0
        and ZERO < b < a < 1
    )
    inventory_fraction = desired_buy / max(1, size)
    bq = int(desired_buy) if can_buy else 0
    aq = (
        int(
            floor_grid(
                min(
                    max(ZERO, inputs.inventory - inputs.other_sell_reserved),
                    budget / (a + inputs.sell_fee),
                ),
                inputs.lot_step,
            )
        )
        if inputs.can_reduce and inputs.rules_current and ZERO < b < a < 1
        else 0
    )
    guards = {"BUY": b, "SELL": a}
    edges: dict[str, str | None] = {}
    filters: dict[str, str] = {}
    for side in ("BUY", "SELL"):
        signal = (inputs.public_signal or {}).get(side, {})
        adverse = inputs.buy_adverse if side == "BUY" else inputs.sell_adverse
        fee = inputs.buy_fee if side == "BUY" else inputs.sell_fee
        ready = signal.get("status") == "READY_EMPIRICAL_PROXY"
        forecast = Decimal(signal["conservative_change"]) if ready else None
        if forecast is not None or adverse is not None:
            candidates = []
            if forecast is not None:
                candidates.append(mid + forecast - fee if side == "BUY" else mid + forecast + fee)
            if adverse is not None:
                candidates.append(mid - fee - adverse if side == "BUY" else mid + fee + adverse)
            guard = min(candidates) if side == "BUY" else max(candidates)
            guards[side] = min(b, guard) if side == "BUY" else max(a, guard)
            edge = guard - b if side == "BUY" else a - guard
            edges[side] = str(edge)
            filters[side] = "MEASURED_SIDE_EDGE" if edge > 0 else "ADVERSE_SELECTION_WITHDRAW"
            if edge <= 0:
                if side == "BUY":
                    bq = 0
                else:
                    aq = 0
        else:
            edges[side] = None
            filters[side] = "UNKNOWN_BOUNDED_AUTHORIZED_BASELINE"
    defer = inputs.pending_own_ack
    result.update(
        bid=str(b),
        ask=str(a),
        buy_guard=str(guards["BUY"]),
        sell_guard=str(guards["SELL"]),
        buy_quantity=str(bq),
        sell_quantity=str(aq),
        target_buy_leaves=None if defer else str(bq),
        target_sell_leaves=str(aq),
        maximum_buy_total=str(bq),
        maximum_sell_total=str(aq),
        additional_buy_quantity=str(max(ZERO, bq - inputs.committed_buy_quantity)),
        buy_directive="DEFER" if defer else "QUOTE" if bq else "WITHDRAW",
        sell_directive="QUOTE" if aq else "WITHDRAW",
        buy_directive_reason="WAIT_OWN_ACK"
        if defer
        else "AUTHORIZED_REAL_CALIBRATION"
        if bq
        else filters["BUY"]
        if filters["BUY"] == "ADVERSE_SELECTION_WITHDRAW"
        else "CALIBRATION_RISK_OR_SAMPLE_GATE",
        sell_directive_reason="AUTHORIZED_COVERED_CALIBRATION"
        if aq
        else filters["SELL"]
        if filters["SELL"] == "ADVERSE_SELECTION_WITHDRAW"
        else "NO_AVAILABLE_OWN_INVENTORY",
        buy_competitive=bool(bq and bid <= b < ask),
        sell_competitive=bool(aq and bid < a <= ask),
        quote_spread=str(a - b),
        net_spread=str(a - b - inputs.buy_fee - inputs.sell_fee),
        mode="BASIC_PASSIVE_MM",
        pricing_policy="DISCRETE_BBO_PARTICIPATION_WITH_INVENTORY_AND_MEASURED_SIDE_FILTERS",
        basic_core_execution_active=True,
        advanced_calibration_required_for_baseline=False,
        forced_distance_exploration=False,
        baseline_side_filters=filters,
        buy_edge_proxy=edges["BUY"],
        sell_edge_proxy=edges["SELL"],
        discrete_actions={
            "BUY": ("IMPROVE_BID" if b > bid else "JOIN_BID") if bq else "WITHDRAW",
            "SELL": ("IMPROVE_ASK" if a < ask else "JOIN_ASK") if aq else "WITHDRAW",
        },
        inventory_accumulation_fraction=str(inventory_fraction),
        complementary_reduce_quantity=str(inputs.complementary_reduce_quantity),
        inventory_target=str(working_target),
        inventory_working_upper=str(working_upper),
        inventory_risk_capacity=str(capacity),
        quote_lot_quantity=str(size),
        binary_quote_sizing="EQUAL_SHARES_BOTH_OUTCOMES_DEARER_LEG_FUNDED"
        if settings.complementary_quotes_enabled
        else "NATIVE_OUTCOME_CASH_BUDGET",
        inventory_state="REDUCE"
        if not can_buy or inputs.event_inventory >= working_upper
        else "NORMAL_QUOTING",
        allow_new_orders=not defer and inputs.rules_current,
        calibration_limits={
            "per_order_cash": str(budget),
            "strategy_committed_cash": str(inputs.calibration_committed_cost),
            "strategy_exposure_limit": str(limit),
            "profitability": "NOT_EVALUATED",
        },
    )
    return result


def quote(book, tick, settings, inputs):
    from sigbot.strategies.mm_sig_v1.live.policy import quote as legacy_quote

    legacy = legacy_quote(
        book, tick, settings.model_copy(update={"pricing_model": "LEGACY_132"}), inputs
    )
    legacy_name = (
        "LEGACY_132" if settings.participation_model == "CDJ_BOOK_IMBALANCE" else "LEGACY_131"
    )
    bid, ask = external_best(book)
    mid = (bid + ask) / 2
    capital = policy_capital(settings, inputs.capital)
    budget = quote_cash_budget(settings, settings.initial_capital_limit)
    # Frozen risk preference expressed in currency units, not a fitted market
    # parameter. It does not shrink when current measured variance increases.
    gamma = settings.model_risk_aversion if settings.model_risk_aversion is not None else 1 / budget
    horizon = min(
        Decimal(settings.capacity_unwind_minutes * 60), max(ZERO, inputs.seconds_to_close)
    )
    if inputs.oldest_lot_seconds is not None and inputs.inventory:
        horizon = min(
            horizon,
            max(ZERO, Decimal(settings.hard_oldest_lot_minutes * 60) - inputs.oldest_lot_seconds),
        )
    # Reuse an unchanged local HJB without extending the old lot's deadline.
    horizon = floor_grid(horizon, Decimal(30))
    external = [(level.price, level.quantity - level.own_quantity) for level in book.bids]
    depth = sum(
        (
            quantity
            for price, quantity in external
            if price >= bid - tick * settings.exit_depth_band_ticks
        ),
        ZERO,
    )
    capacity = int(
        floor_grid(
            min(
                event_cash_budget(settings, capital) / (ask + inputs.buy_fee),
                depth * settings.exit_depth_fraction,
            ),
            inputs.lot_step,
        )
    )
    inventory = int(inputs.inventory)
    diagnostics = {
        "objective": "EXPECTED_CASH_PLUS_INVENTORY_VALUE_MINUS_TERMINAL_LIQUIDATION_AND_RUNNING_PRICE_RISK",
        "gamma_inverse_susqie": str(gamma),
        "gamma_source": "CONFIGURED_RISK_PREFERENCE"
        if settings.model_risk_aversion is not None
        else "ONE_OVER_APPROVED_QUOTE_CASH_BUDGET",
        "horizon_seconds": str(horizon),
        "inventory_deadline_extended": False,
        "order_ttl_is_model_horizon": False,
        "local_parameters": inputs.standard_parameters,
        "liquidation_scenarios": {},
        "rho_used_in_core": False,
        "adverse_selection_assumption": "NOT_SOLVED_BY_ZERO_DRIFT_POISSON_MODEL",
    }
    # Preserve approved, evidence-bound hard inventory exit. No fitted model
    # can delay the existing lot's deadline or pretend depth guarantees a fill.
    if legacy["sell_kind"] == "INVENTORY_EXIT":
        legacy.update(
            pricing_policy="GUEANT_FINITE_GRID_WITH_EXISTING_HARD_EXIT",
            implementation_revision="1.5.0",
            standard_model={**diagnostics, "status": "AUTHORIZED_INVENTORY_EXIT"},
        )
        return legacy
    result = {
        **legacy,
        "implementation_revision": "1.5.0",
        "pricing_policy": "GUEANT_FINITE_GRID",
        "participation_model": "GUEANT_FINITE_GRID",
        "pricing_risk_source": "OWN_MARKED_FILL_INTENSITY_AND_LOCAL_PRICE_VARIANCE",
        "buy_quantity": "0",
        "sell_quantity": "0",
        "target_buy_leaves": "0",
        "target_sell_leaves": "0",
        "maximum_buy_total": "0",
        "maximum_sell_total": str(max(ZERO, inputs.inventory - inputs.other_sell_reserved)),
        "additional_buy_quantity": "0",
        "inventory_target": None,
        "inventory_capacity": str(capacity),
        "buy_directive": "WITHDRAW",
        "sell_directive": "WITHDRAW",
        "buy_directive_reason": "STANDARD_MODEL_UNCALIBRATED",
        "sell_directive_reason": "STANDARD_MODEL_UNCALIBRATED",
        "buy_competitive": False,
        "sell_competitive": False,
        "join_probe": False,
        "parameters_return_calibrated": False,
        "mode": "MODEL_UNCALIBRATED",
        "standard_model": diagnostics,
        "portfolio_risk": inputs.portfolio_risk,
        "account_scope_blockers": inputs.account_scope_blockers,
        "replacement_latency_seconds": str(inputs.replacement_latency_seconds)
        if inputs.replacement_latency_seconds is not None
        else None,
        "buy_fee_per_share": str(inputs.buy_fee),
        "sell_fee_per_share": str(inputs.sell_fee),
    }
    if inputs.inventory != int(inputs.inventory) or inputs.lot_step != 1:
        diagnostics["status"] = "UNSUPPORTED_ACTUAL_INVENTORY_LOT_GRID"
        return result
    if settings.model_comparisons:
        result["model_comparisons"] = {
            legacy_name: {
                key: legacy.get(key)
                for key in (
                    "bid",
                    "ask",
                    "buy_quantity",
                    "sell_quantity",
                    "mode",
                    "buy_directive_reason",
                )
            },
            "same_input_and_risk_limits": True,
            "execution": "ONLY_SELECTED_CORE; NO_SIMULATED_FILLS",
            "profit_comparison": "NOT_EVALUATED_NO_NEW_COMPLETE_CYCLES",
        }
    parameters = inputs.standard_parameters or {}
    diagnostics["AS_2008_BASELINE"] = {
        "status": "NOT_EVALUATED_UNCALIBRATED_INTENSITY_OR_VARIANCE",
        "execution": "COMPARISON_ONLY",
    }
    ready = parameters_ready(parameters, inputs.inventory)
    blockers = []
    if not ready or inputs.complementary_reduce_quantity:
        diagnostics["status"] = "PARAMETERS_UNCALIBRATED"
        if settings.calibration_sampling_enabled:
            return sampling(result, bid, ask, tick, settings, inputs)
        if settings.standard_calibration_execution == "EXISTING_APPROVED_POLICY":
            legacy.update(
                implementation_revision="1.5.0",
                pricing_policy=legacy_name + "_WHILE_STANDARD_PARAMETERS_CALIBRATE",
                selected_quote_core=legacy_name,
                standard_model=diagnostics,
                model_comparisons=result.get("model_comparisons"),
                standard_core_execution_active=False,
            )
            return legacy
        return result
    if not inputs.new_risk_allowed or not inputs.rules_current:
        blockers.append("NEW_RISK_OR_RULES_GATE_CLOSED")
    if inputs.event_inventory + inputs.committed_buy_quantity >= capacity:
        blockers.append("INVENTORY_CAPACITY")
    if (
        inputs.oldest_lot_seconds is not None
        and inputs.oldest_lot_seconds >= settings.soft_oldest_lot_minutes * 60
        or inputs.seconds_to_close <= settings.no_new_inventory_hours_before_close * 3600
    ):
        blockers.append("INVENTORY_AGE_OR_CLOSE_SCHEDULE")
    if capacity < inventory or capacity > 4096:
        diagnostics["status"] = "INVENTORY_GRID_OUTSIDE_BOUND"
        return result

    def intensity(side):
        p = parameters[side]
        return Intensity(
            Decimal(p["a_per_second"]),
            Decimal(p["k_inverse_price"]),
            Decimal(p["distance_min"]),
            Decimal(p["distance_max"]),
            p["order_quantity"],
            tuple((q, Decimal(prob)) for q, prob in p["marks"]),
        )

    buy = intensity("BUY")
    sell = (
        intensity("SELL")
        if parameters.get("SELL", {}).get("status") == "READY_OWN_ORDER_INTENSITY"
        else None
    )
    if buy.order_quantity * (ask - tick + inputs.buy_fee) > quote_cash_budget(settings, capital):
        blockers.append("CALIBRATED_SIZE_EXCEEDS_QUOTE_BUDGET")
    if inputs.event_cost + inputs.committed_buy_cost + buy.order_quantity * (
        ask - tick + inputs.buy_fee
    ) > event_cash_budget(settings, capital):
        blockers.append("EVENT_COST_LIMIT")
    if (
        buy.order_quantity * (ask - tick + inputs.buy_fee)
        > inputs.cash_available + inputs.committed_buy_cost
    ):
        blockers.append("SHARED_CASH_LIMIT")
    if (
        not inputs.can_reduce
        or sell is not None
        and sell.order_quantity > max(ZERO, inputs.inventory - inputs.other_sell_reserved)
        or sell is not None
        and sell.order_quantity * (bid + inputs.sell_fee) > quote_cash_budget(settings, capital)
    ):
        sell = None
    if inputs.inventory + inputs.committed_buy_quantity + buy.order_quantity > capacity:
        blockers.append("COMMITTED_BUY_CAPACITY")
    curve = liquidation(mid, external, range(capacity + 1), inputs.sell_fee)
    stress = liquidation(
        mid,
        external,
        range(capacity + 1),
        inputs.sell_fee,
        depth_fraction=settings.model_depth_stress_fraction,
    )
    for key, scenario in (("CURRENT_DEPTH", curve), ("DEPTH_WITHDRAWAL_STRESS", stress)):
        diagnostics["liquidation_scenarios"][key] = {
            str(q): {k: str(v) for k, v in scenario[q].items()}
            for q in sorted({0, inventory, min(capacity, inventory + buy.order_quantity), capacity})
        }
    variance = Decimal(parameters["price_risk"]["variance_price_squared_per_second"])
    baseline = as_baseline(
        mid,
        inputs.inventory,
        variance,
        horizon,
        gamma,
        buy.k,
        trade_size=Decimal(buy.order_quantity),
    )
    diagnostics["AS_2008_BASELINE"] = baseline
    try:
        solved = solve_cached(
            (
                bid,
                ask,
                tick,
                inventory,
                capacity,
                gamma,
                variance,
                horizon,
                inputs.buy_fee + (inputs.buy_adverse or ZERO),
                inputs.sell_fee + (inputs.sell_adverse or ZERO),
            ),
            tuple(stress[q]["cost"] for q in range(capacity + 1)),
            None if blockers else buy,
            sell,
            gamma * Decimal(inputs.portfolio_risk.get("target_cross_variance_penalty") or "0"),
        )
    except ValueError as exc:
        diagnostics["status"] = str(exc)
        return result
    b = Decimal(solved["bid"]) if solved["bid"] is not None else bid
    a = Decimal(solved["ask"]) if solved["ask"] is not None else ask
    bq = buy.order_quantity if solved["bid"] is not None and not blockers else 0
    aq = sell.order_quantity if solved["ask"] is not None and sell else 0
    diagnostics.update(
        status="READY_NUMERICAL_LOCAL_MODEL",
        solution={k: v for k, v in solved.items() if k != "theta"},
        theta_current=solved["theta"][inventory],
        terminal_current_cost=str(stress[inventory]["cost"]),
    )
    # Keep a safe acknowledged order while its own partial fill is awaiting
    # account confirmation. Withdrawal remains valid when new risk is blocked.
    defer = inputs.pending_own_ack and not blockers
    result.update(
        selected_quote_core="GUEANT_FINITE_GRID",
        standard_core_execution_active=True,
        bid=str(b),
        ask=str(a),
        buy_guard=str(b),
        sell_guard=str(a),
        buy_quantity=str(bq),
        sell_quantity=str(aq),
        target_buy_leaves=None if defer else str(bq),
        target_sell_leaves=str(aq),
        maximum_buy_total=str(bq),
        additional_buy_quantity=str(max(ZERO, bq - inputs.committed_buy_quantity)),
        buy_directive="DEFER" if defer else "QUOTE" if bq else "WITHDRAW",
        sell_directive="QUOTE" if aq else "WITHDRAW",
        buy_directive_reason="WAIT_OWN_ACK"
        if defer
        else blockers[0]
        if blockers
        else "POSITIVE_MODEL_VALUE"
        if bq
        else "WITHDRAW_OPTIMAL_OR_DISTANCE_UNSUPPORTED",
        sell_directive_reason="POSITIVE_MODEL_VALUE"
        if aq
        else "NO_AVAILABLE_INVENTORY_OR_SUPPORTED_SELL",
        buy_competitive=b >= bid and bool(bq),
        sell_competitive=a <= ask and bool(aq),
        net_spread=str(a - b - inputs.buy_fee - inputs.sell_fee),
        quote_spread=str(a - b),
        parameters_return_calibrated=True,
        mode="TWO_SIDED"
        if bq and aq
        else "PASSIVE_ACCUMULATION"
        if bq
        else "REDUCE_ONLY"
        if aq
        else "NO_POSITIVE_MODEL_ACTION",
        buy_blocking_reasons=blockers,
        allow_new_orders=not inputs.pending_own_ack
        and inputs.rules_current
        and inputs.seconds_to_close > 0,
    )
    return result
