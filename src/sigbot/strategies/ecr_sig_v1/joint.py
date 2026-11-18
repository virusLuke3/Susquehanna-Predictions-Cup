"""Quantity-aware SIG forecasts and joint exit/risk sizing; no exchange writes.

The fitted target is future executable SIG bid VWAP minus current bid VWAP.
An external price difference is a conditioning feature, never promised profit.
"""

import hashlib
import json
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

from sigbot.strategies.ecr_sig_v1 import core

HORIZONS = (300, 900, 1800, 3600, 7200, 14400, 21600)
QUANTITIES = (100, 250, 500, 1000, 2500)


def group_key(office, gap, quantity):
    probe = next((q for q in QUANTITIES if quantity <= q), None)
    if probe is None:
        raise core.Reject("MODEL_QUANTITY_OUT_OF_SUPPORT")
    return f"{office}:{int(gap / D('.02'))}:{probe}"


def load_model(parameters, now):
    path = Path(parameters.model_file)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != parameters.model_hash:
        raise core.Reject("MODEL_ARTIFACT_HASH_MISMATCH")
    model = json.loads(raw)
    if model.get("target") != "FUTURE_SIG_NET_BID_VWAP_CHANGE":
        raise core.Reject("MODEL_TARGET_MISMATCH")
    if not model["trained_through"] <= now <= model["valid_until"]:
        raise core.Reject("MODEL_NOT_AVAILABLE_ASOF")
    return model


def forecast(c, quantity, model, parameters, remaining=None):
    bid = core.consume(c.sig_book.bids, quantity, False)[0] - c.sell_fee_bound
    reference = core.consume(c.external_books[0][1].bids, max(D(100), 5 * quantity), False)[0]
    gap = reference - c.sig_book.ask
    key = group_key(c.office, gap, quantity)
    horizons = model["groups"].get(key, {})
    choices = []
    supported = False
    for text, row in horizons.items():
        horizon = int(text)
        if horizon > parameters.holding_seconds:
            continue
        if (row["clusters"] < parameters.model_min_clusters or row["events"] < 4
                or D(row.get("complete_fraction", "0")) < D(".8")):
            continue
        supported = True
        if remaining is not None and horizon > remaining:
            continue
        uncertainty = D(row["mean_standard_error"]) * D("1.96")
        carrying = parameters.holding_cost_per_hour * D(horizon) / 3600
        move = D(row["mean_change"])
        choices.append({
            "horizon": horizon, "expected_net_bid": bid + move,
            "continuation_value": move - uncertainty - carrying,
            "uncertainty": uncertainty, "holding_cost": carrying,
            "stop_distance": max(c.price_tick, D(row["mae_quantiles"][str(parameters.stop_quantile)])),
            "group": key, "clusters": row["clusters"], "events": row["events"],
        })
    if not choices:
        raise core.Reject("MODEL_REMAINING_HORIZON_TOO_SHORT" if supported else "MODEL_NO_SUPPORTED_HORIZON")
    return max(choices, key=lambda x: (x["continuation_value"], -x["horizon"]))


def proposal(c, account, now, parameters, model):
    # Retain reviewed identity, five-second books, raw discount and spread guards.
    from sigbot.strategies.ecr_sig_v1.reference import reference_signal

    policy = replace(core.DEFAULT_POLICY, max_holding_seconds=parameters.holding_seconds)
    account.check(policy)
    reference_signal(c, now, policy)
    if c.event_key in account.occupied_events:
        raise core.Reject("one live thesis per underlying event")
    if len(account.occupied_events) >= policy.max_slots:
        raise core.Reject("position slots full")
    budget = min(
        parameters.child_cash, parameters.market_cash,
        parameters.strategy_cash - account.total_exposure,
        parameters.state_cash - account.state_exposure.get(c.state, core.ZERO),
        parameters.direction_cash - account.direction_exposure.get(c.direction_bucket, core.ZERO),
        account.cash - account.reserved_and_future_fees - policy.cash_floor_fraction * account.initial,
    )
    qty = core.floor_lot(min(
        budget / (c.sig_book.ask + c.buy_fee_bound + c.sell_fee_bound),
        core.depth(c.sig_book.asks, core.ZERO, c.sig_book.ask + policy.ask_band) * parameters.depth_fraction,
        core.depth(c.sig_book.bids, c.sig_book.bid - policy.bid_band, core.ONE) * parameters.depth_fraction,
        core.depth(c.external_books[0][1].bids, core.ZERO, core.ONE) / 5,
    ), c.lot)
    last_rejection = "JOINT_NO_RISK_OR_EXIT_CAPACITY"
    for _ in range(40):
        if qty < c.min_qty:
            raise core.Reject(last_rejection)
        try:
            estimate = forecast(c, qty, model, parameters)
            avg, limit = core.consume(c.sig_book.asks, qty, True)
            if limit > c.sig_book.ask + policy.ask_band or limit % c.price_tick:
                raise core.Reject("entry slippage or tick")
            entry = limit + c.buy_fee_bound
            net = core.consume(c.sig_book.bids, qty, False)[0] - c.sell_fee_bound
            stop = estimate["stop_distance"]
            if stop <= entry - net:
                raise core.Reject("JOINT_STOP_INSIDE_CURRENT_ROUNDTRIP_LOSS")
            if stop > D(".06"):
                raise core.Reject("JOINT_STOP_OUTSIDE_EXISTING_RISK_BOUND")
            risk_quantity = core.floor_lot(parameters.signal_risk / (stop + parameters.exit_buffer), c.lot)
            if qty > risk_quantity:
                qty = risk_quantity
                continue
            edge = estimate["expected_net_bid"] - entry - estimate["uncertainty"] - estimate["holding_cost"]
            if edge < parameters.minimum_expected_edge:
                raise core.Reject("JOINT_EXPECTED_SIG_RETURN_INSUFFICIENT")
            reference = core.consume(c.external_books[0][1].bids, max(D(100), 5 * qty), False)[0]
            gap = reference - entry - c.sell_fee_bound - policy.reference_haircut
            if gap < policy.single_gap:
                raise core.Reject("full-size opportunity insufficient")
            return core.Plan(
                c.key, c.event_key, c.side, "SIG_FUTURE_BID_JOINT_V2", qty, limit,
                avg, entry, qty * (entry + c.sell_fee_bound), reference, gap, net,
                edge, stop, edge / (stop + parameters.exit_buffer), c.state,
                c.shock_group, c.direction_bucket, now, now + 5,
            )
        except core.Reject as exc:
            last_rejection = str(exc)
            qty = core.floor_lot(qty / 2, c.lot)
    raise core.Reject("JOINT_QUANTITY_SEARCH_EXHAUSTED")


def exit_reason(c, quantity, plan, opened, now, parameters, model, stop_price=None):
    """The stop and deadline are frozen at first fill; losses never extend either."""
    net = core.consume(c.sig_book.bids, quantity, False)[0] - c.sell_fee_bound
    floor = max(plan.entry_cost_bound_per_share - plan.stop_trigger_per_share, stop_price or core.ZERO)
    if net <= floor:
        return "STOP_TRIGGER", None
    remaining = opened + parameters.holding_seconds - now
    if remaining <= 0:
        return "TIME_EXIT", None
    try:
        estimate = forecast(c, quantity, model, parameters, remaining)
    except core.Reject as exc:
        if str(exc) == "MODEL_REMAINING_HORIZON_TOO_SHORT":
            return None, None  # No shorter forecast is not zero remaining value.
        if str(exc) == "MODEL_NO_SUPPORTED_HORIZON":
            return "MODEL_SIGNAL_UNSUPPORTED", None
        raise
    return ("REMAINING_EDGE_EXHAUSTED" if estimate["continuation_value"] <= 0 else None), estimate
