"""Decimal A–S comparison and SIG adaptation of Guéant Model B.

Prices: SUSQies/share; variance: price²/second; gamma: 1/SUSQie;
intensity: actual partial-fill events/second, k: inverse price. No defaults
for unobserved intensities. This freezes local prices/parameters, and is not
the paper's continuous-price theorem or a prediction of election outcomes.
"""

from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any

from sigbot.domain import ZERO


@dataclass(frozen=True)
class Intensity:
    a: Decimal
    k: Decimal
    distance_min: Decimal
    distance_max: Decimal
    order_quantity: int
    marks: tuple[tuple[int, Decimal], ...]

    def __post_init__(self):
        if (
            not all(v.is_finite() for v in (self.a, self.k, self.distance_min, self.distance_max))
            or self.a <= 0
            or self.k <= 0
            or self.distance_min > self.distance_max
            or type(self.order_quantity) is not int
            or self.order_quantity < 1
            or not self.marks
            or any(
                type(q) is not int
                or q < 1
                or q > self.order_quantity
                or not p.is_finite()
                or p <= 0
                for q, p in self.marks
            )
            or abs(sum((p for _, p in self.marks), ZERO) - 1) > Decimal("1e-20")
        ):
            raise ValueError("INVALID_MARKED_INTENSITY")

    def rate(self, distance):
        if not self.distance_min <= distance <= self.distance_max:
            return None
        return self.a * (-self.k * distance).exp()


def as_baseline(
    mid, inventory, variance_per_second, horizon_seconds, gamma, k, *, trade_size=Decimal(1)
):
    """Original approximate formula, with explicit fixed-size unit conversion."""
    values = (mid, inventory, variance_per_second, horizon_seconds, gamma, k, trade_size)
    if (
        not all(v.is_finite() for v in values)
        or min(inventory, variance_per_second, horizon_seconds, gamma) < 0
        or k <= 0
        or trade_size <= 0
    ):
        raise ValueError("INVALID_AS_UNITS_OR_PARAMETERS")
    effective_gamma = gamma * trade_size
    liquidity = 1 / k if not effective_gamma else (1 + effective_gamma / k).ln() / effective_gamma
    center = mid - gamma * inventory * variance_per_second * horizon_seconds
    spread = effective_gamma * variance_per_second * horizon_seconds + 2 * liquidity
    return {
        "reservation_price": str(center),
        "continuous_bid": str(center - spread / 2),
        "continuous_ask": str(center + spread / 2),
        "spread": str(spread),
        "fixed_trade_size": str(trade_size),
        "execution": "COMPARISON_ONLY_CONTINUOUS_APPROXIMATION",
    }


def portfolio_bound(exposures, target_exchange):
    """Binary netting only within one verified exchange; no correlation guesses."""
    groups: dict[str, dict[str, Any]] = {}
    for row in exposures:
        if row["outcome"] not in {"YES", "NO"} or not row["exchange_id"]:
            raise ValueError("UNVERIFIED_PORTFOLIO_BINARY_SCOPE")
        group = groups.setdefault(
            row["exchange_id"], {"net": ZERO, "pending": ZERO, "variance": None}
        )
        group["net"] += row["quantity"] * (1 if row["outcome"] == "YES" else -1)
        # Pending opposite buys are not a completed hedge.
        group["pending"] += row.get("pending_quantity", ZERO)
        variance = row.get("variance")
        if variance is not None:
            if not variance.is_finite() or variance < 0:
                raise ValueError("INVALID_PORTFOLIO_VARIANCE")
            group["variance"] = max(group["variance"] or ZERO, variance)
    missing = [
        eid
        for eid, g in groups.items()
        if g["variance"] is None and (g["net"] or g["pending"] or eid == target_exchange)
    ]
    result = {
        "method": "ARBITRARY_CORRELATION_VOLATILITY_UPPER_BOUND",
        "status": "PARTIAL_PRICE_RISK" if missing else "READY_ROBUST_PORTFOLIO_BOUND",
        "missing_variance_exchanges": sorted(missing),
        "covariance_estimated": False,
        "cross_market_hedge_credit": False,
        "variance_upper_bound": None,
        "target_cross_variance_penalty": None,
        "binary_net_inventory": {eid: str(g["net"]) for eid, g in groups.items()},
        "pending_buys_netted": False,
    }
    if missing or target_exchange not in groups:
        return result
    risks = {
        eid: (abs(g["net"]) + g["pending"]) * (g["variance"] or ZERO).sqrt()
        for eid, g in groups.items()
    }
    result["variance_upper_bound"] = str(sum(risks.values(), ZERO) ** 2)
    result["target_cross_variance_penalty"] = str(
        (groups[target_exchange]["variance"] or ZERO).sqrt()
        * sum((v for eid, v in risks.items() if eid != target_exchange), ZERO)
    )
    return result


def liquidation(mid, bids, quantities, fee, *, depth_fraction=Decimal(1)):
    """Walk external depth; unmatched shares have zero scenario value, remain held."""
    if not 0 < depth_fraction <= 1 or fee < 0:
        raise ValueError("INVALID_LIQUIDATION_SCENARIO")
    result = {}
    levels = sorted(bids, key=lambda x: x[0], reverse=True)
    if any(not (0 < price < 1) or quantity < 0 for price, quantity in levels):
        raise ValueError("INVALID_EXTERNAL_DEPTH")
    for q in quantities:
        remaining, revenue = Decimal(q), ZERO
        if remaining < 0:
            raise ValueError("NEGATIVE_INVENTORY")
        for price, available in levels:
            taken = min(remaining, available * depth_fraction)
            revenue += taken * max(ZERO, price - fee)
            remaining -= taken
        result[q] = {
            "net_revenue": revenue,
            "cost": Decimal(q) * mid - revenue,
            "unmatched_quantity": remaining,
        }
    return result


def candidates(bid, ask, tick, intensity, side):
    """Enumerate legal supported ticks, including join/improve/retreat."""
    if tick <= 0 or bid % tick or ask % tick or not 0 < bid < ask < 1:
        raise ValueError("INVALID_PRICE_GRID")
    mid = (bid + ask) / 2
    return tuple(
        (price, rate)
        for index in range(1, int((1 / tick).to_integral_value(rounding=ROUND_CEILING)))
        if 0 < (price := tick * index) < 1
        and (price < ask if side == "BUY" else price > bid)
        and (rate := intensity.rate(mid - price if side == "BUY" else price - mid)) is not None
    )


def side_values(theta, q, quotes, intensity, mid, fee, side):
    if intensity is None:
        return [(None, ZERO)]
    # Rates were measured for this posted size. Do not extrapolate to a smaller
    # boundary quote, or turn a one-share partial fill into a whole-order fill.
    if (
        side == "BUY"
        and q + intensity.order_quantity >= len(theta)
        or side == "SELL"
        and q < intensity.order_quantity
    ):
        return [(None, ZERO)]
    values = [(None, ZERO)]  # Withdrawal is always an admissible action.
    for price, rate in quotes:
        distance = mid - price if side == "BUY" else price - mid
        reward = sum(
            (
                probability
                * (
                    quantity * (distance - fee)
                    + theta[q + quantity if side == "BUY" else q - quantity]
                    - theta[q]
                )
                for quantity, probability in intensity.marks
            ),
            ZERO,
        )
        values.append((price, rate * reward))
    return values


def best_pair(buys, sells, total_fee):
    # Net spread is a constraint when quoting both sides, not proof of profit.
    pairs = (
        (b, a, vb + va)
        for b, vb in buys
        for a, va in sells
        if b is None or a is None or a - b > total_fee
    )
    return max(pairs, key=lambda item: (item[2], item[0] is None, item[1] is None))


def finite_grid(
    *,
    bid,
    ask,
    tick,
    inventory,
    capacity,
    gamma,
    variance_per_second,
    horizon_seconds,
    terminal_costs,
    buy=None,
    sell=None,
    buy_fee=ZERO,
    sell_fee=ZERO,
    max_step_seconds=Decimal(30),
    max_updates=2_000_000,
    cross_penalty_per_second=ZERO,
):
    """Explicit monotone time discretization of the finite-state Model B HJB.

    Actual integer partial-fill marks define state jumps; long-only bounds and
    positive pair spread are project constraints. dt*(lambda_b+lambda_a)<=.1.
    A calculation exceeding the bound fails, never becomes a guessed quote.
    """
    values = (
        gamma,
        variance_per_second,
        horizon_seconds,
        buy_fee,
        sell_fee,
        cross_penalty_per_second,
        max_step_seconds,
    )
    if (
        not all(v.is_finite() for v in values)
        or min(values[:-1]) < 0
        or max_step_seconds <= 0
        or not 0 <= inventory <= capacity
        or capacity != int(capacity)
        or inventory != int(inventory)
    ):
        raise ValueError("INVALID_FINITE_GRID_INPUT")
    capacity, inventory = int(capacity), int(inventory)
    if (
        capacity > 4096
        or set(terminal_costs) != set(range(capacity + 1))
        or any(not v.is_finite() for v in terminal_costs.values())
    ):
        raise ValueError("INVALID_OR_EXCESSIVE_INVENTORY_GRID")
    mid = (bid + ask) / 2
    buys = candidates(bid, ask, tick, buy, "BUY") if buy else ()
    sells = candidates(bid, ask, tick, sell, "SELL") if sell else ()
    maximum_rate = max((rate for _, rate in buys), default=ZERO) + max(
        (rate for _, rate in sells), default=ZERO
    )
    step = min(max_step_seconds, Decimal(".1") / maximum_rate) if maximum_rate else max_step_seconds
    steps = max(1, int((horizon_seconds / step).to_integral_value(rounding=ROUND_CEILING)))
    work = steps * (capacity + 1) * max(1, (len(buys) + 1) * (len(sells) + 1))
    if work > max_updates:
        raise ValueError("FINITE_GRID_COMPUTATION_BOUND")
    dt = horizon_seconds / steps
    theta = [-terminal_costs[q] for q in range(capacity + 1)]
    for _ in range(steps):
        next_theta = []
        for q in range(capacity + 1):
            bv = side_values(theta, q, buys, buy, mid, buy_fee, "BUY")
            av = side_values(theta, q, sells, sell, mid, sell_fee, "SELL")
            _, _, reward = best_pair(bv, av, buy_fee + sell_fee)
            next_theta.append(
                theta[q]
                + dt
                * (reward - gamma * variance_per_second * q * q / 2 - cross_penalty_per_second * q)
            )
        theta = next_theta
    bv = side_values(theta, inventory, buys, buy, mid, buy_fee, "BUY")
    av = side_values(theta, inventory, sells, sell, mid, sell_fee, "SELL")
    b, a, value = best_pair(bv, av, buy_fee + sell_fee)
    return {
        "bid": str(b) if b is not None else None,
        "ask": str(a) if a is not None else None,
        "value_rate_susqies_per_second": str(value),
        "theta": [str(v) for v in theta],
        "steps": steps,
        "step_seconds": str(dt),
        "horizon_seconds": str(horizon_seconds),
        "inventory": inventory,
        "capacity": capacity,
        "method": "FINITE_STATE_MARKED_MODEL_B_EXPLICIT_TIME_GRID",
        "price_dynamics": "FROZEN_LOCAL_ZERO_DRIFT_VARIANCE",
        "quantity_model": "OBSERVED_PARTIAL_FILL_MARKS",
        "queue_position": "UNOBSERVABLE_AGGREGATED_BOOK",
        "tail_bound": False,
    }
