"""Descriptive daily traded-price ranges. No strategy admission or execution model."""

from datetime import timedelta
from decimal import Decimal
from statistics import median
from zoneinfo import ZoneInfo

from sigbot.errors import SchemaMismatch
from sigbot.history import price_movement, stamp

LOCAL = ZoneInfo("Asia/Shanghai")
STEP = timedelta(minutes=5)
DEADBAND = Decimal("0.005")


def percentile(values, fraction):
    ordered = sorted(values)
    position = Decimal(str(fraction)) * (len(ordered) - 1)
    index = int(position)
    return ordered[index] + (ordered[min(index + 1, len(ordered) - 1)] - ordered[index]) * (
        position - index
    )


def oscillations(points, center):
    """Deadband side changes; gaps reset continuous counts, never imply intervening prices."""
    observed_side = continuous_side = 0
    observed_changes = continuous_changes = continuous_round_trips = segment_changes = 0
    previous = None
    for at, price in points:
        if previous is not None and at - previous != STEP:
            continuous_round_trips += segment_changes // 2
            segment_changes = 0
            continuous_side = 0
        side = -1 if price <= center - DEADBAND else 1 if price >= center + DEADBAND else 0
        if side:
            if observed_side and observed_side != side:
                observed_changes += 1
            if continuous_side and continuous_side != side:
                continuous_changes += 1
                segment_changes += 1
            observed_side = continuous_side = side
        previous = at
    return {
        "observed_side_changes": observed_changes,
        "observed_side_round_trips": observed_changes // 2,
        "continuous_side_changes": continuous_changes,
        "continuous_side_round_trips": continuous_round_trips + segment_changes // 2,
    }


def daily_range_study(rows, start, end, *, binary=True):
    rows = sorted(rows, key=lambda r: stamp(r["time"]))
    days, all_points = [], []
    cursor = start
    while cursor < end:
        boundary = cursor + timedelta(days=1)
        selected = [r for r in rows if cursor <= stamp(r["time"]) < boundary]
        prices = price_movement(selected, cursor, boundary)
        day = {
            "date": cursor.astimezone(LOCAL).date().isoformat(),
            "status": prices["status"],
            "trade_count": sum(r["tradeCount"] for r in selected),
            "observed_5m_buckets": len(selected),
            "unobserved_5m_buckets": 288 - len(selected),
            "active_hours": len({stamp(r["time"]).hour for r in selected}),
            "prices": prices,
            "NO_price_source": "DERIVED_1_MINUS_YES_SAME_NORMALIZED_EXECUTION"
            if binary
            else "NOT_EVALUATED",
        }
        days.append(day)
        cursor = boundary
        points = []
        for row in selected:
            if row["vwap"] is None:
                continue
            value = Decimal(str(row["vwap"]))
            if not value.is_finite() or not 0 <= value <= 1:
                raise SchemaMismatch("HISTORY_INVALID_VWAP")
            points.append((stamp(row["time"]), value))
        day["valid_5m_vwap_buckets"] = len(points)
        if prices["status"] != "PASS" or len(points) != len(selected):
            if prices["status"] == "PASS":
                day.update(status="UNKNOWN", code="MISSING_5M_VWAP")
            continue
        values = [p for _, p in points]
        center = median(values)
        gaps = [
            (b[0] - a[0]).total_seconds() / 60 for a, b in zip(points, points[1:], strict=False)
        ]
        day.update(
            yes_low=prices["low"],
            yes_high=prices["high"],
            NO_low=str(1 - Decimal(prices["high"])) if binary else None,
            NO_high=str(1 - Decimal(prices["low"])) if binary else None,
            vwap_median=str(center),
            NO_vwap_median=str(1 - center) if binary else None,
            vwap_p10=str(percentile(values, ".1")),
            vwap_p90=str(percentile(values, ".9")),
            vwap_p90_minus_p10_pp=str((percentile(values, ".9") - percentile(values, ".1")) * 100),
            vwap_range_pp=str((max(values) - min(values)) * 100),
            largest_gap_between_observed_buckets_minutes=max(gaps, default=None),
            **oscillations(points, center),
        )
        all_points.extend(points)
    # Empty or unknown days must advance too; callers always provide complete days.
    complete = [d for d in days if d["status"] == "PASS"]
    if not complete:
        return {
            "status": "NOT_EVALUATED",
            "days": days,
            "classification": "INSUFFICIENT_OBSERVATIONS",
        }
    centers = [Decimal(d["vwap_median"]) for d in complete]
    center = median(centers)
    values = [p for _, p in all_points]
    center_span = (max(centers) - min(centers)) * 100
    inside = sum(abs(v - center) <= Decimal(".02") for v in values)
    inside_pct = Decimal(inside) * 100 / len(values)
    sampled_days = sum(d["observed_5m_buckets"] >= 24 for d in complete)
    repeated_days = sum(d["observed_side_round_trips"] >= 2 for d in complete)
    continuous_repeated_days = sum(d["continuous_side_round_trips"] >= 2 for d in complete)
    if sampled_days < 5:
        classification = "INSUFFICIENT_OBSERVATIONS"
    elif center_span > 2 or inside_pct < 90:
        classification = "CENTER_SHIFT_OR_WIDE_OBSERVED_VWAP"
    elif repeated_days < 3:
        classification = "STABLE_CENTER_FEW_OBSERVED_OSCILLATIONS"
    else:
        classification = "RANGE_LIKE_OBSERVED_VWAP"
    return {
        "status": "PASS_DESCRIPTIVE"
        if len(complete) == len(days)
        else "PARTIAL_DAILY_PRICE_OBSERVATIONS",
        "days": days,
        "classification": classification,
        "days_with_prices": len(complete),
        "days_with_at_least_24_observed_buckets": sampled_days,
        "fixed_center_median_of_daily_vwap_medians": str(center),
        "NO_fixed_center_derived": str(1 - center) if binary else None,
        "daily_center_span_pp": str(center_span),
        "weekly_traded_range_pp": str(
            (
                max(Decimal(d["yes_high"]) for d in complete)
                - min(Decimal(d["yes_low"]) for d in complete)
            )
            * 100
        ),
        "observed_5m_vwap_buckets": len(values),
        "observed_vwap_buckets_inside_fixed_center_plus_minus_2pp": inside,
        "observed_vwap_inside_fixed_band_pct": str(inside_pct),
        "days_with_at_least_2_observed_side_round_trips": repeated_days,
        "days_with_at_least_2_continuous_side_round_trips": continuous_repeated_days,
        "days_with_traded_extreme_range_above_5pp": sum(
            Decimal(d["prices"]["range_pp"]) > 5 for d in complete
        ),
        "research_thresholds": "At least 5 days with >=24 nonempty 5m bars; daily center span <=2pp; >=90% observed VWAP bars within center +/-2pp; >=3 days with >=2 observed side round trips across a +/-0.5pp deadband",
        "interpretation": "Ex-post descriptive centers; observed counts may bridge no-trade gaps, continuous counts reset on gaps; no history bid/ask or side-specific NO tape, no fill/PnL proof, no admission",
        "historical_midprice_test": "NOT_EVALUATED_NO_HISTORICAL_BOOKS",
        "live_admission": False,
    }
