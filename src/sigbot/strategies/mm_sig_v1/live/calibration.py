"""Past-only own-order exposure likelihood; public trades are never own fills."""

from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from sigbot.domain import ZERO, utc_time
from sigbot.storage import Audit, FillRow, IntentRow, Order


def price_risk(rows, now, *, max_gap=5, min_seconds=300):
    segment: list[tuple[datetime, Decimal, Any]] = []
    gaps, previous = 0, None
    seconds, squared_changes, pairs = ZERO, ZERO, 0
    held_binding = None
    last_valid = None
    gap_seconds, gap_changes, gap_pairs = ZERO, ZERO, 0
    for at, mid, binding in rows:
        if at > now:
            continue
        if held_binding is not None and binding != held_binding:
            # A rule/mapping change starts a different statistical population.
            segment, previous = [], None
            seconds, squared_changes, pairs = ZERO, ZERO, 0
            last_valid = None
            gap_seconds, gap_changes, gap_pairs = ZERO, ZERO, 0
        held_binding = binding
        if mid is None:
            segment, previous = [], None
            gaps += 1
            continue
        if previous and (
            not 0 < (at - previous[0]).total_seconds() <= max_gap or binding != previous[2]
        ):
            segment = []
            gaps += 1
        elif previous:
            # Retain verified intervals across interruptions, never the return
            # across a gap. Wall-clock downtime is not observation exposure.
            seconds += Decimal(str((at - previous[0]).total_seconds()))
            squared_changes += (mid - previous[1]) ** 2
            pairs += 1
        if last_valid and (previous is None or (at - last_valid[0]).total_seconds() > max_gap):
            delta = Decimal(str((at - last_valid[0]).total_seconds()))
            if delta > 0:
                # Both endpoints are actually observed. Their price change is
                # risk evidence, not a reconstruction of the missing path.
                gap_seconds += delta
                gap_changes += (mid - last_valid[1]) ** 2
                gap_pairs += 1
        segment.append((at, mid, binding))
        previous = at, mid, binding
        last_valid = at, mid
    elapsed = (
        Decimal(str((segment[-1][0] - segment[0][0]).total_seconds())) if len(segment) > 1 else ZERO
    )
    fresh = bool(segment and 0 <= (now - segment[-1][0]).total_seconds() <= max_gap)
    ready = fresh and seconds >= min_seconds
    variance = squared_changes / seconds if seconds else None
    gap_rate = gap_changes / gap_seconds if gap_seconds else None
    used_variance = max(variance, gap_rate or ZERO) if variance is not None else None
    return {
        "status": "READY_LOCAL_VARIANCE" if ready else "PRICE_RISK_WARMUP",
        "variance_price_squared_per_second": str(used_variance) if ready else None,
        "within_segment_variance_per_second": str(variance) if variance is not None else None,
        "observed_gap_endpoint_rate": str(gap_rate) if gap_rate is not None else None,
        "observed_gap_endpoint_returns": gap_pairs,
        "variance_selection": "MAX_VERIFIED_INTERVAL_AND_OBSERVED_GAP_ENDPOINT_RATE",
        "continuous_seconds": str(elapsed),
        "valid_observation_seconds": str(seconds),
        "valid_return_intervals": pairs,
        "gap_time_excluded_from_valid_observation_seconds": True,
        "missing_path_imputed": False,
        "observations": len(segment),
        "gaps": gaps,
        "fresh": fresh,
        "final_payout_variance_used": False,
        "tail_risk_bound": False,
    }


def fit_intensity(rows, *, minimum_seconds=300, minimum_events=20):
    """Keep exact quantity supports; a single distance still has an observable rate."""
    if not rows:
        return _fit_quantity(rows, minimum_seconds=minimum_seconds, minimum_events=minimum_events)
    groups: dict[int, list[dict]] = {}
    for row in rows:
        groups.setdefault(row["order_quantity"], []).append(row)
    models = {
        str(q): _fit_quantity(
            groups[q],
            minimum_seconds=minimum_seconds,
            minimum_events=minimum_events,
        )
        for q in sorted(groups)
    }
    selected = max(models, key=lambda q: Decimal(models[q].get("exposure_seconds", 0)))
    return {**models[selected], "quantity_models": models}


def _fit_quantity(rows, *, minimum_seconds=300, minimum_events=20):
    """Profile Poisson MLE with zero-event exposures, chronological validation.

    Rows are actual observed order intervals. Exact original posted quantity
    groups stay separate. Two distance supports are necessary to identify k.
    """
    result: dict[str, Any] = {
        "status": "NO_RESTING_ORDER_EXPOSURE",
        "a_per_second": None,
        "k_inverse_price": None,
        "queue_position": "UNKNOWN",
        "event_definition": "DEDUPLICATED_ACTUAL_PARTIAL_FILL",
        "public_trade_proxy_used": False,
    }
    if not rows:
        return result
    counts: dict[int, Decimal] = {}
    for row in rows:
        quantity = row["order_quantity"]
        counts[quantity] = counts.get(quantity, ZERO) + row["seconds"]
    quantity = max(counts, key=lambda q: counts[q])
    rows = sorted((r for r in rows if r["order_quantity"] == quantity), key=lambda r: r["at"])
    seconds = sum((r["seconds"] for r in rows), ZERO)
    events = sum(len(r["fills"]) for r in rows)
    result.update(
        order_quantity=quantity,
        exposure_seconds=str(seconds),
        actual_fill_events=events,
        zero_fill_intervals=sum(not r["fills"] for r in rows),
        distinct_distances=len({r["distance"] for r in rows}),
    )
    points = []
    split = max(1, len(rows) * 7 // 10)
    for distance in sorted({r["distance"] for r in rows}):
        selected = [r for r in rows if r["distance"] == distance]
        point_seconds = sum((r["seconds"] for r in selected), ZERO)
        point_marks = [q for r in selected for q in r["fills"]]
        train_n = sum(len(r["fills"]) for r in rows[:split] if r["distance"] == distance)
        valid_n = sum(len(r["fills"]) for r in rows[split:] if r["distance"] == distance)
        valid_marks = all(type(q) is int and 0 < q <= quantity for q in point_marks)
        ready = (
            valid_marks
            and point_seconds >= minimum_seconds
            and train_n >= minimum_events
            and valid_n >= max(5, minimum_events // 2)
        )
        points.append(
            {
                "status": "READY_DISTANCE_RATE" if ready else "INSUFFICIENT_DISTANCE_EXPOSURE",
                "order_quantity": quantity,
                "distance": str(distance),
                "exposure_seconds": str(point_seconds),
                "partial_fill_events": len(point_marks),
                "zero_fill_intervals": sum(not r["fills"] for r in selected),
                "observed_events_per_second": str(Decimal(len(point_marks)) / point_seconds)
                if point_seconds and valid_marks
                else None,
                "observed_shares_per_second": str(Decimal(sum(point_marks)) / point_seconds)
                if point_seconds and valid_marks
                else None,
                "mean_partial_quantity": str(Decimal(sum(point_marks)) / len(point_marks))
                if point_marks and valid_marks
                else None,
                "distance_extrapolation_allowed": False,
                "queue_conditioning": "AGGREGATED_UNKNOWN_POSITION",
            }
        )
    result["distance_models"] = points
    if seconds < minimum_seconds or not events:
        result["status"] = (
            "INSUFFICIENT_EXPOSURE" if seconds < minimum_seconds else "NO_FILL_EVENTS"
        )
        # Exact P(N=0)=.05 upper rate under the homogeneous Poisson assumption.
        result["zero_event_95pct_upper_per_second"] = (
            str(-Decimal(".05").ln() / seconds) if seconds and not events else None
        )
        return result
    split = max(1, len(rows) * 7 // 10)
    train, validation = rows[:split], rows[split:]
    exposure: dict[Decimal, Decimal] = {}
    event_distances = []
    for r in train:
        exposure[r["distance"]] = exposure.get(r["distance"], ZERO) + r["seconds"]
        event_distances.extend([r["distance"]] * len(r["fills"]))
    n = len(event_distances)
    vn = sum(len(r["fills"]) for r in validation)
    result.update(
        train_events=n,
        validation_events=vn,
        validation_seconds=str(sum((r["seconds"] for r in validation), ZERO)),
    )
    if (
        n < minimum_events
        or vn < max(5, minimum_events // 2)
        or len(exposure) < 2
        or len({r["distance"] for r in validation}) < 2
    ):
        result["status"] = "INSUFFICIENT_IDENTIFIABLE_TRAIN_VALIDATION"
        return result
    low, high = min(exposure), max(exposure)
    observed_mean = sum(event_distances, ZERO) / n

    def weighted(k):
        weights = [(d, t * (-k * (d - low)).exp()) for d, t in exposure.items()]
        denominator = sum((w for _, w in weights), ZERO)
        return sum((d * w for d, w in weights), ZERO) / denominator

    if not low < observed_mean < weighted(ZERO):
        result["status"] = "NON_DECAYING_OR_BOUNDARY_DISTANCE_FIT"
        return result
    lo, hi = ZERO, Decimal(1) / (high - low)
    while weighted(hi) > observed_mean and hi < Decimal("1e9"):
        hi *= 2
    if weighted(hi) > observed_mean:
        result["status"] = "UNIDENTIFIABLE_DISTANCE_SLOPE"
        return result
    for _ in range(80):
        k = (lo + hi) / 2
        if weighted(k) > observed_mean:
            lo = k
        else:
            hi = k
    k = (lo + hi) / 2
    a = Decimal(n) / sum((t * (-k * d).exp() for d, t in exposure.items()), ZERO)
    weights = [(d, t * (-k * d).exp()) for d, t in exposure.items()]
    total = sum((weight for _, weight in weights), ZERO)
    distance_variance = (
        sum((weight * (d - observed_mean) ** 2 for d, weight in weights), ZERO) / total
    )
    k_se = 1 / (n * distance_variance).sqrt()
    log_a_se = (Decimal(1) / n + observed_mean**2 / (n * distance_variance)).sqrt()
    constant = Decimal(n) / sum(exposure.values(), ZERO)
    score, baseline = ZERO, ZERO
    for r in validation:
        rate = a * (-k * r["distance"]).exp()
        score += len(r["fills"]) * rate.ln() - rate * r["seconds"]
        baseline += len(r["fills"]) * constant.ln() - constant * r["seconds"]
    result.update(validation_log_likelihood=str(score), constant_rate_log_likelihood=str(baseline))
    if score < baseline:
        result["status"] = "VALIDATION_WORSE_THAN_CONSTANT_RATE"
        return result
    marks = Counter(q for r in train for q in r["fills"])
    if any(q < 1 or q > quantity for q in marks):
        result["status"] = "INVALID_ACTUAL_PARTIAL_FILL_QUANTITY"
        return result
    result.update(
        status="READY_OWN_ORDER_INTENSITY",
        a_per_second=str(a),
        k_inverse_price=str(k),
        distance_min=str(low),
        distance_max=str(high),
        marks=[[q, str(Decimal(count) / n)] for q, count in sorted(marks.items())],
        assumptions="LOCAL_POISSON_EXACT_POSTED_SIZE; QUEUE_AND_MARK_STATIONARITY_UNVERIFIED",
        parameter_uncertainty={
            "k_standard_error": str(k_se),
            "log_a_standard_error": str(log_a_se),
            "method": "ASYMPTOTIC_POISSON_PROFILE_INFORMATION",
            "queue_safe_confidence_guarantee": False,
        },
    )
    return result


def estimate(session, account_id, instrument_id, timestamp, settings, binding):
    from sigbot.strategies.mm_sig_v1.live.analytics import owned

    observations = list(
        session.scalars(
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
    )
    order_rows = {
        order.id: (order, row)
        for order, row in session.execute(
            select(Order, IntentRow)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(Order.account_id == account_id, Order.instrument_id == instrument_id)
        )
        if owned(row)
    }
    orders = {oid: pair[0] for oid, pair in order_rows.items()}
    fills = []
    for f in session.scalars(
        select(FillRow).where(FillRow.account_id == account_id, FillRow.order_id.in_(orders))
    ):
        at = utc_time(f.payload["filled_at"])
        if at <= timestamp:
            fills.append((at, f.order_id, Decimal(f.payload["quantity"])))
    rows: dict[str, list[dict]] = {"BUY": [], "SELL": []}
    price_rows = []
    for obs in observations:
        usable = obs.data.get(
            "price_reference_usable", obs.data.get("reference_usable", False)
        ) and tuple(obs.data.get("signal_binding", ())) == tuple(binding)
        price_rows.append(
            (
                obs.at,
                Decimal(obs.data["mid"]) if usable else None,
                tuple(obs.data.get("signal_binding", binding)),
            )
        )
    for before, after in zip(observations, observations[1:], strict=False):
        data = before.data
        seconds = Decimal(str((after.at - before.at).total_seconds()))
        if (
            not 0 < seconds <= settings.book_ttl_seconds
            or not data.get(
                "order_exposure_usable",
                data.get("reference_usable") and data.get("quote_usable", True),
            )
            or tuple(data.get("signal_binding", ())) != tuple(binding)
            or tuple(after.data.get("signal_binding", ())) != tuple(binding)
            or not after.data.get(
                "order_exposure_usable",
                after.data.get("reference_usable") and after.data.get("quote_usable", True),
            )
        ):
            continue
        for side in rows:
            for oid in data.get("eligible_orders", {}).get(side, []):
                order = orders.get(oid)
                if order is None:
                    continue
                note = order_rows[oid][1].payload.get("note")
                import json

                if json.loads(note or "{}").get("kind") != "MM_REAL_PASSIVE":
                    continue
                observed_fills = [
                    (at, q) for at, fid, q in fills if fid == oid and before.at < at <= after.at
                ]
                # Unknown cancellation time is censored, not counted as a full
                # exposure interval or interpreted as a zero-fill observation.
                still_present = oid in after.data.get("eligible_orders", {}).get(side, [])
                if not still_present and not observed_fills:
                    continue
                elapsed = (
                    seconds
                    if still_present
                    else Decimal(
                        str((max(at for at, _ in observed_fills) - before.at).total_seconds())
                    )
                )
                if any(q != int(q) for _, q in observed_fills) or order.quantity != int(
                    order.quantity
                ):
                    continue
                mid = Decimal(data["mid"])
                rows[side].append(
                    {
                        "at": before.at,
                        "seconds": elapsed,
                        "distance": mid - order.price if side == "BUY" else order.price - mid,
                        "order_quantity": int(order.quantity),
                        "fills": [int(q) for _, q in observed_fills],
                    }
                )
    synthetic = account_id.startswith("SIM-") or any(
        a.data.get("synthetic") or a.data.get("layer") == "MOCK" for a in observations
    )
    return {
        "at": timestamp.isoformat(),
        "binding": list(binding),
        "BUY": fit_intensity(
            rows["BUY"],
            minimum_seconds=settings.quality_min_quote_seconds,
            minimum_events=settings.quality_min_fills,
        ),
        "SELL": fit_intensity(
            rows["SELL"],
            minimum_seconds=settings.quality_min_quote_seconds,
            minimum_events=settings.quality_min_fills,
        ),
        "price_risk": price_risk(price_rows, timestamp, max_gap=settings.book_ttl_seconds),
        "synthetic_data_used": synthetic,
    }
