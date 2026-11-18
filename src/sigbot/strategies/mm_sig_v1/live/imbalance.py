"""CDJ-inspired empirical participation, not the paper's full HJB or a fill simulator."""

from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from weakref import WeakKeyDictionary

from sqlalchemy import select

from sigbot.domain import ZERO, utc_time
from sigbot.storage import Audit
from sigbot.strategies.mm_sig_v1.core import external_best


def book_features(book, tick):
    if book.quality != "HEALTHY" or not book.own_orders_known:
        return None
    if not any(x.quantity > x.own_quantity for x in book.bids) or not any(
        x.quantity > x.own_quantity for x in book.asks
    ):
        return None
    bid, ask = external_best(book)
    if bid >= ask or bid % tick or ask % tick:
        return None
    vb = sum((x.quantity - x.own_quantity for x in book.bids if x.price == bid), ZERO)
    va = sum((x.quantity - x.own_quantity for x in book.asks if x.price == ask), ZERO)
    rho = (vb - va) / (vb + va)
    state = -1 if rho < -Decimal(1) / 3 else 1 if rho > Decimal(1) / 3 else 0
    return {
        "bid": str(bid),
        "ask": str(ask),
        "bid_quantity": str(vb),
        "ask_quantity": str(va),
        "imbalance": str(rho),
        "imbalance_state": state,
        "spread_ticks": int((ask - bid) / tick),
        "odds_bucket": int((bid + ask) / 2 * 10),
        "sequence": book.sequence,
        "own_orders_removed": True,
    }


def state_key(features):
    return features["imbalance_state"], features["spread_ticks"], features["odds_bucket"]


def fit(rows, timestamp, settings, tick, current, *, max_observation_gap=None):
    """Only matured, nonoverlapping labels; all gaps and binding changes break a segment."""
    evidence = []
    for row in rows:
        f = row.data.get("book_features")
        if row.at <= timestamp and f and row.data.get("price_reference_usable"):
            evidence.append((row.at, f, row.data.get("signal_binding")))
        elif row.at <= timestamp:
            evidence.append((row.at, None, None))
    evidence.sort(key=lambda r: r[0])
    # Duplicate database imports/receipts at the same time are one observation, not extra samples.
    unique: dict = {}
    for row in evidence:
        if row[0] not in unique or row[1] and not unique[row[0]][1]:
            unique[row[0]] = row
        elif row[1] and unique[row[0]][1] and row[1:] != unique[row[0]][1:]:
            unique[row[0]] = (row[0], None, None)
    evidence = list(unique.values())
    times = [r[0] for r in evidence]
    segments, segment = [], 0
    for index, (at, f, binding) in enumerate(evidence):
        previous = evidence[index - 1] if index else None
        if (
            not f
            or previous
            and (
                not previous[1]
                or binding != previous[2]
                or (at - previous[0]).total_seconds()
                > (max_observation_gap or settings.book_ttl_seconds)
                or f["sequence"] < previous[1]["sequence"]
            )
        ):
            segment += 1
        segments.append(segment)
    result = {
        "status": "WARMUP",
        "state": list(state_key(current)),
        "feature": current,
        "model": "CDJ_INSPIRED_BOOK_DEPLETION_PROXY",
        "aggressor_direction": "UNKNOWN",
        "own_fill_AS_replaced_with_zero": False,
        "horizons": {},
        "observations": sum(r[1] is not None for r in evidence),
        "price_reference_gaps": segment,
    }
    labels = []
    for horizon in settings.signal_horizons_seconds:
        last_origin = None
        for index, (at, f, _binding) in enumerate(evidence):
            if not f or last_origin and (at - last_origin).total_seconds() < horizon:
                continue
            j = bisect_left(times, at + timedelta(seconds=horizon))
            if j >= len(evidence):
                continue
            future_at, future, _ = evidence[j]
            if (
                (future_at - at).total_seconds() > horizon + settings.book_ttl_seconds
                or not future
                or segments[index] != segments[j]
            ):
                continue
            last_origin = at
            delta = (
                Decimal(future["bid"])
                + Decimal(future["ask"])
                - Decimal(f["bid"])
                - Decimal(f["ask"])
            ) / 2
            depleted = {"BUY": False, "SELL": False}
            for previous, following in zip(
                evidence[index:j], evidence[index + 1 : j + 1], strict=True
            ):
                before, after = previous[1], following[1]
                for side, key in (("BUY", "bid"), ("SELL", "ask")):
                    p0, p1 = Decimal(before[key]), Decimal(after[key])
                    adverse_move = p1 < p0 if side == "BUY" else p1 > p0
                    decreased = p1 == p0 and Decimal(after[key + "_quantity"]) < Decimal(
                        before[key + "_quantity"]
                    )
                    depleted[side] |= adverse_move or decreased
            labels.append((horizon, at, future_at, state_key(f), delta, depleted))
    for side in ("BUY", "SELL"):
        estimates, statuses = [], []
        for horizon in settings.signal_horizons_seconds:
            state_labels = [x for x in labels if x[0] == horizon and x[3] == state_key(current)]
            eligible = [
                x for x in labels if x[0] == horizon and x[3] == state_key(current) and x[5][side]
            ]
            split = int(len(eligible) * Decimal("0.7"))
            validation = eligible[split:]
            first_validation = validation[0][1] if validation else timestamp
            training = [x for x in eligible[:split] if x[2] < first_validation]
            details = {
                "train_blocks": len(training),
                "validation_blocks": len(validation),
                "nonoverlap_seconds": horizon,
                "depletion_is_trade": False,
                "state_blocks": len(state_labels),
                "depletion_probability_proxy": str(Decimal(len(eligible)) / len(state_labels))
                if state_labels
                else None,
            }
            if (
                len(training) < settings.signal_min_train_blocks
                or len(validation) < settings.signal_min_validation_blocks
            ):
                details["status"] = "INSUFFICIENT_MATURED_BLOCKS"
            else:
                values = [x[4] for x in training]
                mean = sum(values, ZERO) / len(values)
                variance = sum(((x - mean) ** 2 for x in values), ZERO) / (len(values) - 1)
                mse = sum(((x[4] - mean) ** 2 for x in validation), ZERO) / len(validation)
                baseline = sum((x[4] ** 2 for x in validation), ZERO) / len(validation)
                floor = tick * settings.signal_uncertainty_ticks
                uncertainty = max(
                    floor,
                    2 * (variance / len(values)).sqrt(),
                    abs(sum((x[4] for x in validation), ZERO) / len(validation) - mean),
                )
                validation_mean = sum((x[4] for x in validation), ZERO) / len(validation)
                valid = mse <= max(baseline * Decimal("1.05"), floor**2) and not (
                    mean * validation_mean < 0 and abs(mean - validation_mean) > floor
                )
                details.update(
                    status="READY_EMPIRICAL_PROXY" if valid else "OUT_OF_SAMPLE_REJECTED",
                    mean_change=str(mean),
                    uncertainty=str(uncertainty),
                    validation_mse=str(mse),
                    zero_drift_mse=str(baseline),
                    last_label_available_at=max(x[2] for x in eligible).isoformat(),
                    formal_confidence_bound=False,
                )
                if valid:
                    estimates.append(mean - uncertainty if side == "BUY" else mean + uncertainty)
            statuses.append(details["status"])
            result["horizons"][side + ":" + str(horizon)] = details
        ready = len(estimates) == len(settings.signal_horizons_seconds)
        change = min(estimates) if side == "BUY" and ready else max(estimates) if ready else None
        mid = (Decimal(current["bid"]) + Decimal(current["ask"])) / 2
        boundary_rejected = change is not None and not ZERO < mid + change < 1
        if boundary_rejected:
            ready = False
        result[side] = {
            "status": "READY_EMPIRICAL_PROXY"
            if ready
            else "PRICE_BOUNDARY_REJECTED"
            if boundary_rejected
            else "OUT_OF_SAMPLE_REJECTED"
            if "OUT_OF_SAMPLE_REJECTED" in statuses
            else "WARMUP",
            "conservative_change": str(change) if ready else None,
        }
    result["status"] = (
        "READY_EMPIRICAL_PROXY"
        if any(result[s]["status"] == "READY_EMPIRICAL_PROXY" for s in ("BUY", "SELL"))
        else "WARMUP"
    )
    return result


_CACHE: WeakKeyDictionary = WeakKeyDictionary()


def estimate(session, account_id, instrument_id, timestamp, settings, tick, features, binding):
    # ponytail: cache only fitted history for 5 seconds; the current state/book is always rechecked.
    key = (
        account_id,
        instrument_id,
        str(tick),
        tuple(binding),
        tuple(settings.signal_horizons_seconds),
        settings.signal_min_train_blocks,
        settings.signal_min_validation_blocks,
        str(settings.signal_uncertainty_ticks),
    )
    cache = _CACHE.setdefault(session.bind, {})
    cached = cache.get(key)
    if cached is None or not 0 <= (timestamp - cached[0]).total_seconds() < 5:
        rows = list(
            session.scalars(
                select(Audit)
                .where(
                    Audit.account_id == account_id,
                    Audit.kind.in_(["MM13_OBSERVATION", "MM132_BOOK_SAMPLE"]),
                    Audit.data["instrument_id"].as_string() == instrument_id,
                    Audit.at >= timestamp - timedelta(hours=2),
                    Audit.at <= timestamp,
                )
                .order_by(Audit.at)
            )
        )
        # Binding changes invalidate history rather than combining different contracts.
        rows = [r for r in rows if r.data.get("signal_binding") in (None, list(binding))]
        cache[key] = (timestamp, rows)
    return fit(cache[key][1], timestamp, settings, tick, features)


def import_verified_archive(store, account_id, roots, since, until):
    """Backfill top-depth features only for exact sequences of already verified MM observations.

    No old audit row is changed. Own-free history is required; touching a price is never a fill.
    """
    import json
    from pathlib import Path

    from sigbot.strategies.mm_sig_v1.data import normalized_book

    if store.namespace != "live":
        from sigbot.errors import AuthorizationRequired

        raise AuthorizationRequired("REAL_ARCHIVE_IMPORT_REQUIRES_LIVE_STORE")
    books: dict = {}
    conflicts: set = set()
    invalid = 0
    for root in roots:
        for path in Path(root).glob("*/broadcasts/*.json"):
            data = json.loads(path.read_text(), parse_float=Decimal)
            received = utc_time(data["received_at"])
            if not since <= received <= until or data.get("event") != "markets_batch":
                continue
            for market in data["payload"]["markets"]:
                for raw in market.get("batch", {}).get("books", []):
                    if raw.get("asOf") is None:
                        invalid += 1
                        continue
                    key = str(raw["exchangeId"]), raw["asOf"]["sequence"]
                    value = (received, raw)
                    if key in books and (books[key][1]["bids"], books[key][1]["asks"]) != (
                        raw["bids"],
                        raw["asks"],
                    ):
                        conflicts.add(key)
                    else:
                        books.setdefault(key, value)
    # REST baselines are real archived responses too. Match exact sequences, never infer depth
    # from a nearby price or silently bridge an absent baseline.
    for path in Path("runtime/readonly/sessions").glob("*/*-orderbook.json"):
        received = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        if not since <= received <= until:
            continue
        raw = json.loads(path.read_text(), parse_float=Decimal)
        if not raw.get("asOf") or not raw.get("exchangeId"):
            continue
        key = str(raw["exchangeId"]), raw["asOf"]["sequence"]
        if key in books and (books[key][1]["bids"], books[key][1]["asks"]) != (
            raw["bids"],
            raw["asks"],
        ):
            conflicts.add(key)
        else:
            books.setdefault(key, (received, raw))
    imported, missing = 0, 0
    with store.transaction() as session:
        from sigbot.storage import Instrument, Order, RequestAttempt

        # No automatic order was sent in this recovery interval. Legacy references also verify
        # account-wide own-order identity, and the import below requires no live quote/inventory.
        sent = session.scalar(
            select(RequestAttempt.id)
            .join(Order, Order.id == RequestAttempt.order_id)
            .where(
                Order.account_id == account_id,
                RequestAttempt.action == "SUBMIT",
                RequestAttempt.sent_at >= since,
                RequestAttempt.sent_at <= until,
                RequestAttempt.state != "NOT_SENT",
            )
            .limit(1)
        )
        if sent is not None:
            return {"status": "REQUIRES_HISTORICAL_OWN_ORDER_RECONSTRUCTION", "imported": 0}
        existing = set(
            session.scalars(
                select(Audit.data["origin_audit_id"].as_integer()).where(
                    Audit.account_id == account_id, Audit.kind == "MM132_BOOK_SAMPLE"
                )
            )
        )
        rows = list(
            session.scalars(
                select(Audit)
                .where(
                    Audit.account_id == account_id,
                    Audit.kind == "MM13_OBSERVATION",
                    Audit.at >= since,
                    Audit.at <= until,
                )
                .order_by(Audit.at)
            )
        )
        for row in rows:
            quote = row.data.get("quote", {})
            recovered_price_reference = bool(
                quote.get("external_bid") and quote.get("external_ask")
            ) and set(quote.get("blocking_reasons", [])) <= {
                "MM_ACCOUNT_CHECKPOINT_NOT_CURRENT",
                "MM13_TWO_CONSISTENT_CHECKS_REQUIRED",
                "MM13_JUMP_COOLDOWN",
            }
            if (
                row.id in existing
                or row.data.get("book_features")
                or row.data.get("synthetic")
                or not (row.data.get("reference_usable") or recovered_price_reference)
            ):
                continue
            inst = session.get(Instrument, row.data["instrument_id"])
            key = str(inst.exchange_id), row.data.get("sequence")
            if key not in books or key in conflicts:
                missing += 1
                continue
            received, raw = books[key]
            if (
                received > row.at
                or any(row.data.get("eligible_orders", {}).values())
                or Decimal(quote.get("inventory_quantity", "-1")) != 0
            ):
                continue
            if utc_time(raw["asOf"]["at"]) > row.at:
                invalid += 1
                continue
            view = normalized_book(raw, [], received)
            feature = book_features(view, inst.tick)
            if (
                not feature
                or (Decimal(feature["bid"]), Decimal(feature["ask"]))
                != (Decimal(quote["external_bid"]), Decimal(quote["external_ask"]))
                or [quote.get("rules_hash"), quote.get("mapping_hash")]
                != [inst.rules_hash, inst.mapping_hash]
            ):
                invalid += 1
                continue
            store.audit(
                session,
                account_id,
                "MM132_BOOK_SAMPLE",
                {
                    "instrument_id": inst.id,
                    "origin_audit_id": row.id,
                    "book_features": feature,
                    "price_reference_usable": True,
                    "signal_binding": [inst.rules_hash, inst.mapping_hash],
                    "source": "EXACT_REAL_ARCHIVE_SEQUENCE_AND_VERIFIED_OBSERVATION",
                    "legacy_financial_reference_usable": row.data.get("reference_usable"),
                    "financial_checkpoint_not_used_as_price_continuity": True,
                    "layer": "REAL_READ",
                    "synthetic": False,
                },
                row.at,
            )
            imported += 1
    _CACHE.clear()
    return {
        "status": "IMPORTED_VERIFIED_SUBSET",
        "imported": imported,
        "missing_exact_sequences": missing,
        "conflicting_sequences": len(conflicts),
        "invalid": invalid,
    }
