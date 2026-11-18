"""Full discovery, versioned admission, bounded rotation and durable inventory tracking."""

import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.config import Config
from sigbot.domain import ZERO, content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.storage import Audit, Instrument, IntentRow, Order, Position
from sigbot.strategies.mm_sig_v1.data import reviewed_schema
from sigbot.strategies.mm_sig_v1.live.analytics import owned, statistics, unresolved
from sigbot.strategies.mm_sig_v1.live.inputs import review_market
from sigbot.strategies.mm_sig_v1.live.models import quote_cash_budget


def pinned_review(runtime, exchange_id, observed, timestamp):
    """A new document cannot revoke an existing admitted instrument (AGENTS 18)."""
    with runtime.live_store.transaction() as session:
        inst = session.get(Instrument, observed["instrument_id"])
        previous = inst.mapping_evidence.get("review", {}) if inst else {}
        if (
            not inst
            or not inst.reviewed
            or previous.get("status") != "VERIFIED_OFFICIAL_BINARY_SCOPE_AND_PREDICATE"
        ):
            return observed
        if (inst.tournament, inst.exchange_id, previous.get("market_id")) != (
            runtime.tid,
            exchange_id,
            observed["market_id"],
        ):
            raise SchemaMismatch("MM_CURRENT_INSTRUMENT_IDENTITY_MISMATCH")
        if (observed["rules_hash"], observed["mapping_hash"]) == (
            inst.rules_hash,
            inst.mapping_hash,
        ):
            return observed
        runtime.live_store.audit(
            session,
            runtime.aid,
            "OFFICIAL_RULE_DOCUMENT_CHANGE",
            {
                "policy": "REPORT_ONLY",
                "source": "MM_CATALOG_REVIEW",
                "instrument": inst.id,
                "old": previous,
                "new": observed,
                "old_hash": inst.rules_hash,
                "new_hash": observed["rules_hash"],
            },
            timestamp,
        )
        return {
            **previous,
            "instrument_id": inst.id,
            "rules_hash": inst.rules_hash,
            "mapping_hash": inst.mapping_hash,
        }


def read_prices(markets, reads, tournament):
    mapping = {e["id"]: m for m in markets for e in m["exchanges"]}
    prices: list[dict] = []
    ids = list(mapping)
    for start in range(0, len(ids), 100):
        response = reads.call(
            "bulk_prices",
            query={"tournamentId": tournament, "ids": ",".join(ids[start : start + 100])},
        )["data"]
        received = datetime.now(timezone.utc).isoformat()
        prices.extend({**row, "_read_received_at": received} for row in response)
    if (
        len(prices) != len(ids)
        or {p["exchangeId"] for p in prices} != set(ids)
        or any(p["marketId"] != mapping[p["exchangeId"]]["id"] for p in prices)
    ):
        raise SchemaMismatch("MM13_BBO_SCOPE_OR_COVERAGE_INVALID")
    return {p["exchangeId"]: p for p in prices}


def refresh_prices(runtime, reads):
    if runtime.settings.shared_sig_source:
        from sigbot.shared_marketdata import SharedSigCollector

        runtime.bbo = SharedSigCollector(
            runtime.settings.shared_sig_source,
            runtime.tid,
            runtime.aid,
            [
                {"api_market_id": m["id"], "exchange_ids": [e["id"] for e in m["exchanges"]]}
                for m in runtime.markets
            ],
        ).prices()
    else:
        runtime.bbo = read_prices(runtime.markets, reads, runtime.tid)
    runtime.state.update(quote_coverage=len(runtime.bbo), bbo_at=runtime.timestamp().isoformat())


def discover(runtime, reads, snapshot=None):
    timestamp = runtime.timestamp()
    markets = (
        snapshot["markets"]
        if snapshot
        else reads.all_pages(
            "market_list", query={"tournamentId": runtime.tid, "status": "any", "limit": 100}
        )
    )
    mapping = {e["id"]: m for m in markets for e in m["exchanges"]}
    if len(mapping) != sum(len(m["exchanges"]) for m in markets):
        raise SchemaMismatch("MM_CATALOG_MAPPING_AMBIGUOUS")
    runtime.markets = markets
    if snapshot:
        runtime.bbo = snapshot["prices"]
        runtime.state.update(quote_coverage=len(runtime.bbo), bbo_at=snapshot["at"])
    else:
        refresh_prices(runtime, reads)
    allowed = set()
    if runtime.settings.application_config and runtime.settings.approval_file:
        app = Config.load(runtime.settings.application_config)
        approval = json.loads(Path(runtime.settings.approval_file).read_text())
        if timestamp < utc_time(approval["new_orders_until"]):
            allowed = set(app.allowed_instruments) & set(approval["instrument_allowlist"])
    else:
        approval = {}
    _, schema = reviewed_schema()
    input_schema = schema.spec["components"]["schemas"]["OrderInput"]["properties"]
    grid = re.search(r"on the ([0-9]+\.[0-9]+) tick", input_schema["price"]["description"])
    if (
        grid is None
        or input_schema["quantity"]["type"] != "integer"
        or input_schema["quantity"]["exclusiveMinimum"] != 0
    ):
        raise SchemaMismatch("MM13_TICK_OR_LOT_CONTRACT_UNVERIFIED")
    tick, step = Decimal(grid[1]), Decimal(1)
    managed = set()
    with runtime.live_store.transaction() as session:
        positions = {
            p.instrument_id: p.quantity
            for p in session.scalars(select(Position).where(Position.account_id == runtime.aid))
        }
        for order in session.scalars(select(Order).where(Order.account_id == runtime.aid)):
            if owned(session.get(IntentRow, order.intent_id)) and (
                positions.get(order.instrument_id, ZERO) or unresolved(order)
            ):
                inst = session.get(Instrument, order.instrument_id)
                managed.add(inst.exchange_id)
    reviews, rejected = {}, {}
    cached_nodes = {}
    if runtime.settings.reviewed_nodes_file and not snapshot:
        cached = json.loads(Path(runtime.settings.reviewed_nodes_file).read_text())
        if cached.get("tournament_id") == runtime.tid and cached.get("account_id") == runtime.aid:
            cached_nodes = cached.get("nodes", {})
    targets = {eid for eid in mapping if f"SIG-{runtime.tid}-{eid}-YES" in allowed} | managed
    for eid in sorted(targets):
        if eid not in mapping:
            rejected[eid] = "MARKET_DISAPPEARED"
            continue
        if snapshot and mapping[eid]["id"] not in snapshot["nodes"]:
            rejected[eid] = "OFFICIAL_RULE_REVIEW_UNAVAILABLE"
            continue
        try:
            cached = cached_nodes.get(mapping[eid]["id"], {})
            cached_current = bool(
                cached.get("official_get_schema_and_scope_validated")
                and cached.get("observed_at")
                and utc_time(cached["observed_at"]) <= timestamp
                and content_hash(cached.get("nodes")) == cached.get("digest")
            )
            review = review_market(
                mapping[eid],
                snapshot["nodes"].get(mapping[eid]["id"], {})
                if snapshot
                else cached["nodes"]
                if cached_current
                else reads.market_nodes(mapping[eid]["id"]),
                runtime.tid,
            )
            review.update(
                tick=str(tick),
                lot_step=str(step),
                grid_source="VERIFIED_OFFICIAL_ORDER_INPUT_SCHEMA",
                us_state=review["contract_details"].get("usState"),
            )
            if review["instrument_id"] in allowed:
                review = pinned_review(runtime, eid, review, timestamp)
            if review["instrument_id"] in allowed and (
                review["rules_hash"],
                review["mapping_hash"],
            ) != (
                approval.get("rules_hashes", {}).get(review["instrument_id"]),
                approval.get("mapping_hashes", {}).get(review["instrument_id"]),
            ):
                rejected[eid] = "AUTHORITY_RULES_OR_MAPPING_CHANGED"
            if runtime.settings.complementary_quotes_enabled:
                no_id = review["instrument_id"].removesuffix("-YES") + "-NO"
                if no_id not in allowed or (
                    approval.get("rules_hashes", {}).get(no_id),
                    approval.get("mapping_hashes", {}).get(no_id),
                ) != (review["rules_hash"], review["mapping_hash"]):
                    rejected[eid] = "COMPLEMENT_AUTHORITY_RULES_OR_MAPPING_CHANGED"
            reviews[eid] = review
        except SchemaMismatch as exc:
            rejected[eid] = exc.code
            if eid in runtime.reviews:
                reviews[eid] = runtime.reviews[eid]
    runtime.reviews, runtime.managed = reviews, managed
    runtime.state.update(
        catalog_markets=len(markets),
        catalog_at=timestamp.isoformat(),
        rules_refresh_status=snapshot.get("rule_refresh_status", "REVIEWED_BOUND_VERSIONS")
        if snapshot
        else "REVIEWED_BOUND_VERSIONS",
        admission_rejected=rejected,
        discovered_universe=list(mapping),
        reviewed_universe=[eid for eid in reviews if eid not in rejected],
        managed_inventory=sorted(managed),
    )
    select_pool(runtime)


def select_pool(runtime):
    timestamp = runtime.timestamp()
    candidates, rates, scores, cold = [], {}, {}, []
    quoting = set()
    app = (
        Config.load(runtime.settings.application_config)
        if runtime.settings.application_config
        else None
    )
    deferred_fit = (
        runtime.settings.max_events is None
        and (app is None or app.risk.max_open_orders is None)
        and runtime.settings.pricing_model == "GUEANT_FINITE_GRID"
    )
    with runtime.live_store.transaction() as session:
        positions = {
            p.instrument_id: p.quantity
            for p in session.scalars(select(Position).where(Position.account_id == runtime.aid))
        }
        current_managed = set()
        owned_instruments = set()
        for order in session.scalars(select(Order).where(Order.account_id == runtime.aid)):
            if owned(session.get(IntentRow, order.intent_id)):
                owned_instruments.add(order.instrument_id)
                inst = session.get(Instrument, order.instrument_id)
                if positions.get(order.instrument_id, ZERO) or unresolved(order):
                    current_managed.add(inst.exchange_id)
                if (
                    order.state in {"OPEN", "PARTIAL"}
                    and order.remote_id
                    and order.request_state == "ACKNOWLEDGED"
                ):
                    quoting.add(inst.exchange_id)
        runtime.managed = current_managed
        for eid, review in runtime.reviews.items():
            if eid in runtime.state.get("admission_rejected", {}):
                continue
            raw = runtime.bbo.get(eid, {})
            if raw.get("bestBid") is None or raw.get("bestAsk") is None:
                continue
            spread = Decimal(str(raw["bestAsk"])) - Decimal(str(raw["bestBid"]))
            if spread <= 0:
                continue
            if deferred_fit:
                # With no allocation slots to rank, candidate selection must not
                # refit every model before any market can be evaluated. The
                # per-instrument quote and executor still perform admission.
                candidates.append(eid)
                cold.append(eid)
                scores[eid] = max(ZERO, spread - 2 * (runtime.fee or ZERO))
                continue
            # Without an own order there cannot be a calibrated own-fill model.
            # Keep the existing cold ranking/admission; avoid scanning its audit
            # history twice before the independent sampling workers can start.
            cold_only = (
                runtime.settings.pricing_model == "GUEANT_FINITE_GRID"
                and review["instrument_id"] not in owned_instruments
                and (
                    runtime.settings.calibration_sampling_enabled
                    or runtime.settings.standard_calibration_execution != "EXISTING_APPROVED_POLICY"
                )
            )
            stats = (
                {}
                if cold_only
                else statistics(
                    session,
                    runtime.aid,
                    review["instrument_id"],
                    timestamp,
                    runtime.settings,
                    tick=Decimal(review["tick"]),
                )
            )
            rates[eid] = stats
            if runtime.settings.pricing_model == "GUEANT_FINITE_GRID":
                from sigbot.strategies.mm_sig_v1.live.calibration import (
                    estimate as standard_estimate,
                )

                inst = session.get(Instrument, review["instrument_id"])
                parameters = (
                    standard_estimate(
                        session,
                        runtime.aid,
                        review["instrument_id"],
                        timestamp,
                        runtime.settings,
                        (review["rules_hash"], review["mapping_hash"]),
                    )
                    if inst and not cold_only
                    else {}
                )
                from sigbot.strategies.mm_sig_v1.live.standard_policy import parameters_ready

                position = session.get(Position, (runtime.aid, review["instrument_id"]))
                ready = parameters_ready(parameters, position.quantity if position else ZERO)
                # Selection must not hold every instrument's write lock while
                # fitting the full catalog. Per-instrument evaluation stores its cache.
                if ready:
                    scores[eid] = max(ZERO, spread - 2 * (runtime.fee or ZERO)) * min(
                        Decimal(parameters[side]["a_per_second"])
                        for side in ("BUY", "SELL")
                        if parameters.get(side, {}).get("status") == "READY_OWN_ORDER_INTENSITY"
                    )
                    candidates.append(eid)
                    continue
                if (
                    runtime.settings.calibration_sampling_enabled
                    or runtime.settings.standard_calibration_execution != "EXISTING_APPROVED_POLICY"
                ):
                    cold.append(eid)
                    flow = getattr(runtime, "activity", {}).get(eid, {}).get("public_flow", {})
                    flow_current = bool(
                        flow.get("status") == "VERIFIED_PUBLIC_WINDOW"
                        and flow.get("public_shares_per_second") is not None
                        and 0
                        <= (timestamp - utc_time(flow["to"])).total_seconds()
                        <= runtime.settings.activity_refresh_seconds
                        + runtime.settings.bbo_refresh_seconds
                        + runtime.settings.book_ttl_seconds
                    )
                    scores[eid] = (
                        max(ZERO, spread - 2 * (runtime.fee or ZERO))
                        * Decimal(flow["public_shares_per_second"])
                        if flow_current
                        else ZERO
                    )
                    candidates.append(eid)
                    continue
            if runtime.settings.participation_model == "CDJ_BOOK_IMBALANCE":
                from sigbot.strategies.mm_sig_v1.live.imbalance import book_features, estimate
                from sigbot.strategies.mm_sig_v1.models import Book

                inst = session.get(Instrument, review["instrument_id"])
                feature = (
                    book_features(
                        Book.model_validate(
                            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
                        ),
                        inst.tick,
                    )
                    if inst and inst.book and inst.book_at
                    else None
                )
                signal = (
                    estimate(
                        session,
                        runtime.aid,
                        inst.id,
                        timestamp,
                        runtime.settings,
                        inst.tick,
                        feature,
                        (inst.rules_hash, inst.mapping_hash),
                    )
                    if feature
                    else {}
                )
                ready = signal.get("BUY", {}).get("status") == "READY_EMPIRICAL_PROXY"
                if not ready:
                    cold.append(eid)
                    scores[eid] = ZERO
                else:
                    scores[eid] = max(
                        ZERO,
                        spread / 2
                        + Decimal(signal["BUY"]["conservative_change"])
                        - (runtime.fee or ZERO),
                    )
                candidates.append(eid)
                continue
            nu_b, nu_a = stats["buy_rate_per_minute"], stats["sell_rate_per_minute"]
            if (
                nu_b is None
                or nu_a is None
                or stats["buy_adverse"] is None
                or stats["sell_adverse"] is None
            ):
                cold.append(eid)
                scores[eid] = ZERO
            else:
                position = session.get(Position, (runtime.aid, review["instrument_id"]))
                locked = (position.cost or ZERO) if position else ZERO
                locked += sum(
                    (
                        o.reserve_cash
                        for o in session.scalars(
                            select(Order).where(
                                Order.account_id == runtime.aid,
                                Order.instrument_id == review["instrument_id"],
                            )
                        )
                    ),
                    ZERO,
                )
                fee = runtime.fee
                if fee is None:
                    continue
                scores[eid] = (
                    min(nu_b, nu_a)
                    * max(ZERO, spread - 2 * fee - stats["buy_adverse"] - stats["sell_adverse"])
                    / max(
                        locked,
                        quote_cash_budget(runtime.settings, runtime.settings.initial_capital_limit),
                    )
                )
            candidates.append(eid)
        old = session.scalar(
            select(Audit)
            .where(Audit.account_id == runtime.aid, Audit.kind == "MM13_UNIVERSE")
            .order_by(Audit.id.desc())
            .limit(1)
        )
        previous = old.data if old else {}
    ranked = sorted(
        (e for e in candidates if deferred_fit or e not in cold), key=lambda e: (-scores[e], e)
    )
    probe_authorized = False
    if runtime.settings.join_probe_enabled and runtime.settings.approval_file:
        import json

        authority = json.loads(Path(runtime.settings.approval_file).read_text())
        probe_authorized = bool(
            authority.get("accept_join_probe")
            and authority.get("operator_approved")
            and timestamp < utc_time(authority["new_orders_until"])
        )
    ordered_cold = sorted(cold, key=lambda e: (e not in runtime.managed, -scores[e], e))
    calibration = []
    if runtime.settings.calibration_sampling_enabled and runtime.settings.approval_file:
        import json

        authority = json.loads(Path(runtime.settings.approval_file).read_text())
        if (
            authority.get("operator_approved")
            and authority.get("accept_mm_calibration")
            and authority.get("mm_calibration_until")
            and authority.get("account_id") == runtime.aid
            and authority.get("tournament_id") == runtime.tid
            and utc_time(authority["issued_at"])
            <= timestamp
            < utc_time(authority["new_orders_until"])
            and timestamp < utc_time(authority["mm_calibration_until"])
        ):
            calibration = [
                e
                for e in ordered_cold
                if runtime.reviews[e]["instrument_id"] in authority["instrument_allowlist"]
            ]
    previous_probe = previous.get("exploration_universe", [])
    if (
        previous_probe
        and previous_probe[0] in ordered_cold
        and previous_probe[0] not in runtime.managed
    ):
        offset = ordered_cold.index(previous_probe[0]) + 1
        ordered_cold = ordered_cold[offset:] + ordered_cold[:offset]
    exploration = ordered_cold[: runtime.settings.exploration_event_cap] if probe_authorized else []
    ranked += exploration
    ranked += [e for e in calibration if e not in ranked]
    desired: list[str] = []
    cap = runtime.settings.max_events if runtime.settings.max_events is not None else len(ranked)
    if app is not None:
        if app.risk.max_open_orders is not None:
            cap = min(cap, app.risk.max_open_orders // 2)
    for eid in ranked:
        if len(desired) >= cap:
            break
        desired.append(eid)
        if len(desired) >= cap:
            break
    current = [
        e
        for e in previous.get("selection_universe", previous.get("quoting_universe", []))
        if e in candidates
        and (deferred_fit or e not in cold or e in exploration or e in calibration)
    ]
    confirmations = previous.get("rotation_confirmations", {})
    for incoming in desired:
        if incoming in current:
            continue
        if len(current) < cap:
            current.append(incoming)
            continue
        weakest = min(current, key=lambda e: scores[e])
        key = incoming + ":" + weakest
        better = scores[incoming] > scores[weakest] * (
            1 + runtime.settings.rotation_improvement_fraction
        )
        confirmations[key] = confirmations.get(key, 0) + 1 if better else 0
        if confirmations[key] >= runtime.settings.rotation_confirmation_rounds:
            current.remove(weakest)
            current.append(incoming)
    runtime.selected, runtime.exploration = current[:cap], set(exploration)
    # The already approved events all receive independent evaluations, even while one waits.
    runtime.tracking_exchanges = sorted(set(runtime.reviews) | runtime.managed)
    data = {
        "discovered_universe": runtime.state.get("discovered_universe", []),
        "reviewed_universe": runtime.state.get("reviewed_universe", []),
        "candidate_universe": [
            e
            for e in candidates
            if runtime.state.get("quotes", {}).get(e, {}).get("blocking_reasons") == []
            and (timestamp - utc_time(runtime.state["quotes"][e]["validated_at"])).total_seconds()
            <= runtime.settings.book_ttl_seconds
        ],
        "preliminary_observation_universe": candidates,
        "selection_universe": runtime.selected,
        "quoting_universe": sorted(quoting),
        "managed_inventory": sorted(runtime.managed),
        "exploration_universe": exploration,
        "calibration_universe": calibration,
        "scores": {k: str(v) for k, v in scores.items()},
        "cold_score_semantics": "PUBLIC_VOLUME_SPREAD_PROXY_NOT_OWN_FILL_PROBABILITY_OR_EXPECTED_PROFIT",
        "selection_fit_policy": "DEFERRED_TO_PER_INSTRUMENT_QUOTE_AND_EXECUTOR"
        if deferred_fit
        else "FIT_FOR_BOUNDED_ALLOCATION",
        "rotation_confirmations": confirmations,
        "scope_expansion": False,
        "authorized_event_evaluations": {
            eid: {
                "status": "REJECTED"
                if eid in runtime.state.get("admission_rejected", {})
                else "UNKNOWN"
                if eid in cold
                else "EVALUATING"
                if eid in candidates
                else "DATA_PENDING",
                "reason": runtime.state.get("admission_rejected", {}).get(eid)
                or (
                    "PARAMETERS_EVALUATED_PER_INSTRUMENT"
                    if deferred_fit and eid in candidates
                    else "STANDARD_MODEL_PARAMETERS_UNCALIBRATED"
                    if runtime.settings.pricing_model == "GUEANT_FINITE_GRID" and eid in cold
                    else "CDJ_PUBLIC_BOOK_MODEL_WARMUP"
                    if runtime.settings.participation_model == "CDJ_BOOK_IMBALANCE"
                    else "AS_UNKNOWN_PROBE_DISABLED"
                    if eid in cold and not probe_authorized
                    else "AWAITING_CURRENT_BOOK_DECISION"
                ),
                "at": timestamp.isoformat(),
                "evaluations": runtime.state.get("authorized_event_evaluations", {})
                .get(eid, {})
                .get("evaluations", 0),
            }
            for eid in runtime.reviews
        },
    }
    runtime.state.update(
        data, market_selection_status="READY" if current else "WAITING_ELIGIBLE_MARKETS"
    )
    with runtime.live_store.transaction() as session:
        runtime.live_store.audit(session, runtime.aid, "MM13_UNIVERSE", data, timestamp)
