"""Dynamic full-catalog SIG watchlist and opportunity preparation, read-only.

No inference of own fills, authorization or settlement equivalence from a
positive public spread. Observations survive restarts in one compact journal.
"""

import json
from collections import Counter, deque
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

from sigbot.domain import ZERO, content_hash, utc_time
from sigbot.storage_policy import bounded_storage
from sigbot.strategies.mm_sig_v1.live.calibration import price_risk
from sigbot.strategies.mm_sig_v1.live.imbalance import book_features, fit, state_key
from sigbot.strategies.mm_sig_v1.live.models import quote_cash_budget
from sigbot.strategies.mm_sig_v1.live.standard import liquidation


class MarketMonitor:
    def __init__(self, root, settings, now):
        self.root, self.settings = root, settings
        self.journal = root / "market-observations.jsonl"
        self.rows, self.next_at = {}, now
        self.signal_rows, self.signal_cache = {}, {}
        self.catalog_binding, self.previous = None, None
        # ponytail: a daily compact journal, streamed on restart; replace with
        # partitioned columnar retention if measured startup cost requires it.
        journal = root / f"market-observations-{now.date().isoformat()}.jsonl"
        self.journal = journal
        periods = [now - timedelta(hours=h) for h in (2, 1, 0)] if bounded_storage() else [now - timedelta(days=1), now]
        for day in periods:
            stamp = day.strftime("%Y-%m-%dT%H") if bounded_storage() else day.date().isoformat()
            old = root / f"market-observations-{stamp}.jsonl"
            if old.is_file():
                with old.open() as source:
                    for line in source:
                        row = json.loads(line)
                        at = utc_time(row["at"])
                        if now - timedelta(minutes=65) <= at <= now:
                            self.append(row)

    def append(self, row):
        at = utc_time(row["at"])
        for eid, data in row["prices"].items():
            refs = self.rows.setdefault(eid, deque())
            refs.append(
                (at, Decimal(data["mid"]) if data["mid"] is not None else None, data["binding"])
            )
            while refs and refs[0][0] < at - timedelta(minutes=65):
                refs.popleft()
            signals = self.signal_rows.setdefault(eid, deque())
            signals.append(
                SimpleNamespace(
                    at=at,
                    data={
                        "book_features": data.get("book_features"),
                        "price_reference_usable": bool(data.get("book_features")),
                        "signal_binding": data["binding"],
                    },
                )
            )
            while signals and signals[0].at < at - timedelta(minutes=65):
                signals.popleft()

    def outcome_signals(self, eid, entry, feature, binding, now, quotes):
        """Both outcome observations exist independently of execution permission."""
        if feature is None:
            signal: dict = {
                "status": "DATA_PENDING",
                "BUY": {"status": "DATA_PENDING", "conservative_change": None},
                "SELL": {"status": "DATA_PENDING", "conservative_change": None},
            }
        else:
            cache = self.signal_cache.get(eid)
            key = (state_key(feature), binding)
            if not cache or cache[1] != key or (now - cache[0]).total_seconds() >= 30:
                signal = fit(
                    self.signal_rows[eid],
                    now,
                    self.settings,
                    Decimal(entry["tick"]),
                    feature,
                    max_observation_gap=self.settings.monitor_interval_seconds + 1,
                )
                self.signal_cache[eid] = (now, key, signal)
            else:
                signal = cache[2]
            signal = {
                **deepcopy(signal),
                "feature": feature,
                "statistical_model_as_of": self.signal_cache[eid][0].isoformat(),
            }
        outcomes = {}
        for outcome in ("YES", "NO"):
            view = deepcopy(signal)
            if outcome == "NO":
                view["BUY"], view["SELL"] = deepcopy(signal["SELL"]), deepcopy(signal["BUY"])
                for side in ("BUY", "SELL"):
                    change = view[side].get("conservative_change")
                    if change is not None:
                        view[side]["conservative_change"] = str(-Decimal(change))
                view["horizons"] = {
                    ("SELL" if key.startswith("BUY:") else "BUY") + ":" + key.split(":")[1]: {
                        name: str(-Decimal(value))
                        if name in {"mean_change", "validation_mean"} and value is not None
                        else value
                        for name, value in details.items()
                    }
                    for key, details in signal.get("horizons", {}).items()
                }
                if feature:
                    view["feature"] = {
                        **feature,
                        "bid": str(1 - Decimal(feature["ask"])),
                        "ask": str(1 - Decimal(feature["bid"])),
                        "bid_quantity": feature["ask_quantity"],
                        "ask_quantity": feature["bid_quantity"],
                        "imbalance": str(-Decimal(feature["imbalance"])),
                        "imbalance_state": -feature["imbalance_state"],
                        "odds_bucket": int(
                            (1 - (Decimal(feature["bid"]) + Decimal(feature["ask"])) / 2) * 10
                        ),
                    }
                    view["state"] = list(state_key(view["feature"]))
                if not entry["binary_outcome_mapping_verified"]:
                    view = {
                        "status": "OUTCOME_MAPPING_UNVERIFIED",
                        "BUY": {
                            "status": "OUTCOME_MAPPING_UNVERIFIED",
                            "conservative_change": None,
                        },
                        "SELL": {
                            "status": "OUTCOME_MAPPING_UNVERIFIED",
                            "conservative_change": None,
                        },
                    }
            key = eid if outcome == "YES" else eid + ":NO"
            quote = quotes.get(key, {})
            evaluated_at = quote.get("validated_at")
            decision_current = bool(
                evaluated_at
                and 0
                <= (now - utc_time(evaluated_at)).total_seconds()
                <= self.settings.book_ttl_seconds
            )
            execution_enabled = outcome == "YES" or self.settings.complementary_quotes_enabled
            blockers = list(quote.get("blocking_reasons", []))
            if not execution_enabled:
                blockers.append("COMPLEMENTARY_EXECUTION_DISABLED")
            if not decision_current:
                blockers.append(
                    "EXECUTION_DECISION_STALE" if evaluated_at else "EXECUTION_NOT_EVALUATED"
                )
            if entry["rules_review"] != "REVIEWED":
                blockers.append("RULE_REVIEW_REQUIRED")
            blockers.extend(entry["book_blockers"])
            blockers.extend(entry.get("account_scope_blockers", []))
            parameters = quote.get("standard_model", {}).get("local_parameters", {})
            outcomes[outcome] = {
                "outcome": outcome,
                "observed_at": now.isoformat(),
                "source_at": entry["source_at"],
                "source_sequence": entry["source_sequence"],
                "validation_received_at": entry["validation_received_at"],
                "book_status": entry["book_status"]
                if outcome == "YES" or entry["binary_outcome_mapping_verified"]
                else "OUTCOME_MAPPING_UNVERIFIED",
                "book_blockers": entry["book_blockers"],
                "derived_from": "OFFICIAL_YES_NORMALIZED_BOOK" if outcome == "NO" else None,
                "public_book_signal": view,
                "own_order_parameters": parameters
                or {"status": "NO_OWN_ORDER_CALIBRATION", "public_flow_is_own_fill": False},
                "rules_review": entry["rules_review"],
                "execution_admission": "SEPARATE_FROM_READ_ONLY_OBSERVATION",
                "execution_enabled": execution_enabled,
                "execution_decision": quote.get("mode", "NOT_EVALUATED")
                if decision_current
                else "STALE_DECISION"
                if evaluated_at
                else "NOT_EVALUATED",
                "execution_decision_as_of": evaluated_at,
                "execution_decision_current": decision_current,
                "execution_blockers": sorted(set(blockers)),
                "orders_submitted_by_monitor": 0,
            }
        return outcomes

    def update(
        self,
        *,
        markets,
        books,
        feed,
        reviews,
        quotes,
        own_orders,
        fee,
        now,
        catalog_at,
        account_current,
        bulk_prices=None,
        bulk_received_at=None,
        blocked_exchanges=(),
    ):
        from sigbot.errors import SchemaMismatch
        from sigbot.strategies.mm_sig_v1.data import normalized_book

        if now < self.next_at:
            return self.previous
        self.next_at = now + timedelta(seconds=self.settings.monitor_interval_seconds)
        checked = feed.get("connection_checked_at") or feed.get("last_received_at")
        alive = (
            feed["subscription_status"] == "SUBSCRIBED"
            and checked is not None
            and 0 <= (now - utc_time(checked)).total_seconds() <= 15
        )
        catalog_current = bool(
            catalog_at
            and 0
            <= (now - utc_time(catalog_at)).total_seconds()
            <= self.settings.catalog_refresh_seconds + 60
        )
        prices, states = {}, {}
        for market in markets:
            mid = str(market["id"])
            for exchange in market["exchanges"]:
                eid = str(exchange["id"])
                # Quote/display-price changes are data, not a new contract binding.
                binding = content_hash(
                    [
                        mid,
                        eid,
                        market.get("title", market.get("name")),
                        market.get("status"),
                        market.get("settlementDate"),
                        market.get("isComposite"),
                        market.get("isMultiOutcome"),
                        exchange.get("option"),
                        [
                            (
                                c.get("type"),
                                c.get("tournament", {}).get("id"),
                                c.get("status"),
                                [e["id"] for e in c.get("exchanges", [])],
                            )
                            for c in market.get("contexts", [])
                        ],
                        reviews.get(eid, {}).get("rules_hash"),
                        reviews.get(eid, {}).get("mapping_hash"),
                    ]
                )
                raw = books.get(eid)
                reasons = list(feed["pending_reasons"].get(mid, []))
                if not alive:
                    reasons.append("STREAM_NOT_CURRENT")
                if not catalog_current:
                    reasons.append("CATALOG_NOT_CURRENT")
                if raw is None:
                    reasons.append("NO_BOOK")
                elif raw.get("validation_received_at") is None:
                    reasons.append("NO_REST_BASELINE")
                elif (
                    not 0
                    <= (now - utc_time(raw["validation_received_at"])).total_seconds()
                    <= self.settings.monitor_rest_audit_seconds + 60
                ):
                    reasons.append("REST_BASELINE_AUDIT_OVERDUE")
                entry = {
                    "market_id": mid,
                    "exchange_id": eid,
                    "tick": str(reviews.get(eid, {}).get("tick", "0.005")),
                    "binary_outcome_mapping_verified": len(market["exchanges"]) == 1
                    and market.get("isComposite") is False
                    and market.get("isMultiOutcome") is False
                    and exchange.get("option") == "YES",
                    "title": market["name"] if "name" in market else market.get("title"),
                    "catalog_status": market.get("status", "UNKNOWN"),
                    "book_status": "DATA_PENDING" if reasons else "CURRENT_VERSIONED_BOOK",
                    "book_blockers": sorted(set(reasons)),
                    "account_scope_blockers": ["UNRESOLVED_REQUEST_IN_MARKET"]
                    if eid in blocked_exchanges
                    else [],
                    "rules_review": "REVIEWED" if eid in reviews else "NOT_REVIEWED_FOR_MM",
                    "live_execution_from_monitor": False,
                    "public_market_flow": feed.get("public_flow", {}).get(
                        eid, {"status": "NOT_OBSERVED"}
                    ),
                    "execution_admission": "NOT_EVALUATED_BY_READ_ONLY_MONITOR",
                    "public_trade_history": "GAP"
                    if mid in feed["trade_tape_pending"]
                    else "OBSERVED_SESSION_ONLY_NOT_FULL_HISTORY",
                    "source_at": raw.get("asOf", {}).get("at") if raw and raw.get("asOf") else None,
                    "validation_received_at": raw.get("validation_received_at") if raw else None,
                    "source_sequence": raw.get("asOf", {}).get("sequence")
                    if raw and raw.get("asOf")
                    else None,
                    "buy_intensity": quotes.get(eid, {})
                    .get("standard_model", {})
                    .get("local_parameters", {})
                    .get("BUY", {"status": "NO_RESTING_ORDER_EXPOSURE"}),
                    "sell_intensity": quotes.get(eid, {})
                    .get("standard_model", {})
                    .get("local_parameters", {})
                    .get("SELL", {"status": "NO_RESTING_ORDER_EXPOSURE"}),
                }
                flow = entry["public_market_flow"]
                flow["current"] = bool(
                    flow.get("status") == "VERIFIED_PUBLIC_WINDOW"
                    and flow.get("to")
                    and 0
                    <= (now - utc_time(flow["to"])).total_seconds()
                    <= self.settings.activity_refresh_seconds
                    + self.settings.bbo_refresh_seconds
                    + self.settings.book_ttl_seconds
                )
                reference = None
                feature = None
                top = (bulk_prices or {}).get(eid)
                received = (
                    top.get("_read_received_at", bulk_received_at) if top else bulk_received_at
                )
                top_age = (now - utc_time(received)).total_seconds() if received else None
                top_current = (
                    top_age is not None
                    and 0
                    <= top_age
                    <= self.settings.bbo_refresh_seconds + self.settings.book_ttl_seconds
                )
                entry["top_of_book_watch"] = {
                    "status": "CURRENT_REST_BBO"
                    if top is not None and top_current
                    else "STALE_OR_MISSING_REST_BBO",
                    "received_at": received,
                    "best_bid": str(top["bestBid"])
                    if top and top.get("bestBid") is not None
                    else None,
                    "best_ask": str(top["bestAsk"])
                    if top and top.get("bestAsk") is not None
                    else None,
                    "source": "OFFICIAL_BULK_PRICES_SCOPED_TO_TOURNAMENT",
                    "depth_or_continuity_proven": False,
                    "own_orders_included": True,
                }
                if not reasons:
                    try:
                        book = normalized_book(raw, own_orders, now)
                    except SchemaMismatch as exc:
                        entry["book_status"] = "INVALID_BOOK"
                        entry["book_blockers"].append(exc.code)
                        prices[eid] = {"mid": None, "binding": binding}
                        states[eid] = entry
                        continue
                    bids = [
                        (level.price, level.quantity - level.own_quantity)
                        for level in book.bids
                        if level.quantity > level.own_quantity
                    ]
                    asks = [
                        (level.price, level.quantity - level.own_quantity)
                        for level in book.asks
                        if level.quantity > level.own_quantity
                    ]
                    if bids and asks:
                        bid, ask = max(p for p, _ in bids), min(p for p, _ in asks)
                        if bid < ask:
                            if account_current and eid not in blocked_exchanges:
                                feature = book_features(book, Decimal(entry["tick"]))
                            reference = (bid + ask) / 2
                            net = ask - bid - 2 * fee if fee is not None else None
                            base = int(
                                quote_cash_budget(
                                    self.settings, self.settings.initial_capital_limit
                                )
                                / ask
                            )
                            curve = liquidation(reference, bids, [base], fee or ZERO)
                            stress = liquidation(
                                reference,
                                bids,
                                [base],
                                fee or ZERO,
                                depth_fraction=self.settings.model_depth_stress_fraction,
                            )
                            entry.update(
                                best_bid=str(bid),
                                best_ask=str(ask),
                                spread=str(ask - bid),
                                net_spread=str(net) if net is not None else None,
                                own_orders_removed="RECONCILED_SNAPSHOT"
                                if account_current
                                else "ACCOUNT_SNAPSHOT_STALE_NOT_VERIFIED",
                                liquidity_quote_size=base,
                                liquidation_current={k: str(v) for k, v in curve[base].items()}
                                if fee is not None
                                else None,
                                liquidation_stress={k: str(v) for k, v in stress[base].items()}
                                if fee is not None
                                else None,
                                fee_status="OPERATOR_ACCEPTED_ESTIMATE"
                                if fee is not None
                                else "UNKNOWN",
                                spread_candidate=net is not None and net > 0,
                                ranking_score_susqies=str(
                                    max(ZERO, Decimal(base) * net - stress[base]["cost"])
                                )
                                if net is not None
                                else None,
                            )
                        else:
                            entry["book_status"] = "CROSSED_BOOK"
                    else:
                        entry["book_status"] = "ONE_SIDED_NO_TWO_SIDED_QUOTE"
                prices[eid] = {
                    "mid": str(reference) if reference is not None else None,
                    "binding": binding,
                    "book_features": feature,
                }
                states[eid] = entry
        synthetic = bool(markets) and all(str(m["id"]).startswith("SIM-") for m in markets)
        row = {
            "at": now.isoformat(),
            "layer": "MOCK" if synthetic else "REAL_READ",
            "synthetic_data_used": synthetic,
            "prices": prices,
        }
        self.append(row)
        for eid, entry in states.items():
            entry["outcomes"] = self.outcome_signals(
                eid, entry, prices[eid].get("book_features"), prices[eid]["binding"], now, quotes
            )
            entry["price_risk"] = price_risk(
                self.rows.get(eid, ()), now, max_gap=self.settings.monitor_interval_seconds + 1
            )
            entry["opportunity_status"] = (
                "DATA_PENDING"
                if entry["book_status"] != "CURRENT_VERSIONED_BOOK"
                else "FEE_UNKNOWN"
                if fee is None
                else "NO_POSITIVE_PUBLIC_SPREAD"
                if not entry.get("spread_candidate")
                else "RULE_REVIEW_REQUIRED"
                if eid not in reviews
                else "ACCOUNT_NOT_CURRENT"
                if not account_current
                else "BASELINE_EVALUATED_ADVANCED_CALIBRATING"
                if quotes.get(eid, {}).get("basic_core_execution_active")
                and not quotes[eid].get("blocking_reasons")
                and quotes[eid].get("validated_at")
                and 0
                <= (now - utc_time(quotes[eid]["validated_at"])).total_seconds()
                <= self.settings.book_ttl_seconds
                else "PARAMETERS_UNCALIBRATED"
                if quotes.get(eid, {}).get("standard_model", {}).get("status")
                != "READY_NUMERICAL_LOCAL_MODEL"
                else "MODEL_EVALUATED"
            )
        row["outcomes"] = {
            eid: {
                side: {
                    "signal_status": view["public_book_signal"]["status"],
                    "book_status": view["book_status"],
                    "rules_review": view["rules_review"],
                    "source_at": view["source_at"],
                    "source_sequence": view["source_sequence"],
                }
                for side, view in entry["outcomes"].items()
            }
            for eid, entry in states.items()
        }
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        stamp = now.strftime("%Y-%m-%dT%H") if bounded_storage() else now.date().isoformat()
        if self.journal.name != f"market-observations-{stamp}.jsonl":
            self.journal = self.root / f"market-observations-{stamp}.jsonl"
            if bounded_storage():
                keep = {f"market-observations-{(now - timedelta(hours=h)).strftime('%Y-%m-%dT%H')}.jsonl" for h in (0, 1, 2)}
                for old in self.root.glob("market-observations-*.jsonl"):
                    if old.name not in keep:
                        old.unlink()
        with self.journal.open("a") as output:
            output.write(json.dumps(row) + "\n")
        # Removed markets never survive in the active denominator or ranking.
        self.rows = {eid: values for eid, values in self.rows.items() if eid in states}
        self.signal_rows = {
            eid: values for eid, values in self.signal_rows.items() if eid in states
        }
        self.signal_cache = {
            eid: values for eid, values in self.signal_cache.items() if eid in states
        }
        counts = Counter(entry["opportunity_status"] for entry in states.values())
        ranked = sorted(
            (e for e in states if states[e].get("spread_candidate")),
            key=lambda e: (-Decimal(states[e]["ranking_score_susqies"]), e),
        )
        self.previous = {
            "at": now.isoformat(),
            "layer": row["layer"],
            "synthetic_data_used": synthetic,
            "catalog_markets": len(markets),
            "catalog_exchanges": sum(len(m["exchanges"]) for m in markets),
            "monitored_exchanges": len(states),
            "monitored_outcomes": sum(len(entry["outcomes"]) for entry in states.values()),
            "outcome_signal_counts": dict(
                Counter(
                    view["public_book_signal"]["status"]
                    for entry in states.values()
                    for view in entry["outcomes"].values()
                )
            ),
            "subscription_current": alive,
            "catalog_current": catalog_current,
            "current_books": sum(
                e["book_status"] == "CURRENT_VERSIONED_BOOK" for e in states.values()
            ),
            "fresh_bulk_price_exchanges": sum(
                e["top_of_book_watch"]["status"] == "CURRENT_REST_BBO" for e in states.values()
            ),
            "all_books_current": bool(states)
            and all(e["book_status"] == "CURRENT_VERSIONED_BOOK" for e in states.values()),
            "opportunity_counts": dict(counts),
            "spread_candidates": ranked,
            "markets": states,
            "orders_submitted_by_monitor": 0,
            "journal": str(self.journal),
        }
        return self.previous
