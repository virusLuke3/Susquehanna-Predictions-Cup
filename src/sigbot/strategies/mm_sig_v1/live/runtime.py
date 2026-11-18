"""Continuous official data, actual inventory and qualified price plans; no fake fills."""

from __future__ import annotations

import argparse
import fcntl
import json
import signal
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.accountcheck import persist_checkpoint
from sigbot.domain import TERMINAL, ZERO, content_hash, utc_time
from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    RateLimited,
    RiskRejected,
    SchemaMismatch,
    SigbotError,
    TransportUnknown,
)
from sigbot.history import tape
from sigbot.storage import (
    Account,
    AccountReadCheckpoint,
    Audit,
    Instrument,
    IntentRow,
    Order,
    Position,
)
from sigbot.strategies.mm_sig_v1.data import normalized_book
from sigbot.strategies.mm_sig_v1.live.database import open_store
from sigbot.strategies.mm_sig_v1.live.inputs import actual_inventory, actual_prices, review_market
from sigbot.strategies.mm_sig_v1.live.models import load_settings
from sigbot.strategies.mm_sig_v1.live.staging import stage_reviews
from sigbot.strategies.mm_sig_v1.preflight import REPORTS, ROOT, RealReads, save


def now():
    return datetime.now(timezone.utc)


class Runtime:
    def __init__(self, settings, env_file, *, instance_root=None, status_file=None):
        self.settings, self.env_file = settings, env_file
        self.instance_root = Path(instance_root) if instance_root else ROOT
        self.status_file = Path(status_file) if status_file else REPORTS / "CURRENT_STATUS.json"
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.account_data, self.account_result = None, None
        self.account_at = None
        self.store = open_store("readonly")
        self.live_store = open_store("live")
        self.reviews, self.selected, self.markets, self.activity = {}, [], [], {}
        self.read_count, self.account_read_count, self.fatal = 0, 0, None
        self.runner = None
        self.publisher = None
        self.collector = None
        self.market_monitor = None
        self.managed, self.exploration = set(), set()
        self.tracking_exchanges, self.bbo = [], {}
        self.valid_books = {}
        self.market_updates: dict = {}
        self.state: dict = {
            "strategy_id": "mm_sig",
            "strategy_version": settings.strategy_version,
            "data_mode": "REAL_ONLY",
            "lifecycle": "STARTING",
            "started_at": now().isoformat(),
            "quotes": {},
            "samples": 0,
            "real_orders": 0,
            "real_cancels": 0,
            "synthetic_data_used": False,
            "trading_enabled": False,
        }
        self.fee = None
        self.fee_reason: str | None = "OFFICIAL_FUTURE_TRADING_FEE_FORMULA_NOT_VERIFIED"
        if settings.fee_file:
            try:
                from sigbot.strategies.mm_sig_v1.live.fees import load_fee

                fee = load_fee(settings.fee_file)
                fee.check(False)
                self.fee = fee.per_share
                self.state["fee_evidence_kind"] = getattr(fee, "kind", "OFFICIAL_FIXED_RATE")
                self.state["official_future_fee_schedule_verified"] = (
                    getattr(fee, "kind", None) != "EMPIRICAL_ACTUAL_TRADE_CALIBRATION"
                )
                self.fee_reason = None
            except (SigbotError, ValueError, OSError) as exc:
                self.fee_reason = exc.code if isinstance(exc, SigbotError) else "FEE_FILE_INVALID"

    @staticmethod
    def timestamp():
        return now()

    def publish(self):
        with self.lock:
            timestamp = now()
            self.state["isolated_unknown_orders"] = getattr(self, "isolated_unknown_orders", [])
            account_age = (
                (timestamp - utc_time(self.account_at)).total_seconds() if self.account_at else None
            )
            account_current = bool(
                self.account_result
                and self.account_result["status"] == "PASS"
                and account_age is not None
                and 0 <= account_age <= self.settings.account_ttl_seconds
            )
            quotes = list(self.state.get("quotes", {}).values())
            eligible_quotes = [
                q
                for q in quotes
                if q.get("validated_at")
                and 0
                <= (timestamp - utc_time(q["validated_at"])).total_seconds()
                <= self.settings.book_ttl_seconds
                and not q.get("blocking_reasons")
            ]
            positive_target = any(
                Decimal(q.get("buy_quantity") or "0") > 0
                or Decimal(q.get("sell_quantity") or "0") > 0
                for q in eligible_quotes
            )
            readiness = (
                "ACCOUNT_DATA_NOT_CURRENT"
                if not account_current
                else "EXECUTABLE_QUOTE_CANDIDATE"
                if positive_target
                else "BOOK_DATA_PENDING"
                if not eligible_quotes or any(q.get("blocking_reasons") for q in quotes)
                else "CALIBRATION_AUTHORIZATION_REQUIRED"
                if any(
                    q.get("mode") == "CALIBRATION_AUTHORIZATION_REQUIRED" for q in eligible_quotes
                )
                else "SIGNAL_OR_PARAMETER_WARMUP"
            )
            state = {
                **self.state,
                "trading_enabled": bool(
                    self.state.get("trading_enabled")
                    or self.publisher is not None
                    and account_current
                    and positive_target
                ),
                "trading_enabled_scope": "QUALIFIED_SHARED_PROPOSALS"
                if self.publisher is not None
                else "LOCAL_EXECUTOR",
                "trading_readiness": readiness,
                "account_snapshot_current": account_current,
                "account_snapshot_age_seconds": account_age,
                "eligible_quote_count": len(eligible_quotes),
                "positive_quote_target_exists": positive_target,
                "heartbeat_at": now().isoformat(),
                "selected_exchanges": self.selected,
                "business_get_attempts": self.read_count
                + self.account_read_count
                + self.state.get("active_main_read_attempts", 0)
                + self.state.get("active_account_read_attempts", 0),
                "account_last_checked_at": self.account_at,
                "account_status": self.account_result["status"]
                if self.account_result
                else "NOT_READ",
                "future_fee_status": (
                    "EMPIRICAL_ESTIMATE_ACCEPTED_FOR_BOUNDED_EXECUTION"
                    if self.runner is not None
                    or self.state.get("empirical_fee_accepted_by_operator")
                    else "EMPIRICAL_ESTIMATE_OPERATOR_ACCEPTANCE_REQUIRED"
                )
                if self.state.get("fee_evidence_kind") == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION"
                else "VERIFIED"
                if self.fee is not None
                else self.fee_reason,
                "fatal": self.fatal,
            }
            reference_at = self.state.get("reference_sampler", {}).get("at")
            reference_age = (
                (timestamp - utc_time(reference_at)).total_seconds() if reference_at else None
            )
            state["reference_sampler_age_seconds"] = reference_age
            state["reference_sampler_status"] = (
                "FAILED"
                if self.state.get("reference_sampler_error")
                else "CURRENT"
                if reference_age is not None and 0 <= reference_age <= self.settings.book_ttl_seconds
                else "STALE"
                if reference_age is not None
                else "STARTING"
            )
            save(self.instance_root / "state.json", state, private=True)
            save(self.status_file, state)

    def update_account(self, reads):
        result, data = reads.checked_account()
        self.apply_account(result, data, reads.reader.config.expected_account_id)

    def apply_account(self, result, data, account_id, observed_at=None):
        observed_at = observed_at or now()
        persist_checkpoint(self.store, account_id, result, data, observed_at)
        with self.lock:
            self.account_data, self.account_result, self.account_at = (
                data,
                result,
                observed_at.isoformat(),
            )
            self.state["exact_cash"] = result["projected_cash"]
            self.state["actual_positions"] = result["positions"]
            self.state["account_display_differences"] = result["display_differences"]
            self.state["observed_fee_transaction_count"] = sum(
                tx["event_type"] == "fee" for tx in data["transactions"]
            )
            self.state["account_open_orders"] = result["open_orders"]
            self.state["strategy_capital"] = str(
                Decimal(result["projected_cash"])
                + sum((Decimal(p["cost"]) for p in result["positions"].values()), Decimal(0))
            )
            self.state["account_capital"] = self.state["strategy_capital"]
            if self.settings.initial_capital_limit is not None:
                from sigbot.accounting import strategy_finances

                with self.live_store.transaction() as session:
                    funds = strategy_finances(
                        session, account_id, "mm_sig", self.settings.initial_capital_limit
                    )
                self.state["strategy_finances"] = {k: str(v) for k, v in funds.items()}
                self.state["strategy_capital"] = str(
                    max(ZERO, min(Decimal(self.state["account_capital"]), funds["equity_at_fifo_cost"]))
                )
            if (
                self.settings.strategy_version == "1.3"
                and self.settings.initial_capital_limit
                > Decimal(str(data["cup"]["initialBalance"]))
            ):
                raise SchemaMismatch("MM13_INITIAL_CAPITAL_EXCEEDS_OFFICIAL_INITIAL_BALANCE")
        save(
            REPORTS / "ACCOUNT_CURRENT.json",
            {**result, "layer": "REAL_READ", "synthetic_data_used": False},
        )

    def shared_account(self):
        policy = getattr(self, "settings", None)
        ttl = (
            policy.account_ttl_seconds
            if policy is not None and policy.strategy_version == "1.3"
            else 30
        )
        with self.live_store.transaction() as session:
            account = session.get(Account, self.aid)
            if (
                account is None
                or account.tournament != self.tid
                or account.lifecycle != "HEALTHY"
                or account.reconciled_at is None
                or not 0 <= (now() - account.reconciled_at).total_seconds() <= ttl
            ):
                return None
            from sigbot.uncertainty import scopes

            pending = scopes(session, account)
            if any(not item["isolatable"] for item in pending):
                return None
            audit = session.scalar(
                select(Audit)
                .where(Audit.account_id == self.aid, Audit.kind == "RECONCILIATION")
                .order_by(Audit.id.desc())
                .limit(1)
            )
            if not audit or audit.data.get("status") != "PASS" or audit.at != account.reconciled_at:
                return None
            if {item["order_id"] for item in pending} != {
                item["order_id"] for item in audit.data.get("isolated_unknown_orders", [])
            }:
                return None
            checkpoint, observed_at, cash = (
                audit.data["checkpoint"],
                account.reconciled_at,
                account.cash,
            )
            positions = {}
            for pos in session.scalars(select(Position).where(Position.account_id == self.aid)):
                if pos.quantity:
                    inst = session.get(Instrument, pos.instrument_id)
                    if inst is None or inst.exchange_id is None or pos.cost is None:
                        return None
                    key = inst.exchange_id + ":" + inst.outcome
                    if key in positions:
                        return None
                    positions[key] = (pos.quantity, pos.cost)
        with self.store.transaction() as session:
            record = session.get(
                AccountReadCheckpoint, (self.store.namespace, self.aid, self.tid, checkpoint)
            )
            if not record or record.status != "PASS" or content_hash(record.data) != record.digest:
                return None
            result = deepcopy(record.data["result"])
            financial = deepcopy(record.data["financial_state"])
        if cash != Decimal(result["projected_cash"]) or positions != {
            k: (Decimal(p["quantity"]), Decimal(p["cost"])) for k, p in result["positions"].items()
        }:
            return None
        # Scope admission is mutable runtime state, never part of the immutable
        # official financial checkpoint (same checkpoint must retain its digest).
        self.isolated_unknown_orders = pending
        assert self.account_data is not None
        data = {k: financial[k] for k in ("orders", "fills", "transactions", "settlements")}
        data.update(
            cup={
                **self.account_data["cup"],
                "myBalance": financial["cash"],
                "initialBalance": financial["initial"],
            },
            positions={"positions": financial["positions"]},
            pagination_complete=True,
        )
        return result, data, observed_at

    def wait_shared_account(self, until):
        while not self.stop.is_set() and time.monotonic() < until:
            shared = self.shared_account()
            if shared is not None:
                self.state.pop("account_last_error", None)
                return shared
            self.state["account_last_error"] = "WAITING_SHARED_REAL_ACCOUNT_CHECKPOINT"
            self.publish()
            self.stop.wait(min(1, max(0, until - time.monotonic())))
        raise TransportUnknown("MM_SHARED_ACCOUNT_CHECKPOINT_UNAVAILABLE")

    def account_worker(self):
        reads = None
        try:
            while not self.stop.is_set():
                if self.publisher is not None or self.settings.execution_route == "OBSERVE":
                    shared = self.shared_account()
                    if shared:
                        if self.account_at is None or utc_time(self.account_at) != shared[2]:
                            self.apply_account(shared[0], shared[1], self.aid, shared[2])
                        self.state.pop("account_last_error", None)
                        self.state["account_checkpoint_source"] = "SHARED_VERIFIED_REAL_SNAPSHOT"
                    else:
                        self.state["account_last_error"] = "WAITING_SHARED_REAL_ACCOUNT_CHECKPOINT"
                    self.publish()
                    self.stop.wait(2)
                    continue
                started = time.monotonic()
                if (
                    reads is None
                    or reads.total_attempts >= 350
                    or time.monotonic() + 60 > reads.stop_at
                ):
                    if reads is not None:
                        self.account_read_count += reads.total_attempts
                        reads.close()
                    reads = RealReads(env_file=self.env_file, verify_reference=False)
                    aid, tid = reads.bootstrap()
                    if (aid, tid) != (self.aid, self.tid):
                        raise SchemaMismatch("MM_ACCOUNT_WORKER_SCOPE_CHANGED")
                try:
                    self.update_account(reads)
                except RateLimited as exc:
                    with self.lock:
                        self.state["account_last_error"] = exc.code
                self.state["active_account_read_attempts"] = reads.total_attempts
                self.publish()
                self.stop.wait(
                    max(1, self.settings.account_refresh_seconds - (time.monotonic() - started))
                )
        except Exception as exc:
            # Fatal boundary only: stop the worker and service, never claim healthy continuation.
            self.fatal = (
                exc.code
                if isinstance(exc, SigbotError)
                else "MM_ACCOUNT_WORKER_FATAL_" + type(exc).__name__
            )
            self.stop.set()
        finally:
            if reads is not None:
                self.account_read_count += reads.total_attempts
                self.state["active_account_read_attempts"] = 0
                reads.close()

    def discover(self, reads, snapshot=None):
        if self.settings.strategy_version == "1.3":
            from sigbot.strategies.mm_sig_v1.live.universe import discover

            discover(self, reads, snapshot=snapshot)
            if self.collector is not None:
                self.collector.update_catalog(
                    [
                        {
                            "api_market_id": m["id"],
                            "exchange_ids": [e["id"] for e in m["exchanges"]],
                        }
                        for m in self.markets
                    ]
                )
                self.collector.set_targets(
                    [],
                    market_ids=[
                        self.reviews[e]["market_id"]
                        for e in self.tracking_exchanges
                        if e in self.reviews
                    ],
                )
            return
        assert self.account_data is not None
        scoped = {"tournamentId": self.tid}
        markets = reads.all_pages("market_list", query={**scoped, "status": "any", "limit": 100})
        mapping = {e["id"]: m for m in markets for e in m["exchanges"]}
        if len(mapping) != sum(len(m["exchanges"]) for m in markets):
            raise SchemaMismatch("MM_CATALOG_MAPPING_AMBIGUOUS")
        prices, ids = [], list(mapping)
        for start in range(0, len(ids), 100):
            raw = reads.call(
                "bulk_prices", query={**scoped, "ids": ",".join(ids[start : start + 100])}
            )
            prices.extend(raw["data"])
        if len(prices) != len(ids):
            raise SchemaMismatch("MM_CATALOG_PRICE_COVERAGE_INCOMPLETE")
        ranked = sorted(
            [
                p
                for p in prices
                if p["bestBid"] is not None
                and p["bestAsk"] is not None
                and 0 < p["bestBid"] < p["bestAsk"] < 1
            ],
            key=lambda p: p["bestAsk"] - p["bestBid"],
            reverse=True,
        )
        allowed = None
        if self.settings.application_config:
            from pathlib import Path

            from sigbot.config import Config

            app = Config.model_validate_json(Path(self.settings.application_config).read_text())
            allowed = set(app.allowed_instruments)
            ranked = [p for p in ranked if f"SIG-{self.tid}-{p['exchangeId']}-YES" in allowed]
        if self.selected:
            current = {}
            for eid in self.reviews:
                if eid not in mapping:
                    raise SchemaMismatch("MM_CURRENT_MARKET_DISAPPEARED")
                checked = review_market(
                    mapping[eid], reads.market_nodes(mapping[eid]["id"]), self.tid
                )
                from sigbot.strategies.mm_sig_v1.live.universe import pinned_review

                current[eid] = pinned_review(self, eid, checked, self.timestamp())
            self.reviews, self.markets = current, markets
            self.state.update(
                catalog_markets=len(markets),
                quote_coverage=len(prices),
                catalog_at=now().isoformat(),
            )
            return
        reviews, occupied = {}, set()
        for pos in self.account_data["positions"]["positions"]:
            if pos["quantity"]:
                mid = pos["marketId"]
                review = review_market(
                    mapping[pos["exchangeId"]], reads.market_nodes(mid), self.tid
                )
                reviews[pos["exchangeId"]] = review
                occupied.add(review["event_key"])
        managed = set()
        if allowed is not None:
            with self.live_store.transaction() as s:
                for order in s.scalars(select(Order).where(Order.account_id == self.aid)):
                    intent = s.get(IntentRow, order.intent_id)
                    if (
                        intent.payload.get("strategy_id"),
                        intent.payload.get("strategy_version"),
                    ) == ("mm_sig", self.settings.strategy_version) and (
                        order.filled
                        or order.state not in TERMINAL
                        or order.reserve_cash
                        or order.reserve_quantity
                    ):
                        instrument = s.get(Instrument, order.instrument_id)
                        assert instrument is not None
                        managed.add(instrument.exchange_id)
        selected: list[str] = []
        selected_events = set()
        for eid, review in reviews.items():
            if (
                allowed is not None
                and eid in managed
                and review["instrument_id"] in allowed
                and review["event_key"] not in selected_events
                and len(selected) < self.settings.max_events
            ):
                selected.append(eid)
                selected_events.add(review["event_key"])
        rejected: list[dict] = []
        for item in ranked:
            if len(selected) >= self.settings.max_events:
                break
            eid = item["exchangeId"]
            if eid in reviews:
                continue
            if len(rejected) + len(selected) >= 12:
                break
            try:
                detail = reads.call(
                    "market_detail", path_params={"id": mapping[eid]["id"]}, query=scoped
                )
                review = review_market(detail, reads.market_nodes(detail["id"]), self.tid)
                if review["event_key"] in occupied:
                    continue
                reviews[eid] = review
                selected.append(eid)
                occupied.add(review["event_key"])
            except SchemaMismatch as exc:
                rejected.append({"exchange_id": eid, "reason": exc.code})
            if len(selected) == self.settings.max_events:
                break
        self.reviews, self.selected, self.markets = reviews, selected, markets
        self.state.update(
            market_selection_status="READY" if selected else "WAITING_ELIGIBLE_MARKETS",
            catalog_markets=len(markets),
            quote_coverage=len(prices),
            market_review_rejections=rejected,
            verified_market_reviews=len(reviews),
            catalog_at=now().isoformat(),
        )
        save(
            REPORTS / "MARKET_REVIEWS.json",
            {
                "layer": "REAL_READ",
                "selected_exchanges": selected,
                "reviews": reviews,
                "rejected": rejected,
                "synthetic_data_used": False,
            },
        )
        save(ROOT / "catalog.json", markets, private=True)

    def sample(self, reads, eid):
        if self.settings.strategy_version == "1.3":
            return self.sample13(reads, eid)
        review = self.reviews[eid]
        with self.lock:
            data, account, at = self.account_data, self.account_result, self.account_at
        assert data is not None and account is not None
        reasons = []
        if account["status"] != "PASS" or (now() - utc_time(at)).total_seconds() > 60:
            reasons.append("MM_ACCOUNT_CHECKPOINT_NOT_CURRENT")
        if account["open_orders"]:
            from sqlalchemy import select

            from sigbot.storage import Order

            with self.live_store.transaction() as s:
                tracked = {
                    o.remote_id
                    for o in s.scalars(select(Order).where(Order.account_id == self.aid))
                }
            if any(o["open"] and str(o["id"]) not in tracked for o in data["orders"]):
                reasons.append("MM_EXTERNAL_OR_UNKNOWN_OPEN_ORDERS")
        activity = self.activity.get(eid)
        if (
            activity is None
            or (now() - utc_time(activity["at"])).total_seconds()
            > self.settings.activity_refresh_seconds
        ):
            until = now().replace(microsecond=0)
            observed = tape(
                reads,
                review["market_id"],
                eid,
                self.tid,
                until - timedelta(minutes=10),
                until,
                max_pages=3,
            )
            activity = {
                "at": until.isoformat(),
                "trade_count": len(observed["trades"]),
                "complete": observed["pagination_complete"],
            }
            self.activity[eid] = activity
        if not activity["complete"] or not activity["trade_count"]:
            reasons.append("MM_NO_COMPLETE_RECENT_TRADING_DEMAND")
        if self.runner is not None and self.settings.strategy_version == "1.2":
            book = self.runner.exchange.refresh_book(review["instrument_id"])
            with self.live_store.transaction() as s:
                from sigbot.storage import Instrument

                inst = s.get(Instrument, review["instrument_id"])
                inst.review_until = self.runner.exchange.approval.new_orders_until
        else:
            raw = reads.call(
                "orderbook", path_params={"id": eid}, query={"tournamentId": self.tid, "depth": 200}
            )
            received = now()
            if raw["marketId"] != review["market_id"]:
                raise SchemaMismatch("MM_CURRENT_BOOK_MARKET_CHANGED")
            book = normalized_book(raw, data["orders"], received)
        qty = actual_inventory(data, eid)
        prices = actual_prices(
            book,
            qty,
            Decimal("0.005"),
            self.settings,
            fee=self.fee,
            capital=Decimal(self.state["strategy_capital"]),
        )
        if self.fee_reason:
            reasons.append(self.fee_reason)
        if not self.settings.approval_file:
            reasons.append("BOUNDED_ORDER_APPROVAL_REQUIRED")
        if not self.settings.application_config:
            reasons.append("MM_LIVE_APPLICATION_CONFIG_REQUIRED")
        prices.update(
            title=review["title"],
            event_key=review["event_key"],
            exchange_id=eid,
            rules_hash=review["rules_hash"],
            mapping_hash=review["mapping_hash"],
            rules_verified=True,
            account_checkpoint=account["checkpoint"],
            actual_inventory_source="COMPLETE_CURRENT_REAL_ACCOUNT_POSITION_READ",
            recent_trade_count=activity["trade_count"],
            blocking_reasons=reasons,
        )
        if self.publisher is not None:
            self.publisher.publish(review, prices, book)
        if self.runner is not None and not reasons:
            try:
                self.runner.tick(
                    review, prices, book=book if self.settings.strategy_version == "1.2" else None
                )
            except RiskRejected as exc:
                self.state["last_execution_rejection"] = exc.code
                if exc.code in {
                    "RECONCILIATION_REQUIRED",
                    "RECONCILIATION_REQUIRED_OR_PAUSED",
                    "UNRESOLVED_REQUEST",
                }:
                    self.runner.next_reconcile = now()
                    self.state["trading_enabled"] = False
            finally:
                calls = self.runner.exchange.call_count
                self.state["real_submit_attempts"] = calls["POST"]
                self.state["real_cancel_attempts"] = calls["DELETE"]
                if calls["POST"]:
                    self.state["real_orders"] = "SEE_PERSISTED_OMS_AND_RECEIPTS"
                if calls["DELETE"]:
                    self.state["real_cancels"] = "SEE_PERSISTED_OMS_AND_RECEIPTS"
        with self.lock:
            self.state["quotes"][eid] = prices
            self.state["samples"] += 1
            self.state["active_main_read_attempts"] = reads.total_attempts
            self.state["lifecycle"] = (
                "RUNNING_REAL_TRADING" if self.state["trading_enabled"] else "RUNNING_REAL_DATA"
            )
        self.publish()

    def market_worker13(self, reads):
        """One bounded official reader; network work never blocks price evaluation."""
        from sigbot.strategies.mm_sig_v1.live.universe import read_prices

        catalog_at = time.monotonic() + self.settings.catalog_refresh_seconds
        prices_at = time.monotonic() + self.settings.bbo_refresh_seconds
        activity_due: dict[str, float] = {}
        try:
            while not self.stop.is_set():
                try:
                    completed_read = False
                    if reads.total_attempts >= 350 or time.monotonic() + 60 > reads.stop_at:
                        self.read_count += reads.total_attempts
                        reads.close()
                        reads = self.new_reads()
                        if reads.bootstrap() != (self.aid, self.tid):
                            raise SchemaMismatch("MM_RUNTIME_ACCOUNT_SCOPE_CHANGED")
                    with self.lock:
                        markets, reviews = deepcopy(self.markets), deepcopy(self.reviews)
                    if time.monotonic() >= catalog_at:
                        catalog_started = time.monotonic()
                        markets = reads.all_pages(
                            "market_list",
                            query={
                                "tournamentId": self.tid,
                                "status": "any",
                                "limit": 100,
                            },
                        )
                        prices = (
                            self.collector.prices()
                            if self.settings.shared_sig_source and self.collector is not None
                            else read_prices(markets, reads, self.tid)
                        )
                        completed_read = True
                        observed = now().isoformat()
                        nodes = {r["market_id"]: r["nodes"] for r in reviews.values()}
                        # Publish the real catalog receipt immediately, with the
                        # previously reviewed node versions. A large rule sweep
                        # must not block full-market observation or baseline quotes.
                        with self.lock:
                            self.market_updates["catalog"] = {
                                "markets": markets,
                                "prices": prices,
                                "nodes": dict(nodes),
                                "at": observed,
                                "rule_refresh_status": "IN_PROGRESS",
                            }
                        next_catalog_receipt = (
                            catalog_started + self.settings.catalog_refresh_seconds
                        )
                        for market in markets:
                            if self.stop.is_set():
                                break
                            if any(e["id"] in reviews for e in market["exchanges"]):
                                if (
                                    reads.total_attempts >= 350
                                    or time.monotonic() + 60 > reads.stop_at
                                ):
                                    self.read_count += reads.total_attempts
                                    reads.close()
                                    reads = self.new_reads()
                                    if reads.bootstrap() != (self.aid, self.tid):
                                        raise SchemaMismatch("MM_RUNTIME_ACCOUNT_SCOPE_CHANGED")
                                nodes[market["id"]] = reads.market_nodes(market["id"])
                            if time.monotonic() >= next_catalog_receipt and not self.stop.is_set():
                                refreshed = reads.all_pages(
                                    "market_list",
                                    query={"tournamentId": self.tid, "status": "any", "limit": 100},
                                )
                                with self.lock:
                                    self.market_updates["catalog"] = {
                                        "markets": refreshed,
                                        "prices": prices,
                                        "nodes": dict(nodes),
                                        "at": now().isoformat(),
                                        "rule_refresh_status": "IN_PROGRESS",
                                    }
                                next_catalog_receipt = (
                                    time.monotonic() + self.settings.catalog_refresh_seconds
                                )
                        if self.stop.is_set():
                            break
                        # The large rule sweep must not leave the catalog timestamp
                        # at its old starting receipt while baseline quoting continues.
                        markets = reads.all_pages(
                            "market_list",
                            query={"tournamentId": self.tid, "status": "any", "limit": 100},
                        )
                        observed = now().isoformat()
                        if not self.stop.is_set():
                            with self.lock:
                                self.market_updates["catalog"] = {
                                    "markets": markets,
                                    "prices": prices,
                                    "nodes": nodes,
                                    "at": observed,
                                    "rule_refresh_status": "COMPLETE_FOR_REVIEWED_SCOPE",
                                }
                        catalog_at = max(
                            time.monotonic(),
                            catalog_started + self.settings.catalog_refresh_seconds,
                        )
                        prices_at = time.monotonic() + self.settings.bbo_refresh_seconds
                    elif time.monotonic() >= prices_at:
                        prices_started = time.monotonic()
                        prices = (
                            self.collector.prices()
                            if self.settings.shared_sig_source and self.collector is not None
                            else read_prices(markets, reads, self.tid)
                        )
                        completed_read = True
                        with self.lock:
                            self.market_updates["prices"] = (prices, now().isoformat())
                        prices_at = prices_started + (
                            2
                            if self.settings.shared_sig_source
                            else self.settings.bbo_refresh_seconds
                        )
                    due = sorted(
                        (eid for eid in reviews if time.monotonic() >= activity_due.get(eid, 0)),
                        key=lambda eid: (activity_due.get(eid, 0), eid),
                    )
                    # Read one activity window then reconsider overdue BBO;
                    # three sequential tapes used to postpone all-market prices.
                    if due and not self.stop.is_set() and not self.settings.shared_sig_source:
                        eid = due[0]
                        review = reviews[eid]
                        until = now().replace(microsecond=0)
                        result = tape(
                            reads,
                            review["market_id"],
                            eid,
                            self.tid,
                            until - timedelta(minutes=10),
                            until,
                            max_pages=3,
                        )
                        completed_read = True
                        with self.lock:
                            self.activity[eid] = {
                                "at": until.isoformat(),
                                "complete": result["pagination_complete"],
                                "trade_count": len(result["trades"]),
                            }
                        activity_due[eid] = (
                            time.monotonic() + self.settings.activity_refresh_seconds - 5
                        )
                    with self.lock:
                        self.state["active_main_read_attempts"] = reads.total_attempts
                        if completed_read:
                            self.state.pop("market_read_error", None)
                            self.state.pop("market_read_error_blocks_quotes", None)
                except SigbotError as exc:
                    with self.lock:
                        self.state["market_read_error"] = exc.code
                        self.state["market_read_error_blocks_quotes"] = isinstance(
                            exc, SchemaMismatch
                        )
                    if isinstance(exc, (AuthorizationRequired, ContractNotVerified)):
                        self.fatal = exc.code
                        self.stop.set()
                    self.stop.wait(10)
                except (OSError, ValueError, KeyError):
                    self.fatal = "MM_MARKET_WORKER_STORAGE_OR_PAYLOAD_INVALID"
                    self.stop.set()
                self.stop.wait(0.5)
        finally:
            self.read_count += reads.total_attempts
            reads.close()
            with self.lock:
                self.state["active_main_read_attempts"] = 0

    def apply_market_updates13(self):
        with self.lock:
            updates, self.market_updates = self.market_updates, {}
        if "catalog" in updates:
            self.discover(None, snapshot=updates["catalog"])
            stage_reviews(
                self.live_store,
                self.aid,
                self.tid,
                self.reviews,
                self.markets,
                self.account_result,
                self.account_data,
                update_trading_metadata=bool(self.publisher),
            )
        if "prices" in updates:
            first_prices = not self.bbo and bool(updates["prices"][0])
            self.bbo, observed = updates["prices"]
            self.state.update(quote_coverage=len(self.bbo), bbo_at=observed)
            if first_prices and not self.selected:
                from sigbot.strategies.mm_sig_v1.live.universe import select_pool

                select_pool(self)

    def recovered_book13(self, eid, *, wait_for_recovery=True):
        assert self.collector is not None
        cached, _, collector_status = self.collector.snapshot()
        if eid not in cached and self.settings.shared_sig_source and wait_for_recovery:
            # Recover a short delivery/expiry gap before deciding to cancel.
            # No old book is promoted and no new order is produced while waiting.
            deadline = time.monotonic() + self.settings.book_ttl_seconds
            while eid not in cached and time.monotonic() < deadline:
                if self.stop.wait(0.2):
                    break
                cached, _, collector_status = self.collector.snapshot()
        return cached.get(eid), collector_status

    def normalized_owned_book13(self, eid, raw, own):
        assert self.collector is not None
        deadline = time.monotonic() + self.settings.book_ttl_seconds
        while True:
            try:
                return normalized_book(raw, own, utc_time(raw["received_at"]))
            except SchemaMismatch as exc:
                if not self.settings.shared_sig_source or exc.code not in {
                    "MM_OWN_ORDER_NEWER_THAN_BOOK",
                    "MM_OWN_ORDER_BOOK_MISMATCH",
                    "MM_OWN_ORDERS_OUTSIDE_VISIBLE_DEPTH",
                }:
                    raise
                # The POST receipt can arrive before its full book broadcast.
                # Wait for actual matching depth, never subtract from an older book.
                while time.monotonic() < deadline:
                    if self.stop.wait(0.2):
                        raise exc
                    cached, _, _ = self.collector.snapshot()
                    candidate = cached.get(eid)
                    if (
                        candidate is not None
                        and candidate.get("validation_received_at")
                        and (
                            0
                            <= (
                                now() - utc_time(candidate["validation_received_at"])
                            ).total_seconds()
                            <= self.settings.tracked_book_refresh_seconds
                            + self.settings.book_ttl_seconds
                        )
                    ):
                        raw = candidate
                        break
                else:
                    raise exc

    def sample13(self, reads, eid, outcome="YES"):
        from sigbot.strategies.mm_sig_v1.live.analytics import policy_inputs, record
        from sigbot.strategies.mm_sig_v1.live.standard_policy import parameters_ready
        from sigbot.strategies.mm_sig_v1.models import Book

        assert (
            self.collector is not None
            and self.account_result is not None
            and self.account_data is not None
        )
        timestamp, review = now(), dict(self.reviews[eid])
        key = eid if outcome == "YES" else eid + ":NO"
        if outcome == "NO":
            review["instrument_id"] = review["instrument_id"].removesuffix("-YES") + "-NO"
        activity = self.activity.get(eid)
        activity_status = (
            "UNKNOWN"
            if activity is None
            or not activity["complete"]
            or not 0
            <= (timestamp - utc_time(activity["at"])).total_seconds()
            <= self.settings.activity_refresh_seconds + self.settings.book_ttl_seconds
            else "ACTIVE"
            if activity["trade_count"]
            else "QUIET"
        )
        timestamp = now()
        reasons = []
        raw, collector_status = self.recovered_book13(
            eid, wait_for_recovery=eid in getattr(self, "book_recovery_priority", {eid})
        )
        timestamp = now()
        flow = collector_status.get("public_flow", {}).get(eid)
        flow_current = bool(
            flow
            and flow.get("market_id") == review["market_id"]
            and flow.get("exchange_id") == eid
            and flow.get("status") == "VERIFIED_PUBLIC_WINDOW"
            and 0
            <= (timestamp - utc_time(flow["to"])).total_seconds()
            <= self.settings.activity_refresh_seconds
            + self.settings.bbo_refresh_seconds
            + self.settings.book_ttl_seconds
        )
        if flow_current:
            activity_status = "ACTIVE" if flow["trade_count"] else "QUIET"
            self.activity[eid] = {
                "at": flow["to"],
                "complete": True,
                "trade_count": flow["trade_count"],
                "public_flow": flow,
            }
        self.state["sig_realtime"] = collector_status
        if (
            raw is None
            or raw.get("validation_received_at") is None
            or (timestamp - utc_time(raw["validation_received_at"])).total_seconds()
            > self.settings.tracked_book_refresh_seconds + self.settings.book_ttl_seconds
        ):
            with self.live_store.transaction() as session:
                inst = session.get(Instrument, review["instrument_id"], with_for_update=True)
                inst.mapping_evidence = {
                    **inst.mapping_evidence,
                    "mm13_lifecycle": {
                        **inst.mapping_evidence.get("mm13_lifecycle", {}),
                        "consistent_checks": 0,
                        "last_healthy_at": None,
                    },
                }
            if self.publisher:
                self.publisher.suspend(
                    review["instrument_id"], "MM13_BOOK_CONTINUITY_OR_REST_CHECK_UNKNOWN"
                )
            self.state["quotes"][key] = {
                "mode": "DATA_UNCERTAIN",
                "blocking_reasons": ["MM13_BOOK_CONTINUITY_OR_REST_CHECK_UNKNOWN"],
                "validated_at": timestamp.isoformat(),
            }
            return
        if (
            self.account_result["status"] != "PASS"
            or (timestamp - utc_time(self.account_at)).total_seconds()
            > self.settings.account_ttl_seconds
            or self.state.get("account_last_error")
        ):
            reasons.append("MM_ACCOUNT_CHECKPOINT_NOT_CURRENT")
        if self.fee_reason:
            reasons.append(self.fee_reason)
        if self.state.get("market_read_error_blocks_quotes"):
            reasons.append("MM13_BACKGROUND_SCOPE_OR_SCHEMA_INVALID")
        if self.state.get("reference_sampler_error"):
            reasons.append("MM_REFERENCE_SAMPLER_UNAVAILABLE")
        if eid in self.state.get("admission_rejected", {}):
            reasons.append(self.state["admission_rejected"][eid])
        own = {str(o["id"]): deepcopy(o) for o in self.account_data["orders"] if o["open"]}
        unknown_local_orders = False
        with self.live_store.transaction() as session:
            from sigbot.uncertainty import scope_blockers, scopes

            account = session.get(Account, self.aid)
            target = session.get(Instrument, review["instrument_id"])
            scope_reasons = scope_blockers(scopes(session, account), target)
            reasons.extend(scope_reasons)
            unknown_local_orders = bool(scope_reasons)
            tracked_remote = {
                str(o.remote_id)
                for o in session.scalars(select(Order).where(Order.account_id == self.aid))
                if o.remote_id
            }
            if any(str(order["id"]) not in tracked_remote for order in own.values()):
                reasons.append("MM_EXTERNAL_OR_UNKNOWN_OPEN_ORDERS")
            for order in session.scalars(select(Order).where(Order.account_id == self.aid)):
                if not order.remote_id:
                    continue
                if order.state in TERMINAL and order.request_state == "RESOLVED":
                    own.pop(str(order.remote_id), None)
                    continue
                inst = session.get(Instrument, order.instrument_id)
                row = session.get(IntentRow, order.intent_id)
                if order.request_state == "ACKNOWLEDGED" and inst:
                    own[str(order.remote_id)] = {
                        "id": order.remote_id,
                        "exchangeId": inst.exchange_id,
                        "side": inst.outcome.lower(),
                        "action": order.side.lower(),
                        "priceLimit": order.price,
                        "quantity": order.quantity - order.filled,
                        "open": True,
                        "expirationDate": None,
                        "createdAt": row.payload["created_at"],
                    }
        try:
            from sigbot.strategies.mm_sig_v1.data import outcome_book

            book = outcome_book(self.normalized_owned_book13(eid, raw, list(own.values())), outcome)
        except SchemaMismatch:
            if self.publisher:
                self.publisher.suspend(review["instrument_id"], "MM13_BOOK_RECONCILE_REQUIRED")
            raise
        timestamp = now()
        if (
            self.account_result["status"] != "PASS"
            or self.state.get("account_last_error")
            or (timestamp - utc_time(self.account_at)).total_seconds()
            > self.settings.account_ttl_seconds
        ) and "MM_ACCOUNT_CHECKPOINT_NOT_CURRENT" not in reasons:
            reasons.append("MM_ACCOUNT_CHECKPOINT_NOT_CURRENT")
        book = Book.model_validate(
            {**book.model_dump(), "validated_at": timestamp, "continuity_verified": True}
        )
        with self.live_store.transaction() as session:
            account = (self.live_store.lock_account(session,self.aid) if self.settings.risk_policy is not None
                       else session.get(Account, self.aid))
            inst = session.get(Instrument, review["instrument_id"], with_for_update=True)
            inst.book = book.model_dump(mode="json", exclude={"received_at", "quality"})
            inst.book_at, inst.quality = book.received_at, book.quality
            inst.fee_per_share = self.fee
            inputs, stats = policy_inputs(
                session,
                account,
                inst,
                book,
                self.settings,
                timestamp,
                utc_time(self.account_data["cup"]["endDate"]),
                exploration=eid in self.exploration,
            )
            inputs = inputs.model_copy(
                update={
                    "new_risk_allowed": inputs.new_risk_allowed
                    and eid in self.selected
                    and not reasons
                    and (
                        activity_status != "UNKNOWN"
                        or inputs.calibration_authorized
                        or self.settings.pricing_model == "GUEANT_FINITE_GRID"
                        and parameters_ready(inputs.standard_parameters or {}, inputs.inventory)
                    )
                }
            )
            quote = actual_prices(
                book, inputs.inventory, inst.tick, self.settings, policy_inputs=inputs
            )
            if self.settings.risk_policy is not None:
                from sigbot.mm_risk import overlay_quote

                quote=overlay_quote(session,account,inst,self.settings,inputs,self.settings.risk_policy,
                    timestamp,utc_time(self.account_data["cup"]["endDate"]),quote)
            life = dict(inst.mapping_evidence.get("mm13_lifecycle", {}))
            mid = (
                (Decimal(quote["external_bid"]) + Decimal(quote["external_ask"])) / 2
                if quote.get("reference_usable", True)
                else None
            )
            if (
                mid is not None
                and life.get("mid") is not None
                and life.get("at")
                and 0
                < (timestamp - utc_time(life["at"])).total_seconds()
                <= self.settings.jump_window_seconds
                and abs(mid - Decimal(life["mid"])) >= inst.tick * self.settings.jump_ticks
            ):
                life["cooldown_until"] = (
                    timestamp + timedelta(seconds=self.settings.jump_cooldown_seconds)
                ).isoformat()
                life["consistent_checks"] = 0
            if reasons:
                life["consistent_checks"] = 0
            else:
                life["consistent_checks"] = min(2, life.get("consistent_checks", 0) + 1)
            from sigbot.strategies.mm_sig_v1.live.execution import safety_checkpoint_current

            if life["consistent_checks"] < 2 and not safety_checkpoint_current(
                inst, account, self.settings, timestamp
            ):
                reasons.append("MM13_TWO_CONSISTENT_CHECKS_REQUIRED")
            if life.get("cooldown_until") and timestamp < utc_time(life["cooldown_until"]):
                reasons.append("MM13_JUMP_COOLDOWN")
            life.update(
                at=timestamp.isoformat(),
                mid=str(mid) if mid is not None else None,
                sequence=book.sequence,
            )
            life["last_healthy_at"] = timestamp.isoformat() if not reasons else None
            inst.mapping_evidence = {**inst.mapping_evidence, "mm13_lifecycle": life}
        quote.update(
            title=review["title"],
            event_key=review["event_key"],
            exchange_id=eid,
            outcome=outcome,
            rules_hash=review["rules_hash"],
            mapping_hash=review["mapping_hash"],
            tick=review["tick"],
            validated_at=timestamp.isoformat(),
            blocking_reasons=reasons,
            account_checkpoint=self.account_result["checkpoint"],
            rules_verified=not self.state.get("admission_rejected", {}).get(eid),
            actual_inventory_source="PERSISTED_RECONCILED_MM_LOTS",
            effective_quote_seconds={k: str(v) for k, v in stats["quote_seconds"].items()},
            fill_quality_samples=stats["markouts"],
            synthetic_data_used=False,
            activity_status=activity_status,
            public_market_flow={**flow, "current": flow_current}
            if flow
            else {"status": "NOT_OBSERVED", "current": False},
            reference_observation_count=stats["reference_observation_count"],
            reference_gap_count=stats["reference_gap_count"],
            continuous_hour_verified=stats["continuous_hour_verified"],
        )
        with self.live_store.transaction() as session:
            inst = session.get(Instrument, review["instrument_id"], with_for_update=True)
            inst.mapping_evidence = {
                **inst.mapping_evidence,
                "mm131_latest": {
                    "at": timestamp.isoformat(),
                    "quote": {k: v for k, v in quote.items() if k != "fill_quality_samples"},
                    "settings": self.settings.model_dump(mode="json"),
                },
            }
            self.live_store.audit(
                session,
                self.aid,
                "MM131_SIDE_DECISIONS",
                {
                    "instrument_id": inst.id,
                    "buy": quote.get("buy_directive"),
                    "sell": quote.get("sell_directive"),
                    "buy_reason": quote.get("buy_directive_reason"),
                    "sell_reason": quote.get("sell_directive_reason"),
                },
                timestamp,
            )
        record(
            self.live_store,
            self.aid,
            review["instrument_id"],
            book,
            quote,
            timestamp,
            price_reference_usable=(
                book.quality == "HEALTHY"
                and book.own_orders_known
                and not unknown_local_orders
                and "MM_EXTERNAL_OR_UNKNOWN_OPEN_ORDERS" not in reasons
                and eid not in self.state.get("admission_rejected", {})
            ),
            reference_usable=(
                "MM_EXTERNAL_OR_UNKNOWN_OPEN_ORDERS" not in reasons
                and eid not in self.state.get("admission_rejected", {})
                and 0
                <= (timestamp - utc_time(self.account_at)).total_seconds()
                <= self.settings.account_ttl_seconds
            ),
            quote_usable=not reasons,
            activity=activity_status,
        )
        if self.publisher:
            self.publisher.publish(review, quote, book)
        self.state["quotes"][key] = quote
        if not reasons:
            self.valid_books[review["instrument_id"]] = book
        self.state["samples"] += 1
        self.state["lifecycle"] = "RUNNING_REAL_DATA"
        self.publish()

    def sample_tracked13(self, reads):
        self.state["implementation_revision"] = (
            "1.5.0"
            if self.settings.pricing_model == "GUEANT_FINITE_GRID"
            else "1.3.2"
            if self.settings.participation_model == "CDJ_BOOK_IMBALANCE"
            else "1.3.1"
        )
        tracked = self.tracking_exchanges
        offset = self.state.get("evaluation_cursor", 0) % len(tracked) if tracked else 0
        rotated = tracked[offset:] + tracked[:offset]
        with self.live_store.transaction() as session:
            working = set(
                session.scalars(
                    select(Instrument.exchange_id)
                    .join(Order, Order.instrument_id == Instrument.id)
                    .join(IntentRow, IntentRow.id == Order.intent_id)
                    .where(
                        Order.account_id == self.aid,
                        IntentRow.payload["strategy_id"].as_string() == "mm_sig",
                        (Order.reserve_cash > ZERO) | (Order.reserve_quantity > ZERO),
                    )
                )
            )
        priority = working | self.managed
        self.book_recovery_priority = working
        collector = getattr(self, "collector", None)
        available = set(collector.snapshot()[0]) if collector is not None else set(tracked)
        ready_priority = [eid for eid in rotated if eid in priority and eid in available]
        ready_other = [eid for eid in rotated if eid not in priority and eid in available]
        # One slow managed market must not consume every turn. Interleave ready
        # markets and service at least one ordinary candidate before yielding.
        from itertools import zip_longest

        ordered = [
            eid
            for pair in zip_longest(ready_priority, ready_other)
            for eid in pair
            if eid is not None
        ] + [eid for eid in rotated if eid not in available]
        other_evaluated = False
        deadline = time.monotonic() + self.settings.monitor_interval_seconds
        for eid in ordered:
            if eid not in self.reviews:
                continue
            for outcome in (
                ("YES", "NO") if self.settings.complementary_quotes_enabled else ("YES",)
            ):
                key = eid if outcome == "YES" else eid + ":NO"
                iid = self.reviews[eid]["instrument_id"]
                if outcome == "NO":
                    iid = iid.removesuffix("-YES") + "-NO"
                try:
                    if outcome == "YES":
                        self.sample(reads, eid)
                    else:
                        self.sample13(reads, eid, outcome)
                except (RateLimited, SchemaMismatch, TransportUnknown) as exc:
                    if self.publisher:
                        self.publisher.suspend(iid, exc.code)
                    self.state["quotes"][key] = {
                        "mode": "DATA_UNCERTAIN",
                        "blocking_reasons": [exc.code],
                        "validated_at": now().isoformat(),
                    }
            quote = self.state["quotes"].get(eid, {})
            evaluations = self.state.setdefault("authorized_event_evaluations", {})
            previous = evaluations.get(eid, {})
            evaluations[eid] = {
                "status": "DATA_PENDING"
                if quote.get("blocking_reasons")
                else quote.get("mode", "UNKNOWN"),
                "reason": quote.get("blocking_reasons") or quote.get("buy_directive_reason"),
                "at": now().isoformat(),
                "evaluations": previous.get("evaluations", 0) + 1,
            }
            self.state["evaluation_cursor"] = tracked.index(eid) + 1
            other_evaluated |= eid in ready_other
            if time.monotonic() >= deadline and (not ready_other or other_evaluated):
                break  # Continue the fair sweep next turn; there is no market-count cap.

    def monitor_universe(self):
        if self.market_monitor is None or self.collector is None:
            return
        timestamp = now()
        if timestamp < self.market_monitor.next_at:
            return
        books, feed = self.collector.monitoring_snapshot()
        current_account = bool(
            self.account_result
            and self.account_result["status"] == "PASS"
            and self.account_at
            and 0
            <= (timestamp - utc_time(self.account_at)).total_seconds()
            <= self.settings.account_ttl_seconds
        )
        report = self.market_monitor.update(
            markets=self.markets,
            books=books,
            feed=feed,
            reviews=self.reviews,
            quotes=self.state.get("quotes", {}),
            own_orders=self.account_data["orders"] if self.account_data else [],
            fee=self.fee,
            now=timestamp,
            catalog_at=self.state.get("catalog_at"),
            account_current=current_account,
            blocked_exchanges={
                item["exchange_id"] for item in getattr(self, "isolated_unknown_orders", [])
            },
            bulk_prices=self.bbo,
            bulk_received_at=self.state.get("bbo_at"),
        )
        path = self.instance_root / "all-market-monitor.json"
        save(path, report, private=True)
        with self.lock:
            self.state["all_market_monitor"] = {
                key: value
                for key, value in report.items()
                if key not in {"markets", "spread_candidates"}
            }
            self.state["all_market_monitor"]["status_file"] = str(path)
            self.state["all_market_monitor"]["spread_candidate_count"] = len(
                report["spread_candidates"]
            )

    def ensure_reference_worker13(self, worker):
        if worker is not None and worker.is_alive():
            return worker
        if worker is not None:
            with self.lock:
                self.state["reference_sampler_error"] = "MM_REFERENCE_WORKER_STOPPED"
                self.state["reference_sampler_restarts"] = (
                    self.state.get("reference_sampler_restarts", 0) + 1
                )
        worker = threading.Thread(target=self.reference_worker13, name="mm-real-book-references")
        worker.start()
        return worker

    def reference_worker13(self):
        from sqlalchemy.exc import SQLAlchemyError

        from sigbot.strategies.mm_sig_v1.live.references import sample

        while not self.stop.is_set():
            started = time.monotonic()
            try:
                with self.lock:
                    self.state["reference_sampler_started_at"] = now().isoformat()
                result = sample(self, now())
                result["processing_seconds"] = time.monotonic() - started
                result["completed_at"] = now().isoformat()
                with self.lock:
                    self.state["reference_sampler"] = result
                    self.state.pop("reference_sampler_error", None)
                    self.state.pop("reference_sampler_exception_type", None)
            except SigbotError as exc:
                with self.lock:
                    self.state["reference_sampler_error"] = exc.code
            except (OSError, RuntimeError) as exc:
                # Snapshot/file failures previously killed only this thread.
                # Failed rounds remain unusable; never retimestamp old success.
                with self.lock:
                    self.state["reference_sampler_error"] = "MM_REFERENCE_SNAPSHOT_UNAVAILABLE"
                    self.state["reference_sampler_exception_type"] = type(exc).__name__
            except (SQLAlchemyError, KeyError, ValueError, TypeError, AttributeError):
                with self.lock:
                    self.state["reference_sampler_error"] = "MM_REFERENCE_WORKER_FAILED"
                    self.fatal = "MM_REFERENCE_WORKER_FAILED"
                self.stop.set()
                return
            self.stop.wait(max(0.1, 1 - (time.monotonic() - started)))

    def monitor_worker13(self):
        """Full-catalog observation cadence is independent of quote/model evaluation."""
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                self.monitor_universe()
                with self.lock:
                    self.state.pop("monitor_error", None)
            except SigbotError as exc:
                with self.lock:
                    self.state["monitor_error"] = exc.code
                return
            except (OSError, KeyError, ValueError, TypeError):
                with self.lock:
                    self.state["monitor_error"] = "MM_MONITOR_WORKER_FAILED"
                return
            self.stop.wait(
                max(0.1, self.settings.monitor_interval_seconds - (time.monotonic() - started))
            )

    def new_reads(self):
        # Public documentation is observed by a separate worker. Its timeout
        # must not delay account refresh, market reads, or order maintenance.
        reads = RealReads(env_file=self.env_file, verify_reference=False)
        self.state.setdefault(
            "reference_observation", {"policy": "REPORT_ONLY", "status": "OBSERVATION_PENDING"}
        )
        self.state.pop("contract_review_required", None)
        return reads

    def document_worker(self, schema):
        from sigbot.strategies.mm_sig_v1.data import current_reference

        while not self.stop.is_set():
            try:
                report = current_reference(REPORTS / "reference", schema)
            except OSError:
                report = {"policy": "REPORT_ONLY", "status": "EVIDENCE_WRITE_FAILED"}
            with self.lock:
                self.state["reference_observation"] = report
                self.state.pop("contract_review_required", None)
            self.stop.wait(60)

    def run(self, duration_seconds=0):
        self.instance_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        lease = (self.instance_root / "runtime.lock").open("a+")
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        reads, worker, market_worker, reference_worker, monitor_worker, document_worker = (
            None,
            None,
            None,
            None,
            None,
            None,
        )
        until = time.monotonic() + duration_seconds if duration_seconds else float("inf")
        try:
            self.publish()
            reads = self.new_reads()
            document_worker = threading.Thread(
                target=self.document_worker,
                args=(reads.reader.schema,),
                name="mm-document-observer",
                daemon=True,
            )
            document_worker.start()
            self.aid, self.tid = reads.bootstrap()
            if self.settings.strategy_version == "1.3":
                self.account_data = {
                    "cup": reads.call("tournament_identity", path_params={"slug": reads.slug})
                }
                shared = (
                    self.wait_shared_account(until)
                    if self.settings.execution_route == "SHARED"
                    and self.settings.application_config
                    else self.shared_account()
                )
                if shared is not None:
                    self.apply_account(shared[0], shared[1], self.aid, shared[2])
                    self.state["account_checkpoint_source"] = "SHARED_VERIFIED_REAL_SNAPSHOT"
                else:
                    self.update_account(reads)
            else:
                self.update_account(reads)
            assert self.account_result is not None
            if self.account_result["status"] == "INDETERMINATE":
                raise TransportUnknown("MM_ACCOUNT_CHECKPOINT_WINDOW_CHANGED")
            if self.account_result["status"] != "PASS":
                raise SchemaMismatch("MM_ACCOUNT_RECONCILIATION_FAILED")
            self.discover(reads)
            self.state["database_opening"] = stage_reviews(
                self.live_store,
                self.aid,
                self.tid,
                self.reviews,
                self.markets,
                self.account_result,
                self.account_data,
                update_trading_metadata=bool(
                    self.settings.application_config and self.settings.approval_file
                ),
            )
            if self.settings.execution_route == "SHARED" and self.settings.application_config:
                from sigbot.strategies.mm_sig_v1.live.publisher import Publisher

                self.publisher = Publisher(self)
                self.state["execution_role"] = "PRODUCER_ONLY"
            elif self.settings.execution_route == "LOCAL" and (
                self.settings.application_config
                and self.settings.approval_file
                and self.settings.fee_file
            ):
                from sigbot.strategies.mm_sig_v1.live.execution import LiveRunner

                self.runner = LiveRunner(self)
                self.state["trading_enabled"] = self.runner.admission_status() == "AUTHORIZED"
            if self.settings.strategy_version == "1.3":
                from sigbot.readcheck import load_key
                from sigbot.strategies.ecr_sig_v1.depth import DepthCollector
                from sigbot.strategies.ecr_sig_v1.observer import ContinuousReads

                collector_rows = [
                    {"api_market_id": m["id"], "exchange_ids": [e["id"] for e in m["exchanges"]]}
                    for m in self.markets
                ]
                if self.settings.shared_sig_source:
                    from sigbot.shared_marketdata import SharedSigCollector

                    self.collector = SharedSigCollector(
                        self.settings.shared_sig_source, self.tid, self.aid, collector_rows
                    )
                else:
                    self.collector = DepthCollector(
                        self.tid,
                        collector_rows,
                        lambda: ContinuousReads(
                            load_key(self.env_file), "contracts/api-contract.working.json"
                        ),
                        None,
                        self.instance_root / "sig-feed" / reads.run_id,
                        expected_account_id=self.aid,
                        sig_only=True,
                        rest_audit_seconds=self.settings.tracked_book_refresh_seconds,
                        monitor_all=self.settings.monitor_all_markets,
                        universe_audit_seconds=self.settings.monitor_rest_audit_seconds,
                    )
                self.collector.set_targets(
                    [],
                    market_ids=[
                        self.reviews[e]["market_id"]
                        for e in self.tracking_exchanges
                        if e in self.reviews
                    ],
                )
                self.collector.start()
                if self.settings.monitor_all_markets:
                    from sigbot.strategies.mm_sig_v1.live.monitoring import MarketMonitor

                    self.market_monitor = MarketMonitor(
                        self.instance_root / "monitoring", self.settings, now()
                    )
            self.publish()
            # The executor's independent account snapshot already reads complete
            # history. A second polling worker would consume the same request
            # budget and delay order reconciliation while trading.
            if self.runner is None:
                worker = threading.Thread(target=self.account_worker, name="mm-real-account")
                worker.start()
            next_catalog = time.monotonic() + self.settings.catalog_refresh_seconds
            next_selection = time.monotonic() + self.settings.selection_refresh_seconds
            next_metrics = time.monotonic()
            if self.settings.strategy_version == "1.3":
                market_worker = threading.Thread(
                    target=self.market_worker13, args=(reads,), name="mm-real-market-reads"
                )
                market_worker.start()
                reads = None  # The worker exclusively owns this HTTP reader.
                reference_worker = self.ensure_reference_worker13(None)
                if self.market_monitor is not None:
                    monitor_worker = threading.Thread(
                        target=self.monitor_worker13, name="mm-all-market-monitor"
                    )
                    monitor_worker.start()
            index = 0
            while not self.stop.is_set() and time.monotonic() < until:
                if self.settings.strategy_version == "1.3":
                    reference_worker = self.ensure_reference_worker13(reference_worker)
                if reads is not None and (
                    reads.total_attempts >= 350 or time.monotonic() + 60 > reads.stop_at
                ):
                    self.read_count += reads.total_attempts
                    reads.close()
                    reads = self.new_reads()
                    if reads.bootstrap() != (self.aid, self.tid):
                        raise SchemaMismatch("MM_RUNTIME_ACCOUNT_SCOPE_CHANGED")
                try:
                    if self.runner is not None:
                        self.runner.maintain()
                    if self.publisher is not None and self.settings.strategy_version != "1.3":
                        self.publisher.maintain()
                    if self.settings.strategy_version != "1.3" and time.monotonic() >= next_catalog:
                        self.discover(reads)
                        if self.settings.strategy_version == "1.3":
                            stage_reviews(
                                self.live_store,
                                self.aid,
                                self.tid,
                                self.reviews,
                                self.markets,
                                self.account_result,
                                self.account_data,
                                update_trading_metadata=bool(self.publisher),
                            )
                        next_catalog = time.monotonic() + self.settings.catalog_refresh_seconds
                    if self.settings.strategy_version == "1.3":
                        from sigbot.strategies.mm_sig_v1.live.universe import select_pool

                        assert self.collector is not None
                        self.apply_market_updates13()
                        if time.monotonic() >= next_selection:
                            select_pool(self)
                            self.collector.set_targets(
                                [],
                                market_ids=[
                                    self.reviews[e]["market_id"]
                                    for e in self.tracking_exchanges
                                    if e in self.reviews
                                ],
                            )
                            next_selection = (
                                time.monotonic() + self.settings.selection_refresh_seconds
                            )
                        self.sample_tracked13(reads)
                        if self.publisher is not None:
                            self.publisher.maintain()
                        if time.monotonic() >= next_metrics:
                            from sigbot.strategies.mm_sig_v1.live.reporting import summarize

                            metrics = summarize(
                                self.live_store, self.aid, now(), self.valid_books,
                                principal=self.settings.initial_capital_limit,
                            )
                            save(self.instance_root / "metrics.json", metrics, private=True)
                            self.state["metrics_path"] = str(self.instance_root / "metrics.json")
                            next_metrics = time.monotonic() + 60
                    elif self.selected:
                        self.sample(reads, self.selected[index % len(self.selected)])
                        index += 1
                    else:
                        self.state.update(
                            lifecycle="WAITING_ELIGIBLE_MARKETS", trading_enabled=False
                        )
                        self.publish()
                    with self.lock:
                        self.state.pop("last_read_error", None)
                except (RateLimited, SchemaMismatch) as exc:
                    if self.publisher and self.settings.strategy_version == "1.3":
                        for eid in self.tracking_exchanges:
                            if eid in self.reviews:
                                self.publisher.suspend(self.reviews[eid]["instrument_id"], exc.code)
                    with self.lock:
                        self.state["lifecycle"] = "DEGRADED_REAL_DATA"
                        self.state["last_read_error"] = exc.code
                    self.publish()
                    self.stop.wait(10)
                except TransportUnknown as exc:
                    if self.runner is None:
                        if self.publisher and self.settings.strategy_version == "1.3":
                            for eid in self.tracking_exchanges:
                                if eid in self.reviews:
                                    self.publisher.suspend(
                                        self.reviews[eid]["instrument_id"], exc.code
                                    )
                        else:
                            raise
                        self.state.update(
                            lifecycle="DEGRADED_REAL_DATA",
                            trading_enabled=False,
                            last_read_error=exc.code,
                        )
                        self.publish()
                        self.stop.wait(10)
                        continue
                    self.runner.executor.controls.pause(self.aid, "MM_TRANSIENT_READ_UNAVAILABLE")
                    self.runner.next_reconcile = now()
                    self.state.update(
                        lifecycle="DEGRADED_REAL_DATA",
                        trading_enabled=False,
                        last_read_error=exc.code,
                    )
                    self.publish()
                    self.stop.wait(10)
                self.stop.wait(
                    0.5
                    if self.settings.strategy_version == "1.3"
                    else 5
                    if self.settings.strategy_version == "1.2"
                    else 0.25
                )
        finally:
            self.stop.set()
            if document_worker is not None:
                document_worker.join(timeout=1)
            if monitor_worker is not None:
                monitor_worker.join(timeout=40)
                if monitor_worker.is_alive():
                    self.state["monitor_error"] = "MM_MONITOR_WORKER_NOT_STOPPED"
            if reference_worker is not None:
                reference_worker.join(timeout=40)
                if reference_worker.is_alive():
                    self.fatal = "MM_REFERENCE_WORKER_NOT_STOPPED"
            if market_worker is not None:
                market_worker.join(timeout=40)
                if market_worker.is_alive():
                    self.fatal = "MM_MARKET_WORKER_NOT_STOPPED"
            if worker is not None:
                worker.join(timeout=40)
                if worker.is_alive():
                    self.fatal = "MM_ACCOUNT_WORKER_NOT_STOPPED"
            if reads is not None:
                self.read_count += reads.total_attempts
                self.state["active_main_read_attempts"] = 0
                reads.close()
            if self.runner is not None:
                self.runner.close()
            if self.collector is not None:
                self.collector.close()
            self.state["lifecycle"] = "FAILED" if self.fatal else "STOPPED"
            self.publish()
            self.store.engine.dispose()
            self.live_store.engine.dispose()
            lease.close()
        if self.fatal:
            raise SchemaMismatch(self.fatal)
        return self.state


def main():
    parser = argparse.ArgumentParser(
        description="Real SIG data only, explicit persistent monitoring"
    )
    parser.add_argument("--authorize-read", action="store_true", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--settings", default="configs/strategies/mm_sig_v1.real.json")
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--instance-root")
    parser.add_argument("--status-file")
    args = parser.parse_args()
    runtime = Runtime(
        load_settings(args.settings),
        args.env_file,
        instance_root=args.instance_root,
        status_file=args.status_file,
    )
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: runtime.stop.set())
    try:
        runtime.run(args.duration_seconds)
    except Exception as exc:
        # Fatal application boundary: record FAILED and exit; never continue with fake data.
        runtime.fatal = (
            exc.code if isinstance(exc, SigbotError) else "MM_RUNTIME_INPUT_OR_STORAGE_FAILED"
        )
        runtime.state["lifecycle"] = "FAILED"
        frames = []
        tb = exc.__traceback__
        while tb is not None:
            frames.append({"file": Path(tb.tb_frame.f_code.co_filename).name, "line": tb.tb_lineno})
            tb = tb.tb_next
        runtime.state["fatal_exception_type"] = type(exc).__name__
        runtime.state["fatal_locations"] = frames[-3:]
        runtime.publish()
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "code": runtime.fatal,
                    "exception_type": type(exc).__name__,
                    "locations": frames[-3:],
                }
            )
        )
        return (
            6
            if isinstance(exc, (RateLimited, TransportUnknown))
            or runtime.fatal == "MM_CURRENT_OFFICIAL_REFERENCE_UNAVAILABLE"
            else 2
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
