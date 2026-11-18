"""MM 1.1 uses the existing real Exchange, Planner, Executor and account ledger."""

import json
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path

from sqlalchemy import select

from sigbot.authorization import Approval
from sigbot.config import Config
from sigbot.domain import TERMINAL, ZERO, Clock, Intent, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, RateLimited, RiskRejected, SchemaMismatch
from sigbot.execution import Executor
from sigbot.live import SigExecutionExchange, bindings
from sigbot.risk import Planner
from sigbot.storage import (
    Account,
    Audit,
    Instrument,
    IntentRow,
    LiveWire,
    Order,
    Position,
    PositionLot,
    StrategyEvent,
)
from sigbot.strategies.mm_sig_v1.core import external_best
from sigbot.strategies.mm_sig_v1.data import normalized_book, reviewed_schema
from sigbot.strategies.mm_sig_v1.live.inputs import actual_prices
from sigbot.strategies.mm_sig_v1.live.models import Settings, event_cash_budget

RECOVERABLE_PAUSES = {
    "ECR_RUNTIME:LIVE_ORDER_READ_WINDOW_CHANGED",
    "ECR_RUNTIME:READ_SESSION_LIMIT_REACHED",
    "ECR_RUNTIME:SHARED_READ_BUDGET_EXHAUSTED",
    "SHARED_EXECUTOR:LIVE_ORDER_READ_WINDOW_CHANGED",
    "PRE_HTTP_REQUEST_RECOVERED",
    "ABSENCE_NOT_PROOF_OF_NON_ACCEPTANCE",
    "SUBMIT_NOT_RESOLVED",
    "SHARED_EXECUTOR:READ_SESSION_LIMIT_REACHED",
    "SHARED_EXECUTOR:ACCOUNT_OPERATION_BUSY",
    "SUBMIT_UNKNOWN",
    "CANCEL_UNKNOWN",
    "MM_TRANSIENT_READ_UNAVAILABLE",
    "SNAPSHOT_WINDOW_INDETERMINATE",
    "MM_UNRESOLVED_ORDER_RECONCILIATION",
}


def settings_for(intent, *, offline=False):
    if offline and not all(value.startswith("SIM-") for value in [intent.account_id,intent.tournament_id,intent.instrument_id,intent.intent_id]):
        raise AuthorizationRequired("MM_SYNTHETIC_SCOPE_REQUIRED")
    if (
        intent.synthetic and not offline
        or intent.strategy_id != "mm_sig"
        or intent.strategy_version not in {"1.1", "1.2", "1.3"}
    ):
        raise AuthorizationRequired("MM_REAL_ONLY_STRATEGY_SCOPE_REQUIRED")
    data = json.loads(intent.note or "{}")
    if data.get("kind") not in {"MM_REAL_PASSIVE", "MM_REAL_SEED", "INVENTORY_EXIT"}:
        raise RiskRejected("MM_REAL_QUOTE_METADATA_REQUIRED")
    settings = Settings.model_validate(data["settings"])
    if settings.strategy_version != intent.strategy_version:
        raise AuthorizationRequired("MM_SETTINGS_VERSION_MISMATCH")
    if intent.strategy_version == "1.3" and data.get("kind") == "MM_REAL_SEED":
        raise RiskRejected("MM13_PASSIVE_INVENTORY_ONLY")
    return settings


def account_capital(s, account):
    return account.cash + sum(
        (
            p.cost or ZERO
            for p in s.scalars(select(Position).where(Position.account_id == account.id))
        ),
        ZERO,
    )


def safety_checkpoint_current(inst, account, settings, timestamp):
    """The same real safety proof applies to entry, covered sales and exits."""
    reference = inst.mapping_evidence.get("mm13_reference", {})
    life = inst.mapping_evidence.get("mm13_lifecycle", {})
    if any(
        evidence.get("cooldown_until") and timestamp < utc_time(evidence["cooldown_until"])
        for evidence in (reference, life)
    ):
        return False
    if (
        reference.get("at")
        and not reference.get("current")
        and (
            not life.get("last_healthy_at")
            or utc_time(reference["at"]) >= utc_time(life["last_healthy_at"])
        )
    ):
        return False
    independent = bool(
        reference.get("current")
        and reference.get("consistent_checks", 0) >= 2
        and reference.get("binding") == [inst.rules_hash, inst.mapping_hash]
        and 0
        <= (timestamp - utc_time(reference["at"])).total_seconds()
        <= settings.book_ttl_seconds
    )
    lifecycle = bool(
        life.get("consistent_checks", 0) >= 2
        and life.get("last_healthy_at")
        and 0
        <= (timestamp - utc_time(life["last_healthy_at"])).total_seconds()
        <= settings.book_ttl_seconds
    )
    return bool(
        (independent or lifecycle)
        and account is not None
        and account.lifecycle == "HEALTHY"
        and account.reconciled_at
        and 0 <= (timestamp - account.reconciled_at).total_seconds() <= settings.account_ttl_seconds
    )


def current_verified_quote(session, order, inst, latest, timestamp):
    """Recompute a cancellation target from a real independent checkpoint."""
    from sigbot.strategies.mm_sig_v1.live.analytics import policy_inputs
    from sigbot.strategies.mm_sig_v1.models import Book

    settings = Settings.model_validate(latest["settings"])
    reference = inst.mapping_evidence.get("mm13_reference", {})
    account = session.get(Account, order.account_id)
    if (
        not safety_checkpoint_current(inst, account, settings, timestamp)
        or not reference.get("current")
        or reference.get("consistent_checks", 0) < 2
        or reference.get("binding") != [inst.rules_hash, inst.mapping_hash]
        or not reference.get("ends_at")
        or not 0
        <= (timestamp - utc_time(reference["at"])).total_seconds()
        <= settings.book_ttl_seconds
        or reference.get("cooldown_until")
        and timestamp < utc_time(reference["cooldown_until"])
        or account is None
        or account.lifecycle != "HEALTHY"
        or not account.reconciled_at
        or not 0
        <= (timestamp - account.reconciled_at).total_seconds()
        <= settings.account_ttl_seconds
        or not inst.reviewed
        or (latest["quote"].get("rules_hash"), latest["quote"].get("mapping_hash"))
        != (inst.rules_hash, inst.mapping_hash)
    ):
        return None
    book = Book.model_validate({**inst.book, "received_at": inst.book_at, "quality": inst.quality})
    if (
        not book.validated_at
        or not 0 <= (timestamp - book.validated_at).total_seconds() <= settings.book_ttl_seconds
    ):
        return None
    try:
        inputs, _ = policy_inputs(
            session, account, inst, book, settings, timestamp, utc_time(reference["ends_at"])
        )
        quote = actual_prices(book, inputs.inventory, inst.tick, settings, policy_inputs=inputs)
        if settings.risk_policy is not None:
            from sigbot.mm_risk import overlay_quote

            quote = overlay_quote(session, account, inst, settings, inputs, settings.risk_policy,
                                  timestamp, utc_time(reference["ends_at"]), quote)
    except (RiskRejected, SchemaMismatch):
        return None  # Failed revalidation retains the original safety cancel.
    return {**quote, "rules_hash": inst.rules_hash, "mapping_hash": inst.mapping_hash}


class RealExchange(SigExecutionExchange):
    def __init__(self, *args, **kwargs):
        self.last_bodies = {}
        self.last_page_rows = {}
        self.empirical_fee_accepted_by_operator = False
        super().__init__(*args, **kwargs)
        if not self.contract.data.get("schema_path"):
            _, schema = reviewed_schema()
            self.schema = self.reader.schema = schema

    def authorized(self):
        super().authorized()
        if getattr(self.fee, "kind", None) == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION" and not getattr(
            self, "empirical_fee_accepted_by_operator", False
        ):
            raise AuthorizationRequired("MM_OPERATOR_MUST_ACCEPT_EMPIRICAL_FEE_UNCERTAINTY")

    def archive_read(self, name, status, raw):
        super().archive_read(name, status, raw)
        if status == 200:
            self.last_bodies[name] = json.loads(raw, parse_float=Decimal)

    def snapshot(self):
        snapshot = super().snapshot()
        if not self.mock and self.store.namespace == "live":
            from sigbot.accountcheck import persist_checkpoint
            from sigbot.storage import Store

            readonly = Store(self.store.engine.url, "readonly")
            try:
                persist_checkpoint(
                    readonly,
                    self.account_id,
                    self.last_account_check,
                    self.last_account_data,
                    self.clock.now,
                )
            finally:
                readonly.engine.dispose()
        return snapshot

    def pages(self, name, **kwargs):
        rows, count = super().pages(name, **kwargs)
        self.last_page_rows[name] = rows
        return rows, count

    def refresh_book(self, instrument_id):
        super().refresh_book(instrument_id)
        raw = self.last_bodies["orderbook"]
        own = self.last_page_rows["open_orders"]
        with self.store.transaction() as s:
            inst = s.get(Instrument, instrument_id)
            book = normalized_book(raw, own, self.clock.now, outcome=inst.outcome)
            book = book.model_copy(
                update={"validated_at": self.clock.now, "continuity_verified": True}
            )
            inst.book = book.model_dump(mode="json", exclude={"received_at", "quality"})
            inst.book_at, inst.quality = book.received_at, book.quality
            row = inst.mapping_evidence.get("mm131_latest", {})
            if row.get("settings", {}).get("strategy_version") == "1.3":
                settings = Settings.model_validate(row["settings"])
                previous = inst.mapping_evidence.get("mm13_reference", {})
                bid, ask = external_best(book)
                mid = (bid + ask) / 2
                binding = [inst.rules_hash, inst.mapping_hash]
                recent = bool(
                    previous.get("current")
                    and previous.get("binding") == binding
                    and previous.get("at")
                    and 0
                    <= (self.clock.now - utc_time(previous["at"])).total_seconds()
                    <= settings.book_ttl_seconds
                )
                cooldown = previous.get("cooldown_until")
                if (
                    recent
                    and previous.get("mid") is not None
                    and abs(mid - Decimal(previous["mid"])) >= inst.tick * settings.jump_ticks
                ):
                    cooldown = (
                        self.clock.now + timedelta(seconds=settings.jump_cooldown_seconds)
                    ).isoformat()
                inst.mapping_evidence = {
                    **inst.mapping_evidence,
                    "mm13_reference": {
                        "at": self.clock.now.isoformat(),
                        "current": True,
                        "binding": binding,
                        "consistent_checks": min(2, previous.get("consistent_checks", 0) + 1)
                        if recent
                        else 1,
                        "mid": str(mid),
                        "cooldown_until": cooldown,
                        "ends_at": previous.get("ends_at"),
                        "sequence": book.sequence,
                        "source": "AUTHORITATIVE_REST_BOOK_AND_COMPLETE_OWN_ORDER_REVALIDATION",
                        "quote_retimestamped": False,
                    },
                }
        return book


class LivePlanner(Planner):
    def settings(self, intent):
        from sigbot.domain import Mode

        if not intent.synthetic:
            return settings_for(intent)
        return settings_for(intent, offline=intent.synthetic and self.store.namespace in {"offline","shadow"}
                            and self.config.mode in {Mode.OFFLINE,Mode.SHADOW})
    def market_data_time(self, inst, intent):
        if intent.strategy_version == "1.3":
            verified = inst.book.get("validated_at")
            if verified is None:
                raise RiskRejected("MM13_BOOK_VALIDATION_TIME_REQUIRED")
            return utc_time(verified)
        return super().market_data_time(inst, intent)

    def quote13(self, s, account, inst, settings):
        from sigbot.strategies.mm_sig_v1.live.analytics import policy_inputs
        from sigbot.strategies.mm_sig_v1.models import Book

        self.clock.sync()
        if not safety_checkpoint_current(inst, account, settings, self.clock.now):
            raise RiskRejected("MM13_PRODUCER_SAFETY_CHECKPOINT_REQUIRED")
        book = Book.model_validate(
            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
        )
        pool = s.scalar(
            select(Audit)
            .where(Audit.account_id == account.id, Audit.kind == "MM13_UNIVERSE")
            .order_by(Audit.id.desc())
            .limit(1)
        )
        inputs, stats = policy_inputs(
            s,
            account,
            inst,
            book,
            settings,
            self.clock.now,
            self.ends_at,
            exploration=bool(
                pool and inst.exchange_id in pool.data.get("exploration_universe", [])
            ),
        )
        if pool is None or inst.exchange_id not in pool.data.get(
            "selection_universe", pool.data.get("quoting_universe", [])
        ):
            inputs = inputs.model_copy(update={"new_risk_allowed": False})
        quote = actual_prices(book, inputs.inventory, inst.tick, settings, policy_inputs=inputs)
        if self.config.mm_risk_policy is not None:
            from sigbot.mm_risk import overlay_quote

            quote = overlay_quote(s, account, inst, settings, inputs, self.config.mm_risk_policy,
                                  self.clock.now, self.ends_at, quote)
        return quote

    def rebuild_intent(self, s, account, intent):
        settings = self.settings(intent)
        inst = s.get(Instrument, intent.instrument_id)
        quote = self.quote13(s, account, inst, settings)
        pos = s.get(Position, (account.id, inst.id))
        held = pos.quantity if pos else ZERO
        side = "BUY" if intent.action == "INCREASE_TO_TARGET" else "SELL"
        quantity = Decimal(quote["buy_quantity" if side == "BUY" else "sell_quantity"])
        note = json.loads(intent.note)
        note["kind"] = quote["sell_kind"] if side == "SELL" else "MM_REAL_PASSIVE"
        note["policy"] = quote
        return intent.model_copy(
            update={
                "target_quantity": held + quantity if side == "BUY" else max(ZERO, held - quantity),
                "limit_price": Decimal(quote["bid" if side == "BUY" else "ask"])
                if quote["bid" if side == "BUY" else "ask"] is not None
                else intent.limit_price,
                "created_at": self.clock.now,
                "expires_at": self.clock.now + timedelta(seconds=30),
                "note": json.dumps(note),
            }
        )

    def can_coexist(self, intent, existing):
        return (
            existing.get("strategy_id") == "mm_sig"
            and existing.get("strategy_version") in {"1.1", "1.2", "1.3"}
            and existing.get("action") != intent.action
        )

    def check_reduce_pending_buy(self, intent, pending_buy):
        self.settings(intent)

    def check_liquidity(self, inst, side, price, quantity, intent):
        settings = self.settings(intent)
        from sigbot.strategies.mm_sig_v1.models import Book

        book = Book.model_validate(
            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
        )
        if json.loads(intent.note)["kind"] == "MM_REAL_SEED":
            if settings.strategy_version != "1.2" or not settings.bootstrap_inventory:
                raise RiskRejected("MM_SEED_POLICY_NOT_AUTHORIZED")
            _, ask = external_best(book)
            depth = sum(
                (
                    level.quantity - level.own_quantity
                    for level in book.asks
                    if level.price <= price
                ),
                ZERO,
            )
            if side != "BUY" or price != ask or depth < quantity:
                raise RiskRejected("MM_SEED_CURRENT_ASK_DEPTH_REQUIRED")
            return
        if (
            settings.strategy_version == "1.3"
            and json.loads(intent.note)["kind"] == "INVENTORY_EXIT"
        ):
            from sigbot.strategies.mm_sig_v1.live.policy import ceil_grid

            bids = [level for level in book.bids if level.quantity > level.own_quantity]
            best = max((level.price for level in bids), default=ZERO)
            depth = sum(
                (level.quantity - level.own_quantity for level in bids if level.price >= price),
                ZERO,
            )
            if (
                side != "SELL"
                or settings.inventory_exit_min_price is None
                or price
                < max(
                    settings.inventory_exit_min_price,
                    best - settings.exit_depth_band_ticks * inst.tick,
                    ceil_grid(best * (1 - settings.exit_max_price_impact_fraction), inst.tick),
                )
                or quantity > depth
            ):
                raise RiskRejected("MM13_EXIT_PRICE_PROTECTION_REQUIRED")
            return
        bid, ask = external_best(book)
        if settings.strategy_version == "1.3":
            if settings.pricing_model == "GUEANT_FINITE_GRID":
                # calculate/check_dispatch recompute the current policy inside
                # their existing transaction. A second writable transaction can
                # wait on the instrument row locked by the first one.
                current = json.loads(intent.note).get("policy", {})
                if not current:
                    raise RiskRejected("MM_STANDARD_POLICY_EVIDENCE_REQUIRED")
                if json.loads(intent.note).get("policy", {}).get("calibration_sampling"):
                    authority_data = json.loads(Path(settings.approval_file).read_text())
                    authority_data.pop("eligibility_confirmed_by_operator", None)
                    authority = Approval.model_validate(authority_data)
                    if (
                        not authority.accept_mm_calibration
                        or authority.mm_calibration_until is None
                        or self.clock.now >= authority.mm_calibration_until
                    ):
                        raise RiskRejected("MM_CALIBRATION_AUTHORIZATION_REQUIRED")
                bound = Decimal(current["bid" if side == "BUY" else "ask"])
                target = Decimal(current["buy_quantity" if side == "BUY" else "sell_quantity"])
                if quantity > target or (price > bound if side == "BUY" else price < bound):
                    raise RiskRejected("MM_STANDARD_CURRENT_POLICY_REJECTED")
                if side == "BUY" and price >= ask or side == "SELL" and price <= bid:
                    raise RiskRejected("MM_REAL_WOULD_TAKE_LIQUIDITY")
                return
            if json.loads(intent.note)["kind"] == "INVENTORY_EXIT":
                if (
                    side != "SELL"
                    or settings.inventory_exit_min_price is None
                    or price < settings.inventory_exit_min_price
                ):
                    raise RiskRejected("MM13_EXIT_PRICE_PROTECTION_REQUIRED")
                return
            if settings.participation_model == "CDJ_BOOK_IMBALANCE":
                with self.store.transaction() as session:
                    account = session.get(Account, intent.account_id)
                    current = self.quote13(
                        session, account, session.get(Instrument, inst.id), settings
                    )
                bound = Decimal(current["buy_guard" if side == "BUY" else "sell_guard"])
                ready = (
                    current.get("public_book_signal", {}).get(side, {}).get("status")
                    == "READY_EMPIRICAL_PROXY"
                )
                if side == "BUY" and (not ready or price >= bound or price >= ask):
                    raise RiskRejected("MM132_CURRENT_BUY_SIGNAL_REJECTED")
                if side == "SELL" and (price <= bid or price < bound or ready and price == bound):
                    raise RiskRejected("MM132_CURRENT_SELL_SIGNAL_REJECTED")
                return
            eta = inst.tick * settings.per_side_margin_ticks
            mid = (bid + ask) / 2
            with self.store.transaction() as session:
                from sigbot.strategies.mm_sig_v1.live.analytics import statistics

                stats = statistics(
                    session, intent.account_id, inst.id, self.clock.now, settings, tick=inst.tick
                )
            adverse = stats["buy_adverse" if side == "BUY" else "sell_adverse"]
            probe_authority = None
            if settings.join_probe_enabled and settings.approval_file:
                authority_data = json.loads(Path(settings.approval_file).read_text())
                authority_data.pop("eligibility_confirmed_by_operator", None)
                probe_authority = Approval.model_validate(authority_data)
            probe = bool(
                side == "BUY"
                and adverse is None
                and settings.join_probe_enabled
                and probe_authority
                and probe_authority.accept_join_probe
                and json.loads(intent.note).get("policy", {}).get("join_probe")
            )
            if adverse is None and side == "BUY" and not probe:
                raise RiskRejected("MM131_UNKNOWN_AS_REQUIRES_PROBE_AUTHORITY")
            if probe:
                if price != bid or ask - bid - 2 * inst.fee_per_share <= 0:
                    raise RiskRejected("MM131_PROBE_MUST_JOIN_WITH_POSITIVE_FEE_SPREAD")
                return
            adverse = inst.tick * settings.adverse_prior_ticks if adverse is None else adverse
            margin = mid - price if side == "BUY" else price - mid
            if margin - inst.fee_per_share - adverse < eta:
                raise RiskRejected("MM13_SIDE_QUALITY_REJECTED")
        if side == "BUY" and price >= ask or side == "SELL" and price <= bid:
            raise RiskRejected("MM_REAL_WOULD_TAKE_LIQUIDITY")

    def guard(self, s, account, inst, side, price, quantity, settings, exclude=None):
        if side == "BUY":
            from sigbot.accounting import lot_origins

            opposite_ids = list(s.scalars(select(Instrument.id).where(
                Instrument.tournament == inst.tournament,
                Instrument.exchange_id == inst.exchange_id,
                Instrument.outcome != inst.outcome,
            )))
            if any(origin.get("strategy_id") != "mm_sig"
                   for _, origin, _ in lot_origins(s, account.id, opposite_ids)):
                raise RiskRejected("MM_REAL_FOREIGN_INVENTORY_PROTECTED")
        pos = s.get(Position, (account.id, inst.id))
        held = pos.quantity if pos else ZERO
        if held:
            lots = list(
                s.scalars(
                    select(PositionLot).where(
                        PositionLot.account_id == account.id, PositionLot.instrument_id == inst.id
                    )
                )
            )
            from sigbot.storage import FillRow

            for lot in lots:
                if not lot.quantity:
                    continue
                fill = s.get(FillRow, (account.id, lot.economic_id.removeprefix("fill:")))
                origin = s.get(Order, fill.order_id) if fill else None
                row = s.get(IntentRow, origin.intent_id) if origin else None
                if row is None or (
                    row.payload.get("strategy_id"),
                    row.payload.get("strategy_version"),
                ) not in {("mm_sig", "1.1"), ("mm_sig", "1.2"), ("mm_sig", "1.3")}:
                    raise RiskRejected("MM_REAL_FOREIGN_INVENTORY_PROTECTED")
        orders = [
            o
            for o in s.scalars(select(Order).where(Order.account_id == account.id))
            if o.id != exclude and (o.reserve_cash or o.reserve_quantity)
        ]
        same = [o for o in orders if o.instrument_id == inst.id]
        from sigbot.uncertainty import check_scope

        check_scope(s, account, inst, self.config.parallel_strategy_execution, exclude_order=exclude)
        if any(
            (side == "BUY" and o.side == "SELL" and price >= o.price)
            or (side == "SELL" and o.side == "BUY" and price <= o.price)
            for o in same
        ):
            raise RiskRejected("MM_REAL_SELF_CROSS")
        if settings.strategy_version == "1.3":
            for order in orders:
                opposite = s.get(Instrument, order.instrument_id)
                if opposite.exchange_id != inst.exchange_id or opposite.outcome == inst.outcome:
                    continue
                mirrored_price = 1 - order.price
                mirrored_side = "SELL" if order.side == "BUY" else "BUY"
                if (
                    side == "BUY"
                    and mirrored_side == "SELL"
                    and price >= mirrored_price
                    or side == "SELL"
                    and mirrored_side == "BUY"
                    and price <= mirrored_price
                ):
                    raise RiskRejected("MM13_MIRRORED_ACCOUNT_SELF_CROSS")
        if (
            side == "BUY"
            and settings.max_inventory is not None
            and held
            + sum((o.quantity - o.filled for o in same if o.side == "BUY"), ZERO)
            + quantity
            > settings.max_inventory
        ):
            raise RiskRejected("MM_REAL_INVENTORY_LIMIT")
        if side == "SELL" and quantity > held - sum((o.reserve_quantity for o in same), ZERO):
            raise RiskRejected("MM_REAL_UNCOVERED_SELL")
        return held

    def calculate(self, s, account, intent):
        settings = self.settings(intent)
        plan = super().calculate(s, account, intent)
        if plan.quantity == ZERO:
            return plan
        inst = s.get(Instrument, intent.instrument_id)
        self.guard(s, account, inst, plan.side, plan.price, plan.quantity, settings)
        if settings.strategy_version == "1.3":
            # The shared Scheduler enforces the configured official account budget.
            quote = self.quote13(s, account, inst, settings)
            total = Decimal(quote["buy_quantity" if plan.side == "BUY" else "sell_quantity"])
            if (
                plan.quantity > total
                or plan.side == "BUY"
                and plan.price > Decimal(quote["buy_guard"])
                or plan.side == "SELL"
                and plan.price
                < Decimal(
                    quote["ask"] if quote["sell_kind"] == "INVENTORY_EXIT" else quote["sell_guard"]
                )
            ):
                raise RiskRejected("MM13_DYNAMIC_SIZE_OR_QUALITY_REJECTED")
            if plan.side == "BUY":
                positions = list(
                    s.scalars(select(Position).where(Position.account_id == account.id))
                )
                all_orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
                gross = (
                    sum((p.cost or ZERO for p in positions), ZERO)
                    + sum((o.reserve_cash for o in all_orders), ZERO)
                    + plan.reserve_cash
                    + plan.fee_bound
                )
                if (
                    settings.global_cost_fraction is not None
                    and gross > Decimal(quote["capital"]) * settings.global_cost_fraction
                ):
                    raise RiskRejected("MM13_GLOBAL_COST_LIMIT")
                if settings.state_cost_fraction is not None:
                    state = (
                        inst.mapping_evidence.get("review", {})
                        .get("contract_details", {})
                        .get("usState")
                    )
                    if not state:
                        raise RiskRejected("MM13_STATE_RISK_SCOPE_UNKNOWN")
                    state_ids = {
                        i.id
                        for i in s.scalars(
                            select(Instrument).where(Instrument.tournament == account.tournament)
                        )
                        if i.mapping_evidence.get("review", {})
                        .get("contract_details", {})
                        .get("usState")
                        == state
                    }
                    state_gross = (
                        sum(
                            (p.cost or ZERO for p in positions if p.instrument_id in state_ids),
                            ZERO,
                        )
                        + sum(
                            (o.reserve_cash for o in all_orders if o.instrument_id in state_ids),
                            ZERO,
                        )
                        + plan.reserve_cash
                        + plan.fee_bound
                    )
                    if state_gross > Decimal(quote["capital"]) * settings.state_cost_fraction:
                        raise RiskRejected("MM13_STATE_COST_LIMIT")
            from sigbot.strategies.mm_sig_v1.live.analytics import owned, unresolved

            if any(
                o.side == plan.side and unresolved(o) and owned(s.get(IntentRow, o.intent_id))
                for o in s.scalars(
                    select(Order).where(
                        Order.account_id == account.id, Order.instrument_id == inst.id
                    )
                )
            ):
                raise RiskRejected("MM13_QUOTE_PARENT_ALREADY_ACTIVE")
        if settings.quote_quantity is not None and plan.quantity > settings.quote_quantity:
            raise RiskRejected("MM_REAL_QUOTE_SIZE_LIMIT")
        if plan.side == "BUY":
            mm_ids = {
                r.instrument_id
                for r in s.scalars(select(IntentRow).where(IntentRow.account_id == account.id))
                if (r.payload.get("strategy_id"), r.payload.get("strategy_version"))
                in {("mm_sig", "1.1"), ("mm_sig", "1.2"), ("mm_sig", "1.3")}
            }
            mm_ids.add(inst.id)
            positions = list(s.scalars(select(Position).where(Position.account_id == account.id)))
            orders = list(s.scalars(select(Order).where(Order.account_id == account.id)))
            events = list(
                s.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == account.id,
                        StrategyEvent.strategy_id == "mm_sig",
                        StrategyEvent.strategy_version == intent.strategy_version,
                    )
                )
            )
            committed = (
                sum((p.cost or ZERO for p in positions if p.instrument_id in mm_ids), ZERO)
                + sum((o.reserve_cash for o in orders if o.instrument_id in mm_ids), ZERO)
                + sum((e.future_fee_reserve for e in events), ZERO)
            )
            addition = plan.reserve_cash + plan.fee_bound
            event_cost = (
                sum(
                    (
                        p.cost or ZERO
                        for p in positions
                        if s.get(Instrument, p.instrument_id).event_group == inst.event_group
                    ),
                    ZERO,
                )
                + sum(
                    (
                        o.reserve_cash
                        for o in orders
                        if s.get(Instrument, o.instrument_id).event_group == inst.event_group
                    ),
                    ZERO,
                )
                + sum(
                    (e.future_fee_reserve for e in events if e.event_key == inst.event_group), ZERO
                )
            )
            occupied = {e.event_key for e in events if e.state != "CLOSED"}
            if (
                (
                    settings.max_new_exposure is not None
                    and committed + addition > settings.max_new_exposure
                )
                or event_cost + addition > event_cash_budget(settings, account_capital(s, account))
                or settings.max_events is not None
                and len(occupied | {inst.event_group}) > settings.max_events
            ):
                raise RiskRejected("MM_REAL_ADDITIONAL_RISK_BUDGET_EXCEEDED")
        return plan

    def on_reserved(self, s, account, intent, plan, order_id):
        inst = s.get(Instrument, intent.instrument_id)
        event = s.get(
            StrategyEvent, (account.id, "mm_sig", intent.strategy_version, inst.event_group)
        )
        if event is None:
            event = StrategyEvent(
                account_id=account.id,
                strategy_id="mm_sig",
                strategy_version=intent.strategy_version,
                event_key=inst.event_group,
                instrument_id=inst.id,
                state="QUOTING",
                state_group="REAL",
                shock_group=inst.event_group,
                direction_bucket="YES",
                future_fee_reserve=ZERO,
                data={"fee_per_share": str(inst.fee_per_share)},
            )
            s.add(event)
        event.state = "QUOTING"
        if plan.side == "BUY":
            event.future_fee_reserve += plan.fee_bound
            position = s.get(Position, (account.id, inst.id))
            if not position or not position.quantity:
                exits = dict(event.data.get("mm13_exit_by_instrument", {}))
                exits.pop(inst.id, None)
                legacy = event.data.get("mm13_exit", {})
                event.data = {
                    **{
                        k: v
                        for k, v in event.data.items()
                        if k != "mm13_exit"
                        or legacy.get("instrument_id", event.instrument_id) != inst.id
                    },
                    "mm13_exit_by_instrument": exits,
                }
        note = json.loads(intent.note)
        if intent.strategy_version == "1.3" and note["kind"] == "INVENTORY_EXIT":
            policy = note["policy"]
            if not policy.get("exit_epoch"):
                raise RiskRejected("MM13_EXIT_COST_PROVENANCE_REQUIRED")
            exits = dict(event.data.get("mm13_exit_by_instrument", {}))
            legacy = event.data.get("mm13_exit", {})
            if legacy and not exits:
                exits[legacy.get("instrument_id", event.instrument_id)] = legacy
            previous = exits.get(inst.id, {})
            if previous.get("epoch", policy["exit_epoch"]) != policy["exit_epoch"]:
                raise RiskRejected("MM13_EXIT_EPOCH_CHANGED")
            if inst.id not in exits:
                exits[inst.id] = {
                    "instrument_id": inst.id,
                    "epoch": policy["exit_epoch"],
                    "reference_bid": policy["exit_reference_bid"],
                    "quantity_basis": policy["exit_quantity_basis"],
                    "slippage_budget": policy["exit_slippage_budget"],
                }
                event.data = {
                    **event.data,
                    "mm13_exit_by_instrument": exits,
                }
            event.data = {**event.data, "mm13_exit_by_instrument": exits}
            if "mm13_exit" not in event.data:
                event.data = {**event.data, "mm13_exit": exits[inst.id]}
        if json.loads(intent.note)["kind"] == "MM_REAL_SEED":
            event.data = {**event.data, "seed_attempted": True}

    def project(self, s, account, order):
        inst = s.get(Instrument, order.instrument_id)
        version = s.get(IntentRow, order.intent_id).payload["strategy_version"]
        event = s.get(StrategyEvent, (account.id, "mm_sig", version, inst.event_group))
        ids = list(
            s.scalars(
                select(Instrument.id).where(
                    Instrument.tournament == account.tournament,
                    Instrument.event_group == inst.event_group,
                )
            )
        )
        positions = list(
            s.scalars(
                select(Position).where(
                    Position.account_id == account.id,
                    Position.instrument_id.in_(ids),
                )
            )
        )
        quantity = sum((pos.quantity for pos in positions), ZERO)
        holding = next((pos for pos in positions if pos.quantity), None)
        event.direction_bucket = (
            s.get(Instrument, holding.instrument_id).outcome if holding else "FLAT"
        )
        if holding:
            event.instrument_id = holding.instrument_id
        orders = list(
            s.scalars(
                select(Order).where(Order.account_id == account.id, Order.instrument_id.in_(ids))
            )
        )
        pending = sum(
            (o.quantity - o.filled for o in orders if o.side == "BUY" and o.reserve_cash), ZERO
        )
        event.future_fee_reserve = (quantity + pending) * Decimal(event.data["fee_per_share"])
        if self.config.mm_risk_policy is not None:
            from sigbot.accounting import lot_origins

            owned_lots=lot_origins(s,account.id,ids)
            total_exit_fee=sum((lot.quantity*s.get(Instrument,lot.instrument_id).fee_per_share
                for lot,origin,_ in owned_lots if origin.get("strategy_id")=="mm_sig"),ZERO)
            total_exit_fee+=sum(((o.quantity-o.filled)*s.get(Instrument,o.instrument_id).fee_per_share
                for o in orders if o.side=="BUY" and o.reserve_cash
                and s.get(IntentRow,o.intent_id).payload.get("strategy_id")=="mm_sig"),ZERO)
            for other in s.scalars(select(StrategyEvent).where(StrategyEvent.account_id==account.id,
                StrategyEvent.strategy_id=="mm_sig",StrategyEvent.event_key==inst.event_group)):
                other.future_fee_reserve=ZERO
            event.future_fee_reserve=total_exit_fee
        event.state = (
            "QUOTING"
            if any(o.reserve_cash or o.reserve_quantity for o in orders)
            else "INVENTORY"
            if quantity
            else "CLOSED"
        )

    def on_observed(self, s, account, order, observation):
        self.project(s, account, order)

    def on_unsent_expired(self, s, account, intent, order):
        self.project(s, account, order)
        if (
            not order.remote_id
            and order.request_state == "RESOLVED"
            and json.loads(intent.note)["kind"] == "MM_REAL_SEED"
        ):
            inst = s.get(Instrument, order.instrument_id)
            event = s.get(
                StrategyEvent, (account.id, "mm_sig", intent.strategy_version, inst.event_group)
            )
            event.data = {**event.data, "seed_attempted": False}

    def on_unsent_canceled(self, s, account, intent, order):
        self.on_unsent_expired(s, account, intent, order)

    def check_dispatch(self, s, account, intent, order):
        settings = self.settings(intent)
        inst = s.get(Instrument, order.instrument_id)
        held = self.guard(
            s, account, inst, order.side, order.price, order.quantity, settings, order.id
        )
        from sigbot.strategies.mm_sig_v1.models import Book

        book = Book.model_validate(
            {**inst.book, "received_at": inst.book_at, "quality": inst.quality}
        )
        if settings.strategy_version == "1.3":
            current = self.quote13(s, account, inst, settings)
            total = Decimal(current["buy_quantity" if order.side == "BUY" else "sell_quantity"])
            if (
                order.quantity > total
                or order.side == "BUY"
                and order.price > Decimal(current["buy_guard"])
                or order.side == "SELL"
                and order.price
                < Decimal(
                    current["ask"]
                    if current["sell_kind"] == "INVENTORY_EXIT"
                    else current["sell_guard"]
                )
            ) or json.loads(intent.note)["kind"] != (
                current["sell_kind"] if order.side == "SELL" else "MM_REAL_PASSIVE"
            ):
                raise RiskRejected("MM13_POLICY_CHANGED_BEFORE_SUBMIT")
            self.check_liquidity(inst, order.side, order.price, order.quantity, intent)
            return
        if json.loads(intent.note)["kind"] == "MM_REAL_SEED":
            self.check_liquidity(inst, order.side, order.price, order.quantity, intent)
            return
        current = actual_prices(
            book,
            held,
            inst.tick,
            settings,
            fee=inst.fee_per_share,
            capital=account_capital(s, account),
        )
        if Decimal(current["bid" if order.side == "BUY" else "ask"]) != order.price:
            raise RiskRejected("MM_REAL_PRICE_CHANGED_BEFORE_SUBMIT")


class LiveRunner:
    def __init__(self, runtime):
        settings = runtime.settings
        if not settings.application_config or not settings.approval_file or not settings.fee_file:
            raise AuthorizationRequired("MM_REAL_EXECUTION_CONFIGURATION_AND_APPROVAL_REQUIRED")
        app = Config.model_validate_json(Path(settings.application_config).read_text())
        approval_data = json.loads(Path(settings.approval_file).read_text())
        if approval_data.pop("eligibility_confirmed_by_operator", False) is not True:
            raise AuthorizationRequired("MM_PERSONAL_ELIGIBILITY_CONFIRMATION_REQUIRED")
        empirical_accepted = approval_data.get("accept_empirical_fee_model", False) is True
        approval = Approval.model_validate(approval_data)
        if any(getattr(approval, key) != value for key, value in bindings(app).items()):
            raise AuthorizationRequired("APPROVAL_BINDING_CHANGED")
        if not approval.issued_at <= datetime.now(timezone.utc) < approval.new_orders_until:
            raise AuthorizationRequired("APPROVAL_EXPIRED")
        for eid in runtime.selected:
            reviewed = runtime.reviews[eid]
            iid = reviewed["instrument_id"]
            if (
                iid not in approval.instrument_allowlist
                or approval.rules_hashes.get(iid) != reviewed["rules_hash"]
                or approval.mapping_hashes.get(iid) != reviewed["mapping_hash"]
            ):
                raise AuthorizationRequired("APPROVAL_MARKET_VERSION_CHANGED")
        if (app.expected_account_id, app.expected_tournament_id) != (runtime.aid, runtime.tid):
            raise AuthorizationRequired("MM_REAL_EXECUTION_IDENTITY_MISMATCH")
        if not runtime.account_result or runtime.account_result["status"] != "PASS":
            raise RiskRejected("MM_REAL_ACCOUNT_CHECKPOINT_REQUIRED")
        self.runtime, self.settings, self.app = runtime, settings, app
        self.clock = Clock()
        from sigbot.readcheck import load_key

        self.exchange = RealExchange(
            runtime.live_store, app, self.clock, load_key(runtime.env_file), approval
        )
        self.exchange.empirical_fee_accepted_by_operator = empirical_accepted
        self.exchange.authorized()
        cup = runtime.account_data["cup"]
        self.policy = LivePlanner(
            runtime.live_store,
            app,
            self.clock,
            utc_time(cup["startDate"]),
            utc_time(cup["endDate"]),
        )
        from sigbot.strategies.router import StrategyRouter

        router = StrategyRouter(
            runtime.live_store, app, self.clock, self.policy.starts_at, self.policy.ends_at
        )
        self.executor = Executor(
            runtime.live_store, app, self.clock, self.exchange, router, runtime.aid
        )
        with runtime.live_store.transaction() as s:
            existing = s.get(Account, runtime.aid)
            if existing.lifecycle == "PAUSED" and existing.reason not in {
                "MM_ORDER_APPROVAL_AND_FEE_EVIDENCE_REQUIRED",
                *RECOVERABLE_PAUSES,
                *({"ECR_RUNTIME_STOPPED"} if app.parallel_strategy_execution else set()),
            }:
                raise AuthorizationRequired("MM_MANUAL_OR_RISK_PAUSE_REQUIRES_EXPLICIT_RESUME")
        self.executor.start()
        while not runtime.stop.is_set():
            self.clock.now = datetime.now(timezone.utc)
            try:
                self.poll()
                check = self.executor.reconcile()
                break
            except RateLimited as exc:
                if exc.code != "ACCOUNT_OPERATION_BUSY":
                    self.close()
                    raise
                runtime.state.update(lifecycle="WAITING_ACCOUNT_OPERATION", trading_enabled=False)
                runtime.publish()
                runtime.stop.wait(1)
        else:
            self.close()
            raise RiskRejected("MM_STARTUP_STOPPED")
        pending = check["status"] == "INDETERMINATE" or check["differences"] == [
            "UNRESOLVED_REQUEST"
        ]
        if check["status"] != "PASS" and not pending:
            self.close()
            raise RiskRejected("MM_REAL_STARTUP_RECONCILIATION_FAILED")
        if check["status"] == "PASS":
            self.executor.controls.resume(runtime.aid, approval.operator)
        elif check["differences"] == ["UNRESOLVED_REQUEST"]:
            self.executor.controls.pause(runtime.aid, "MM_UNRESOLVED_ORDER_RECONCILIATION")
        self.runtime.apply_account(
            self.exchange.last_account_check, self.exchange.last_account_data, runtime.aid
        )
        self.next_reconcile = self.clock.now + timedelta(seconds=10 if pending else 30)
        self.next_poll = self.clock.now + timedelta(
            seconds=30 if self.settings.strategy_version == "1.2" else 10
        )

    def admission_status(self):
        approval = self.exchange.approval
        assert approval is not None
        if self.clock.now >= approval.new_orders_until:
            return "APPROVAL_EXPIRED"
        if self.executor.controls.status(self.runtime.aid)["lifecycle"] != "HEALTHY":
            return "ACCOUNT_PAUSED"
        with self.runtime.live_store.transaction() as s:
            wires = list(
                s.scalars(select(LiveWire).where(LiveWire.approval_id == approval.approval_id))
            )
        if approval.budget_exceeded(
            len(wires),
            sum((Decimal(w.payload["quantity"]) for w in wires), ZERO),
            sum(
                (Decimal(w.payload["quantity"]) * Decimal(w.payload["price"]) for w in wires),
                ZERO,
            ),
            at_limit=True,
        ):
            return "APPROVAL_BUDGET_EXHAUSTED"
        return "AUTHORIZED"

    def orders(self):
        with self.runtime.live_store.transaction() as s:
            result = []
            for order in s.scalars(select(Order).where(Order.account_id == self.runtime.aid)):
                row = s.get(IntentRow, order.intent_id)
                if row.payload.get("strategy_id") != "mm_sig":
                    continue
                if (
                    order.state not in TERMINAL
                    or order.request_state != "RESOLVED"
                    or order.reserve_cash
                    or order.reserve_quantity
                    or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
                ):
                    result.append(order)
            return result

    def poll(self):
        for order in self.orders():
            if order.request_state != "SEND_PENDING":
                self.executor.resolve(order.id)

    def maintain(self):
        self.clock.now = datetime.now(timezone.utc)
        if self.clock.now >= self.next_reconcile:
            check = self.executor.reconcile()
            self.next_reconcile = self.clock.now + timedelta(seconds=30)
            self.next_poll = self.clock.now + timedelta(
                seconds=30 if self.settings.strategy_version == "1.2" else 10
            )
            self.runtime.apply_account(
                self.exchange.last_account_check,
                self.exchange.last_account_data,
                self.runtime.aid,
            )
            if check["status"] != "PASS":
                if check["status"] == "INDETERMINATE" or check["differences"] == [
                    "UNRESOLVED_REQUEST"
                ]:
                    if check["differences"] == ["UNRESOLVED_REQUEST"]:
                        self.executor.controls.pause(
                            self.runtime.aid, "MM_UNRESOLVED_ORDER_RECONCILIATION"
                        )
                    self.next_reconcile = self.clock.now + timedelta(seconds=10)
                    self.runtime.state.update(
                        trading_enabled=False,
                        order_admission_status="ACCOUNT_SNAPSHOT_OR_ORDER_QUERY_PENDING",
                    )
                    return
                raise RiskRejected("MM_REAL_PERIODIC_RECONCILIATION_FAILED")
            account = self.executor.controls.status(self.runtime.aid)
            if (
                account["lifecycle"] == "PAUSED"
                and account["reason"] in RECOVERABLE_PAUSES
                and not account["unknown_count"]
                and not account["cancel_pending_count"]
            ):
                assert self.exchange.approval is not None
                self.executor.controls.resume(self.runtime.aid, self.exchange.approval.operator)
                self.runtime.state["last_recovered_pause"] = account["reason"]
                self.runtime.state["confirmed_recoveries"] = (
                    self.runtime.state.get("confirmed_recoveries", 0) + 1
                )
        elif self.clock.now >= self.next_poll:
            self.poll()
            self.next_poll = self.clock.now + timedelta(
                seconds=30 if self.settings.strategy_version == "1.2" else 10
            )
        admission = self.admission_status()
        self.runtime.state["trading_enabled"] = admission == "AUTHORIZED"
        self.runtime.state["order_admission_status"] = admission
        with self.runtime.live_store.transaction() as s:
            expired = []
            for order in self.orders():
                if order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}:
                    continue  # Query the known request; never repeat its DELETE.
                row = s.get(IntentRow, order.intent_id)
                if (
                    (
                        order.request_state == "SEND_PENDING"
                        and self.clock.now >= utc_time(row.payload["expires_at"])
                    )
                    or (
                        self.settings.resting_order_seconds is not None
                        and self.clock.now >= utc_time(row.payload["expires_at"])
                    )
                    or admission
                    in {
                        "APPROVAL_EXPIRED",
                        "APPROVAL_BUDGET_EXHAUSTED",
                    }
                ):
                    expired.append(order.id)
        for oid in expired:
            if not self.executor.cancel(oid):
                self.next_reconcile = self.clock.now
                self.runtime.state.update(
                    trading_enabled=False,
                    order_admission_status="CANCEL_QUERY_PENDING",
                )
                return
        self.runtime.state["execution_status"] = self.executor.controls.status(self.runtime.aid)
        self.runtime.state["real_submit_attempts"] = self.exchange.call_count["POST"]
        self.runtime.state["real_cancel_attempts"] = self.exchange.call_count["DELETE"]

    def tick(self, review, prices, book=None):
        self.clock.now = datetime.now(timezone.utc)
        if self.admission_status() != "AUTHORIZED":
            return
        iid = review["instrument_id"]
        active = [o for o in self.orders() if o.instrument_id == iid]
        if self.settings.strategy_version == "1.1" and active:
            return
        book = book or self.exchange.refresh_book(iid)
        with self.runtime.live_store.transaction() as s:
            account = self.runtime.live_store.lock_account(s, self.runtime.aid)
            inst = s.get(Instrument, iid)
            assert self.exchange.fee is not None
            inst.fee_per_share = self.exchange.fee.per_share
            pos = s.get(Position, (account.id, iid))
            held = pos.quantity if pos else ZERO
            capital = account_capital(s, account)
            quote = actual_prices(
                book, held, inst.tick, self.settings, fee=inst.fee_per_share, capital=capital
            )
            event = s.get(
                StrategyEvent,
                (account.id, "mm_sig", self.settings.strategy_version, inst.event_group),
            )
            seed = bool(
                self.settings.bootstrap_inventory
                and not held
                and not active
                and not (event and event.data.get("seed_attempted"))
            )
        if seed:
            ask = Decimal(quote["external_ask"])
            available = sum(
                (level.quantity - level.own_quantity for level in book.asks if level.price == ask),
                ZERO,
            )
            requests = [
                (
                    "BUY",
                    min(Decimal(quote["buy_quantity"]), available).to_integral_value(
                        rounding=ROUND_FLOOR
                    ),
                    ask,
                    "MM_REAL_SEED",
                )
            ]
        else:
            requests = [
                ("BUY", Decimal(quote["buy_quantity"]), Decimal(quote["bid"]), "MM_REAL_PASSIVE"),
                ("SELL", Decimal(quote["sell_quantity"]), Decimal(quote["ask"]), "MM_REAL_PASSIVE"),
            ]
        for side, quantity, price, kind in requests:
            if quantity <= 0:
                continue
            same_side = [o for o in self.orders() if o.instrument_id == iid and o.side == side]
            if same_side:
                if all(o.price == price for o in same_side):
                    continue  # Stable quotes keep their queue position; no timer churn.
                for order in same_side:
                    if order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}:
                        return
                    if not self.executor.cancel(order.id):
                        self.next_reconcile = self.clock.now
                        self.runtime.state["trading_enabled"] = False
                        return
            with self.runtime.live_store.transaction() as s:
                account = self.runtime.live_store.lock_account(s, self.runtime.aid)
                inst = s.get(Instrument, iid)
                pos = s.get(Position, (account.id, iid))
                held = pos.quantity if pos else ZERO
                orders = list(
                    s.scalars(
                        select(Order).where(
                            Order.account_id == account.id, Order.instrument_id == iid
                        )
                    )
                )
                pending_buy = sum(
                    (o.quantity - o.filled for o in orders if o.side == "BUY" and o.reserve_cash),
                    ZERO,
                )
                pending_sell = sum((o.reserve_quantity for o in orders), ZERO)
                event = s.get(
                    StrategyEvent,
                    (account.id, "mm_sig", self.settings.strategy_version, inst.event_group),
                )
                event_cap = event_cash_budget(self.settings, account_capital(s, account))
                if side == "BUY":
                    committed = (
                        (pos.cost or ZERO if pos else ZERO)
                        + sum((o.reserve_cash for o in orders), ZERO)
                        + (event.future_fee_reserve if event else ZERO)
                    )
                    quantity = min(
                        quantity,
                        max(
                            ZERO,
                            (
                                (event_cap - committed) / (price + 2 * inst.fee_per_share)
                            ).to_integral_value(rounding=ROUND_FLOOR),
                        ),
                    )
                    if self.settings.max_inventory is not None:
                        quantity = min(
                            quantity, max(ZERO, self.settings.max_inventory - held - pending_buy)
                        )
                else:
                    quantity = min(quantity, held - pending_sell)
                if quantity <= 0:
                    continue
                approval = self.exchange.approval
                assert approval is not None
                wires = list(
                    s.scalars(select(LiveWire).where(LiveWire.approval_id == approval.approval_id))
                )
                if approval.budget_exceeded(
                    len(wires) + 1,
                    sum((Decimal(w.payload["quantity"]) for w in wires), ZERO) + quantity,
                    sum(
                        (
                            Decimal(w.payload["quantity"]) * Decimal(w.payload["price"])
                            for w in wires
                        ),
                        ZERO,
                    )
                    + quantity * price,
                ):
                    self.runtime.state["order_admission_status"] = (
                        "APPROVAL_REMAINING_BUDGET_TOO_SMALL"
                    )
                    continue
                target = Intent.model_validate(
                    {
                        "schema_version": "1.0",
                        "intent_id": "SIG-MM-"
                        + content_hash([account.id, iid, side, kind, self.clock.now.isoformat()]),
                        "synthetic": False,
                        "account_id": account.id,
                        "tournament_id": account.tournament,
                        "instrument_id": iid,
                        "outcome_side": "YES",
                        "action": "INCREASE_TO_TARGET" if side == "BUY" else "REDUCE_TO_TARGET",
                        "target_quantity": held + pending_buy + quantity
                        if side == "BUY"
                        else held - pending_sell - quantity,
                        "limit_price": price,
                        "max_order_notional": self.app.risk.max_order_notional
                        or quantity * price + quantity * inst.fee_per_share,
                        "created_at": self.clock.now,
                        "expires_at": self.clock.now
                        + timedelta(seconds=self.settings.resting_order_seconds or 30),
                        "rules_hash": inst.rules_hash,
                        "mapping_hash": inst.mapping_hash,
                        "strategy_id": "mm_sig",
                        "strategy_version": self.settings.strategy_version,
                        "note": json.dumps(
                            {"kind": kind, "settings": self.settings.model_dump(mode="json")}
                        ),
                    }
                )
                oid = self.policy.reserve_locked(s, account, target)
            if not oid:
                continue
            try:
                self.executor.dispatch(oid)
            except RiskRejected as exc:
                if exc.code not in {
                    "MM_REAL_PRICE_CHANGED_BEFORE_SUBMIT",
                    "MM_REAL_WOULD_TAKE_LIQUIDITY",
                    "MM_SEED_CURRENT_ASK_DEPTH_REQUIRED",
                    "DATA_NOT_READY",
                    "RECONCILIATION_REQUIRED",
                }:
                    raise
                with self.runtime.live_store.transaction() as s:
                    order = s.get(Order, oid)
                    if order.request_state != "SEND_PENDING":
                        raise
                self.executor.cancel(oid)
                self.runtime.state["last_unsent_rejection"] = exc.code
                self.next_reconcile = self.clock.now
            self.runtime.state["execution_status"] = self.executor.controls.status(self.runtime.aid)
            if self.runtime.state["execution_status"]["lifecycle"] != "HEALTHY":
                self.next_reconcile = self.clock.now
                self.runtime.state["trading_enabled"] = False
                return

    def close(self):
        self.executor.close()
        self.exchange.close()
