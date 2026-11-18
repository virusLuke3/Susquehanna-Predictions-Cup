"""ECR signal/exit proposals to the common account queue; no trading client."""

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from sigbot.command_queue import CommandQueue
from sigbot.config import Config
from sigbot.domain import Clock, utc_time
from sigbot.errors import AuthorizationRequired
from sigbot.live import stage_ecr
from sigbot.storage import Account, Instrument, Store
from sigbot.strategies.ecr_sig_v1 import core
from sigbot.strategies.ecr_sig_v1.automatic import apply_empirical_fee
from sigbot.strategies.ecr_sig_v1.inputs import RuntimeReviews, epoch
from sigbot.strategies.ecr_sig_v1.observer import atomic_json
from sigbot.strategies.ecr_sig_v1.oms import EcrPlanner, entry_intent
from sigbot.strategies.ecr_sig_v1.runtime_inputs import ReviewedFeed


class Publisher:
    def __init__(self, settings_file, review_file):
        document = json.loads(Path(settings_file).read_text())
        scope = next(
            s
            for s in document["strategies"]
            if (s["strategy_id"], s["strategy_version"]) == ("ecr_sig", "1.0")
        )
        self.config = Config.load(scope["application_config"])
        self.template = RuntimeReviews.model_validate_json(Path(review_file).read_text())
        from sigbot.authorization import Approval

        approval = Approval.model_validate_json(Path(scope["approval_file"]).read_text())
        apply_empirical_fee(self.template, self.config, approval)
        self.store = Store(self.config.database_url, "live")
        self.clock = Clock()
        self.feed = ReviewedFeed(self.template)
        self.queue = CommandQueue(self.store, self.template.account_id, "ecr_sig", "1.0")
        self.policy = None
        self.published = 0

    def started(self, observer):
        if (observer.tid, observer.reads.reader.config.expected_account_id) != (
            self.template.tournament_id,
            self.template.account_id,
        ):
            raise AuthorizationRequired("ECR_SHARED_PRODUCER_ACCOUNT_MISMATCH")
        admitted = [
            c
            for c in self.template.candidates
            if c.review.approved
            and c.review.mappings
            and all(m.approved for m in c.review.mappings)
        ]
        # Existing physical mappings are never duplicated/remapped by a producer.
        with self.store.transaction() as s:
            for raw in admitted:
                old = s.scalar(
                    select(Instrument).where(
                        Instrument.tournament == self.template.tournament_id,
                        Instrument.exchange_id == raw.exchange_id,
                        Instrument.outcome == raw.side,
                    )
                )
                if old is not None and old.id != raw.instrument_id:
                    raise AuthorizationRequired("ECR_SHARED_PHYSICAL_INSTRUMENT_REVIEW_CONFLICT")
        stage_ecr(
            self.store,
            self.template.model_copy(update={"candidates": admitted}),
            reviews_only=True,
            now=self.clock.now,
        )
        self.policy = EcrPlanner(
            self.store,
            self.config,
            self.clock,
            utc_time(observer.cup["startDate"]),
            utc_time(observer.cup["endDate"]),
        )
        observer.state.update(
            execution_role="PRODUCER_ONLY",
            shared_executor="sigbot-account-executor.service",
            normal_live_bridge_enabled=False,
        )

    def cycle(self, observer):
        self.clock.now = datetime.now(timezone.utc)
        snapshot = self.feed.snapshot(observer, self.clock.now)
        rejected = {}
        plans = []
        atomic_json(
            Path("runtime/live/strategies/ecr_sig_v1/shared-references.local.json"),
            {
                "at": self.clock.now.isoformat(),
                "account_id": self.template.account_id,
                "references": self.feed.references(
                    self.store, self.template.account_id, self.clock.now
                ),
            },
        )
        if snapshot.candidates:
            assert self.policy is not None
            with self.store.transaction() as s:
                account = s.get(Account, self.template.account_id)
                reference = self.policy.shared_account(s, account, self.template.account)
            plans, rejected = core.allocate(
                [c.candidate(self.clock.now, False) for c in snapshot.candidates],
                reference,
                epoch(self.clock.now),
            )
            bykey = {c.key: c for c in snapshot.candidates}
            for plan in plans:
                self.queue.publish(entry_intent(snapshot, bykey[plan.key], plan))
                self.published += 1
        observer.state["shared_execution"] = {
            "role": "PRODUCER_ONLY",
            "qualified_before_allocation": len(plans),
            "queued_proposals": self.published,
            "rejected": rejected,
            "feed_rejections": dict(self.feed.rejected),
            "broker": "sigbot-account-executor.service",
            "real_orders_sent_by_producer": 0,
        }

    def close(self):
        self.store.engine.dispose()
