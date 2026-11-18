"""SSR producer entrypoint; uses registered authority and the common account consumer.

Default is a configuration audit. --execute queues algorithmic intents only; this
process has no API key and cannot call order or cancellation endpoints.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from sigbot.domain import Clock, utc_time
from sigbot.errors import AuthorizationRequired, SigbotError
from sigbot.history import save
from sigbot.shared_execution import Authority
from sigbot.storage import Store
from sigbot.strategies.ssr import IDENTITY, SsrPlanner
from sigbot.strategies.ssr_runtime import Parameters, SsrProducer


def main(args):
    settings = json.loads(Path(args.gateway_settings).read_text())
    item = next(
        (
            row
            for row in settings["strategies"]
            if (row["strategy_id"], row["strategy_version"]) == IDENTITY
        ),
        None,
    )
    status = {
        "strategy": list(IDENTITY),
        "mode": "CONFIGURATION_AUDIT" if not args.execute else "SHARED_OMS_PRODUCER",
        "api_keys_loaded": 0,
        "http_requests": 0,
        "direct_venue_writes": 0,
        "extra_fee_per_share": "0",
        "fee_parameter_source": "USER_FIXED_PARAMETER",
        "registered_for_common_consumer": item is not None,
        "cycles": [],
        "realized_profit": None,
    }
    try:
        if item is None:
            raise AuthorizationRequired("SSR_AUTHORITY_NOT_REGISTERED")
        authority = Authority(item)
        authority.check_binding()
        authority.fee.check()
        if getattr(authority.fee, "kind", None) != "OPERATOR_FIXED_FEE":
            raise AuthorizationRequired("SSR_USER_FIXED_COST_PARAMETER_REQUIRED")
        pairs = settings.get("ssr_pairs", [])
        if not pairs:
            raise AuthorizationRequired("SSR_APPROVED_PAIR_INSTRUMENT_BINDINGS_REQUIRED")
        receipt = json.loads(Path(settings["ssr_tournament_receipt"]).read_text())
        cup = receipt["tournament"]
        cfg = authority.config
        if cup["id"] != cfg.expected_tournament_id:
            raise AuthorizationRequired("SSR_TOURNAMENT_RECEIPT_SCOPE_MISMATCH")
        if not args.execute:
            status["status"] = "CONFIGURATION_ACCEPTED_NOT_A_LIVE_RUN"
            return 0
        clock = Clock(realtime=True)
        store = Store(cfg.database_url, cfg.namespace)
        try:
            planner = SsrPlanner(
                store, cfg, clock, utc_time(cup["startDate"]), utc_time(cup["endDate"])
            )
            producer = SsrProducer(
                planner,
                cfg.expected_account_id,
                Parameters.model_validate(settings.get("ssr_parameters", {})),
            )
            until = time.monotonic() + args.seconds
            while True:
                clock.sync()
                if clock.now >= authority.approval.new_orders_until:
                    raise AuthorizationRequired("SSR_PRODUCER_AUTHORITY_EXPIRED")
                current = [
                    producer.cycle(p["pair_id"], p["instrument_ids"], execute=True) for p in pairs
                ]
                status["cycles"].append({"at": clock.now.isoformat(), "pairs": current})
                status["status"] = (
                    "BLOCKED"
                    if any(r["state"] == "BLOCKED" for r in current)
                    else "PRODUCER_CYCLE_COMPLETED_NOT_FILL_CONFIRMATION"
                )
                status["finished_at"] = datetime.now(timezone.utc).isoformat()
                save(Path(args.output), status)
                if args.once or time.monotonic() >= until:
                    return 6 if status["status"] == "BLOCKED" else 0
                time.sleep(min(args.interval, max(0, until - time.monotonic())))
        finally:
            store.engine.dispose()
    except SigbotError as exc:
        status.update(status="BLOCKED", stop_code=exc.code)
        return 6
    finally:
        status["finished_at"] = datetime.now(timezone.utc).isoformat()
        save(Path(args.output), status)
        print(
            json.dumps(
                {
                    k: status.get(k)
                    for k in [
                        "status",
                        "stop_code",
                        "registered_for_common_consumer",
                        "direct_venue_writes",
                    ]
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-settings", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--interval", type=float, default=2)
    args = parser.parse_args()
    if not 0 < args.seconds <= 3600 or not 0 < args.interval <= 5:
        parser.error("bounded run: 1..3600 seconds and 0..5 second interval")
    raise SystemExit(main(args))
