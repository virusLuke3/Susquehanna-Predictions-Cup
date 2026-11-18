"""Finite official tournament history survey. No order, strategy, or ledger writes."""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from sigbot.errors import AuthorizationRequired, RateLimited, SchemaMismatch, SigbotError
from sigbot.readcheck import ReadBudget, ReadCheck, load_key, private_write


def stamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise SchemaMismatch("HISTORY_INVALID_TIMESTAMP") from None
    if result.tzinfo is None:
        raise SchemaMismatch("HISTORY_TIMEZONE_REQUIRED")
    return result.astimezone(timezone.utc)


def save(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, default=str, ensure_ascii=False, indent=2) + "\n")
    temp.replace(path)


class HistoryReads(ReadCheck):
    """Finite outer bound, renewable sessions, same shared account/key throttle."""

    def __init__(self, key, *, max_requests=2000, seconds=1800, minute_limit=100, **kwargs):
        super().__init__(key, max_requests=80, seconds=600, max_pages=20, **kwargs)
        if not 1 <= minute_limit <= 100:
            raise ValueError("invalid history shared minute limit")
        self.minute_limit = minute_limit
        self.total_attempts = 0
        self.request_limit = max_requests
        self.stop_at = time.monotonic() + seconds
        self.reader.transport.charge = self.charge

    def charge(self, category, weight):
        if self.total_attempts >= self.request_limit or time.monotonic() >= self.stop_at:
            raise RateLimited("HISTORY_FINITE_BOUND_REACHED")
        if self.budget.count >= 80 or time.monotonic() >= self.budget.deadline:
            self.budget = ReadBudget(self.root / "shared-read-budget.json", 80, 600)
        self.budget.deadline = min(self.budget.deadline, self.stop_at)
        if category != "READ" or weight != 1:
            raise AuthorizationRequired("READ_ONLY_GET_REQUIRED")
        self.budget.reserve_history_get(self.minute_limit, wait=True)
        self.total_attempts += 1


def identity(body: dict, market: str, exchange: str) -> None:
    if body["marketId"] != market or body["exchangeId"] != exchange:
        raise SchemaMismatch("HISTORY_IDENTITY_MISMATCH")


def candles(
    reads, market, exchange, tournament, start, end, *, limit=1000, max_pages=100, resolution="1h"
):
    """Use the official exclusive response-to continuation; never fill sparse prices."""
    if resolution not in {"1m", "5m", "1h"}:
        raise ValueError("unsupported history study resolution")
    interval_seconds = {"1m": 60, "5m": 300, "1h": 3600}[resolution]
    rows, coverage, seen = [], [], set()
    cursor = start
    for _ in range(max_pages):
        body = reads.call(
            "price_history",
            path_params={"id": exchange},
            query={
                "tournamentId": tournament,
                "resolution": resolution,
                "from": cursor.isoformat(),
                "to": end.isoformat(),
                "limit": limit,
            },
        )
        identity(body, market, exchange)
        boundary = stamp(body["to"])
        if (
            body["resolution"] != resolution
            or stamp(body["from"]) != cursor
            or not cursor < boundary <= end
            or body["coverage"]["complete"]
            and boundary != end
        ):
            raise SchemaMismatch("CANDLE_WINDOW_MISMATCH")
        last = None
        for row in body["candles"]:
            at = stamp(row["time"])
            if (
                at in seen
                or not cursor <= at < boundary
                or int(at.timestamp()) % interval_seconds
                or at.microsecond
                or last is not None
                and at <= last
                or row["volume"] < 0
                or row["tradeCount"] <= 0
            ):
                raise SchemaMismatch("CANDLE_CONTENT_MISMATCH")
            seen.add(at)
            last = at
            rows.append(row)
        coverage.append(body["coverage"])
        if body["coverage"]["complete"]:
            return {
                "candles": rows,
                "pages": len(coverage),
                "window_complete": True,
                "projection_checkpoint_present": all(
                    c["projectedThroughSequence"] is not None for c in coverage
                ),
                "coverage": coverage,
            }
        if not body["candles"]:
            raise SchemaMismatch("CANDLE_CONTINUATION_EMPTY")
        cursor = boundary
    raise RateLimited("HISTORY_PAGE_BOUND_REACHED")


def tape(reads, market, exchange, tournament, start, end, *, limit=200, max_pages=100):
    rows, coverage, cursors, seen = [], [], set(), set()
    query = {
        "tournamentId": tournament,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "limit": limit,
    }
    previous = None
    for _ in range(max_pages):
        body = reads.call("public_trades", path_params={"id": exchange}, query=query)
        identity(body, market, exchange)
        if (
            stamp(body["from"]) != start
            or stamp(body["to"]) != end
            or not body["coverage"]["complete"]
        ):
            raise SchemaMismatch("TRADE_WINDOW_MISMATCH")
        for row in body["data"]:
            at = stamp(row["createdAt"])
            # Legacy and engine IDs occupy separate spaces: bind timestamp as well.
            key = (row["id"], row["createdAt"])
            if (
                key in seen
                or not start <= at < end
                or previous is not None
                and at > previous
                or row["size"] <= 0
                or row["volume"] < 0
            ):
                raise SchemaMismatch("TRADE_CONTENT_MISMATCH")
            seen.add(key)
            previous = at
            rows.append(row)
        coverage.append(body["coverage"])
        pg = body["pagination"]
        if not pg["hasMore"]:
            if pg["nextCursor"] is not None:
                raise SchemaMismatch("TRADE_TERMINAL_CURSOR_CONFLICT")
            return {
                "trades": rows,
                "pages": len(coverage),
                "pagination_complete": True,
                "projection_checkpoint_present": all(
                    c["projectedThroughSequence"] is not None for c in coverage
                ),
                "coverage": coverage,
            }
        cursor = pg["nextCursor"]
        if not cursor or cursor in cursors or not body["data"]:
            raise SchemaMismatch("TRADE_CURSOR_LOOP_OR_EMPTY_PAGE")
        cursors.add(cursor)
        query["cursor"] = cursor
    raise RateLimited("HISTORY_PAGE_BOUND_REACHED")


def activity(rows, start, end) -> dict:
    selected = [r for r in rows if start <= stamp(r["time"]) < end]
    active_hours = {stamp(r["time"]).replace(minute=0, second=0, microsecond=0) for r in selected}
    return {
        "trade_count": sum(r["tradeCount"] for r in selected),
        "candle_volume_raw": str(sum((Decimal(r["volume"]) for r in selected), Decimal(0))),
        "hours_with_trades": len(active_hours),
        "hours_without_trades": int((end - start).total_seconds() // 3600) - len(active_hours),
        "latest_trade_hour": max((r["time"] for r in selected), default=None),
    }


def distribution(markets, window, expected_count=None):
    complete = [r for r in markets if r["status"] == "PASS"]
    counts = sorted(r[window]["trade_count"] for r in complete)
    total = sum(counts)
    n = len(counts)
    return {
        "markets_analyzed": n,
        "markets_unknown_or_failed": (len(markets) if expected_count is None else expected_count)
        - n,
        "zero_trades": sum(x == 0 for x in counts),
        "at_most_5_trades": sum(x <= 5 for x in counts),
        "at_most_10_trades": sum(x <= 10 for x in counts),
        "at_most_50_trades": sum(x <= 50 for x in counts),
        "median_trades": str((Decimal(counts[(n - 1) // 2]) + counts[n // 2]) / 2) if n else None,
        "min_trades": min(counts, default=None),
        "max_trades": max(counts, default=None),
        "total_trades": total,
        "top_10_share_of_trades": str(Decimal(sum(counts[-10:])) / total) if total else None,
    }


def price_movement(rows, start, end):
    """Observed traded prices, in probability percentage points; never fill sparse bars."""
    selected = sorted(
        (r for r in rows if start <= stamp(r["time"]) < end), key=lambda r: stamp(r["time"])
    )
    if not selected:
        return {"status": "NOT_EVALUATED", "code": "NO_TRADES_IN_WINDOW"}
    values = []
    for row in selected:
        if any(row[k] is None for k in ("open", "high", "low", "close")):
            return {"status": "UNKNOWN", "code": "NULL_TRADED_OHLC"}
        prices = {k: Decimal(str(row[k])) for k in ("open", "high", "low", "close")}
        if any(not p.is_finite() or not 0 <= p <= 1 for p in prices.values()) or not (
            prices["low"]
            <= min(prices["open"], prices["close"])
            <= max(prices["open"], prices["close"])
            <= prices["high"]
        ):
            raise SchemaMismatch("HISTORY_INVALID_TRADED_OHLC")
        values.append(prices)
    prior = [r for r in rows if stamp(r["time"]) < start]
    baseline = max(prior, key=lambda r: stamp(r["time"])) if prior else None
    baseline_price = None
    if baseline is not None and baseline["close"] is not None:
        baseline_price = Decimal(str(baseline["close"]))
        if not baseline_price.is_finite() or not 0 <= baseline_price <= 1:
            raise SchemaMismatch("HISTORY_INVALID_BASELINE_PRICE")
    first, last = values[0]["open"], values[-1]["close"]
    low, high = min(r["low"] for r in values), max(r["high"] for r in values)
    return {
        "status": "PASS",
        "first_trade_hour": selected[0]["time"],
        "last_trade_hour": selected[-1]["time"],
        "first_open": str(first),
        "last_close": str(last),
        "low": str(low),
        "high": str(high),
        "net_open_to_close_pp": str((last - first) * 100),
        "abs_net_open_to_close_pp": str(abs(last - first) * 100),
        "range_pp": str((high - low) * 100),
        "baseline_trade_hour": baseline["time"] if baseline is not None else None,
        "baseline_close": str(baseline_price) if baseline_price is not None else None,
        "net_from_previous_close_pp": str((last - baseline_price) * 100)
        if baseline_price is not None
        else None,
        "abs_net_from_previous_close_pp": str(abs(last - baseline_price) * 100)
        if baseline_price is not None
        else None,
    }


def price_distribution(markets, expected_count):
    result = {}
    for metric in ("abs_net_from_previous_close_pp", "abs_net_open_to_close_pp", "range_pp"):
        values = sorted(
            Decimal(r["price_24h"][metric])
            for r in markets
            if r["status"] == "PASS"
            and r.get("price_24h", {}).get("status") == "PASS"
            and r["price_24h"].get(metric) is not None
        )
        n = len(values)
        result[metric] = {
            "markets_analyzed": n,
            "markets_unknown_or_failed": expected_count - n,
            "median_pp": str(median(values)) if n else None,
            "mean_pp": str(sum(values, Decimal(0)) / n) if n else None,
            "max_pp": str(max(values)) if n else None,
            "thresholds": {
                str(threshold): {
                    "count": sum(v <= threshold for v in values),
                    "pct_of_analyzed": str(Decimal(sum(v <= threshold for v in values)) * 100 / n)
                    if n
                    else None,
                    "pct_of_catalog": str(
                        Decimal(sum(v <= threshold for v in values)) * 100 / expected_count
                    )
                    if expected_count
                    else None,
                }
                for threshold in (Decimal("0.5"), Decimal("1"), Decimal("2"), Decimal("5"))
            },
        }
    return result


def run_history(args, contract_file):
    if not args.authorize_read:
        raise AuthorizationRequired("EXPLICIT_HISTORY_READ_REQUIRED")
    if (
        not 1 <= args.days <= 30
        or not 10 <= args.max_requests <= 2000
        or not 60 <= args.seconds <= 1800
    ):
        raise ValueError("invalid finite history bounds")
    output = Path(args.output)
    if "reports" not in output.parts or output.suffix != ".json":
        raise ValueError("history output must be a reports JSON file")
    resolution = getattr(args, "resolution", "1h")
    daily_study = getattr(args, "daily_study", False)
    if daily_study and resolution != "5m":
        raise ValueError("daily study requires --resolution 5m")
    end = datetime.now(timezone.utc)
    if daily_study:
        end = end.astimezone(ZoneInfo("Asia/Shanghai")).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = end.astimezone(timezone.utc)
    else:
        interval_minutes = {"1m": 1, "5m": 5, "1h": 60}[resolution]
        end = end.replace(
            minute=(end.minute // interval_minutes) * interval_minutes, second=0, microsecond=0
        )
    start = end - timedelta(days=args.days)
    day = end - timedelta(days=1)
    reads = HistoryReads(
        load_key(args.env_file),
        tournament_slug=args.tournament_slug,
        root=Path("runtime/readonly"),
        contract_file=contract_file,
        max_requests=args.max_requests,
        seconds=args.seconds,
    )
    report = {
        "layer": "REAL_READ",
        "status": "RUNNING",
        "run_id": reads.run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "window_from_inclusive": start.isoformat(),
        "window_to_exclusive": end.isoformat(),
        "days": args.days,
        "resolution": resolution,
        "calendar_timezone": "Asia/Shanghai" if daily_study else None,
        "daily_study_enabled": daily_study,
        "markets": [],
        "trade_tape_audits": [],
        "historical_reach_samples": [],
        "real_writes": 0,
        "real_orders": 0,
        "real_cancels": 0,
        "account_modifications": 0,
        "prices": "YES-normalized traded OHLCV, not historic orderbook quotes",
        "volume_unit": "Candle.volume has no unit description; compare tape size and notional below",
        "thresholds": "Research descriptions, not venue rules: zero, <=5, <=10, <=50 trades per requested window",
        "scope": "Current tournament catalog; API projection coverage, not an atomic engine snapshot",
        "price_metric_semantics": {
            "range_pp": "100 * (highest traded YES price - lowest traded YES price) within the fixed 24h window",
            "net_open_to_close_pp": "100 * (last traded close - first traded open) within the window",
            "net_from_previous_close_pp": "100 * (last traded close - last observed close before the window); baseline hour retained, not an exact-time executable quote or webpage 24h label",
            "missing": "No trades, null OHLC, missing baseline, failed coverage or multiple exchanges remain unknown/not evaluated; never zero-filled",
        },
        "trade_tape_audit_status": "NOT_RUN_BY_REQUEST"
        if getattr(args, "skip_tape_audits", False)
        else "RUNNING",
        "shared_local_throttle": "80 READ/minute cap and >=1s spacing; renewable session slices; finite outer bound",
    }
    save(output, report)
    try:
        account = reads.call("account_identity")
        if not account["id"] or account["id"].startswith("SIM-"):
            raise AuthorizationRequired("REAL_ACCOUNT_REQUIRED")
        reads.reader.config.expected_account_id = account["id"]
        cups, _ = reads.check(
            "tournament_list",
            lambda: reads.pages(
                "tournament_list", query={"status": "any", "limit": 100, "offset": 0}
            ),
        )
        matches = [
            r for r in cups if r["slug"] == args.tournament_slug and not r["isPendingEnrolment"]
        ]
        if len(matches) != 1:
            raise AuthorizationRequired("HISTORY_TOURNAMENT_AMBIGUOUS_OR_PENDING")
        cup = matches[0]
        tid = cup["id"]
        reads.reader.config.expected_tournament_id = tid
        report.update(
            tournament_id=tid,
            tournament_start_date=cup["startDate"],
            tournament_status=cup["status"],
        )
        markets, _ = reads.check(
            "market_list",
            lambda: reads.pages(
                "market_list", query={"tournamentId": tid, "status": "any", "limit": 100}
            ),
        )
        report["catalog"] = dict(reads.checks["market_list"])
        report["catalog_market_count"] = len(markets)
        exchange_ids = set()
        for market in markets:
            contexts = market["contexts"]
            if len(contexts) != 1 or {e["id"] for e in contexts[0]["exchanges"]} != {
                e["id"] for e in market["exchanges"]
            }:
                raise SchemaMismatch("HISTORY_CATALOG_CONTEXT_MISMATCH")
            row = {
                "market_id": market["id"],
                "title": market["title"],
                "market_status": contexts[0]["status"],
                "created_at": market["createdAt"],
                "binary_single_exchange": not market["isMultiOutcome"]
                and len(market["exchanges"]) == 1,
                "exchange_ids": [],
                "status": "RUNNING",
            }
            report["markets"].append(row)
            try:
                combined, checkpoints, pages = [], [], 0
                for exchange in market["exchanges"]:
                    eid = exchange["id"]
                    if eid in exchange_ids:
                        raise SchemaMismatch("HISTORY_DUPLICATE_EXCHANGE")
                    exchange_ids.add(eid)
                    row["exchange_ids"].append(eid)
                    result = candles(
                        reads, market["id"], eid, tid, start, end, resolution=resolution
                    )
                    # Aggregate multiple exchanges by bucket without double-counting hours.
                    combined.extend(result["candles"])
                    checkpoints.extend(result["coverage"])
                    pages += result["pages"]
                buckets: dict[str, dict] = {}
                for candle in combined:
                    at = candle["time"]
                    bucket = buckets.setdefault(
                        at, {"time": at, "tradeCount": 0, "volume": Decimal(0)}
                    )
                    bucket["tradeCount"] += candle["tradeCount"]
                    bucket["volume"] += Decimal(candle["volume"])
                row.update(
                    status="PASS"
                    if row["exchange_ids"]
                    and all(c["projectedThroughSequence"] is not None for c in checkpoints)
                    else "UNKNOWN",
                    coverage=checkpoints,
                    pages=pages,
                    last_24h=activity(buckets.values(), day, end),
                    full_window=activity(buckets.values(), start, end),
                    price_24h=price_movement(combined, day, end)
                    if len(row["exchange_ids"]) == 1
                    and all(c["projectedThroughSequence"] is not None for c in checkpoints)
                    else {"status": "UNKNOWN", "code": "MULTIPLE_EXCHANGES_OR_MISSING_CHECKPOINT"},
                )
                if daily_study and row["status"] == "PASS" and len(row["exchange_ids"]) == 1:
                    from sigbot.history_study import daily_range_study

                    row["daily_study"] = daily_range_study(
                        combined, start, end, binary=row["binary_single_exchange"]
                    )
            except (AuthorizationRequired, RateLimited):
                raise
            except SigbotError as exc:
                row.update(status="FAIL", code=exc.code, **reads.reader.transport.last_response)
            report["read_requests"] = reads.total_attempts
            save(output, report)
        complete = sorted(
            [r for r in report["markets"] if r["status"] == "PASS" and len(r["exchange_ids"]) == 1],
            key=lambda r: (r["last_24h"]["trade_count"], r["market_id"]),
        )
        samples = {r["market_id"]: r for r in complete[:3] + complete[-3:]}
        if getattr(args, "skip_tape_audits", False):
            samples = {}
        for row in samples.values():
            mid, eid = row["market_id"], row["exchange_ids"][0]
            result = tape(reads, mid, eid, tid, day, end)
            trades = result.pop("trades")
            shares = sum(t["size"] for t in trades)
            notional = sum((Decimal(t["volume"]) for t in trades), Decimal(0))
            raw_volume = Decimal(row["last_24h"]["candle_volume_raw"])
            report["trade_tape_audits"].append(
                {
                    "market_id": mid,
                    "exchange_id": eid,
                    "trade_count": len(trades),
                    "shares": shares,
                    "side_notional_SUSQies": str(notional),
                    "candle_trade_count_matches": len(trades) == row["last_24h"]["trade_count"],
                    "candle_volume_matches_shares": raw_volume == shares,
                    "candle_volume_matches_notional": raw_volume == notional,
                    **result,
                }
            )
            private_write(
                reads.private / f"tape-{eid}.json", json.dumps(trades, default=str).encode()
            )
            history = candles(reads, mid, eid, tid, datetime(1970, 1, 1, tzinfo=timezone.utc), end)
            historical = history.pop("candles")
            report["historical_reach_samples"].append(
                {
                    "market_id": mid,
                    "exchange_id": eid,
                    "first_trade_hour": min((c["time"] for c in historical), default=None),
                    "last_trade_hour": max((c["time"] for c in historical), default=None),
                    "hours_with_trades": len(historical),
                    "trade_count": sum(c["tradeCount"] for c in historical),
                    **history,
                }
            )
            save(output, report)
        if not getattr(args, "skip_tape_audits", False):
            report["trade_tape_audit_status"] = "PASS" if samples else "NOT_EVALUATED"
        report["status"] = (
            "PASS"
            if len(report["markets"]) == len(markets)
            and all(r["status"] == "PASS" for r in report["markets"])
            and all(r["candle_trade_count_matches"] for r in report["trade_tape_audits"])
            and all(r["projection_checkpoint_present"] for r in report["trade_tape_audits"])
            and all(r["projection_checkpoint_present"] for r in report["historical_reach_samples"])
            else "PARTIAL"
        )
    except SigbotError as exc:
        report.update(
            status="PARTIAL", stop_code=exc.code, stop_response=reads.reader.transport.last_response
        )
        if reads.reader.transport.last_response.get("http_status") == 429:
            reads.budget.backoff(
                reads.reader.transport.last_response.get("retry_after_seconds", 60)
            )
    finally:
        reads.reader.close()
        reads.key = ""
        report.update(
            read_requests=reads.total_attempts,
            finished_at=datetime.now(timezone.utc).isoformat(),
            request_receipts=reads.requests,
            errors=dict(
                Counter(
                    r.get("code", r["status"]) for r in report["markets"] if r["status"] != "PASS"
                )
            ),
            distribution_24h=distribution(
                report["markets"], "last_24h", report.get("catalog_market_count")
            ),
            distribution_full_window=distribution(
                report["markets"], "full_window", report.get("catalog_market_count")
            ),
            price_distribution_24h=price_distribution(
                report["markets"], report.get("catalog_market_count", len(report["markets"]))
            ),
        )
        save(output, report)
    return {
        k: v
        for k, v in report.items()
        if k not in {"markets", "request_receipts", "historical_reach_samples"}
    }, 0 if report["status"] == "PASS" else 5
