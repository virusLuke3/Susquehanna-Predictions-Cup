"""Anonymous official reads, including POST /books. No trading methods."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from sigbot.domain import content_hash
from sigbot.errors import SchemaMismatch, TransportUnknown
from sigbot.readcheck import private_write
from sigbot.storage_policy import bounded_storage
from sigbot.strategies.prc_sig_v1.catalog import STATES, title_event

ORIGINS = {
    "POLYMARKET_SEARCH": "https://gamma-api.polymarket.com/public-search",
    "POLYMARKET_BOOK": "https://clob.polymarket.com/book",
    "POLYMARKET_BOOKS": "https://clob.polymarket.com/books",
    "POLYMARKET_MARKETS": "https://gamma-api.polymarket.com/markets",
    "POLYMARKET_INFO": "https://clob.polymarket.com/clob-markets/{condition_id}",
    "KALSHI_SERIES": "https://external-api.kalshi.com/trade-api/v2/series",
    "KALSHI_MARKETS": "https://external-api.kalshi.com/trade-api/v2/markets",
    "KALSHI_BOOK": "https://external-api.kalshi.com/trade-api/v2/markets/{ticker}/orderbook",
}


def utcnow():
    return datetime.now(timezone.utc)


class PublicReads:
    """Use the existing proxy route without changing it; ignore user curlrc.

    curl supports the host's SOCKS proxy without adding an SDK. Child environment
    contains routing/certificate variables only, and never the loaded SIG key.
    """

    def __init__(self, root: Path, *, proxy: str | None = None):
        if proxy is not None:
            u = urlsplit(proxy)
            if (
                u.scheme not in {"http", "socks5h"}
                or u.hostname != "127.0.0.1"
                or not u.port
                or u.username
                or u.password
                or u.path
                or u.query
                or u.fragment
            ):
                raise ValueError(
                    "public proxy must be an explicit localhost HTTP or SOCKS listener"
                )
        self.root = root
        self.proxy = proxy
        self.count = 0
        self.last_receipt: dict = {}

    def get(
        self,
        name: str,
        query: dict | None = None,
        ticker: str | None = None,
        condition_id: str | None = None,
    ):
        if name == "POLYMARKET_BOOKS":
            raise SchemaMismatch("ECR_BATCH_BOOKS_REQUIRES_DOCUMENTED_POST")
        return self._request(name, query, ticker, condition_id=condition_id)

    def books(self, token_ids: list[str]):
        if (
            not token_ids
            or len(set(token_ids)) != len(token_ids)
            or any(not isinstance(t, str) or not t.isdecimal() for t in token_ids)
        ):
            raise SchemaMismatch("ECR_INVALID_BATCH_BOOK_TOKENS")
        # Official anonymous market-data POST, not POST /order. Local chunk size
        # belongs to the caller; the docs do not specify a batch cardinality cap.
        return self._request("POLYMARKET_BOOKS", payload=[{"token_id": t} for t in token_ids])

    def _request(self, name, query=None, ticker=None, payload=None, condition_id=None):
        if not bounded_storage():
            return self._request_saved(name, query, ticker, payload, condition_id)
        from tempfile import TemporaryDirectory
        with TemporaryDirectory(prefix="sig-market-read-") as temporary:
            try:
                return self._request_saved(name, query, ticker, payload, condition_id, Path(temporary))
            finally:
                self.last_receipt["raw_path"] = None
                self.last_receipt["storage_policy"] = "CURRENT_MEMORY_ONLY"

    def _request_saved(self, name, query=None, ticker=None, payload=None, condition_id=None, output_root=None):
        if name not in ORIGINS:
            raise SchemaMismatch("ECR_UNKNOWN_PUBLIC_CAPABILITY")
        url = ORIGINS[name]
        if name == "KALSHI_BOOK":
            if not ticker or not re.fullmatch(r"[A-Za-z0-9_-]+", ticker):
                raise SchemaMismatch("ECR_INVALID_PUBLIC_TICKER")
            url = url.replace("{ticker}", ticker)
        if name == "POLYMARKET_INFO":
            if not isinstance(condition_id, str) or not re.fullmatch(
                r"0x[0-9a-fA-F]{64}", condition_id
            ):
                raise SchemaMismatch("ECR_INVALID_CONDITION_ID")
            url = url.replace("{condition_id}", condition_id)
        if query:
            url += "?" + urlencode(query, doseq=True)
        destination = (output_root or self.root) / (uuid.uuid4().hex + "-" + name + ".json")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination.parent.chmod(0o700)
        env = {
            k: v
            for k, v in os.environ.items()
            if k.lower()
            in {
                "path",
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
                "ssl_cert_file",
                "ssl_cert_dir",
                "curl_ca_bundle",
            }
        }
        started = utcnow()
        self.count += 1
        method = "POST" if payload is not None else "GET"
        route = ["--proxy", self.proxy, "--noproxy", ""] if self.proxy else []
        body = (
            [
                "--request",
                "POST",
                "--header",
                "Content-Type: application/json",
                "--data-binary",
                "@-",
            ]
            if payload is not None
            else []
        )
        result = subprocess.run(
            [
                "curl",
                "--disable",
                "--silent",
                "--show-error",
                "--proto",
                "=https",
                "--connect-timeout",
                "6",
                "--max-time",
                "30",
                "--max-filesize",
                "10000000",
                "--output",
                str(destination),
                "--write-out",
                "%{http_code}",
                *route,
                *body,
                url,
            ],
            capture_output=True,
            text=True,
            input=json.dumps(payload) if payload is not None else None,
            env=env,
            timeout=32,
        )
        if destination.exists():
            destination.chmod(0o600)
        self.last_receipt = {
            "capability": name,
            "method": method,
            "permission": "ANONYMOUS_MARKET_DATA",
            "explicit_proxy": self.proxy,
            "url": url,
            "started_at": started.isoformat(),
            "received_at": utcnow().isoformat(),
            "http_status": int(result.stdout) if result.stdout.isdecimal() else None,
            "curl_exit_code": result.returncode,
            "raw_path": str(destination),
            "auth_used": False,
            "response_sha256": hashlib.sha256(destination.read_bytes()).hexdigest()
            if destination.exists()
            else None,
        }
        private_write(
            destination.with_suffix(".receipt.json"), json.dumps(self.last_receipt).encode()
        )
        if result.returncode or result.stdout != "200":
            raise TransportUnknown("ECR_PUBLIC_READ_FAILED")
        try:
            raw = destination.read_bytes()
            data = json.loads(raw, parse_float=Decimal)
        except (ValueError, UnicodeError):
            raise SchemaMismatch("ECR_PUBLIC_JSON_INVALID") from None
        self.last_receipt["response_hash"] = content_hash(data)
        return data


def party(text: str) -> str | None:
    dem = bool(re.search(r"\b(democrat(?:ic|s)?|dems)\b|\(D\)", text, re.I))
    rep = bool(re.search(r"\b(republican(?:s)?|gop)\b|\(R\)", text, re.I))
    return "DEMOCRATIC" if dem and not rep else "REPUBLICAN" if rep and not dem else None


def verify_clob_tokens(data: dict, contract: dict) -> None:
    # Official /clob-markets response: t = tokens, t/o = token ID/outcome label.
    # https://docs.polymarket.com/api-reference/markets/get-clob-market-info
    tokens = data.get("t")
    if not isinstance(tokens, list) or len(tokens) != 2:
        raise SchemaMismatch("ECR_CLOB_TOKEN_SCHEMA_UNVERIFIED")
    actual = {}
    for token in tokens:
        if (
            not isinstance(token, dict)
            or not isinstance(token.get("o"), str)
            or not isinstance(token.get("t"), str)
        ):
            raise SchemaMismatch("ECR_CLOB_TOKEN_SCHEMA_UNVERIFIED")
        outcome = token["o"].upper()
        if outcome not in {"YES", "NO"} or outcome in actual or not token["t"].isdecimal():
            raise SchemaMismatch("ECR_CLOB_TOKEN_SCHEMA_UNVERIFIED")
        actual[outcome] = token["t"]
    if actual != contract.get("outcome_tokens"):
        raise SchemaMismatch("ECR_GAMMA_CLOB_TOKEN_BINDING_MISMATCH")


def geography(text: str) -> tuple | None:
    if re.search(r"\bstate\s+(?:senate|house)\b|\bassembly\b", text, re.I):
        return None
    offices = set(re.findall(r"\b(senate|house|governor|gubernatorial)\b", text, re.I))
    offices = {"governor" if x.lower() == "gubernatorial" else x.lower() for x in offices}
    if len(offices) > 1:
        return None
    # Keep West Virginia as one state, but reject a question spanning two states.
    spans = sorted(
        (
            m.span()
            for name in STATES.values()
            for m in re.finditer(r"\b" + name + r"\b", text, re.I)
        ),
        key=lambda s: s[1] - s[0],
        reverse=True,
    )
    selected: list[tuple[int, int]] = []
    for span in spans:
        if not any(a <= span[0] and span[1] <= b for a, b in selected):
            selected.append(span)
    if len(selected) > 1:
        # Repeated mentions of the same state are harmless.
        if len({text[a:b].casefold() for a, b in selected}) > 1:
            return None
    districts = re.findall(r"\b([A-Z]{2})-(\d{1,2}|AL)\b", text, re.I)
    if len({(s.upper(), d.upper().zfill(2)) for s, d in districts}) > 1:
        return None
    named_states = {
        s
        for s, name in STATES.items()
        if any(text[a:b].casefold() == name.casefold() for a, b in selected)
    }
    if districts and named_states and {s.upper() for s, _ in districts} != named_states:
        return None
    numbered_districts = re.findall(
        r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:congressional\s+)?district\b|\bdistrict\s+(\d{1,2})\b",
        text,
        re.I,
    )
    if len({(a or b).zfill(2) for a, b in numbered_districts}) > 1:
        return None
    # A lieutenant-governor election is a different office, even when the
    # question and its first rule paragraph also contain the word governor.
    if re.search(r"\b(?:lieutenant|lt\.?)\s+governor\b", text, re.I):
        return None
    if re.search(r"%|percentage|\bpoints?\b|\bby\s+\d", text, re.I):
        return None
    if re.search(r"\b(primary|runoff|margin|turnout|combo|popular vote|nominee)\b", text, re.I):
        return None
    # Official winner-rule paragraphs use "gubernatorial election" while their
    # questions use "governor race". Normalize the office, keeping geography,
    # election kind and winner checks unchanged.
    text = re.sub(r"\bgubernatorial\b", "governor", text, flags=re.I)
    event = title_event(text)
    if event.get("office") == "UNKNOWN" and re.search(r"congressional district|house", text, re.I):
        names = [
            s for s, n in STATES.items() if re.search(r"\b" + re.escape(n) + r"\b", text, re.I)
        ]
        if names:
            state = max(names, key=lambda s: len(STATES[s]))
            district = re.search(
                r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:congressional\s+)?district\b|\bdistrict\s+(\d{1,2})\b",
                text,
                re.I,
            )
            if district:
                return state, "HOUSE", (district[1] or district[2]).zfill(2)
            if re.search(r"\bat.large\b", text, re.I):
                return state, "HOUSE", "AL"
    if event.get("office") not in {"HOUSE", "SENATE", "GOVERNOR"}:
        return None
    return event["state"], event["office"], event["district"]


def monitoring_geography(text: str) -> tuple | None:
    geo = geography(text)
    if geo:
        return geo
    # The four national control questions are observation targets. Keep them
    # out of geography(), which is also used by the trading predicate audit.
    control = re.fullmatch(
        r"Will the (?:Democratic|Republican) Party "
        r"(?:win the U\.S\. (House|Senate)|control the (House|Senate) after the 2026 Midterm elections)\?",
        text,
        re.I,
    )
    return ("US", "CHAMBER_CONTROL", (control[1] or control[2]).upper()) if control else None


def discovery_query(row: dict) -> str | None:
    e = row["event_candidate"]
    if e.get("office") == "CHAMBER_CONTROL":
        geo = monitoring_geography(row["title"])
        return f"Which party will win the {geo[2].title()} in 2026?" if geo else None
    if e.get("office") not in {"HOUSE", "SENATE", "GOVERNOR"}:
        return None
    district = " " + e["district"] if e["office"] == "HOUSE" else ""
    return f"{STATES[e['state']]} {e['office'].title()}{district} 2026"


def polymarket_candidates(data: dict) -> list[dict]:
    if not isinstance(data.get("events") or [], list) or not isinstance(
        data.get("pagination"), dict
    ):
        raise SchemaMismatch("ECR_SEARCH_SCHEMA_INVALID")
    result = []
    for event in data.get("events") or []:
        for market in event.get("markets", []):
            question = market.get("question", "")
            if not isinstance(question, str):
                continue
            if market.get("closed") is not False or market.get("active") is not True:
                continue
            geo, subject = monitoring_geography(question), party(question)
            if not geo or not subject:
                continue
            years = set(re.findall(r"\b20\d{2}\b", question))
            if years:
                if years != {"2026"}:
                    continue
            else:
                # House questions omit the year; their official winner rule
                # states it explicitly. Closing dates and event names are not
                # evidence of an election year.
                opening = str(market.get("description") or "").split("\n\n", 1)[0]
                if (
                    set(re.findall(r"\b20\d{2}\b", opening)) != {"2026"}
                    or geography(re.sub(r"U\.S\.\s*", "", opening)) != geo
                    or not re.match(
                        r"This market will resolve according to (?:the winner of the|the party of the candidate who wins the)\b",
                        opening,
                        re.I,
                    )
                ):
                    continue
            try:
                outcomes = (
                    json.loads(market["outcomes"])
                    if isinstance(market["outcomes"], str)
                    else market["outcomes"]
                )
                tokens = (
                    json.loads(market["clobTokenIds"])
                    if isinstance(market["clobTokenIds"], str)
                    else market["clobTokenIds"]
                )
                if (
                    not isinstance(outcomes, list)
                    or not isinstance(tokens, list)
                    or len(outcomes) != 2
                    or len(tokens) != 2
                    or any(not isinstance(x, str) for x in outcomes)
                    or {x.casefold() for x in outcomes} != {"yes", "no"}
                    or len(set(tokens)) != 2
                    or not isinstance(market.get("conditionId"), str)
                    or not market["conditionId"]
                ):
                    continue
                if any(not isinstance(t, str) or not t.isdecimal() for t in tokens):
                    continue
                outcome_tokens = dict(zip((x.upper() for x in outcomes), tokens, strict=True))
            except (KeyError, ValueError, TypeError):
                continue
            result.append(
                {
                    "operator": "POLYMARKET",
                    "contract_id": outcome_tokens["YES"],
                    "no_token": outcome_tokens["NO"],
                    "outcome_side": "YES",
                    "outcome_tokens": outcome_tokens,
                    "market_id": str(market["id"]),
                    "condition_id": market.get("conditionId"),
                    "geography": list(geo),
                    "party": subject,
                    "title": question,
                    "year": 2026,
                    "event_id": str(event["id"]),
                    "event_slug": event.get("slug"),
                    "market_slug": market.get("slug"),
                    "description": market.get("description"),
                    "resolution_source": market.get("resolutionSource"),
                    "raw_hash": content_hash(market),
                    "approved": False,
                }
            )
    return result


def kalshi_candidates(data: dict) -> list[dict]:
    if not isinstance(data.get("markets"), list) or not isinstance(data.get("cursor"), str):
        raise SchemaMismatch("ECR_KALSHI_MARKETS_SCHEMA_INVALID")
    result = []
    for m in data["markets"]:
        text = " ".join(str(m.get(k, "")) for k in ["title", "subtitle", "yes_sub_title"])
        geo, subject = geography(text), party(text)
        if m.get("status") not in {"active", "open"} or not geo or not subject:
            continue
        # Explicit year required; do not infer election year from ticker/closing date.
        if not re.search(r"\b2026\b", text):
            continue
        result.append(
            {
                "operator": "KALSHI",
                "contract_id": m["ticker"],
                "geography": list(geo),
                "party": subject,
                "title": text,
                "rules_primary": m.get("rules_primary"),
                "rules_secondary": m.get("rules_secondary"),
                "raw_hash": content_hash(m),
                "approved": False,
            }
        )
    return result


def matched(row: dict, external: list[dict]) -> list[dict]:
    geo, subject = monitoring_geography(row["title"]), party(row["title"])
    if not geo or not subject:
        return []
    return [x for x in external if tuple(x["geography"]) == geo and x["party"] == subject]


def monitoring_matches(
    row: dict, external: list[dict], *, market_id: str | None = None
) -> list[dict]:
    """Unique same-election questions for prices; never a trading approval."""
    candidates = []
    for contract in matched(row, external):
        if (
            market_id
            and contract["operator"] == "POLYMARKET"
            and contract.get("market_id") != market_id
        ):
            continue
        opening = str(contract.get("description") or contract.get("rules_primary") or "").split(
            "\n\n", 1
        )[0]
        if re.search(r"margin|vote share|popular vote|\bprimary\b|\bspecial\b", opening, re.I):
            continue
        title = contract["title"]
        winner = re.fullmatch(
            r"Will (?:the )?(?:Democrats?|Republicans?|(?:Democratic|Republican) Party(?: candidate)?) win (?:the )?.+\?",
            title,
            re.I,
        )
        if winner or tuple(contract["geography"])[1] == "CHAMBER_CONTROL":
            candidates.append(contract)
    operators = [c["operator"] for c in candidates]
    return [c for c in candidates if operators.count(c["operator"]) == 1]


def refresh_known_contracts(public, seeds: list[dict]) -> tuple[list[dict], dict]:
    """Re-fetch known Gamma IDs; this is a restart accelerator, not discovery coverage."""
    ids = sorted({s["market_id"] for s in seeds if s["operator"] == "POLYMARKET"})
    if any(not isinstance(i, str) or not i.isdecimal() for i in ids):
        raise SchemaMismatch("ECR_CACHED_GAMMA_ID_INVALID")
    found, returned = [], set()
    for offset in range(0, len(ids), 50):
        requested = ids[offset : offset + 50]
        data = public.get(
            "POLYMARKET_MARKETS", {"id": requested, "limit": len(requested), "offset": 0}
        )
        if not isinstance(data, list):
            raise SchemaMismatch("ECR_GAMMA_REFRESH_NOT_ARRAY")
        for market in data:
            mid = str(market["id"])
            if mid not in requested or mid in returned:
                raise SchemaMismatch("ECR_GAMMA_REFRESH_SCOPE_OR_DUPLICATE")
            returned.add(mid)
            # Event identity comes from the current official response, never a
            # guessed title/state broadcast or a cached approval.
            events = market.get("events") or []
            if len(events) != 1:
                continue
            parsed = polymarket_candidates(
                {"events": [{**events[0], "markets": [market]}], "pagination": {}}
            )
            for candidate in parsed:
                candidate["metadata_received_at"] = public.last_receipt["received_at"]
            found.extend(parsed)
    return found, {
        "known_ids_requested": len(ids),
        "known_ids_returned": len(returned),
        "missing_ids": sorted(set(ids) - returned),
        "current_candidates": len(found),
        "scope": "KNOWN_ID_REFRESH_NOT_FULL_EXTERNAL_DISCOVERY",
        "approved": False,
    }


def levels(values, quantity_key):
    result = []
    for row in values:
        p, q = Decimal(str(row["price"])), Decimal(str(row[quantity_key]))
        if not p.is_finite() or not q.is_finite() or not 0 < p < 1 or q <= 0:
            raise SchemaMismatch("ECR_INVALID_BOOK_LEVEL")
        result.append({"price": str(p), "size": str(q)})
    return result


def public_book(data: dict, contract: dict, receipt: dict) -> dict:
    operator = contract["operator"]
    if operator == "POLYMARKET":
        if (
            data.get("asset_id") != contract["contract_id"]
            or data.get("market") != contract["condition_id"]
        ):
            raise SchemaMismatch("ECR_EXTERNAL_BOOK_IDENTITY_MISMATCH")
        bids, asks = levels(data["bids"], "size"), levels(data["asks"], "size")
    elif operator == "KALSHI":
        book = data["orderbook_fp"]
        # Official fixed-point dollar and quantity strings; never use legacy cents.
        bids = levels([{"price": p, "size": q} for p, q in book["yes_dollars"]], "size")
        asks = levels(
            [{"price": str(Decimal(1) - Decimal(p)), "size": q} for p, q in book["no_dollars"]],
            "size",
        )
    else:
        raise SchemaMismatch("ECR_UNKNOWN_EXTERNAL_OPERATOR")
    source = None
    clock_status = "NOT_PROVIDED"
    if operator == "POLYMARKET" and data.get("timestamp") is not None:
        stamp = data["timestamp"]
        if not isinstance(stamp, str) or not stamp.isdecimal():
            raise SchemaMismatch("ECR_POLYMARKET_INVALID_EPOCH_MILLISECONDS")
        # Official OrderBook type: timestamp?: EpochMilliseconds | null.
        # https://docs.polymarket.com/market-data/prices-order-books
        millis = int(stamp)
        try:
            source = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=millis)
        except OverflowError:
            raise SchemaMismatch("ECR_POLYMARKET_INVALID_EPOCH_MILLISECONDS") from None
        received = datetime.fromisoformat(receipt["received_at"])
        if source > received or source.year < 2020:
            raise SchemaMismatch("ECR_POLYMARKET_SOURCE_CLOCK_INVALID")
        clock_status = "OFFICIAL_EPOCH_MILLISECONDS"
    return {
        "operator": operator,
        "contract_id": contract["contract_id"],
        "condition_id": contract.get("condition_id"),
        "outcome_side": "YES",
        "bids": bids,
        "asks": asks,
        "received_at": receipt["received_at"],
        "request_started_at": receipt["started_at"],
        "source_timestamp_raw": data.get("timestamp"),
        "source_at": source.isoformat() if source else None,
        "source_time_status": clock_status,
        "source_time_evidence": "https://docs.polymarket.com/market-data/prices-order-books"
        if source
        else None,
        "bid": max((Decimal(x["price"]) for x in bids), default=None),
        "ask": min((Decimal(x["price"]) for x in asks), default=None),
        "version": data.get("hash"),
        "mapping_approved": False,
    }
