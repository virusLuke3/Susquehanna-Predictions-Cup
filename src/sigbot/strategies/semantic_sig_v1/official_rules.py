"""Extract official Info from public HTML and its linked static renderer.

No cookies, website JSON endpoints, credentials, JavaScript execution or order calls.
HTML/JS remain untrusted data; only JSON and string literals are decoded.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.strategies.semantic_sig_v1.registry import record_rules, write_json


class PageText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip = 0
        self.text = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.skip = max(0, self.skip - 1)

    def handle_data(self, data):
        if not self.skip and data.strip():
            self.text.append(data.strip())


def flight_text(html):
    if len(html.encode()) > 4 * 1024 * 1024:
        raise SchemaMismatch("SEMANTIC_PUBLIC_HTML_TOO_LARGE")
    parts = []
    for match in re.finditer(r"self\.__next_f\.push\(", html):
        try:
            item, _ = json.JSONDecoder().raw_decode(html[match.end() :])
        except ValueError:
            raise SchemaMismatch("SEMANTIC_PUBLIC_FLIGHT_INVALID") from None
        if isinstance(item, list) and len(item) > 1 and isinstance(item[1], str):
            parts.append(item[1])
    if not parts:
        raise SchemaMismatch("SEMANTIC_PUBLIC_FLIGHT_MISSING")
    return "".join(parts)


def resolve_text(value, flight):
    if not isinstance(value, str) or not value.strip():
        raise SchemaMismatch("SEMANTIC_OFFICIAL_RULE_TEXT_MISSING")
    if not value.startswith("$"):
        return value
    if not re.fullmatch(r"\$[0-9a-fA-F]+", value):
        raise SchemaMismatch("SEMANTIC_RULE_REFERENCE_UNSUPPORTED")
    data = flight.encode()
    pattern = rb"(?:^|\n)" + value[1:].encode() + rb":T([0-9a-fA-F]+),"
    matches = list(re.finditer(pattern, data))
    if len(matches) != 1:
        raise SchemaMismatch("SEMANTIC_RULE_REFERENCE_UNRESOLVED")
    match = matches[0]
    size = int(match[1], 16)
    if size > 128 * 1024 or len(data) < match.end() + size:
        raise SchemaMismatch("SEMANTIC_RULE_REFERENCE_TRUNCATED")
    return data[match.end() : match.end() + size].decode()


def public_market(html, expected):
    flight = flight_text(html)
    matches = []
    for match in re.finditer(r'"market":\s*\{', flight):
        start = flight.index("{", match.start())
        item, _ = json.JSONDecoder().raw_decode(flight[start:])
        if str(item.get("id")) == expected["sig_market_id"]:
            matches.append(item)
    if not matches or len({content_hash(m) for m in matches}) != 1:
        raise SchemaMismatch("SEMANTIC_PUBLIC_MARKET_AMBIGUOUS")
    market = matches[0]
    root = market.get("rootNode") or {}
    if (
        " ".join(market.get("title", "").split()) != " ".join(expected["market_title"].split())
        or str(root.get("marketId")) != expected["sig_market_id"]
        or {str(x["id"]) for x in market.get("exchanges", [])} != {expected["exchange_id"]}
        or expected["tournament_id"] not in {str(t["id"]) for t in market.get("tournaments", [])}
        or root.get("nodeType") != "contract"
    ):
        raise SchemaMismatch("SEMANTIC_PUBLIC_PAGE_ID_SCOPE_MISMATCH")
    return root, flight


def election_template(js):
    """Decode the exact literals of the official Party Winner Rules branch."""
    try:
        start = js.index('"Party Winner"===')
        end = js.index('"Margin of Victory"===', start)
        branch = js[start:end]
        match = re.search(r"children:\[(.*?)\]\}\)", branch)
        if match is None:
            raise ValueError("official rule children unavailable")
        literals = re.findall(r""""(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*' """, match[1], re.X)
        values = [ast.literal_eval(value) for value in literals]
        intro = next(
            s for s in values if s.startswith("This market will resolve based on which party wins ")
        )
        suffix = next(s for s in values if s.startswith('. Market resolves to "YES"'))
        election_start = js.index('"Election Outcome"===')
        election_end = js.index('"Sports Outcome"===', election_start)
        bullets = re.findall(
            r"""children:("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')""", js[election_start:election_end]
        )
        bullets = [ast.literal_eval(value) for value in bullets]
        bullets = [
            s
            for s in bullets
            if s.startswith(
                (
                    "Data source:",
                    "Market resolves to",
                    "Resolution is based on",
                    "In case of recounts",
                )
            )
        ]
        if len(bullets) != 4:
            raise ValueError("unexpected rule bullets")
    except (ValueError, AttributeError, StopIteration, SyntaxError):
        raise SchemaMismatch("SEMANTIC_OFFICIAL_INFO_RENDERER_CHANGED") from None
    return {
        "intro": intro,
        "suffix": suffix,
        "bullets": bullets,
        "fusion_ticket_multiple_yes": "fusion ticket" in suffix and "more than one party" in suffix,
    }


def verify_receipt(receipt, *, asset=False):
    url = urlparse(receipt["url"])
    path_allowed = bool(re.fullmatch(r"/markets/[0-9]+", url.path))
    if asset:
        path_allowed = bool(re.fullmatch(r"/_next/static/immutable/chunks/[^/]+\.js", url.path))
    if url.scheme != "https" or url.hostname != "sig.thesuper.market" or not path_allowed:
        raise SchemaMismatch("SEMANTIC_NON_PUBLIC_RULE_SOURCE_FORBIDDEN")
    utc_time(receipt["received_at"])
    body = Path(receipt["path"]).read_bytes()
    if hashlib.sha256(body).hexdigest() != receipt["sha256"]:
        raise SchemaMismatch("SEMANTIC_OFFICIAL_RULE_RECEIPT_CORRUPTED")
    return body


def extract_info(expected, receipt, renderer_receipt, settlement_policy):
    body = verify_receipt(receipt)
    renderer = verify_receipt(renderer_receipt, asset=True).decode()
    policy_url = urlparse(settlement_policy["url"])
    if (
        policy_url.scheme != "https"
        or policy_url.hostname != "sig.thesuper.market"
        or policy_url.path != "/docs/settlement-and-payouts"
    ):
        raise SchemaMismatch("SEMANTIC_GLOBAL_POLICY_SOURCE_INVALID")
    policy_body = Path(settlement_policy["path"]).read_bytes()
    if hashlib.sha256(policy_body).hexdigest() != settlement_policy["sha256"]:
        raise SchemaMismatch("SEMANTIC_GLOBAL_POLICY_HASH_MISMATCH")
    policy_parser = PageText()
    policy_parser.feed(policy_body.decode())
    if settlement_policy["text"] not in "\n".join(policy_parser.text):
        raise SchemaMismatch("SEMANTIC_GLOBAL_POLICY_TEXT_NOT_IN_SOURCE")
    root, flight = public_market(body.decode(), expected)
    details = root.get("contractDetails") or {}
    kind = root.get("contractType")
    template = None
    if kind == "Election Outcome" and details.get("resolutionType") == "Party Winner":
        template = election_template(renderer)
        if not details.get("raceName") or not details.get("winnerName"):
            raise SchemaMismatch("SEMANTIC_OFFICIAL_ELECTION_DETAILS_MISSING")
        info = template["intro"] + details["raceName"] + template["suffix"]
        info += "\n" + "\n".join(template["bullets"])
    elif kind == "Freeform":
        info = resolve_text(details.get("description") or details.get("details"), flight)
        details = {**details, "description": info}
    else:
        raise SchemaMismatch("SEMANTIC_OFFICIAL_RULE_TYPE_UNSUPPORTED")
    metadata = {
        "contractType": kind,
        "contractDetails": details,
        "nominalSettlementDate": root.get("settlementDate"),
        "local_market_key": expected["local_market_key"],
    }
    text = (
        info
        + "\n\n[Official contract metadata]\n"
        + json.dumps(metadata, ensure_ascii=False, sort_keys=True)
    )
    # General policy is separately identified and version-bound; not a per-race invention.
    text += "\n\n[Official global settlement policy]\n" + settlement_policy["text"]
    return {
        "sig_market_id": expected["sig_market_id"],
        "tournament_id": expected["tournament_id"],
        "source_url": receipt["url"],
        "received_at": max(
            receipt["received_at"],
            renderer_receipt["received_at"],
            settlement_policy["received_at"],
            key=utc_time,
        ),
        "text": text,
        "info_text": info,
        "contract_type": kind,
        "contract_details": details,
        "nominal_settlement_date": root.get("settlementDate"),
        "page_market_exchange_binding_verified": True,
        "info_extraction_method": "PUBLIC_SSR_JSON_AND_LINKED_OFFICIAL_RULES_RENDERER",
        "fusion_ticket_multiple_yes": template["fusion_ticket_multiple_yes"] if template else False,
        "selected_party_label_raw": details.get("winnerName"),
        "source_receipts": [receipt, renderer_receipt, settlement_policy],
        "published_at": None,
        "effective_from": None,
        "event_predicate_status": "OFFICIAL_INFO_FETCHED_HUMAN_REVIEW_REQUIRED",
        "trade_enabled": False,
    }


def import_public_rules(
    registry_path, receipts_path, renderer_receipt_path, settlement_path, rules_directory, output
):
    markets = json.loads(Path(registry_path).read_text())["markets"]
    receipts = json.loads(Path(receipts_path).read_text())
    renderer = json.loads(Path(renderer_receipt_path).read_text())
    policy = json.loads(Path(settlement_path).read_text())
    by_id = {}
    for r in receipts:
        mid = r.get("sig_market_id") or urlparse(r["url"]).path.rsplit("/", 1)[-1]
        by_id[mid] = r
    results, failures = [], []
    for market in markets:
        mid = market["sig_market_id"]
        receipt = by_id.get(mid)
        try:
            if receipt is None or receipt.get("http_status") != 200:
                raise SchemaMismatch("SEMANTIC_OFFICIAL_PAGE_MISSING_OR_FAILED")
            item = extract_info(market, receipt, renderer, policy)
            version = record_rules(rules_directory, item)
            results.append(
                {
                    "local_market_key": market["local_market_key"],
                    "race_id": market["race_id"],
                    "sig_market_id": mid,
                    "rules_hash": version["rules_hash"],
                    "source_url": version["source_url"],
                    "received_at": version["received_at"],
                    "contract_type": version["contract_type"],
                    "selected_party_label_raw": version["selected_party_label_raw"],
                    "fusion_ticket_multiple_yes": version["fusion_ticket_multiple_yes"],
                    "status": "RULES_FETCHED",
                    "trade_enabled": False,
                }
            )
        except (SchemaMismatch, KeyError, ValueError) as error:
            failures.append(
                {
                    "sig_market_id": mid,
                    "code": error.code
                    if isinstance(error, SchemaMismatch)
                    else "INVALID_PUBLIC_INFO_INPUT",
                }
            )
    result = {
        "layer": "REAL_READ_PUBLIC_RECEIPT_READBACK",
        "market_denominator": len(markets),
        "rules_imported": len(results),
        "failed": failures,
        "markets": results,
        "human_review_approved": 0,
        "real_orders": 0,
        "real_cancels": 0,
        "private_website_api_used": False,
        "cookies_used": False,
    }
    write_json(Path(output) / "official_rules_acquisition.json", result)
    return result
