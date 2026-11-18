"""Open the operator's existing local trading database."""

import json
from pathlib import Path

from sigbot.errors import StorageUnavailable
from sigbot.storage import Store

CONFIG = Path("runtime/live/strategies/mm_sig_v1/database.local.json")


def open_store(namespace="readonly"):
    if not CONFIG.is_file():
        raise StorageUnavailable("MM_REAL_DATABASE_NOT_CONFIGURED")
    return Store(json.loads(CONFIG.read_text())["database_url"], namespace)
