"""Simple JSON persistence helpers for MigraDB GUI user config."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from migradb_gui.connection import ConnectionPair

_CONFIG_DIR = Path.home() / ".migradb"
_CONFIG_DIR.mkdir(exist_ok=True)
_MAP_FILE = _CONFIG_DIR / "mappings.json"


def _load_file() -> Dict[str, Any]:
    if _MAP_FILE.exists():
        try:
            return json.loads(_MAP_FILE.read_text())
        except Exception:
            # corrupt file
            return {}
    return {}


def _save_file(data: Dict[str, Any]) -> None:
    _MAP_FILE.write_text(json.dumps(data, indent=2))


def _pair_key(pair: ConnectionPair) -> str:
    # use the two database names as a simple key; could include host/port for uniqueness
    return f"{pair.source.database}->{pair.target.database}"


def load_mappings(pair: ConnectionPair) -> Dict[str, Dict[str, str]]:  # noqa: D401
    data = _load_file()
    return data.get(_pair_key(pair), {})


def save_mappings(pair: ConnectionPair, maps: Dict[str, Dict[str, str]]) -> None:  # noqa: D401
    data = _load_file()
    data[_pair_key(pair)] = maps
    _save_file(data)
