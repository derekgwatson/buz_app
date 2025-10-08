# services/config_bridge.py
from __future__ import annotations
from typing import Any, Tuple
from flask import current_app, has_app_context
from services.config_service import ConfigManager

_CM = ConfigManager()

def _dig(d: dict | None, *keys: str) -> Any | None:
    node = d
    for k in keys:
        if not isinstance(node, dict) or k not in node:
            return None
        node = node[k]
    return node

def get_cfg(*keys: str, section: str = "lead_times", default=None):
    """
    Generic, file-backed config accessor (falls back to Flask app.config).
    - get_cfg()                    -> whole `section` dict (default 'lead_times')
    - get_cfg("a","b")            -> section['a']['b']
    - get_cfg("a.b")              -> dotted path
    - Back-compat nicety: if first key equals `section`, it is ignored
      (so get_cfg("lead_times") == get_cfg()).
    """
    # Accept dotted single arg
    if len(keys) == 1 and isinstance(keys[0], str) and "." in keys[0]:
        keys = tuple(keys[0].split("."))

    # Back-compat: drop duplicated root ("lead_times", ...)
    if keys and keys[0] == section:
        keys = keys[1:]

    # File-backed first
    root = _CM.get(section, default=None)

    # If file didn’t load this section, try Flask app.config
    if root is None and has_app_context():
        root = current_app.config.get(section)

    # If still nothing, but you want a clear diagnostic:
    if root is None:
        # If ConfigManager recorded a JSON parse error, surface that
        err = getattr(_CM, "last_load_error", None)
        if err:
            raise RuntimeError(
                "Config JSON is invalid.\n"
                f"File: {getattr(_CM, 'resolved_path', 'config.json')}\n"
                f"Line {err.lineno}, column {err.colno}: {err.msg}"
            )
        # Otherwise it’s just missing
        return default

    # No keys → whole section
    if not keys:
        return root

    # Drill down
    val = _dig(root, *keys)
    if val is None and has_app_context():
        val = _dig(current_app.config.get(section, {}), *keys)
    return default if val is None else val

def where_cfg(section: str = "lead_times") -> str:
    # Helpful for debugging: where is it reading from, and did JSON parse?
    path = getattr(_CM, "resolved_path", "<unknown>")
    loaded = _CM.get(section) is not None
    err = getattr(_CM, "last_load_error", None)
    return f"config.json path={path!r}; section_present={loaded}; last_error={err}"
