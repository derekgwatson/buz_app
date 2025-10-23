# services/buz_cookies.py
from __future__ import annotations
import json
from http.cookiejar import Cookie, CookieJar
from pathlib import Path
from typing import Dict


def cookies_from_storage_state(path: str | Path, domain: str) -> Dict[str, str]:
    """
    Read Playwright storageState (JSON) and return a dict of cookies for given domain.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    jar: Dict[str, str] = {}
    for c in data.get("cookies", []):
        # include subdomains too (".go.buzmanager.com")
        if c.get("domain") and domain.endswith(c["domain"].lstrip(".")) or c["domain"].lstrip(".").endswith(domain):
            jar[c["name"]] = c["value"]
    return jar
