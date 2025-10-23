# services/buz_web.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import requests


DEFAULT_BASE = os.environ.get("BUZ_LOGIN_BASE", "https://login.buzmanager.com")
DEFAULT_TIMEOUT = (5, 30)  # (connect, read) seconds


@dataclass
class NewUser:
    firstName: str
    lastName: str
    email: str
    assignedGroupId: int
    organizationId: int
    customerPkId: str
    address: str = ""
    mobile: str = ""
    id: str = "0"  # "0" means create
    culture: str = "en-AU"
    timezone: str = "AUS Eastern Standard Time"
    isForcedMFAEnabled: bool = False


class BuzClient:
    def __init__(self, token: str, base: str = DEFAULT_BASE, timeout: tuple = DEFAULT_TIMEOUT):
        """
        token: Bearer JWT for login.buzmanager.com
        base:  e.g. https://login.buzmanager.com
        """
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

    # ---- low-level HTTP ----
    def _get(self, path: str, **kwargs) -> requests.Response:
        url = f"{self.base}{path}"
        return self.s.get(url, timeout=self.timeout, **kwargs)

    def _post(self, path: str, json: Dict[str, Any]) -> requests.Response:
        url = f"{self.base}{path}"
        headers = {"Content-Type": "application/json"}
        return self.s.post(url, json=json, headers=headers, timeout=self.timeout)

    # ---- helpers ----
    def _normalize_email(self, email: str) -> str:
        return email.strip().lower()

    def find_user_by_email(self, email: str, page_size: int = 500) -> Optional[Dict[str, Any]]:
        """
        Basic scan of the Users list to find a matching email.
        If Buz exposes a search endpoint, swap this to that.
        """
        email_norm = self._normalize_email(email)
        page = 1
        while True:
            r = self._get(f"/Users?pageSize={page_size}&pageNo={page}")
            r.raise_for_status()
            data = r.json()

            users = data.get("data") or data.get("items") or data  # be defensive
            if not isinstance(users, list):
                # Unexpected shape; try best-effort
                users = []

            for u in users:
                u_email = (u.get("email") or u.get("Email") or "").strip().lower()
                if u_email == email_norm:
                    return u

            # Stop if there's no "next" signal; many APIs rely on count/total
            total = data.get("total") or data.get("TotalCount")
            if total and page * page_size >= int(total):
                break

            # Fallback: stop if we didn't get a full page
            if len(users) < page_size:
                break

            page += 1
            time.sleep(0.1)  # be nice

        return None

    def create_user(self, user: NewUser) -> Dict[str, Any]:
        """
        POST /identity/Organization/Users/ with the exact payload you captured.
        Returns the server response JSON (created user or status).
        """
        payload = asdict(user)
        r = self._post("/identity/Organization/Users/", json=payload)
        if r.status_code not in (200, 201):
            # Surface useful server error details
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"Create user failed ({r.status_code}): {detail}")
        # Some APIs return the created entity; others return a status/envelope
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code, "text": r.text}

    def ensure_user(self, user: NewUser) -> Dict[str, Any]:
        """
        Idempotent create: returns existing user if found, otherwise creates it.
        """
        existing = self.find_user_by_email(user.email)
        if existing:
            return {"status": "exists", "user": existing}
        created = self.create_user(user)
        return {"status": "created", "user": created}


# ---- example usage (wire up in your Flask command or admin route) ----
# from services.buz_web import BuzClient, NewUser
#
# token = os.environ["BUZ_BEARER_TOKEN"]  # <- populate this via your login flow/secret store
# client = BuzClient(token)
# result = client.ensure_user(NewUser(
#     firstName="test",
#     lastName="user",
#     email="a@c.com",
#     assignedGroupId=1075,
#     organizationId=134,
#     customerPkId="a93222a8-a251-48ab-ae3c-e1dfae62f01e",
# ))
# print(result)
