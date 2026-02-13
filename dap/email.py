from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Set


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def send_emails(
    cfg: Any,
    prospects: List[Dict[str, Any]],
    updates: List[Dict[str, Any]],
    contacted_emails: Set[str],
) -> Dict[str, Any]:
    """Builds the email send queue AND the sheet log updates.

    NOTE: This function still does NOT send SMTP. It only prepares:
      - to_email: list of {prospect, email}
      - log_updates: list of row updates keyed by website_url
    """

    to_email: List[Dict[str, Any]] = []
    log_updates: List[Dict[str, Any]] = []
    now = _utc_now_iso()

    for p in prospects:
        if p.get("status") == "contacted":
            continue

        raw = p.get("all_emails") or p.get("primary_email") or ""
        emails = [e.strip() for e in raw.split(",") if e.strip()]

        # only take emails we haven't contacted
        selected = [e for e in emails if e not in contacted_emails]
        if not selected:
            continue

        for email in selected:
            to_email.append({"prospect": p, "email": email})

        # one log row update per prospect
        log_updates.append(
            {
                "website_url": p.get("website_url", ""),
                "status": "contacted",
                "last_emailed_at": now,
                "emailed_to": ",".join(selected),
            }
        )

    return {"to_email": to_email, "log_updates": log_updates}
