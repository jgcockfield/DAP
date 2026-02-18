from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Set
import os
import time


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def send_emails(
    cfg: Any,
    prospects: List[Dict[str, Any]],
    updates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Builds the email send queue, executes SendGrid send, and returns results."""

    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    sg = SendGridAPIClient(getattr(cfg, "sendgrid_api_key", None) or os.getenv("SENDGRID_API_KEY", ""))

    to_email: List[Dict[str, Any]] = []

    for p in prospects:
        if (p.get("status") or "").strip().lower() == "contacted":
            continue
        if (p.get("send_status") or "").strip().lower() in {"pending", "queued", "sent", "suppressed"}:
            continue

        raw = p.get("all_emails") or p.get("primary_email") or ""
        emails = [e.strip() for e in raw.split(",") if e.strip()]

        selected = emails
        if not selected:
            continue

        for email in selected:
            # Optional suppression check
            if os.getenv("ENABLE_SUPPRESSION_CHECK", "false").lower() == "true":
                try:
                    sup_resp = sg.client.suppression.unsubscribes.get(query_params={"email": email})
                    if getattr(sup_resp, "status_code", None) == 200 and sup_resp.body:
                        item = {"prospect": p, "email": email, "send_status": "suppressed"}
                        to_email.append(item)
                        continue
                except Exception:
                    pass

            message = Mail(
                from_email=getattr(cfg, "from_email", None) or os.getenv("FROM_EMAIL", ""),
                to_emails=email,
                subject=getattr(cfg, "email_subject", None) or os.getenv("EMAIL_SUBJECT", ""),
                plain_text_content=getattr(cfg, "email_body", None) or os.getenv("EMAIL_BODY", ""),
            )

            item: Dict[str, Any] = {"prospect": p, "email": email}

            try:
                # Retry transient 5xx up to 3 attempts with exponential backoff (1s, 2s)
                resp = None
                status_code = None
                attempts_made = 0
                retried = False
                for attempt in range(3):
                    attempts_made = attempt + 1
                    resp = sg.client.mail.send.post(request_body=message.get())
                    status_code = getattr(resp, "status_code", None)
                    if status_code == 202:
                        break
                    if isinstance(status_code, int) and 500 <= status_code <= 599 and attempt < 2:
                        retried = True
                        time.sleep(2 ** attempt)
                        continue
                    break

                # Telemetry
                item["attempts"] = attempts_made
                item["retried"] = retried
                item["sendgrid_status_code"] = status_code

                headers = getattr(resp, "headers", {}) or {}

                msg_id = (
                    headers.get("X-Message-Id")
                    or headers.get("x-message-id")
                )

                if status_code == 202:
                    item["send_status"] = "sent"
                    item["sendgrid_message_id"] = msg_id
                else:
                    item["send_status"] = "failed"
                    item["error"] = f"SendGrid status={status_code}"
            except Exception as e:
                item["send_status"] = "failed"
                item["error"] = str(e)

            to_email.append(item)

    return {"to_email": to_email}


def mark_sent(
    website_url: str,
    sendgrid_message_id: str | None = None,
) -> Dict[str, Any]:
    """Builds a sheet update payload after a successful SendGrid send."""

    now = _utc_now_iso()

    return {
        "website_url": website_url,
        "send_status": "sent",
        "sent_at": now,
        "last_emailed_at": now,
        "sendgrid_message_id": sendgrid_message_id or "",
    }


def mark_suppressed(
    website_url: str,
) -> Dict[str, Any]:
    """Builds a sheet update payload when an email is on SendGrid suppression/unsubscribe."""

    now = _utc_now_iso()

    return {
        "website_url": website_url,
        "send_status": "suppressed",
        "suppressed_at": now,
    }


def mark_failed(
    website_url: str,
    error: str,
) -> Dict[str, Any]:
    """Builds a sheet update payload after a failed SendGrid send."""

    now = _utc_now_iso()

    return {
        "website_url": website_url,
        "send_status": "failed",
        "failed_at": now,
        "send_error": (error or "")[:500],
    }
