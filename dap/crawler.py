from __future__ import annotations

import re
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def run(items, timeout_s: int = 10):
    """Fetch pages and extract emails.

    items: list[dict] where each item has at least {"url": "https://..."}
    Returns list[dict] with keys: url, status, title, description, primary_email, all_emails
    """

    def fetch(u: str):
        req = urllib.request.Request(u, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            html0 = resp.read().decode("utf-8", errors="ignore")
            status0 = getattr(resp, "status", 200)

        # strip scripts/styles
        html1 = re.sub(r"<script.*?>.*?</script>", " ", html0, flags=re.S | re.I)
        html1 = re.sub(r"<style.*?>.*?</style>", " ", html1, flags=re.S | re.I)

        # visible-ish text
        text0 = re.sub(r"<[^>]+>", " ", html1)
        text0 = re.sub(r"\s+", " ", text0).strip()
        return status0, html1, text0

    def extract_title(html: str) -> str:
        lo = html.lower()
        start = lo.find("<title>")
        end = lo.find("</title>")
        if start != -1 and end != -1 and end > start:
            return html[start + 7 : end].strip()
        return ""

    def extract_description(html: str) -> str:
        lo = html.lower()
        marker = 'name="description"'
        idx = lo.find(marker)
        if idx == -1:
            return ""
        content_idx = lo.find("content=", idx)
        if content_idx == -1:
            return ""
        quote = html[content_idx + 8 : content_idx + 9]
        if quote not in ("\"", "'"):
            return ""
        endq = html.find(quote, content_idx + 9)
        if endq == -1:
            return ""
        return html[content_idx + 9 : endq].strip()

    def extract_emails(text: str) -> list[str]:
        return sorted(set(_EMAIL_RE.findall(text or "")))

    results = []

    for item in items:
        raw_url = (item.get("url") or "").strip()
        if not raw_url:
            continue
        parts = urlsplit(raw_url)
        url = urlunsplit((parts.scheme, parts.netloc, "", "", ""))

        try:
            status, html, text = fetch(url)
            title = extract_title(html)
            description = extract_description(html)

            emails = extract_emails(text)

            # fallback: common contact paths
            if not emails:
                parts = urlsplit(url)
                base = urlunsplit((parts.scheme, parts.netloc, "", "", ""))
                for path in ("/contact", "/contact-us", "/contact/", "/contact-us/"):
                    try:
                        _, _, t2 = fetch(base + path)
                        emails = extract_emails(t2)
                        if emails:
                            break
                    except Exception:
                        continue

            primary_email = emails[0] if emails else ""

            results.append(
                {
                    "url": url,
                    "status": status,
                    "title": title,
                    "description": description,
                    "primary_email": primary_email,
                    "all_emails": ",".join(emails),
                }
            )

        except urllib.error.HTTPError as e:
            results.append({"url": url, "status": e.code, "primary_email": "", "all_emails": ""})
        except Exception as e:
            results.append({"url": url, "status": "error", "error": str(e)[:200], "primary_email": "", "all_emails": ""})

    return results



