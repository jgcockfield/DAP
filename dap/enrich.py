# dap/enrich.py

from urllib.parse import urlsplit, urlunsplit
from datetime import datetime
import html
import re


def enrich(prospects, crawl_results):
    updates = []

    def _clean_company_name(t: str) -> str:
        t = html.unescape((t or "").strip())
        if not t:
            return ""

        # Split on common title separators into chunks
        chunks = [c.strip() for c in re.split(r"\s*(?:\||—|–| - | :: | : )\s*", t) if c.strip()]
        if not chunks:
            return ""

        # Heuristic: prefer the chunk that looks like a real brand name (e.g., "Zollinger Law")
        generic_re = re.compile(r"\b(attorney|attorneys|lawyer|lawyers|immigration|criminal|defense|new\s+orleans|louisiana|baton\s+rouge|alexandria|lafayette)\b", re.I)
        brand_re = re.compile(r"\b(law|llc|pllc|pc|inc|ltd|group|firm|partners|associates|network)\b", re.I)

        def score(s: str) -> int:
            s2 = re.sub(r"\s+", " ", s).strip()
            sc = 0
            if brand_re.search(s2):
                sc += 5
            if not generic_re.search(s2):
                sc += 3
            if any(ch.isupper() for ch in s2):
                sc += 1
            if len(s2) <= 40:
                sc += 2
            elif len(s2) <= 60:
                sc += 1
            return sc

        best = max(chunks, key=score)
        best = re.sub(r"\s*\b(homepage|home)\b\s*$", "", best, flags=re.I).strip()
        best = re.sub(r"\s+", " ", best).strip()
        return best[:80]

    def _norm(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return ""
        p = urlsplit(u)
        if not p.scheme:
            p = urlsplit("https://" + u)
        return urlunsplit((p.scheme, p.netloc, "", "", ""))

    by_url = {_norm(r.get("url")): r for r in (crawl_results or []) if r.get("url")}

    for p in prospects or []:
        url = _norm(p.get("website_url"))
        if not url:
            continue

        r = by_url.get(url)
        if not r:
            continue

        primary = (r.get("primary_email") or "").strip()
        updates.append(
            {
                "website_url": url,
                "company_name": _clean_company_name(r.get("title", "")),
                "title": r.get("title", ""),
                "description": r.get("description", ""),
                "primary_email": primary,
                "all_emails": r.get("all_emails", ""),
                "contact_method": "email" if primary else "",
                "last_checked_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            }
        )

    return updates
