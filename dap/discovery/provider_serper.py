from __future__ import annotations

import os
from typing import Dict, List
import requests


SERPER_ENDPOINT = "https://google.serper.dev/search"


def serper_search(query: str, limit: int = 10) -> List[Dict]:
    """
    Returns items like:
      {"title": "...", "link": "https://...", "snippet": "..."}
    """
    api_key = os.getenv("SERPER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing env var: SERPER_API_KEY")

    payload = {"q": query, "num": min(max(limit, 1), 100)}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}

    r = requests.post(SERPER_ENDPOINT, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json() or {}

    organic = data.get("organic") or []
    out: List[Dict] = []
    for item in organic:
        link = (item.get("link") or "").strip()
        if not link:
            continue
        out.append(
            {
                "title": (item.get("title") or "").strip(),
                "link": link,
                "snippet": (item.get("snippet") or "").strip(),
            }
        )
        if len(out) >= limit:
            break
    return out
