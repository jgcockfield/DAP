# dap/discovery/search_seed.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, List


def _repo_root() -> Path:
    # .../DAP/dap/discovery/search_seed.py -> .../DAP
    return Path(__file__).resolve().parents[2]


def _load_keywords_yml() -> Dict:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing dependency: PyYAML. Install with: pip install pyyaml") from e

    path = _repo_root() / "config" / "keywords.yml"
    if not path.exists():
        raise FileNotFoundError(f"keywords.yml not found at: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "packs" not in data or not isinstance(data["packs"], list):
        raise ValueError("keywords.yml must contain top-level key: packs: [ ... ]")
    return data


def _normalize_url(url: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if not u.startswith(("http://", "https://")):
        u = "https://" + u

    parts = urlsplit(u)
    scheme = parts.scheme.lower() or "https"
    netloc = parts.netloc
    path = parts.path or "/"

    # drop query + fragment (tracking)
    return urlunsplit((scheme, netloc, path, "", ""))


def _domain_from_url(url: str) -> str:
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    host = (parts.netloc or "").strip().lower()
    if "@" in host:
        host = host.split("@", 1)[1]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def discover(cfg, dry_run: bool = False) -> List[Dict]:
    """Phase 1 — Keyword discovery.

    Executes Serper searches for enabled keyword packs and returns seed candidates:
      {"url": "https://...", "domain": "example.com", "source_keyword": "...", "query": "...", "pack": "..."}

    This function does NOT write to Sheets.
    """

    from dap.discovery.provider_serper import serper_search

    data = _load_keywords_yml()

    # Build queries from packs
    queries: List[Dict] = []
    for pack in data.get("packs", []):
        if not isinstance(pack, dict):
            continue
        if not pack.get("enabled", False):
            continue

        pack_name = str(pack.get("name") or "").strip() or "unnamed_pack"
        keywords = pack.get("keywords") or []
        geo = pack.get("geo") or []

        for kw in keywords:
            kw_s = str(kw).strip()
            if not kw_s:
                continue

            if geo:
                for g in geo:
                    g_s = str(g).strip()
                    if not g_s:
                        continue
                    queries.append({"query": f"{kw_s} {g_s}", "pack": pack_name, "source_keyword": kw_s})
            else:
                queries.append({"query": kw_s, "pack": pack_name, "source_keyword": kw_s})

    # Execute searches and collect candidates (domain-level unique within this function)
    seen_domains = set()
    out: List[Dict] = []

    for q in queries:
        results = serper_search(q["query"], limit=10)
        for item in results:
            link = (item.get("link") or "").strip()
            if not link:
                continue

            norm_url = _normalize_url(link)
            domain = _domain_from_url(norm_url)
            if not domain:
                continue

            if domain in seen_domains:
                continue
            seen_domains.add(domain)

            out.append(
                {
                    "url": norm_url,
                    "domain": domain,
                    "source_keyword": q["source_keyword"],
                    "query": q["query"],
                    "pack": q["pack"],
                }
            )

    return out
