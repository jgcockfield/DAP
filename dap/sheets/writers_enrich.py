# dap/sheets/writers_enrich.py

from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlsplit, urlunsplit

from .client import SheetsConfig, open_worksheets


def _header_index(header: list[str]) -> dict[str, int]:
    return {h.strip(): i for i, h in enumerate(header) if h.strip()}


def _ensure_row_width(row: list[Any], width: int) -> list[str]:
    out = [(str(x) if x is not None else "") for x in row]
    if len(out) < width:
        out.extend([""] * (width - len(out)))
    return out[:width]


def apply_enrichment(cfg: SheetsConfig, updates: List[Dict[str, Any]]) -> int:
    """Writes enrichment updates back to prospects, keyed by website_url.

    - Never overwrites non-empty cells with empty values.
    - Notes appends with " | ".
    """
    if not updates:
        return 0

    def _norm(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return ""
        p = urlsplit(u)
        if not p.scheme:
            p = urlsplit("https://" + u)
        return urlunsplit((p.scheme, p.netloc, "", "", ""))

    prospects_ws, _ = open_worksheets(cfg)
    values = prospects_ws.get_all_values()
    if not values:
        raise RuntimeError("Prospects sheet is empty (missing header row).")

    header = [h.strip() for h in values[0]]
    idx = _header_index(header)

    if "website_url" not in idx:
        raise RuntimeError("Prospects sheet missing required column: website_url")

    # Map normalized website_url -> (row_num, row_values)
    rows_by_url: dict[str, tuple[int, list[str]]] = {}
    for i, r in enumerate(values[1:], start=2):
        row = _ensure_row_width(r, len(header))
        url = _norm(row[idx["website_url"]])
        if url:
            rows_by_url[url] = (i, row)

    writes = 0

    for up in updates:
        url = _norm(up.get("website_url") or up.get("url") or "")
        if not url:
            continue

        hit = rows_by_url.get(url)
        if not hit:
            continue

        row_num, existing = hit
        updated = existing[:]
        changed = False

        for col, val in up.items():
            if col not in idx:
                continue

            v = "" if val is None else str(val).strip()
            if not v:
                continue

            j = idx[col]

            if col == "notes":
                if updated[j]:
                    updated[j] = f"{updated[j]} | {v}"
                else:
                    updated[j] = v
                changed = True

            elif col == "last_checked_at":
                # Always update timestamp
                if updated[j] != v:
                    updated[j] = v
                    changed = True

            else:
                # Only fill blanks for other fields
                if not updated[j]:
                    updated[j] = v
                    changed = True

        if changed:
            prospects_ws.update(f"A{row_num}", [updated], value_input_option="USER_ENTERED")
            writes += 1

    return writes
