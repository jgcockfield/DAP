# dap/sheets/writers.py

from __future__ import annotations

from datetime import datetime
from typing import Any

from .client import SheetsConfig, open_worksheets
from .schema import PROSPECT_COLUMNS_V1, RUNS_COLUMNS_V1


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _header_index(header: list[str]) -> dict[str, int]:
    return {h.strip(): i for i, h in enumerate(header) if h.strip()}


def _ensure_row_width(row: list[Any], width: int) -> list[str]:
    out = [(str(x) if x is not None else "") for x in row]
    if len(out) < width:
        out.extend([""] * (width - len(out)))
    return out[:width]


def upsert_prospect(cfg: SheetsConfig, row_dict: dict[str, str], key: str = "domain") -> None:
    """
    Upserts a prospect by `key` (default: domain).
    - Never overwrites non-empty cells with empty values.
    - Appends notes rather than replacing.
    """
    prospects_ws, _ = open_worksheets(cfg)
    values = prospects_ws.get_all_values()
    if not values:
        raise RuntimeError("Prospects sheet is empty (missing header row).")

    header = [h.strip() for h in values[0]]
    idx = _header_index(header)

    if key not in idx:
        raise RuntimeError(f"Upsert key '{key}' not found in sheet header.")

    target_val = (row_dict.get(key, "") or "").strip().lower()
    if not target_val:
        raise RuntimeError(f"Upsert key '{key}' value is empty.")

    # Find existing row
    row_num = None
    for i, r in enumerate(values[1:], start=2):
        cell = (r[idx[key]] if idx[key] < len(r) else "").strip().lower()
        if cell == target_val:
            row_num = i
            existing = _ensure_row_width(r, len(header))
            break

    if row_num is None:
        # Insert new row in canonical order
        new_row = [""] * len(header)
        for col, val in row_dict.items():
            if col in idx and val:
                new_row[idx[col]] = val
        prospects_ws.append_row(new_row, value_input_option="USER_ENTERED")
        return

    # Update existing row (merge rules)
    updated = existing[:]
    for col, val in row_dict.items():
        if col not in idx:
            continue
        if not val:
            continue
        if col == "notes":
            if updated[idx[col]]:
                updated[idx[col]] = f"{updated[idx[col]]} | {val}"
            else:
                updated[idx[col]] = val
        else:
            if not updated[idx[col]]:
                updated[idx[col]] = val

    prospects_ws.update(f"A{row_num}", [updated], value_input_option="USER_ENTERED")


def append_run_log(cfg: SheetsConfig, run_row: dict[str, str]) -> None:
    """
    Appends a single run summary row to the `runs` worksheet.
    """
    _, runs_ws = open_worksheets(cfg)
    values = runs_ws.get_all_values()
    if not values:
        raise RuntimeError("Runs sheet is empty (missing header row).")

    header = [h.strip() for h in values[0]]
    idx = _header_index(header)

    row = [""] * len(header)
    for col, val in run_row.items():
        if col in idx and val:
            row[idx[col]] = val

    runs_ws.append_row(row, value_input_option="USER_ENTERED")
