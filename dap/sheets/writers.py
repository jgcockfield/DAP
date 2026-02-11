# dap/sheets/writers.py
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
    """Upserts a single prospect by `key` (default: domain).

    NOTE: This is safe for occasional writes, but it is NOT efficient for seeding many rows
    because it reads the whole sheet each call.
    """
    upsert_prospects(cfg, [row_dict], key=key)


def upsert_prospects(cfg: SheetsConfig, rows: list[dict[str, str]], key: str = "domain") -> int:
    """Batch upsert by `key` with ONE sheet read.

    Returns number of rows written (appends + updates).

    Merge rules:
      - Never overwrite non-empty cells with empty values.
      - `notes` appends with " | ".
    """
    if not rows:
        return 0

    prospects_ws, _ = open_worksheets(cfg)
    values = prospects_ws.get_all_values()
    if not values:
        raise RuntimeError("Prospects sheet is empty (missing header row).")

    header = [h.strip() for h in values[0]]
    idx = _header_index(header)

    if key not in idx:
        raise RuntimeError(f"Upsert key '{key}' not found in sheet header.")

    # Build lookup: key_val -> (row_num, existing_row)
    lookup: dict[str, tuple[int, list[str]]] = {}
    for i, r in enumerate(values[1:], start=2):
        cell = (r[idx[key]] if idx[key] < len(r) else "").strip().lower()
        if not cell:
            continue
        lookup[cell] = (i, _ensure_row_width(r, len(header)))

    to_append: list[list[str]] = []
    to_update: list[tuple[int, list[str]]] = []

    for row_dict in rows:
        target_val = (row_dict.get(key, "") or "").strip().lower()
        if not target_val:
            continue

        hit = lookup.get(target_val)
        if hit is None:
            new_row = [""] * len(header)
            for col, val in row_dict.items():
                if col in idx and val:
                    new_row[idx[col]] = val
            to_append.append(new_row)
            # reserve in lookup so duplicates in this batch don't double-insert
            lookup[target_val] = (-1, new_row)
        else:
            row_num, existing = hit
            # if row_num == -1, it was appended earlier in this batch; skip update
            if row_num == -1:
                continue

            updated = existing[:]
            changed = False
            for col, val in row_dict.items():
                if col not in idx or not val:
                    continue
                j = idx[col]
                if col == "notes":
                    if updated[j]:
                        updated[j] = f"{updated[j]} | {val}"
                    else:
                        updated[j] = val
                    changed = True
                else:
                    if not updated[j]:
                        updated[j] = val
                        changed = True

            if changed:
                to_update.append((row_num, updated))

    writes = 0

    # Append in one call if available, else loop (still OK — no extra reads)
    if to_append:
        append_rows = getattr(prospects_ws, "append_rows", None)
        if callable(append_rows):
            prospects_ws.append_rows(to_append, value_input_option="USER_ENTERED")
            writes += len(to_append)
        else:
            for r in to_append:
                prospects_ws.append_row(r, value_input_option="USER_ENTERED")
                writes += 1

    # Updates (range update per row; still OK for small batches)
    for row_num, updated in to_update:
        prospects_ws.update(f"A{row_num}", [updated], value_input_option="USER_ENTERED")
        writes += 1

    return writes


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


