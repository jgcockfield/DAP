# dap/sheets/readers.py

from __future__ import annotations

from typing import Any

from .client import SheetsConfig, open_worksheets
from .schema import PROSPECT_COLUMNS_V1


def _rows_to_dicts(header: list[str], rows: list[list[Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for r in rows:
        row = [(str(x).strip() if x is not None else "") for x in r]
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        out.append(d)
    return out


def read_all_prospects(cfg: SheetsConfig) -> list[dict[str, str]]:
    """
    Reads the entire `prospects` worksheet into a list of dicts keyed by header columns.
    Assumes row 1 is the header row.
    """
    prospects_ws, _ = open_worksheets(cfg)
    values = prospects_ws.get_all_values()

    if not values:
        return []

    header = [h.strip() for h in values[0]]
    data_rows = values[1:]
    return _rows_to_dicts(header, data_rows)


def read_contacted_emails(prospects: list[dict[str, str]]) -> set[str]:
    """
    Builds a suppression set of primary emails already contacted.
    Uses `status == 'contacted'` and `primary_email` field if present.
    """
    contacted: set[str] = set()
    for p in prospects:
        status = (p.get("status", "") or "").strip().lower()
        if status == "contacted":
            email = (p.get("primary_email", "") or "").strip().lower()
            if email:
                contacted.add(email)
    return contacted


def validate_minimum_prospect_columns(header: list[str]) -> list[str]:
    """
    Returns a list of missing required v1 columns (if any), based on schema.py.
    Does not fail the run; caller decides policy.
    """
    header_set = {h.strip() for h in header if h.strip()}
    missing = [c for c in PROSPECT_COLUMNS_V1 if c not in header_set]
    return missing
