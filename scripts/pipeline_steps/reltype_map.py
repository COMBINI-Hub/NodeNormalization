"""
Step 0 — generate_reltype_map

Reads the curation spreadsheet (ikraph_reltype_to_biolink_mapping_review.xlsx) and
RelTypeInt.json, resolves Biolink predicates (Halil's column > Cesar's > default), and
writes ikraph_reltype_map.csv.

Output columns:
  int_rep, ikraph_relation_type, proposed_biolink_predicate, source, subject_is_node_one

subject_is_node_one encodes whether the Biolink canonical subject maps to node_one in the
iKraph edge record (True) or node_two (False, meaning subject/object should be swapped).
The spreadsheet may optionally contain a "subject_is_node_one" column; if absent, all rows
default to True.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, Tuple

CLEAN_BIOLINK = re.compile(r"^biolink:[a-z][a-z0-9_]*$")
DEFAULT_PREDICATE = "biolink:related_to"


def _resolve_predicate(halil: str | None, cesar: str | None) -> tuple[str, str]:
    halil_clean = (halil or "").strip()
    cesar_clean = (cesar or "").strip()
    if CLEAN_BIOLINK.match(halil_clean):
        return halil_clean, "halil"
    if cesar_clean and CLEAN_BIOLINK.match(cesar_clean):
        return cesar_clean, "cesar"
    return DEFAULT_PREDICATE, "default"


def _load_xlsx(xlsx_path: Path) -> Dict[str, Tuple[str, str, str, bool]]:
    """
    Returns {int_rep: (rel_type_name, predicate, source, subject_is_node_one)}.

    Expected sheet layout (row 2 = headers, row 3+ = data):
      col 0 = intRep, col 1 = ikraph_relation_type,
      col 3 = proposed_biolink_predicate (Cesar), col 8 = Halil,
      col 9 = subject_is_node_one (optional boolean)
    """
    try:
        import openpyxl  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit("openpyxl is required: pip install openpyxl") from exc

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(min_row=2, values_only=True))
    wb.close()

    if not rows:
        raise SystemExit(f"No rows found in {xlsx_path}")

    IDX_INT_REP = 0
    IDX_RELTYPE = 1
    IDX_CESAR = 3
    IDX_HALIL = 8
    IDX_SUBJECT_IS_NODE_ONE = 9  # optional

    header = rows[0]

    def col(idx: int) -> str:
        return str(header[idx] if idx < len(header) and header[idx] is not None else "")

    # Warn if expected columns are misaligned
    for idx, name in {IDX_RELTYPE: "ikraph_relation_type", IDX_CESAR: "proposed_biolink_predicate"}.items():
        if name.lower() not in col(idx).lower():
            print(f"WARNING: col {idx} header {col(idx)!r} doesn't contain {name!r}", file=sys.stderr)

    mapping: Dict[str, Tuple[str, str, str, bool]] = {}
    for row in rows[1:]:
        int_rep = str(row[IDX_INT_REP] or "").strip() if IDX_INT_REP < len(row) else ""
        rel_type = str(row[IDX_RELTYPE] or "").strip() if IDX_RELTYPE < len(row) else ""
        cesar = str(row[IDX_CESAR] or "").strip() if IDX_CESAR < len(row) else ""
        halil = str(row[IDX_HALIL] or "").strip() if IDX_HALIL < len(row) else ""

        if not int_rep or not rel_type:
            continue

        if halil.lower() in ("none", "") or halil.lower().startswith("ok"):
            halil = ""
        if cesar.lower() in ("none", ""):
            cesar = ""

        pred, source = _resolve_predicate(halil, cesar)
        int_rep = int_rep.removesuffix(".0")

        # subject_is_node_one: read from spreadsheet col 9 if present, else default True
        raw_sin = row[IDX_SUBJECT_IS_NODE_ONE] if IDX_SUBJECT_IS_NODE_ONE < len(row) else None
        if raw_sin is None or str(raw_sin).strip().lower() in ("", "none", "true", "1", "yes"):
            subject_is_node_one = True
        else:
            subject_is_node_one = str(raw_sin).strip().lower() not in ("false", "0", "no")

        mapping[int_rep] = (rel_type, pred, source, subject_is_node_one)

    return mapping


def _load_reltypeint(json_path: Path) -> list[tuple[str, str]]:
    with json_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return [
        (str(e["intRep"]), e["relType"])
        for e in data
        if "intRep" in e and "relType" in e
    ]


def generate_reltype_map(
    xlsx_path: Path,
    reltypeint_json: Path,
    output_path: Path,
) -> Path:
    """
    Read the curation spreadsheet + RelTypeInt.json and write ikraph_reltype_map.csv.

    Returns the path to the written CSV.
    """
    print(f"[reltype] loading spreadsheet: {xlsx_path}", file=sys.stderr)
    mapping = _load_xlsx(xlsx_path)

    print(f"[reltype] cross-checking against: {reltypeint_json}", file=sys.stderr)
    for int_rep, rel_type in _load_reltypeint(reltypeint_json):
        if int_rep not in mapping:
            print(
                f"WARNING: int_rep={int_rep!r} ({rel_type!r}) in RelTypeInt.json but not in"
                f" spreadsheet — defaulting to {DEFAULT_PREDICATE}",
                file=sys.stderr,
            )
            mapping[int_rep] = (rel_type, DEFAULT_PREDICATE, "default", True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["int_rep", "ikraph_relation_type", "proposed_biolink_predicate", "source", "subject_is_node_one"]
        )
        for int_rep, (rel_type, predicate, source, sin) in sorted(
            mapping.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0
        ):
            writer.writerow([int_rep, rel_type, predicate, source, sin])

    halil_n = sum(1 for _, _, s, _ in mapping.values() if s == "halil")
    cesar_n = sum(1 for _, _, s, _ in mapping.values() if s == "cesar")
    default_n = sum(1 for _, _, s, _ in mapping.values() if s == "default")
    print(
        f"[reltype] wrote {len(mapping)} rows → {output_path} "
        f"(halil={halil_n}, cesar={cesar_n}, default={default_n})",
        file=sys.stderr,
    )
    return output_path


def load_reltype_map(csv_path: Path) -> Dict[str, Tuple[str, bool]]:
    """
    Load int_rep → (biolink_predicate, subject_is_node_one) from the generated CSV.

    subject_is_node_one defaults to True when the column is absent (backwards-compat
    with CSV files generated before this column was added).
    """
    mapping: Dict[str, Tuple[str, bool]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row.get("int_rep") or row.get("ikraph_relation_type") or "").strip()
            pred = (row.get("proposed_biolink_predicate") or "").strip()
            sin_raw = (row.get("subject_is_node_one") or "true").strip().lower()
            sin = sin_raw not in ("false", "0", "no")
            if key and pred:
                mapping[key] = (pred, sin)
    return mapping
