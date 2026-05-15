#!/usr/bin/env python3
"""
Generate ikraph_reltype_map.csv from the curation spreadsheet.

Resolution order per row:
  1. Halil's column — used if it contains a clean biolink predicate
     (starts with "biolink:" and is snake_case with no spaces or extra punctuation).
  2. Cesar's proposed_biolink_predicate column — fallback when Halil left a comment,
     wrote "OK" (meaning agree with Cesar), or left the cell empty.
  3. "biolink:related_to" — last-resort default when neither reviewer provided a predicate.

The output CSV is keyed by int_rep (the integer relationship type in the raw iKraph JSON
files), so build_semmed_ikraph_normalized_import.py can look up biolink predicates directly
from raw iKraph without a string-name conversion step.

Any relation type present in RelTypeInt.json but missing from the spreadsheet is also
written with the default predicate and a warning to stderr.

Output CSV columns:
  int_rep, ikraph_relation_type, proposed_biolink_predicate, source
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

CLEAN_BIOLINK = re.compile(r"^biolink:[a-z][a-z0-9_]*$")
DEFAULT_PREDICATE = "biolink:related_to"


def resolve_predicate(halil: str | None, cesar: str | None) -> tuple[str, str]:
    """Return (predicate, source) where source is 'halil', 'cesar', or 'default'."""
    halil_clean = (halil or "").strip()
    cesar_clean = (cesar or "").strip()

    if CLEAN_BIOLINK.match(halil_clean):
        return halil_clean, "halil"

    if cesar_clean and CLEAN_BIOLINK.match(cesar_clean):
        return cesar_clean, "cesar"

    return DEFAULT_PREDICATE, "default"


def load_xlsx(xlsx_path: Path) -> dict[str, tuple[str, str]]:
    """
    Return {ikraph_relation_type: (predicate, source)} from the review spreadsheet.

    Expected sheet layout:
      Row 1: title (skipped)
      Row 2: headers — col index 1 = ikraph_relation_type,
                        col index 3 = proposed_biolink_predicate (Cesar),
                        col index 8 = Halil
      Row 3+: data
    """
    try:
        import openpyxl  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit(
            "openpyxl is required: pip install openpyxl"
        ) from exc

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(min_row=2, values_only=True))
    if not rows:
        raise SystemExit(f"No rows found in {xlsx_path}")

    # Row index 0 is the header row (row 2 in the sheet)
    header = rows[0]
    IDX_INT_REP  = 0   # intRep
    IDX_RELTYPE  = 1   # ikraph_relation_type
    IDX_CESAR    = 3   # proposed_biolink_predicate
    IDX_HALIL    = 8   # Halil

    # Validate expected column positions
    def _col(idx: int) -> str:
        v = header[idx] if idx < len(header) else None
        return str(v or "")

    expected = {
        IDX_RELTYPE: "ikraph_relation_type",
        IDX_CESAR:   "proposed_biolink_predicate",
    }
    for idx, name in expected.items():
        if name.lower() not in _col(idx).lower():
            print(
                f"WARNING: column {idx} header is {_col(idx)!r}, expected to contain {name!r}",
                file=sys.stderr,
            )

    # {int_rep_str: (rel_type_name, predicate, source)}
    mapping: dict[str, tuple[str, str, str]] = {}
    for row in rows[1:]:  # skip header row
        int_rep  = str(row[IDX_INT_REP] or "").strip() if IDX_INT_REP < len(row) else ""
        rel_type = str(row[IDX_RELTYPE] or "").strip() if IDX_RELTYPE < len(row) else ""
        cesar    = str(row[IDX_CESAR]   or "").strip() if IDX_CESAR   < len(row) else ""
        halil    = str(row[IDX_HALIL]   or "").strip() if IDX_HALIL   < len(row) else ""

        if not int_rep or not rel_type:
            continue

        # Normalise "None" strings and "OK*" values
        if halil.lower() in ("none", ""):
            halil = ""
        if halil.lower().startswith("ok"):
            halil = ""  # defer to Cesar

        if cesar.lower() in ("none", ""):
            cesar = ""

        pred, source = resolve_predicate(halil, cesar)
        # Strip trailing ".0" that openpyxl emits for integer cells read as float
        int_rep = int_rep.removesuffix(".0")
        mapping[int_rep] = (rel_type, pred, source)

    wb.close()
    return mapping


def load_reltypeint(json_path: Path) -> list[tuple[str, str]]:
    """Return [(int_rep, rel_type_name), ...] from RelTypeInt.json."""
    with json_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return [(str(entry["intRep"]), entry["relType"]) for entry in data if "intRep" in entry and "relType" in entry]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate ikraph_reltype_map.csv")
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=Path(__file__).parent.parent / "ikraph_reltype_to_biolink_mapping_review.xlsx",
        help="Path to the curation spreadsheet (default: repo root)",
    )
    parser.add_argument(
        "--reltypeint-json",
        type=Path,
        default=None,
        help="RelTypeInt.json from iKraph; used to warn about unmapped types",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "semmed_ikraph_normalized" / "ikraph_reltype_map.csv",
        help="Output CSV path (default: semmed_ikraph_normalized/ikraph_reltype_map.csv)",
    )
    args = parser.parse_args()

    print(f"[reltype] loading spreadsheet: {args.xlsx}", file=sys.stderr)
    mapping = load_xlsx(args.xlsx)

    if args.reltypeint_json:
        print(f"[reltype] cross-checking against: {args.reltypeint_json}", file=sys.stderr)
        for int_rep, rel_type in load_reltypeint(args.reltypeint_json):
            if int_rep not in mapping:
                print(f"WARNING: int_rep={int_rep!r} ({rel_type!r}) in RelTypeInt.json but not in spreadsheet — defaulting to {DEFAULT_PREDICATE}", file=sys.stderr)
                mapping[int_rep] = (rel_type, DEFAULT_PREDICATE, "default")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["int_rep", "ikraph_relation_type", "proposed_biolink_predicate", "source"])
        for int_rep, (rel_type, predicate, source) in sorted(mapping.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            writer.writerow([int_rep, rel_type, predicate, source])

    halil_count   = sum(1 for _, _, s in mapping.values() if s == "halil")
    cesar_count   = sum(1 for _, _, s in mapping.values() if s == "cesar")
    default_count = sum(1 for _, _, s in mapping.values() if s == "default")
    print(
        f"[reltype] wrote {len(mapping)} rows to {args.output} "
        f"(halil={halil_count}, cesar={cesar_count}, default={default_count})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
