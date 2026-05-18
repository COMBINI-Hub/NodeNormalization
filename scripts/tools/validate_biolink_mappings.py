#!/usr/bin/env python3
"""
Validate SemMed + iKraph Biolink predicate mappings against the frozen BiOLink-model allowlist.

Does not run normalization or CSV builds — only loads ``SEMMED_PREDICATE_MAP``, the reltype
CSV, and reports every predicate that is non-canonical or missing from the allowlist.

Usage (from repo root, with scripts/ on PYTHONPATH):

    python scripts/tools/validate_biolink_mappings.py --reltype-map semmed_ikraph_normalized/ikraph_reltype_map.csv

Or from ``scripts/``:

    python tools/validate_biolink_mappings.py --reltype-map ../semmed_ikraph_normalized/ikraph_reltype_map.csv
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List

# scripts/ on path (same pattern as tests/test_pipeline.py)
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_steps.build_import_csvs import (  # noqa: E402
    SEMMED_PREDICATE_MAP,
    _PREDICATES_DATA,
    classify_biolink_predicate,
)
from pipeline_steps.reltype_map import load_reltype_map  # noqa: E402


def _gather() -> tuple[DefaultDict[str, List[str]], DefaultDict[str, List[str]]]:
    bad_format: DefaultDict[str, List[str]] = defaultdict(list)
    not_allowlisted: DefaultDict[str, List[str]] = defaultdict(list)

    for semmed_upper, biolink_pred in SEMMED_PREDICATE_MAP.items():
        ctx = f"SemMed:{semmed_upper}"
        kind = classify_biolink_predicate(biolink_pred)
        if kind == "bad_format":
            bad_format[biolink_pred].append(ctx)
        elif kind == "not_in_allowlist":
            not_allowlisted[biolink_pred].append(ctx)

    return bad_format, not_allowlisted


def _gather_ikraph(reltype_csv: Path) -> tuple[DefaultDict[str, List[str]], DefaultDict[str, List[str]]]:
    bad_format: DefaultDict[str, List[str]] = defaultdict(list)
    not_allowlisted: DefaultDict[str, List[str]] = defaultdict(list)

    rmap = load_reltype_map(reltype_csv)
    for int_rep in sorted(rmap.keys(), key=lambda k: (len(k), k)):
        pred, _ = rmap[int_rep]
        ctx = f"iKraph:int_rep={int_rep}"
        kind = classify_biolink_predicate(pred)
        if kind == "bad_format":
            bad_format[pred].append(ctx)
        elif kind == "not_in_allowlist":
            not_allowlisted[pred].append(ctx)

    return bad_format, not_allowlisted


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate pipeline Biolink predicates vs BiOLink-model allowlist.")
    ap.add_argument(
        "--reltype-map",
        type=Path,
        default=None,
        help="Path to ikraph_reltype_map.csv (default: ./semmed_ikraph_normalized/ikraph_reltype_map.csv if present)",
    )
    args = ap.parse_args()

    reltype_csv = args.reltype_map
    if reltype_csv is None:
        cand = Path("semmed_ikraph_normalized") / "ikraph_reltype_map.csv"
        reltype_csv = cand if cand.is_file() else None

    sem_bad, sem_unknown = _gather()

    ik_bad: DefaultDict[str, List[str]] = defaultdict(list)
    ik_unknown: DefaultDict[str, List[str]] = defaultdict(list)
    if reltype_csv is None or not reltype_csv.is_file():
        print(
            "[validate] No reltype map CSV found; pass --reltype-map or generate Step 0 output. "
            "Only SemMed map entries are checked.",
            file=sys.stderr,
        )
    else:
        b, u = _gather_ikraph(reltype_csv.resolve())
        ik_bad.update(b)
        ik_unknown.update(u)

    def merge(
        a: DefaultDict[str, List[str]],
        b: DefaultDict[str, List[str]],
    ) -> Dict[str, List[str]]:
        out: DefaultDict[str, List[str]] = defaultdict(list)
        for k, v in a.items():
            out[k].extend(v)
        for k, v in b.items():
            out[k].extend(v)
        return dict(sorted(out.items(), key=lambda kv: kv[0]))

    bad_all = merge(sem_bad, ik_bad)
    unknown_all = merge(sem_unknown, ik_unknown)

    print(f"Biolink allowlist file: {_PREDICATES_DATA.resolve()}")

    print("\n--- Non-canonical predicate strings (wrong shape for biolink:*) ---")
    if not bad_all:
        print("(none)")
    else:
        for pred, contexts in bad_all.items():
            print(f"  {pred!r}")
            for c in contexts:
                print(f"    - {c}")

    print("\n--- Canonical biolink:* but NOT in frozen BiOLink-model allowlist ---")
    if not unknown_all:
        print("(none)")
    else:
        for pred, contexts in unknown_all.items():
            print(pred)
            for c in contexts:
                print(f"    - {c}")

    n_issues = sum(len(ctxs) for ctxs in bad_all.values()) + sum(len(ctxs) for ctxs in unknown_all.values())
    print(f"\nSummary: {len(bad_all)} distinct malformed predicates; {len(unknown_all)} distinct unknown CURIEs; {n_issues} total mapping rows flagged.")
    if bad_all or unknown_all:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
