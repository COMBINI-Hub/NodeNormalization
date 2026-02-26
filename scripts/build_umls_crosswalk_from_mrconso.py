#!/usr/bin/env python3
"""
Build a COMBO-focused crosswalk TSV from UMLS MRCONSO.RRF.

Output format is compatible with build_combo_seed_mappings.py / cam_subgraph_extract.py:
  input_curie, preferred_identifier, preferred_label, confidence, method, evidence
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate crosswalk TSV from MRCONSO for COMBO seeds.")
    parser.add_argument(
        "--mrconso",
        default="MRCONSO.RRF",
        help="Path to MRCONSO.RRF file.",
    )
    parser.add_argument(
        "--combo-mappings",
        default="combo_seed_mappings.tsv",
        help="TSV with COMBO seed mappings (source_identifier column).",
    )
    parser.add_argument(
        "--output",
        default="combo_crosswalk_from_mrconso.tsv",
        help="Output crosswalk TSV.",
    )
    parser.add_argument(
        "--include-sabs",
        nargs="*",
        default=["MSH", "SNOMEDCT_US", "NCI", "RXNORM", "LNC", "ICD10CM", "ICD9CM", "HPO", "GO"],
        help="Additional UMLS source vocabularies to include for equivalent IDs.",
    )
    parser.add_argument(
        "--max-equivalents-per-cui",
        type=int,
        default=300,
        help="Safety cap on emitted equivalents per CUI.",
    )
    return parser.parse_args()


def normalize_prefix(sab: str) -> str:
    mapping = {
        "MSH": "MESH",
        "SNOMEDCT_US": "SNOMEDCT",
        "NCI": "NCIT",
        "LNC": "LOINC",
    }
    return mapping.get(sab, sab)


def make_curie(sab: str, code: str, cui: str) -> Optional[str]:
    sab = (sab or "").strip()
    code = (code or "").strip()
    cui = (cui or "").strip()
    if not sab or not code:
        return None
    if sab == "MSH":
        return f"MESH:{code}"
    if sab == "MTH" and code.startswith("C") and len(code) >= 8:
        return f"UMLS:{code}"
    if sab == "HPO" and not code.startswith("HP:"):
        return f"HP:{code}"
    if sab in {"SNOMEDCT_US", "NCI", "RXNORM", "LNC", "ICD10CM", "ICD9CM", "GO"}:
        return f"{normalize_prefix(sab)}:{code}"
    # Allow other selected SABs as-is.
    return f"{normalize_prefix(sab)}:{code}"


def read_combo_seed_ids(path: Path) -> Tuple[Set[str], Set[str]]:
    cui_set: Set[str] = set()
    mesh_set: Set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            raw = (row.get("source_identifier") or "").strip()
            if raw.startswith("UMLS:C"):
                cui_set.add(raw.split(":", 1)[1])
            elif raw.startswith("MESH:"):
                mesh_set.add(raw.split(":", 1)[1])
    return cui_set, mesh_set


def write_tsv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "input_curie",
                "preferred_identifier",
                "preferred_label",
                "confidence",
                "method",
                "evidence",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    mrconso_path = Path(args.mrconso)
    combo_path = Path(args.combo_mappings)

    seed_cuis, seed_mesh = read_combo_seed_ids(combo_path)
    include_sabs = set(args.include_sabs) | {"MSH", "MTH"}

    # Pass 1: collect relevant CUIs (from direct UMLS seeds + MeSH seed codes).
    relevant_cuis: Set[str] = set(seed_cuis)
    cui_to_label: Dict[str, str] = {}

    with mrconso_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 15:
                continue
            cui, lat, ts, _, _, _, ispref, _, _, _, _, sab, _, code, string = parts[:15]
            if lat != "ENG":
                continue
            if sab == "MSH" and code in seed_mesh:
                relevant_cuis.add(cui)
            if cui in seed_cuis and ispref == "Y" and ts == "P":
                cui_to_label.setdefault(cui, string)

    # Pass 2: emit equivalences for relevant CUIs.
    rows: List[Dict[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()
    cui_emit_counts: Dict[str, int] = defaultdict(int)

    with mrconso_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 15:
                continue
            cui, lat, _, _, _, _, _, _, _, _, _, sab, _, code, _ = parts[:15]
            if cui not in relevant_cuis or lat != "ENG" or sab not in include_sabs:
                continue
            if cui_emit_counts[cui] >= args.max_equivalents_per_cui:
                continue

            input_curie = make_curie(sab, code, cui)
            preferred_identifier = f"UMLS:{cui}"
            if not input_curie:
                continue

            key = (input_curie, preferred_identifier)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            cui_emit_counts[cui] += 1

            rows.append(
                {
                    "input_curie": input_curie,
                    "preferred_identifier": preferred_identifier,
                    "preferred_label": cui_to_label.get(cui, ""),
                    "confidence": "0.9",
                    "method": "umls_mrconso_cui_crosswalk",
                    "evidence": f"MRCONSO:{sab}",
                }
            )

    write_tsv(Path(args.output), rows)
    print(f"[ok] Seed CUIs (direct): {len(seed_cuis):,}")
    print(f"[ok] Seed MeSH codes: {len(seed_mesh):,}")
    print(f"[ok] Relevant CUIs total: {len(relevant_cuis):,}")
    print(f"[ok] Crosswalk rows emitted: {len(rows):,}")
    print(f"[ok] Wrote: {args.output}")


if __name__ == "__main__":
    main()
