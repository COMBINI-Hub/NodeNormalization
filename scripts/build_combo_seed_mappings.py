#!/usr/bin/env python3
"""
Build deterministic-first COMBO seed mappings for CAM extraction.

Input:
  - combo_concepts_raw.tsv from extract_combo_seed_ids.py

Output:
  - combo_seed_mappings.tsv (compatible with cam_subgraph_extract.py --normalized-files)
  - combo_unresolved_concepts.tsv (for semantic fallback processing)
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cam_subgraph_extract import load_mapping_index, normalize_curie


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical COMBO seed mappings.")
    parser.add_argument("--input", required=True, help="Path to combo_concepts_raw.tsv.")
    parser.add_argument("--output", default="combo_seed_mappings.tsv", help="Output mapping TSV.")
    parser.add_argument(
        "--unresolved-output",
        default="combo_unresolved_concepts.tsv",
        help="Output unresolved concepts TSV for semantic fallback.",
    )
    parser.add_argument(
        "--crosswalk-files",
        nargs="*",
        default=[],
        help="Optional mapping files to crosswalk deterministic IDs into preferred identifiers.",
    )
    parser.add_argument(
        "--crosswalk-min-confidence",
        type=float,
        default=None,
        help="Optional confidence threshold applied while reading crosswalk files.",
    )
    parser.add_argument(
        "--crosswalk-require-confidence",
        action="store_true",
        help="Require crosswalk records to include confidence when loading mapping files.",
    )
    return parser.parse_args()


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [dict(row) for row in reader]


def write_tsv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def split_pipe(raw: str) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split("|") if item.strip()]


def dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        norm = normalize_curie(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def main() -> None:
    args = parse_args()
    rows = read_tsv(Path(args.input))
    crosswalk_index = load_mapping_index(
        args.crosswalk_files,
        min_confidence=args.crosswalk_min_confidence,
        require_confidence=args.crosswalk_require_confidence,
    )

    mapping_rows: List[Dict[str, Any]] = []
    unresolved_rows: List[Dict[str, Any]] = []
    direct_count = 0
    crosswalk_count = 0

    for row in rows:
        concept_id = (row.get("combo_concept_id") or "").strip()
        concept_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        provenance_urls = split_pipe(row.get("provenance_urls") or "")

        deterministic_ids = dedupe_keep_order(
            split_pipe(row.get("umls_ids") or "") + split_pipe(row.get("mesh_ids") or "")
        )

        if not deterministic_ids:
            unresolved_rows.append(
                {
                    "combo_concept_id": concept_id,
                    "combo_label": concept_label,
                    "synonyms": synonyms,
                    "reason": "no_deterministic_umls_or_mesh",
                }
            )
            continue

        evidence = "|".join(provenance_urls) if provenance_urls else "combo_provenance_id"
        for source_identifier in deterministic_ids:
            canonical_id = source_identifier
            preferred_label = concept_label
            method = "deterministic_direct"
            confidence = 1.0

            mapped = crosswalk_index.get(source_identifier)
            if mapped and mapped.canonical_id:
                # Keep direct IDs at highest confidence if crosswalk resolves to same CURIE.
                if normalize_curie(mapped.canonical_id) != source_identifier:
                    canonical_id = normalize_curie(mapped.canonical_id)
                    method = "deterministic_crosswalk"
                    confidence = mapped.confidence if mapped.confidence is not None else 0.9
                    crosswalk_count += 1
                else:
                    direct_count += 1
            else:
                direct_count += 1

            mapping_rows.append(
                {
                    "input_curie": source_identifier,
                    "preferred_identifier": canonical_id,
                    "preferred_label": preferred_label,
                    "confidence": confidence,
                    "method": method,
                    "combo_concept_id": concept_id,
                    "combo_label": concept_label,
                    "source_identifier": source_identifier,
                    "evidence": evidence,
                }
            )

    write_tsv(
        Path(args.output),
        mapping_rows,
        [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ],
    )
    write_tsv(
        Path(args.unresolved_output),
        unresolved_rows,
        ["combo_concept_id", "combo_label", "synonyms", "reason"],
    )

    print(f"[ok] Concepts read: {len(rows)}")
    print(f"[ok] Deterministic mapping rows: {len(mapping_rows)}")
    print(f"[ok] Direct mappings: {direct_count}")
    print(f"[ok] Crosswalk mappings: {crosswalk_count}")
    print(f"[ok] Unresolved concepts: {len(unresolved_rows)}")
    print(f"[ok] Wrote: {args.output}")
    print(f"[ok] Wrote: {args.unresolved_output}")


if __name__ == "__main__":
    main()
