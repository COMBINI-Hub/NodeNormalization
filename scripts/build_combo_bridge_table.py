#!/usr/bin/env python3
"""
Build COMBO bridge table by expanding seed IDs to equivalent IDs.

The script inverts mapping files loaded by cam_subgraph_extract.load_mapping_index:
all IDs that normalize to the same canonical identifier are treated as an
equivalence cluster.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cam_subgraph_extract import load_mapping_index, normalize_curie  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build COMBO ID bridge table from equivalence mappings.")
    parser.add_argument(
        "--combo-mappings",
        default="combo_seed_mappings.with_semantic.tsv",
        help="Input COMBO mapping TSV.",
    )
    parser.add_argument(
        "--mapping-files",
        nargs="*",
        default=[],
        help="Normalized/crosswalk mapping files used to infer equivalent IDs.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Optional confidence threshold while loading mapping files.",
    )
    parser.add_argument(
        "--require-confidence",
        action="store_true",
        help="Require confidence metadata while loading mapping files.",
    )
    parser.add_argument(
        "--max-equivalents-per-seed",
        type=int,
        default=500,
        help="Cap number of equivalent IDs emitted per seed identifier.",
    )
    parser.add_argument(
        "--bridge-output",
        default="combo_bridge_table.tsv",
        help="Detailed bridge table output TSV.",
    )
    parser.add_argument(
        "--crosswalk-output",
        default="combo_bridge_crosswalk.tsv",
        help="Crosswalk-friendly output TSV with input_curie/preferred_identifier format.",
    )
    parser.add_argument(
        "--stats-output",
        default="combo_bridge_table.stats.json",
        help="Stats JSON output.",
    )
    return parser.parse_args()


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def write_tsv(path: Path, rows: Iterable[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    combo_rows = read_tsv(Path(args.combo_mappings))

    mapping_index = load_mapping_index(
        args.mapping_files,
        min_confidence=args.min_confidence,
        require_confidence=args.require_confidence,
    )

    canonical_to_inputs: Dict[str, List[str]] = defaultdict(list)
    for input_id, rec in mapping_index.items():
        canonical_to_inputs[rec.canonical_id].append(input_id)

    bridge_rows = []
    crosswalk_rows = []
    seen_crosswalk = set()

    for row in combo_rows:
        concept_id = (row.get("combo_concept_id") or "").strip()
        concept_label = (row.get("combo_label") or "").strip()
        seed_identifier = normalize_curie((row.get("source_identifier") or row.get("input_curie") or "").strip())
        preferred_identifier = normalize_curie((row.get("preferred_identifier") or "").strip())
        if not seed_identifier or not preferred_identifier:
            continue

        canonical = preferred_identifier
        if seed_identifier in mapping_index:
            canonical = mapping_index[seed_identifier].canonical_id
        elif preferred_identifier in mapping_index:
            canonical = mapping_index[preferred_identifier].canonical_id

        equivalents = canonical_to_inputs.get(canonical, [])
        if not equivalents:
            equivalents = [canonical]

        # Stable + deduped + bounded.
        deduped = []
        seen_eq = set()
        for eq in [canonical] + sorted(equivalents):
            if eq in seen_eq:
                continue
            seen_eq.add(eq)
            deduped.append(eq)
            if len(deduped) >= args.max_equivalents_per_seed:
                break

        for equivalent_id in deduped:
            eq_rec = mapping_index.get(equivalent_id)
            eq_label = eq_rec.label if eq_rec and eq_rec.label else ""
            eq_conf = eq_rec.confidence if eq_rec else None
            eq_source = eq_rec.source_file if eq_rec else "self"
            bridge_rows.append(
                {
                    "combo_concept_id": concept_id,
                    "combo_label": concept_label,
                    "seed_identifier": seed_identifier,
                    "canonical_identifier": canonical,
                    "equivalent_identifier": equivalent_id,
                    "equivalent_label": eq_label,
                    "mapping_confidence": eq_conf,
                    "mapping_source": eq_source,
                }
            )

            key = (equivalent_id, canonical)
            if key in seen_crosswalk:
                continue
            seen_crosswalk.add(key)
            crosswalk_rows.append(
                {
                    "input_curie": equivalent_id,
                    "preferred_identifier": canonical,
                    "preferred_label": eq_label,
                    "confidence": eq_conf if eq_conf is not None else "",
                    "method": "bridge_equivalence",
                    "evidence": f"combo_bridge:{concept_id}",
                }
            )

    write_tsv(
        Path(args.bridge_output),
        bridge_rows,
        [
            "combo_concept_id",
            "combo_label",
            "seed_identifier",
            "canonical_identifier",
            "equivalent_identifier",
            "equivalent_label",
            "mapping_confidence",
            "mapping_source",
        ],
    )
    write_tsv(
        Path(args.crosswalk_output),
        crosswalk_rows,
        [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "evidence",
        ],
    )

    stats = {
        "config": {
            "combo_mappings": args.combo_mappings,
            "mapping_files": args.mapping_files,
            "min_confidence": args.min_confidence,
            "require_confidence": args.require_confidence,
            "max_equivalents_per_seed": args.max_equivalents_per_seed,
        },
        "counts": {
            "combo_rows_read": len(combo_rows),
            "mapping_index_size": len(mapping_index),
            "canonical_clusters": len(canonical_to_inputs),
            "bridge_rows": len(bridge_rows),
            "crosswalk_rows": len(crosswalk_rows),
        },
    }
    Path(args.stats_output).write_text(json.dumps(stats, indent=2), encoding="utf-8")

    print(f"[ok] COMBO rows read: {len(combo_rows):,}")
    print(f"[ok] Bridge rows emitted: {len(bridge_rows):,}")
    print(f"[ok] Crosswalk rows emitted: {len(crosswalk_rows):,}")
    print(f"[ok] Wrote: {args.bridge_output}")
    print(f"[ok] Wrote: {args.crosswalk_output}")
    print(f"[ok] Wrote: {args.stats_output}")


if __name__ == "__main__":
    main()
