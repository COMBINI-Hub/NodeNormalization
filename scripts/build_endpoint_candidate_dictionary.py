#!/usr/bin/env python3
"""
Build endpoint-centric semantic candidate dictionary for COMBO fallback.

This script scans SemMedDB/iKraph edge endpoint IDs and emits a flat TSV with
identifier + label columns compatible with combo_semantic_fallback.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cam_subgraph_extract import (  # noqa: E402
    canonicalize_id,
    load_ikraph_node_dict,
    load_mapping_index,
    pick_first,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build endpoint-based candidate TSV for semantic fallback."
    )
    parser.add_argument(
        "--semmed-edge-file",
        default="semmeddb_edges_cleaned.csv",
        help="SemMedDB edge CSV containing endpoint columns.",
    )
    parser.add_argument(
        "--ikraph-edge-file",
        default="ikraph_edges_cleaned.csv",
        help="iKraph edge CSV containing endpoint columns.",
    )
    parser.add_argument(
        "--ikraph-node-dict",
        default=None,
        help="Optional iKraph node dictionary JSON (biokdeid -> CURIE).",
    )
    parser.add_argument(
        "--normalized-files",
        nargs="*",
        default=[],
        help="Optional mapping files used to canonicalize IDs and recover labels.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Optional confidence threshold applied while reading normalized files.",
    )
    parser.add_argument(
        "--require-confidence",
        action="store_true",
        help="Require confidence metadata in normalized files when loading mappings.",
    )
    parser.add_argument(
        "--output",
        default="combo_endpoint_candidates.tsv",
        help="Output candidate TSV.",
    )
    parser.add_argument(
        "--stats-output",
        default="combo_endpoint_candidates.stats.json",
        help="Output JSON stats.",
    )
    parser.add_argument(
        "--max-unique-endpoints",
        type=int,
        default=3000000,
        help="Safety cap for unique endpoint IDs collected before dedupe output.",
    )
    parser.add_argument(
        "--fallback-label-to-id",
        action="store_true",
        help="If set, emit identifier as label when no label is known.",
    )
    return parser.parse_args()


def iter_semmed_endpoints(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_raw = pick_first(row, [":START_ID", "subject", "source", "source_id"])
            target_raw = pick_first(row, [":END_ID", "object", "target", "target_id"])
            if source_raw:
                yield str(source_raw).strip()
            if target_raw:
                yield str(target_raw).strip()


def iter_ikraph_endpoints(path: Path, id_map: Dict[str, str]) -> Iterable[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_raw = pick_first(row, [":START_ID", "node_one_id", "subject", "source", "source_id"])
            target_raw = pick_first(row, [":END_ID", "node_two_id", "object", "target", "target_id"])
            if source_raw:
                source = str(source_raw).strip()
                yield id_map.get(source, source)
            if target_raw:
                target = str(target_raw).strip()
                yield id_map.get(target, target)


def write_tsv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "preferred_identifier",
                "preferred_label",
                "evidence",
                "source_file",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    mapping_index = load_mapping_index(
        args.normalized_files,
        min_confidence=args.min_confidence,
        require_confidence=args.require_confidence,
    )

    id_map: Dict[str, str] = {}
    if args.ikraph_node_dict:
        node_dict_path = Path(args.ikraph_node_dict)
        if node_dict_path.exists():
            id_map = load_ikraph_node_dict(node_dict_path)
        else:
            print(f"[warn] iKraph node dictionary not found, ignoring: {node_dict_path}")

    semmed_path = Path(args.semmed_edge_file)
    ikraph_path = Path(args.ikraph_edge_file)

    counts = {
        "semmed_endpoints_seen": 0,
        "ikraph_endpoints_seen": 0,
    }
    endpoint_sources: Dict[str, Set[str]] = defaultdict(set)
    source_files: Dict[str, Set[str]] = defaultdict(set)

    if semmed_path.exists():
        for raw_id in iter_semmed_endpoints(semmed_path):
            counts["semmed_endpoints_seen"] += 1
            canonical = canonicalize_id(raw_id, mapping_index)
            if not canonical:
                continue
            endpoint_sources[canonical].add("semmeddb")
            source_files[canonical].add(str(semmed_path))
            if len(endpoint_sources) >= args.max_unique_endpoints:
                print(
                    f"[warn] Reached max unique endpoint cap ({args.max_unique_endpoints}); truncating collection."
                )
                break
    else:
        print(f"[warn] SemMedDB edge file missing, skipping: {semmed_path}")

    if ikraph_path.exists() and len(endpoint_sources) < args.max_unique_endpoints:
        for raw_id in iter_ikraph_endpoints(ikraph_path, id_map):
            counts["ikraph_endpoints_seen"] += 1
            canonical = canonicalize_id(raw_id, mapping_index)
            if not canonical:
                continue
            endpoint_sources[canonical].add("ikraph")
            source_files[canonical].add(str(ikraph_path))
            if len(endpoint_sources) >= args.max_unique_endpoints:
                print(
                    f"[warn] Reached max unique endpoint cap ({args.max_unique_endpoints}); truncating collection."
                )
                break
    elif not ikraph_path.exists():
        print(f"[warn] iKraph edge file missing, skipping: {ikraph_path}")

    out_rows = []
    for identifier in sorted(endpoint_sources.keys()):
        mapping = mapping_index.get(identifier)
        label = mapping.label if mapping and mapping.label else ""
        if not label and args.fallback_label_to_id:
            label = identifier
        if not label:
            continue
        out_rows.append(
            {
                "preferred_identifier": identifier,
                "preferred_label": label,
                "evidence": "|".join(sorted(endpoint_sources[identifier])),
                "source_file": "|".join(sorted(source_files[identifier])),
            }
        )

    write_tsv(Path(args.output), out_rows)
    stats = {
        "config": {
            "semmed_edge_file": args.semmed_edge_file,
            "ikraph_edge_file": args.ikraph_edge_file,
            "ikraph_node_dict": args.ikraph_node_dict,
            "normalized_files": args.normalized_files,
            "min_confidence": args.min_confidence,
            "require_confidence": args.require_confidence,
            "fallback_label_to_id": args.fallback_label_to_id,
        },
        "counts": {
            **counts,
            "unique_endpoint_ids_after_normalization": len(endpoint_sources),
            "candidates_emitted": len(out_rows),
        },
    }
    Path(args.stats_output).write_text(json.dumps(stats, indent=2), encoding="utf-8")

    print(f"[ok] Unique normalized endpoints: {len(endpoint_sources):,}")
    print(f"[ok] Candidate rows emitted: {len(out_rows):,}")
    print(f"[ok] Wrote: {args.output}")
    print(f"[ok] Wrote: {args.stats_output}")


if __name__ == "__main__":
    main()
