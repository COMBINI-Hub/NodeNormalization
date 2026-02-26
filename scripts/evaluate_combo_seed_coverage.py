#!/usr/bin/env python3
"""
Evaluate COMBO seeding outputs and CAM retention metrics.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate COMBO seed mapping and CAM coverage.")
    parser.add_argument("--raw-concepts", required=True, help="combo_concepts_raw.tsv")
    parser.add_argument("--combined-mappings", required=True, help="combo_seed_mappings.with_semantic.tsv")
    parser.add_argument("--semantic-mappings", default=None, help="combo_semantic_mappings.tsv (optional)")
    parser.add_argument("--cam-nodes", required=True, help="CAM nodes TSV from cam_subgraph_extract.py")
    parser.add_argument("--cam-edges", required=True, help="CAM edges TSV from cam_subgraph_extract.py")
    parser.add_argument("--cam-stats", required=True, help="CAM stats JSON from cam_subgraph_extract.py")
    parser.add_argument("--remaining-unresolved", default=None, help="combo_unresolved_after_semantic.tsv (optional)")
    parser.add_argument("--output-json", default="combo_mapping_qc.json", help="QC JSON output path.")
    parser.add_argument("--output-md", default="COMBO_SUBGRAPH_RUN_REPORT.md", help="Markdown report output path.")
    return parser.parse_args()


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [dict(row) for row in reader]


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def read_cam_nodes(path: Path) -> Tuple[Set[str], Set[str]]:
    rows = read_tsv(path)
    node_ids = {row.get("node_id", "").strip() for row in rows if row.get("node_id")}
    canonical_ids = {row.get("canonical_id", "").strip() for row in rows if row.get("canonical_id")}
    return node_ids, canonical_ids


def mapped_concept_sets(mapping_rows: Iterable[Dict[str, str]]) -> Tuple[Set[str], Set[str]]:
    deterministic = set()
    semantic = set()
    for row in mapping_rows:
        concept_id = (row.get("combo_concept_id") or "").strip()
        method = (row.get("method") or "").strip().lower()
        if not concept_id:
            continue
        if method == "semantic_fallback":
            semantic.add(concept_id)
        else:
            deterministic.add(concept_id)
    return deterministic, semantic


def load_json(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        return data
    return {"value": data}


def ratio(num: int, den: int) -> float:
    if den == 0:
        return 0.0
    return num / den


def build_markdown(summary: Dict[str, object]) -> str:
    m = summary["mapping_summary"]
    c = summary["cam_summary"]
    s = summary["seed_presence_summary"]

    return "\n".join(
        [
            "# COMBO Subgraph Run Report",
            "",
            "## Mapping Coverage",
            "",
            f"- Total COMBO concepts: `{m['total_combo_concepts']}`",
            f"- Concepts mapped (any): `{m['mapped_any_count']}` (`{m['mapped_any_rate_pct']:.2f}%`)",
            f"- Concepts mapped deterministically: `{m['mapped_deterministic_count']}` (`{m['mapped_deterministic_rate_pct']:.2f}%`)",
            f"- Concepts mapped semantically: `{m['mapped_semantic_count']}` (`{m['mapped_semantic_rate_pct']:.2f}%`)",
            f"- Remaining unresolved concepts: `{m['remaining_unresolved_count']}`",
            "",
            "## Seed Presence in CAM",
            "",
            f"- Seed identifiers total: `{s['seed_identifier_count']}`",
            f"- Seed identifiers present in CAM node_id/canonical_id: `{s['seed_ids_present_in_cam_count']}` (`{s['seed_ids_present_in_cam_rate_pct']:.2f}%`)",
            "",
            "## CAM Output Snapshot",
            "",
            f"- CAM nodes: `{c['cam_node_count']}`",
            f"- CAM edges: `{c['cam_edge_count']}`",
            f"- SemMedDB edges retained: `{c['semmed_edges_retained']}`",
            f"- iKraph edges retained: `{c['ikraph_edges_retained']}`",
            "",
            "## Notes",
            "",
            "- Deterministic mappings are derived from COMBO provenance UMLS/MeSH annotations.",
            "- Semantic fallback rows are restricted by threshold and top-1/top-2 margin.",
            "- See `combo_mapping_qc.json` for full machine-readable details.",
            "",
        ]
    )


def main() -> None:
    args = parse_args()

    raw_rows = read_tsv(Path(args.raw_concepts))
    mapping_rows = read_tsv(Path(args.combined_mappings))
    semantic_rows = read_tsv(Path(args.semantic_mappings)) if args.semantic_mappings and Path(args.semantic_mappings).exists() else []
    unresolved_rows = (
        read_tsv(Path(args.remaining_unresolved))
        if args.remaining_unresolved and Path(args.remaining_unresolved).exists()
        else []
    )

    cam_node_ids, cam_canonical_ids = read_cam_nodes(Path(args.cam_nodes))
    cam_edges = read_tsv(Path(args.cam_edges))
    cam_stats = load_json(Path(args.cam_stats))

    total_concepts = len(raw_rows)
    deterministic_set, semantic_set = mapped_concept_sets(mapping_rows)
    mapped_any = deterministic_set | semantic_set

    seed_ids = {
        (row.get("preferred_identifier") or "").strip()
        for row in mapping_rows
        if (row.get("preferred_identifier") or "").strip()
    }
    seed_present = {sid for sid in seed_ids if sid in cam_node_ids or sid in cam_canonical_ids}

    semmed_edges_retained = sum(1 for row in cam_edges if (row.get("ontology") or "").strip().lower() == "semmeddb")
    ikraph_edges_retained = sum(1 for row in cam_edges if (row.get("ontology") or "").strip().lower() == "ikraph")

    summary = {
        "mapping_summary": {
            "total_combo_concepts": total_concepts,
            "mapped_any_count": len(mapped_any),
            "mapped_any_rate_pct": ratio(len(mapped_any), total_concepts) * 100.0,
            "mapped_deterministic_count": len(deterministic_set),
            "mapped_deterministic_rate_pct": ratio(len(deterministic_set), total_concepts) * 100.0,
            "mapped_semantic_count": len(semantic_set),
            "mapped_semantic_rate_pct": ratio(len(semantic_set), total_concepts) * 100.0,
            "semantic_row_count": len(semantic_rows),
            "remaining_unresolved_count": len(unresolved_rows),
        },
        "seed_presence_summary": {
            "seed_identifier_count": len(seed_ids),
            "seed_ids_present_in_cam_count": len(seed_present),
            "seed_ids_present_in_cam_rate_pct": ratio(len(seed_present), len(seed_ids)) * 100.0,
        },
        "cam_summary": {
            "cam_node_count": len(cam_node_ids),
            "cam_edge_count": len(cam_edges),
            "semmed_edges_retained": semmed_edges_retained,
            "ikraph_edges_retained": ikraph_edges_retained,
            "cam_stats_file_summary": cam_stats,
        },
    }

    write_json(Path(args.output_json), summary)
    md_report = build_markdown(summary)
    Path(args.output_md).write_text(md_report, encoding="utf-8")

    print(f"[ok] Wrote: {args.output_json}")
    print(f"[ok] Wrote: {args.output_md}")


if __name__ == "__main__":
    main()
