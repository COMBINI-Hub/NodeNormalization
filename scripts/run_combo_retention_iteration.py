#!/usr/bin/env python3
"""
Run one full COMBO retention iteration end-to-end.
"""

from __future__ import annotations

import argparse
import csv
import shlex
import subprocess
import sys
from pathlib import Path
from typing import List


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run COMBO mapping + retention iteration.")
    parser.add_argument("--combo-rtf", required=True, help="Path to COMBO RTF export.")
    parser.add_argument(
        "--crosswalk-files",
        nargs="*",
        default=[],
        help="Crosswalk/normalized mapping files used across the pipeline.",
    )
    parser.add_argument(
        "--extra-candidate-files",
        nargs="*",
        default=[],
        help="Additional candidate dictionaries for semantic fallback.",
    )
    parser.add_argument(
        "--extra-normalized-files",
        nargs="*",
        default=[],
        help="Additional normalized files for cam_subgraph_extract mapping.",
    )
    parser.add_argument(
        "--ontology-dir",
        default="ontologies",
        help="Ontology directory for CAM extraction.",
    )
    parser.add_argument(
        "--semmed-edge-file",
        default="semmeddb_edges_cleaned.csv",
        help="SemMedDB cleaned edge CSV.",
    )
    parser.add_argument(
        "--ikraph-edge-file",
        default="ikraph_edges_cleaned.csv",
        help="iKraph cleaned edge CSV.",
    )
    parser.add_argument(
        "--ikraph-node-dict",
        default=None,
        help="Optional iKraph biokdeid->CURIE dictionary JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default="cam_subgraph_output_combo",
        help="CAM extraction output directory.",
    )
    parser.add_argument(
        "--semantic-threshold",
        type=float,
        default=0.92,
        help="Semantic fallback acceptance threshold.",
    )
    parser.add_argument(
        "--semantic-margin",
        type=float,
        default=0.08,
        help="Semantic fallback top-1 vs top-2 margin threshold.",
    )
    parser.add_argument(
        "--semantic-max-candidates",
        type=int,
        default=100000,
        help="Maximum candidates loaded by semantic fallback.",
    )
    parser.add_argument(
        "--endpoint-max-unique-endpoints",
        type=int,
        default=500000,
        help="Maximum unique endpoints collected for candidate dictionary build.",
    )
    parser.add_argument(
        "--endpoint-fallback-label-to-id",
        action="store_true",
        help="Use identifier as label fallback while building endpoint candidates.",
    )
    parser.add_argument(
        "--python-bin",
        default=sys.executable,
        help="Python interpreter used to execute sub-scripts.",
    )
    return parser.parse_args()


def run_cmd(command: List[str]) -> None:
    pretty = " ".join(shlex.quote(part) for part in command)
    print(f"[run] {pretty}")
    subprocess.run(command, cwd=str(REPO_ROOT), check=True)


def merge_mapping_tsvs(left_path: Path, right_path: Path, out_path: Path) -> None:
    rows = []
    fieldnames: List[str] = []
    seen = set()
    for path in (left_path, right_path):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for fn in list(reader.fieldnames or []):
                if fn not in fieldnames:
                    fieldnames.append(fn)
            for row in reader:
                key = (
                    (row.get("input_curie") or "").strip(),
                    (row.get("preferred_identifier") or "").strip(),
                    (row.get("combo_concept_id") or "").strip(),
                    (row.get("method") or "").strip(),
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(dict(row))

    if not fieldnames:
        fieldnames = [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "mapping_relation",
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ]

    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[ok] Wrote merged mappings: {out_path} ({len(rows):,} rows)")


def main() -> None:
    args = parse_args()

    combo_raw = "combo_concepts_raw.tsv"
    unresolved = "combo_unresolved_concepts.tsv"
    deterministic = "combo_seed_mappings.tsv"
    mrconso_exact = "combo_mrconso_exact_mappings.tsv"
    mrconso_review = "combo_mrconso_exact_review.tsv"
    unresolved_after_mrconso = "combo_unresolved_after_mrconso.tsv"
    mrconso_review_promoted = "combo_mrconso_review_promoted.tsv"
    unresolved_after_mrconso_review = "combo_unresolved_after_mrconso_review.tsv"
    deterministic_plus_mrconso = "combo_seed_mappings.with_mrconso_exact.tsv"
    deterministic_plus_mrconso_plus_review = "combo_seed_mappings.with_mrconso_exact_and_review.tsv"
    variant_parent_rows = "combo_variant_parent_mappings.tsv"
    unresolved_after_variant_parent = "combo_unresolved_after_variant_parent.tsv"
    mappings_after_variant_parent = "combo_seed_mappings.with_variant_parent.tsv"
    cam_lexicon_rows = "combo_cam_lexicon_mappings.tsv"
    unresolved_after_cam_lexicon = "combo_unresolved_after_cam_lexicon.tsv"
    mappings_after_cam_lexicon = "combo_seed_mappings.with_cam_lexicon.tsv"
    manual_template = "combo_manual_curation.tsv"
    manual_rows = "combo_manual_curated_mappings.tsv"
    unresolved_after_manual = "combo_unresolved_after_manual_curation.tsv"
    mappings_after_manual = "combo_seed_mappings.with_manual_curation.tsv"
    modifier_rows = "combo_modifier_stripping_mappings.tsv"
    unresolved_after_modifier = "combo_unresolved_after_modifier.tsv"
    mappings_after_modifier = "combo_seed_mappings.with_modifier.tsv"
    cam_lexicon_expanded_rows = "combo_cam_lexicon_mappings.expanded.tsv"
    unresolved_after_cam_lexicon_expanded = "combo_unresolved_after_cam_lexicon.expanded.tsv"
    mappings_after_cam_lexicon_expanded = "combo_seed_mappings.with_cam_lexicon.expanded.tsv"
    hierarchy_rows = "combo_hierarchy_backoff_mappings.tsv"
    unresolved_after_hierarchy = "combo_unresolved_after_hierarchy_backoff.tsv"
    mappings_after_hierarchy = "combo_seed_mappings.with_hierarchy_backoff.tsv"
    bridge_tsv = "combo_bridge_table.tsv"
    bridge_crosswalk = "combo_bridge_crosswalk.tsv"
    endpoint_candidates = "combo_endpoint_candidates.tsv"
    semantic_rows = "combo_semantic_mappings.tsv"
    combined = "combo_seed_mappings.with_semantic.tsv"
    unresolved_after = "combo_unresolved_after_semantic.tsv"
    qc_json = "combo_mapping_qc.json"
    qc_md = "COMBO_SUBGRAPH_RUN_REPORT.md"

    # 1) Extract COMBO seed IDs from RTF.
    run_cmd(
        [
            args.python_bin,
            str(SCRIPTS_DIR / "extract_combo_seed_ids.py"),
            "--input",
            args.combo_rtf,
            "--output",
            combo_raw,
            "--unresolved-output",
            unresolved,
        ]
    )

    # 2) Deterministic mapping + optional crosswalk.
    cmd = [
        args.python_bin,
        str(SCRIPTS_DIR / "build_combo_seed_mappings.py"),
        "--input",
        combo_raw,
        "--output",
        deterministic,
        "--unresolved-output",
        unresolved,
    ]
    if args.crosswalk_files:
        cmd += ["--crosswalk-files", *args.crosswalk_files]
    run_cmd(cmd)

    # 3) Bridge table expansion (diagnostic + optional downstream crosswalk).
    cmd = [
        args.python_bin,
        str(SCRIPTS_DIR / "build_combo_bridge_table.py"),
        "--combo-mappings",
        deterministic,
        "--bridge-output",
        bridge_tsv,
        "--crosswalk-output",
        bridge_crosswalk,
    ]
    if args.crosswalk_files:
        cmd += ["--mapping-files", *args.crosswalk_files]
    run_cmd(cmd)

    # 4) Endpoint-derived semantic candidates from SemMedDB/iKraph.
    normalized_for_candidates = list(args.crosswalk_files) + [deterministic, bridge_crosswalk]
    cmd = [
        args.python_bin,
        str(SCRIPTS_DIR / "build_endpoint_candidate_dictionary.py"),
        "--semmed-edge-file",
        args.semmed_edge_file,
        "--ikraph-edge-file",
        args.ikraph_edge_file,
        "--output",
        endpoint_candidates,
        "--max-unique-endpoints",
        str(args.endpoint_max_unique_endpoints),
    ]
    if args.endpoint_fallback_label_to_id:
        cmd += ["--fallback-label-to-id"]
    if normalized_for_candidates:
        cmd += ["--normalized-files", *normalized_for_candidates]
    if args.ikraph_node_dict:
        cmd += ["--ikraph-node-dict", args.ikraph_node_dict]
    run_cmd(cmd)

    # 5) Conservative MRCONSO exact-match mapping stage.
    mrconso_path = REPO_ROOT / "MRCONSO.RRF"
    if mrconso_path.exists():
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "apply_mrconso_exact_mappings.py"),
                "--input-unresolved",
                unresolved,
                "--mrconso",
                str(mrconso_path),
                "--accepted-output",
                mrconso_exact,
                "--review-output",
                mrconso_review,
                "--remaining-output",
                unresolved_after_mrconso,
            ]
        )

        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "promote_mrconso_review_rules.py"),
                "--review-input",
                mrconso_review,
                "--unresolved-input",
                unresolved_after_mrconso,
                "--promoted-output",
                mrconso_review_promoted,
                "--unresolved-output",
                unresolved_after_mrconso_review,
            ]
        )

        merge_mapping_tsvs(Path(deterministic), Path(mrconso_exact), Path(deterministic_plus_mrconso))
        merge_mapping_tsvs(
            Path(deterministic_plus_mrconso),
            Path(mrconso_review_promoted),
            Path(deterministic_plus_mrconso_plus_review),
        )

        # 5b) Variant -> parent non-exact mappings.
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "promote_variant_to_parent_mappings.py"),
                "--unresolved-input",
                unresolved_after_mrconso_review,
                "--mapped-input",
                deterministic_plus_mrconso_plus_review,
                "--promoted-output",
                variant_parent_rows,
                "--remaining-output",
                unresolved_after_variant_parent,
            ]
        )
        merge_mapping_tsvs(
            Path(deterministic_plus_mrconso_plus_review),
            Path(variant_parent_rows),
            Path(mappings_after_variant_parent),
        )

        # 5c) CAM lexicon non-exact mappings.
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "apply_cam_lexicon_mappings.py"),
                "--unresolved-input",
                unresolved_after_variant_parent,
                "--mapped-input",
                mappings_after_variant_parent,
                "--lexicon",
                str(SCRIPTS_DIR / "cam_lexicon_seed.tsv"),
                "--promoted-output",
                cam_lexicon_rows,
                "--remaining-output",
                unresolved_after_cam_lexicon,
            ]
        )
        merge_mapping_tsvs(
            Path(mappings_after_variant_parent),
            Path(cam_lexicon_rows),
            Path(mappings_after_cam_lexicon),
        )

        # 5d) Prepare + apply manual curation stage.
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "prepare_manual_curation_from_review.py"),
                "--review-input",
                "combo_mrconso_exact_review.remaining.tsv",
                "--output",
                manual_template,
            ]
        )
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "apply_manual_curation_mappings.py"),
                "--manual-input",
                manual_template,
                "--unresolved-input",
                unresolved_after_cam_lexicon,
                "--promoted-output",
                manual_rows,
                "--remaining-output",
                unresolved_after_manual,
            ]
        )
        merge_mapping_tsvs(
            Path(mappings_after_cam_lexicon),
            Path(manual_rows),
            Path(mappings_after_manual),
        )

        # 5e) Modifier-stripping mappings (non-exact).
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "promote_modifier_stripping_mappings.py"),
                "--unresolved-input",
                unresolved_after_manual,
                "--mapped-input",
                mappings_after_manual,
                "--promoted-output",
                modifier_rows,
                "--remaining-output",
                unresolved_after_modifier,
            ]
        )
        merge_mapping_tsvs(
            Path(mappings_after_manual),
            Path(modifier_rows),
            Path(mappings_after_modifier),
        )

        # 5f) Expanded CAM lexicon pass on reduced remainder.
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "apply_cam_lexicon_mappings.py"),
                "--unresolved-input",
                unresolved_after_modifier,
                "--mapped-input",
                mappings_after_modifier,
                "--lexicon",
                str(SCRIPTS_DIR / "cam_lexicon_seed.tsv"),
                "--promoted-output",
                cam_lexicon_expanded_rows,
                "--remaining-output",
                unresolved_after_cam_lexicon_expanded,
            ]
        )
        merge_mapping_tsvs(
            Path(mappings_after_modifier),
            Path(cam_lexicon_expanded_rows),
            Path(mappings_after_cam_lexicon_expanded),
        )

        # 5g) Hierarchy backoff pass (non-exact).
        run_cmd(
            [
                args.python_bin,
                str(SCRIPTS_DIR / "promote_hierarchy_backoff_mappings.py"),
                "--unresolved-input",
                unresolved_after_cam_lexicon_expanded,
                "--mapped-input",
                mappings_after_cam_lexicon_expanded,
                "--promoted-output",
                hierarchy_rows,
                "--remaining-output",
                unresolved_after_hierarchy,
            ]
        )
        merge_mapping_tsvs(
            Path(mappings_after_cam_lexicon_expanded),
            Path(hierarchy_rows),
            Path(mappings_after_hierarchy),
        )

        semantic_input_unresolved = unresolved_after_hierarchy
        semantic_base_mappings = mappings_after_hierarchy
    else:
        print("[warn] MRCONSO.RRF not found; skipping exact MRCONSO mapping stage.")
        semantic_input_unresolved = unresolved
        semantic_base_mappings = deterministic

    # 6) Semantic fallback.
    candidate_files = [endpoint_candidates, *args.extra_candidate_files]
    run_cmd(
        [
            args.python_bin,
            str(SCRIPTS_DIR / "combo_semantic_fallback.py"),
            "--input-unresolved",
            semantic_input_unresolved,
            "--base-mappings",
            semantic_base_mappings,
            "--candidate-files",
            *candidate_files,
            "--threshold",
            str(args.semantic_threshold),
            "--margin",
            str(args.semantic_margin),
            "--max-candidates",
            str(args.semantic_max_candidates),
            "--semantic-output",
            semantic_rows,
            "--combined-output",
            combined,
            "--remaining-output",
            unresolved_after,
        ]
    )

    # 7) CAM extraction with combined mapping set.
    normalized_files = [combined, bridge_crosswalk, *args.crosswalk_files, *args.extra_normalized_files]
    cmd = [
        args.python_bin,
        str(REPO_ROOT / "cam_subgraph_extract.py"),
        "--ontology-dir",
        args.ontology_dir,
        "--normalized-files",
        *normalized_files,
        "--semmed-edge-file",
        args.semmed_edge_file,
        "--ikraph-edge-file",
        args.ikraph_edge_file,
        "--output-dir",
        args.output_dir,
    ]
    if args.ikraph_node_dict:
        cmd += ["--ikraph-node-dict", args.ikraph_node_dict]
    run_cmd(cmd)

    # 8) QC report.
    run_cmd(
        [
            args.python_bin,
            str(SCRIPTS_DIR / "evaluate_combo_seed_coverage.py"),
            "--raw-concepts",
            combo_raw,
            "--combined-mappings",
            combined,
            "--semantic-mappings",
            semantic_rows,
            "--remaining-unresolved",
            unresolved_after,
            "--cam-nodes",
            str(Path(args.output_dir) / "cam_nodes.tsv"),
            "--cam-edges",
            str(Path(args.output_dir) / "cam_edges.tsv"),
            "--cam-stats",
            str(Path(args.output_dir) / "cam_stats.json"),
            "--output-json",
            qc_json,
            "--output-md",
            qc_md,
        ]
    )

    print("[done] Completed COMBO retention iteration.")
    print(f"[done] QC JSON: {qc_json}")
    print(f"[done] QC report: {qc_md}")


if __name__ == "__main__":
    main()
