#!/usr/bin/env python3
"""
Build semmed_concepts_seed_adjacent.tsv from a SemMed subgraph edge file and
the NodeNorm semmed normalization JSON.

Inputs:
  --semmed-edges-file    semmed_edges_seed_adjacent.tsv  (TSV, :START_ID / :END_ID columns)
  --normalized-json-file semmed_normalized_full_with_pubchem.json (NodeNorm output keyed by CUI)
  --semmed-concepts-csv  concept.csv from SemMedDB (fallback display names + semantic types)
  --output-file          semmed_concepts_seed_adjacent.tsv

Output columns (tab-separated):
  semmed_id, curie, label, display_name, primary_type, types,
  equivalent_identifiers, equivalent_labels, equivalent_types,
  equivalent_identifiers_count, normalized_found, source
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def collect_semmed_ids(edges_path: Path) -> Set[str]:
    ids: Set[str] = set()
    with edges_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            start = (row.get(":START_ID") or "").strip()
            end = (row.get(":END_ID") or "").strip()
            if start:
                ids.add(start)
            if end:
                ids.add(end)
    return ids


def load_raw_concepts(concepts_csv: Path) -> Dict[str, dict]:
    """Load raw SemMedDB concept.csv rows (no header) keyed by CUI."""
    by_cui: Dict[str, dict] = {}
    if not concepts_csv.exists():
        return by_cui
    with concepts_csv.open(encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 3:
                continue
            cui = (row[0] or "").strip()
            if not cui or cui in by_cui:
                continue
            name = (row[1] or "").strip()
            semtype = (row[2] or "").strip()
            by_cui[cui] = {
                "name": name,
                "semtype": semtype,
                "primary_type": f"semmed:{semtype}" if semtype else None,
            }
    return by_cui


def load_normalized_json(json_path: Path) -> Dict[str, dict]:
    """
    Stream-parse the NodeNorm output JSON via ijson if available, else json.load.
    Returns a dict keyed by CUI (external_id) → normalization record.
    """
    try:
        import ijson  # type: ignore[import-untyped]
    except ImportError:
        with json_path.open(encoding="utf-8") as fh:
            return json.load(fh)

    out: Dict[str, dict] = {}
    with json_path.open("rb") as fh:
        for key, value in ijson.kvitems(fh, ""):
            if isinstance(value, dict):
                out[str(key)] = value
    return out


def extract_norm_record(
    cui: str,
    norm_map: Dict[str, dict],
    raw_concepts: Dict[str, dict],
) -> Tuple[str, str, str, str, str, str, str, str, int, bool]:
    """
    Returns:
      curie, label, display_name, primary_type,
      pipe_types, pipe_eq_identifiers, pipe_eq_labels, pipe_eq_types,
      eq_count, normalized_found
    """
    raw = raw_concepts.get(cui, {})
    raw_name = raw.get("name", "")
    raw_type = raw.get("primary_type", "")

    entry = norm_map.get(cui)
    if not entry:
        curie = f"UMLS:{cui}"
        return (
            curie, raw_name, raw_name,
            raw_type or f"semmed:unk",
            "", "", "", "",
            0, False,
        )

    id_block = entry.get("id") or {}
    curie = (id_block.get("identifier") or f"UMLS:{cui}").strip()
    label = (id_block.get("label") or raw_name or "").strip()
    display_name = label or raw_name

    types_list: List[str] = entry.get("type") or []
    primary_type = types_list[0] if types_list else (raw_type or "")
    pipe_types = "|".join(str(t) for t in types_list)

    eq_id_blocks: List[dict] = entry.get("equivalent_identifiers") or []
    eq_identifiers: List[str] = []
    eq_labels: List[str] = []
    eq_types: List[str] = []
    for eq in eq_id_blocks:
        if isinstance(eq, dict):
            eq_id = (eq.get("identifier") or "").strip()
            eq_lbl = (eq.get("label") or "").strip()
            eq_type = primary_type
            if eq_id:
                eq_identifiers.append(eq_id)
                eq_labels.append(eq_lbl)
                eq_types.append(eq_type)

    pipe_eq_ids = "|".join(eq_identifiers)
    pipe_eq_labels = "|".join(eq_labels)
    pipe_eq_types = "|".join(eq_types)

    return (
        curie, label, display_name, primary_type,
        pipe_types, pipe_eq_ids, pipe_eq_labels, pipe_eq_types,
        len(eq_identifiers), True,
    )


def main() -> int:
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))

    parser = argparse.ArgumentParser(
        description="Build semmed_concepts_seed_adjacent.tsv for the CM subgraph pipeline."
    )
    parser.add_argument(
        "--semmed-edges-file",
        type=Path,
        required=True,
        help="semmed_edges_seed_adjacent.tsv (TSV with :START_ID / :END_ID columns).",
    )
    parser.add_argument(
        "--normalized-json-file",
        type=Path,
        required=True,
        help="semmed_normalized_full_with_pubchem.json (NodeNorm output keyed by CUI).",
    )
    parser.add_argument(
        "--semmed-concepts-csv",
        type=Path,
        required=True,
        help="concept.csv from SemMedDB (fallback names + semantic types, no header).",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        required=True,
        help="Output path for semmed_concepts_seed_adjacent.tsv.",
    )
    args = parser.parse_args()

    for p in [args.semmed_edges_file, args.normalized_json_file, args.semmed_concepts_csv]:
        if not p.exists():
            raise SystemExit(f"Missing input file: {p}")

    args.output_file.parent.mkdir(parents=True, exist_ok=True)

    print("[1/4] Collecting SemMed IDs from edges file...", file=sys.stderr)
    semmed_ids = collect_semmed_ids(args.semmed_edges_file)
    print(f"      found {len(semmed_ids):,} unique SemMed IDs", file=sys.stderr)

    print("[2/4] Loading raw SemMed concept.csv...", file=sys.stderr)
    raw_concepts = load_raw_concepts(args.semmed_concepts_csv)
    print(f"      loaded {len(raw_concepts):,} raw concept rows", file=sys.stderr)

    print("[3/4] Loading normalization JSON (may take a while for large files)...", file=sys.stderr)
    norm_map = load_normalized_json(args.normalized_json_file)
    print(f"      loaded {len(norm_map):,} normalization entries", file=sys.stderr)

    print("[4/4] Writing output TSV...", file=sys.stderr)
    columns = [
        "semmed_id", "curie", "label", "display_name", "primary_type",
        "types", "equivalent_identifiers", "equivalent_labels", "equivalent_types",
        "equivalent_identifiers_count", "normalized_found", "source",
    ]

    written = 0
    missing_norm = 0
    with args.output_file.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for cui in sorted(semmed_ids):
            (
                curie, label, display_name, primary_type,
                pipe_types, pipe_eq_ids, pipe_eq_labels, pipe_eq_types,
                eq_count, normalized_found,
            ) = extract_norm_record(cui, norm_map, raw_concepts)

            if not normalized_found:
                missing_norm += 1

            writer.writerow({
                "semmed_id": cui,
                "curie": curie,
                "label": label,
                "display_name": display_name,
                "primary_type": primary_type,
                "types": pipe_types,
                "equivalent_identifiers": pipe_eq_ids,
                "equivalent_labels": pipe_eq_labels,
                "equivalent_types": pipe_eq_types,
                "equivalent_identifiers_count": eq_count,
                "normalized_found": str(normalized_found).lower(),
                "source": "SemMedDB",
            })
            written += 1

    print(f"[done] wrote {written:,} rows -> {args.output_file}", file=sys.stderr)
    if missing_norm:
        print(
            f"       {missing_norm:,} CUIs had no normalization entry (fallback used)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
