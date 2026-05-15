"""
Step 6 — build_cm_concept_table

Enriches the CM subgraph SemMed edges with full concept metadata from the
NodeNorm semmed normalization JSON and the raw SemMedDB concept.csv.

Input:  cm_subgraph/semmed_edges_combo_seeds_and_1hop.tsv
Output: cm_subgraph/semmed_concepts_combo_seeds_and_1hop.tsv

Output columns (tab-separated):
  semmed_id, curie, label, display_name, primary_type, types,
  equivalent_identifiers, equivalent_labels, equivalent_types,
  equivalent_identifiers_count, normalized_found, source
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def _collect_semmed_ids(edges_tsv: Path) -> Set[str]:
    ids: Set[str] = set()
    with edges_tsv.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            for col in (":START_ID", ":END_ID"):
                val = (row.get(col) or "").strip()
                if val:
                    ids.add(val)
    return ids


def _load_raw_concepts(concepts_csv: Path) -> Dict[str, dict]:
    """Load SemMedDB concept.csv (no header) keyed by CUI."""
    by_cui: Dict[str, dict] = {}
    if not concepts_csv.exists():
        return by_cui
    with concepts_csv.open(encoding="utf-8", errors="replace", newline="") as fh:
        for row in csv.reader(fh):
            if len(row) < 3:
                continue
            cui = (row[0] or "").strip()
            if not cui or cui in by_cui:
                continue
            by_cui[cui] = {
                "name": (row[1] or "").strip(),
                "semtype": (row[2] or "").strip(),
            }
    return by_cui


def _load_norm_json(json_path: Path) -> Dict[str, dict]:
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


def _extract_record(
    cui: str,
    norm_map: Dict[str, dict],
    raw_concepts: Dict[str, dict],
) -> Tuple[str, str, str, str, str, str, str, str, int, bool]:
    raw = raw_concepts.get(cui, {})
    raw_name = raw.get("name", "")
    raw_semtype = raw.get("semtype", "")

    entry = norm_map.get(cui)
    if not entry:
        return (
            f"UMLS:{cui}", raw_name, raw_name,
            f"semmed:{raw_semtype}" if raw_semtype else "semmed:unk",
            "", "", "", "", 0, False,
        )

    id_block = entry.get("id") or {}
    curie = (id_block.get("identifier") or f"UMLS:{cui}").strip()
    label = (id_block.get("label") or raw_name or "").strip()

    types_list: List[str] = entry.get("type") or []
    primary_type = types_list[0] if types_list else (f"semmed:{raw_semtype}" if raw_semtype else "")
    pipe_types = "|".join(str(t) for t in types_list)

    eq_blocks: List[dict] = entry.get("equivalent_identifiers") or []
    eq_ids, eq_labels, eq_types = [], [], []
    for eq in eq_blocks:
        if isinstance(eq, dict):
            eq_id = (eq.get("identifier") or "").strip()
            eq_lbl = (eq.get("label") or "").strip()
            if eq_id:
                eq_ids.append(eq_id)
                eq_labels.append(eq_lbl)
                eq_types.append(primary_type)

    return (
        curie, label, label or raw_name, primary_type,
        pipe_types,
        "|".join(eq_ids), "|".join(eq_labels), "|".join(eq_types),
        len(eq_ids), True,
    )


def build_cm_concept_table(
    semmed_edges_tsv: Path,
    semmed_normalized_json: Path,
    semmed_concepts_csv: Path,
    output_tsv: Path,
) -> Path:
    """
    Build semmed_concepts_combo_seeds_and_1hop.tsv from the CM subgraph edge file.

    Returns the output path.
    """
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
    output_tsv.parent.mkdir(parents=True, exist_ok=True)

    print("[cm_concepts] collecting SemMed IDs from edges file...", file=sys.stderr)
    semmed_ids = _collect_semmed_ids(semmed_edges_tsv)
    print(f"[cm_concepts] {len(semmed_ids):,} unique SemMed IDs", file=sys.stderr)

    print("[cm_concepts] loading raw concept.csv...", file=sys.stderr)
    raw_concepts = _load_raw_concepts(semmed_concepts_csv)
    print(f"[cm_concepts] {len(raw_concepts):,} raw concept rows", file=sys.stderr)

    print("[cm_concepts] loading normalization JSON...", file=sys.stderr)
    norm_map = _load_norm_json(semmed_normalized_json)
    print(f"[cm_concepts] {len(norm_map):,} normalization entries", file=sys.stderr)

    columns = [
        "semmed_id", "curie", "label", "display_name", "primary_type",
        "types", "equivalent_identifiers", "equivalent_labels", "equivalent_types",
        "equivalent_identifiers_count", "normalized_found", "source",
    ]

    written = 0
    missing_norm = 0
    with output_tsv.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for cui in sorted(semmed_ids):
            (
                curie, label, display_name, primary_type,
                pipe_types, pipe_eq_ids, pipe_eq_labels, pipe_eq_types,
                eq_count, normalized_found,
            ) = _extract_record(cui, norm_map, raw_concepts)
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

    print(f"[cm_concepts] wrote {written:,} rows → {output_tsv}", file=sys.stderr)
    if missing_norm:
        print(f"[cm_concepts] {missing_norm:,} CUIs had no normalization entry", file=sys.stderr)
    return output_tsv
