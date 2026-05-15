"""
Step 4 — restructure_for_neo4j

Post-processes the four normalized CSVs produced by build_normalized_import_csvs to
set the Neo4j-required :LABEL column on nodes and :TYPE column on edges.

  :LABEL  = "<source_label>;<MostSpecificCategory>"  e.g. "SemmedEntity;Disease"
  :TYPE   = uppercase snake_case biolink predicate    e.g. "TREATS"

Inputs  (from gen_med_graph/):  semmed_nodes_normalized.csv, semmed_edges_normalized.csv,
                                 ikraph_nodes_normalized.csv, ikraph_edges_normalized.csv
Outputs (to gen_med_graph_neo4j/): same four filenames with :LABEL/:TYPE populated.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import List

GENERIC_CATEGORIES: frozenset[str] = frozenset(
    {
        "biolink:NamedThing",
        "biolink:BiologicalEntity",
        "biolink:OrganismalEntity",
        "biolink:SubjectOfInvestigation",
        "biolink:ThingWithTaxon",
        "biolink:PhysicalEssence",
        "biolink:PhysicalEssenceOrOccurrent",
    }
)


def _sanitize_label(value: str) -> str:
    return (
        value.replace("biolink:", "")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
    )


def _most_specific_category(categories_field: str) -> str:
    cats = [c.strip() for c in categories_field.split(";") if c.strip()]
    if not cats:
        return "NamedThing"
    for cat in cats:
        if cat not in GENERIC_CATEGORIES:
            return _sanitize_label(cat)
    return _sanitize_label(cats[0])


def _predicate_to_reltype(predicate: str) -> str:
    pred = (predicate or "").strip()
    if not pred:
        return "RELATED_TO"
    return (
        pred.replace("biolink:", "")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .upper()
    ) or "RELATED_TO"


def _transform_nodes(src: Path, dst: Path, source_label: str) -> int:
    written = 0
    with (
        src.open("r", encoding="utf-8", newline="") as infile,
        dst.open("w", encoding="utf-8", newline="") as outfile,
    ):
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            specific = _most_specific_category(row.get("categories:string[]") or "")
            row[":LABEL"] = f"{source_label};{specific}"
            writer.writerow(row)
            written += 1
    return written


def _transform_edges(src: Path, dst: Path) -> int:
    written = 0
    with (
        src.open("r", encoding="utf-8", newline="") as infile,
        dst.open("w", encoding="utf-8", newline="") as outfile,
    ):
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row[":TYPE"] = _predicate_to_reltype(row.get("biolink_predicate") or "")
            writer.writerow(row)
            written += 1
    return written


def restructure_for_neo4j(input_dir: Path, output_dir: Path) -> List[Path]:
    """
    Read the four CSVs from input_dir, decorate :LABEL/:TYPE, write to output_dir.

    Returns the list of output paths.
    """
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
    output_dir.mkdir(parents=True, exist_ok=True)

    pairs = [
        ("semmed_nodes_normalized.csv", "semmed_nodes_normalized.csv", "nodes", "SemmedEntity"),
        ("ikraph_nodes_normalized.csv", "ikraph_nodes_normalized.csv", "nodes", "IKraphEntity"),
        ("semmed_edges_normalized.csv", "semmed_edges_normalized.csv", "edges", None),
        ("ikraph_edges_normalized.csv", "ikraph_edges_normalized.csv", "edges", None),
    ]

    outputs: List[Path] = []
    for in_name, out_name, kind, label in pairs:
        src = input_dir / in_name
        dst = output_dir / out_name
        if not src.exists():
            raise FileNotFoundError(f"Missing input file: {src}")
        if kind == "nodes":
            n = _transform_nodes(src, dst, label)  # type: ignore[arg-type]
        else:
            n = _transform_edges(src, dst)
        print(f"[restructure] {out_name}: {n:,} rows", file=sys.stderr)
        outputs.append(dst)

    return outputs
