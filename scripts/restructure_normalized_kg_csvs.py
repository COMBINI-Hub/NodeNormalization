#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


GENERIC_CATEGORIES = {
    "biolink:NamedThing",
    "biolink:BiologicalEntity",
    "biolink:OrganismalEntity",
    "biolink:SubjectOfInvestigation",
    "biolink:ThingWithTaxon",
    "biolink:PhysicalEssence",
    "biolink:PhysicalEssenceOrOccurrent",
}


def sanitize_label(value: str) -> str:
    return (
        value.replace("biolink:", "")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
    )


def pick_most_specific_category(categories_field: str) -> str:
    categories = [c.strip() for c in categories_field.split(";") if c.strip()]
    if not categories:
        return "NamedThing"
    for category in categories:
        if category not in GENERIC_CATEGORIES:
            return sanitize_label(category)
    return sanitize_label(categories[0])


def predicate_to_reltype(predicate: str) -> str:
    pred = predicate.strip()
    if not pred:
        return "RELATED_TO"
    pred = pred.replace("biolink:", "")
    pred = pred.replace("-", "_").replace(" ", "_").replace("/", "_")
    pred = pred.upper()
    return pred or "RELATED_TO"


def transform_nodes(src: Path, dst: Path, source_label: str) -> None:
    with src.open("r", encoding="utf-8", newline="") as infile, dst.open(
        "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            specific = pick_most_specific_category(row.get("categories:string[]", ""))
            row[":LABEL"] = f"{source_label};{specific}"
            writer.writerow(row)


def transform_edges(src: Path, dst: Path) -> None:
    with src.open("r", encoding="utf-8", newline="") as infile, dst.open(
        "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row[":TYPE"] = predicate_to_reltype(row.get("biolink_predicate", ""))
            writer.writerow(row)


def main() -> int:
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
    parser = argparse.ArgumentParser(
        description="Restructure normalized SemMed+iKraph CSVs for Neo4j labels/types."
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    semmed_nodes_in = args.input_dir / "semmed_nodes_normalized.csv"
    ikraph_nodes_in = args.input_dir / "ikraph_nodes_normalized.csv"
    semmed_edges_in = args.input_dir / "semmed_edges_normalized.csv"
    ikraph_edges_in = args.input_dir / "ikraph_edges_normalized.csv"

    semmed_nodes_out = args.output_dir / "semmed_nodes_normalized.csv"
    ikraph_nodes_out = args.output_dir / "ikraph_nodes_normalized.csv"
    semmed_edges_out = args.output_dir / "semmed_edges_normalized.csv"
    ikraph_edges_out = args.output_dir / "ikraph_edges_normalized.csv"

    for p in [semmed_nodes_in, ikraph_nodes_in, semmed_edges_in, ikraph_edges_in]:
        if not p.exists():
            raise SystemExit(f"Missing input file: {p}")

    transform_nodes(semmed_nodes_in, semmed_nodes_out, "SemmedEntity")
    transform_nodes(ikraph_nodes_in, ikraph_nodes_out, "IKraphEntity")
    transform_edges(semmed_edges_in, semmed_edges_out)
    transform_edges(ikraph_edges_in, ikraph_edges_out)
    print(f"[done] Wrote transformed CSVs to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
