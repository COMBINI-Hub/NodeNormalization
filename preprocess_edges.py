#!/usr/bin/env python3
"""
Preprocess SemMedDB and iKraph edge files for CAM integration.
- Deduplicate edges (source, target, predicate)
- Remove self-loops
- For iKraph, map integer relation types to string names using RelTypeInt.json
- Output cleaned edge files for downstream use
"""
import csv
import json
from pathlib import Path

def dedupe_and_filter_edges(input_path, output_path, source_col, target_col, pred_col, pred_map=None):
    seen = set()
    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        # For SemMedDB, handle files with no header (positional columns)
        if source_col == 0:
            writer = csv.writer(outfile)
            writer.writerow(["source", "target", "predicate", "extra"])
            for line in infile:
                parts = line.rstrip().split(",")
                if len(parts) < 3:
                    continue
                source, target, pred = parts[0], parts[1], parts[2]
                if source == target:
                    continue
                key = (source, target, pred)
                if key in seen:
                    continue
                seen.add(key)
                # Write all columns, pad if needed
                row = parts + [None] * (4 - len(parts))
                writer.writerow(row[:4])
        else:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + (["predicate_label"] if pred_map else [])
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                source = row[source_col]
                target = row[target_col]
                pred = row[pred_col]
                if source == target:
                    continue  # Remove self-loop
                if pred_map:
                    pred_label = pred_map.get(str(pred), None)
                    if pred_label:
                        row["predicate_label"] = pred_label
                key = (source, target, pred)
                if key in seen:
                    continue
                seen.add(key)
                writer.writerow(row)

def load_ikraph_pred_map(reltype_path):
    with open(reltype_path, 'r', encoding='utf-8') as f:
        reltypes = json.load(f)
    return {rt["intRep"]: rt["relType"] for rt in reltypes}

def preprocess_semmeddb():
    semmed_in = Path("../neo4jexploration/semmed_data/connections.csv")
    semmed_out = Path("semmeddb_edges_cleaned.csv")
    # Process as headerless, assign correct headers for output
    seen = set()
    with open(semmed_in, 'r', encoding='utf-8') as infile, open(semmed_out, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow([':START_ID', ':END_ID', ':TYPE', 'frequency%'])
        for line in infile:
            parts = line.rstrip().split(',')
            if len(parts) < 3:
                continue
            source, target, pred = parts[0], parts[1], parts[2]
            if source == target:
                continue
            key = (source, target, pred)
            if key in seen:
                continue
            seen.add(key)
            row = parts + [None] * (4 - len(parts))
            writer.writerow(row[:4])
    print(f"Wrote cleaned SemMedDB edges to {semmed_out}")
    print(f"Wrote cleaned SemMedDB edges to {semmed_out}")

def preprocess_ikraph():
    ikraph_in = Path("../neo4jexploration/iKraph_raw/iKraph_full/DBRelations.json")
    reltype_path = Path("../neo4jexploration/iKraph_raw/iKraph_full/RelTypeInt.json")
    ikraph_out = Path("ikraph_edges_cleaned.csv")
    pred_map = load_ikraph_pred_map(reltype_path)
    # DBRelations.json is a list of dicts with keys: node_one_id, node_two_id, relationship_type
    with open(ikraph_in, 'r', encoding='utf-8') as infile, open(ikraph_out, 'w', encoding='utf-8', newline='') as outfile:
        data = json.load(infile)
        fieldnames = list(data[0].keys()) + ["predicate_label"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        seen = set()
        for row in data:
            source = row["node_one_id"]
            target = row["node_two_id"]
            pred = str(row["relationship_type"])
            if source == target:
                continue
            pred_label = pred_map.get(pred, None)
            if pred_label:
                row["predicate_label"] = pred_label
            key = (source, target, pred)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow(row)
    print(f"Wrote cleaned iKraph edges to {ikraph_out}")

def output_is_filled(path: Path, min_lines: int = 2) -> bool:
    """Return True if path exists and has at least min_lines lines (e.g. header + data)."""
    if not path.exists():
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, _ in enumerate(f):
                if i >= min_lines:
                    return True
        return False
    except OSError:
        return False


def main():
    semmed_out = Path("semmeddb_edges_cleaned.csv")
    ikraph_out = Path("ikraph_edges_cleaned.csv")
    if not output_is_filled(semmed_out):
        print("SemMedDB cleaned file missing or empty; running SemMedDB preprocess...")
        preprocess_semmeddb()
    else:
        print(f"SemMedDB output already filled ({semmed_out}), skipping.")
    if not output_is_filled(ikraph_out):
        print("iKraph cleaned file missing or empty; running iKraph preprocess...")
        preprocess_ikraph()
    else:
        print(f"iKraph output already filled ({ikraph_out}), skipping.")


if __name__ == "__main__":
    main()
