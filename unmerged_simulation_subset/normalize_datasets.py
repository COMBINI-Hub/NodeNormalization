#!/usr/bin/env python3
"""
Consolidated Dataset Normalization Script

This script handles the complete normalization pipeline for biomedical datasets:
1. CSV → JSONL compendia conversion (individual dataset processing)
2. JSONL → Enhanced normalized JSON conversion (with type hierarchies)

Supports all four datasets: BioKDE, iKGraph, PrimeKG, SemMedDB

Usage:
    # Process all datasets
    python normalize_datasets.py --all
    
    # Process specific dataset
    python normalize_datasets.py --dataset biokde
    
    # Only convert CSV to JSONL (step 1)
    python normalize_datasets.py --dataset biokde --step csv-to-jsonl
    
    # Only convert JSONL to normalized JSON (step 2)  
    python normalize_datasets.py --dataset biokde --step jsonl-to-json
"""

import argparse
import csv
import json
import sys
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import requests
import time


# Dataset configurations
DATASET_CONFIGS = {
    "biokde": {
        "csv_file": "biokde_nodes.csv",
        "compendia_file": "biokde_compendia_sample.jsonl",
        "normalized_file": "biokde_normalized_sample.json",
        "description": "BioKDE: Deep learning-powered search engine and knowledge graph"
    },
    "ikraph": {
        "csv_file": "ikraph_nodes.csv", 
        "compendia_file": "ikraph_compendia_sample.jsonl",
        "normalized_file": "ikraph_normalized_sample.json",
        "description": "iKGraph: Biomedical knowledge graph with gene relationships"
    },
    "primekg": {
        "csv_file": "primekg_nodes.csv",
        "compendia_file": "primekg_compendia_sample.jsonl", 
        "normalized_file": "primekg_normalized_sample.json",
        "description": "PrimeKG: Multimodal knowledge graph for precision medicine"
    },
    "semmeddb": {
        "csv_file": "semmeddb_nodes.csv",
        "compendia_file": "semmeddb_compendia_sample.jsonl",
        "normalized_file": "semmeddb_normalized_sample.json", 
        "description": "SemMedDB: Semantic database from biomedical literature"
    }
}


def map_biokde_to_curie(node_id: str, node_type: str, node_name: str) -> Optional[str]:
    """Map BioKDE node identifiers to CURIEs."""
    node_id = (node_id or "").strip()
    if node_id.startswith("http://mouse.brain-map.org/atlas/index.html#"):
        return f"ABA:{node_id.split('#')[-1]}"
    return None


def map_ikraph_to_curie(node_id: str, node_type: str, node_name: str) -> Optional[str]:
    """Map iKGraph node identifiers to CURIEs."""
    node_id = (node_id or "").strip()
    if node_type == "Gene" and node_id.startswith("NCBI:"):
        return node_id
    return None


def map_primekg_to_curie(node_id: str, node_type: str, node_name: str) -> Optional[str]:
    """Map PrimeKG node identifiers to CURIEs."""
    node_id = (node_id or "").strip()
    node_type = (node_type or "").strip()
    
    # PrimeKG uses numeric IDs that need to be mapped to CURIEs
    if node_type == "gene/protein":
        return f"NCBIGene:{node_id}"
    elif node_type == "disease":
        return f"MONDO:{node_id}"
    elif node_type == "drug":
        return f"CHEBI:{node_id}"
    return None


def map_semmeddb_to_curie(node_id: str, node_type: str, node_name: str) -> Optional[str]:
    """Map SemMedDB node identifiers to CURIEs."""
    node_id = (node_id or "").strip()
    if node_id.startswith("C") and len(node_id) == 8 and node_id[1:].isdigit():
        return f"UMLS:{node_id}"
    return None


# Mapping functions for each dataset
CURIE_MAPPERS = {
    "biokde": map_biokde_to_curie,
    "ikraph": map_ikraph_to_curie, 
    "primekg": map_primekg_to_curie,
    "semmeddb": map_semmeddb_to_curie
}


def iter_biokde_nodes(csv_path: Path) -> Iterable[Dict[str, str]]:
    """Iterate over BioKDE nodes CSV file."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 8:
                yield {
                    "id": row[0],
                    "type": row[1], 
                    "subtype": row[2],
                    "external_id": row[3],
                    "species": row[4],
                    "official_name": row[5],
                    "common_name": row[6],
                    ":LABEL": row[7],
                    "source": row[8] if len(row) > 8 else "BioKDE"
                }


def iter_ikraph_nodes(csv_path: Path) -> Iterable[Dict[str, str]]:
    """Iterate over iKGraph nodes CSV file."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {
                "id": row.get("id", ""),
                "type": row.get("type", ""),
                "external_id": row.get("external_id", ""),
                "official_name": row.get("official_name", ""),
                "source": "iKGraph"
            }


def iter_primekg_nodes(csv_path: Path) -> Iterable[Dict[str, str]]:
    """Iterate over PrimeKG nodes CSV file."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {
                "id": row.get("node_id", ""),
                "type": row.get("node_type", ""),
                "name": row.get("node_name", ""),
                "source": "PrimeKG"
            }


def iter_semmeddb_nodes(csv_path: Path) -> Iterable[Dict[str, str]]:
    """Iterate over SemMedDB nodes CSV file."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 13:
                yield {
                    "0": row[0],  # First column - ID
                    "1": row[1],  # Second column
                    "2": row[2],  # Third column
                    "3": row[3],  # Fourth column - external_id (UMLS CUID)
                    "4": row[4],  # Fifth column - official_name
                    "5": row[5],  # Sixth column - type (semantic type)
                    "6": row[6],  # Seventh column
                    "7": row[7],  # Eighth column
                    "8": row[8],  # Ninth column
                    "9": row[9],  # Tenth column
                    "10": row[10], # Eleventh column
                    "11": row[11], # Twelfth column
                    "12": row[12]  # Thirteenth column - source
                }


# Node iterators for each dataset
NODE_ITERATORS = {
    "biokde": iter_biokde_nodes,
    "ikraph": iter_ikraph_nodes,
    "primekg": iter_primekg_nodes, 
    "semmeddb": iter_semmeddb_nodes
}


def convert_csv_to_jsonl(dataset: str, csv_path: Path, output_path: Path, limit: Optional[int] = None) -> bool:
    """Convert CSV to JSONL compendia format."""
    print(f"Converting {dataset.upper()} CSV to JSONL compendia...")
    
    mapper = CURIE_MAPPERS[dataset]
    iterator = NODE_ITERATORS[dataset]
    
    processed = 0
    mapped = 0
    compendia_records = []
    
    for node_data in iterator(csv_path):
        if limit and processed >= limit:
            break
            
        # Extract relevant fields based on dataset
        if dataset == "biokde":
            node_id = node_data["id"]
            node_type = node_data["type"]
            node_name = node_data["official_name"]
        elif dataset == "ikraph":
            node_id = node_data["external_id"]
            node_type = node_data["type"]
            node_name = node_data["official_name"]
        elif dataset == "primekg":
            node_id = node_data["id"]
            node_type = node_data["type"]
            node_name = node_data["name"]
        elif dataset == "semmeddb":
            node_id = node_data["3"]  # external_id column
            node_type = node_data["5"]  # type column
            node_name = node_data["4"]  # official_name column
        
        # Map to CURIE
        curie = mapper(node_id, node_type, node_name)
        if curie:
            compendia_record = {
                "id": {
                    "identifier": curie,
                    "label": node_name or curie
                },
                "equivalent_identifiers": [{"identifier": curie, "label": node_name or curie}],
                "type": [f"biolink:{node_type}" if node_type else "biolink:NamedThing"],
                "source_databases": [dataset.upper()]
            }
            compendia_records.append(compendia_record)
            mapped += 1
        
        processed += 1
    
    # Write JSONL file
    with open(output_path, 'w') as f:
        for record in compendia_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"Processed {processed} nodes, mapped {mapped} to CURIEs")
    print(f"Saved {len(compendia_records)} compendia records to {output_path}")
    
    return len(compendia_records) > 0


def convert_jsonl_to_normalized_json(dataset: str, compendia_path: Path, output_path: Path) -> bool:
    """Convert JSONL compendia to enhanced normalized JSON using existing data lookup."""
    print(f"Converting {dataset.upper()} JSONL to enhanced normalized JSON...")
    
    # Use the existing jsonl_to_normalized_json.py script
    cmd = [
        sys.executable, "jsonl_to_normalized_json.py",
        "--compendia", str(compendia_path),
        "--output", str(output_path),
        "--dataset", dataset
    ]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Successfully converted {dataset} JSONL to normalized JSON")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error converting {dataset}: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False


def process_dataset(dataset: str, step: str = "all") -> bool:
    """Process a single dataset through the normalization pipeline."""
    if dataset not in DATASET_CONFIGS:
        print(f"Unknown dataset: {dataset}")
        return False
    
    config = DATASET_CONFIGS[dataset]
    base_dir = Path(".")
    
    csv_path = base_dir / config["csv_file"]
    compendia_path = base_dir / "compendia" / config["compendia_file"]
    normalized_path = base_dir / "normalized" / dataset / config["normalized_file"]
    
    # Create output directories
    compendia_path.parent.mkdir(exist_ok=True)
    normalized_path.parent.mkdir(exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Processing {dataset.upper()}: {config['description']}")
    print(f"{'='*60}")
    
    success = True
    
    # Step 1: CSV to JSONL
    if step in ["all", "csv-to-jsonl"]:
        if not csv_path.exists():
            print(f"CSV file not found: {csv_path}")
            return False
        
        success &= convert_csv_to_jsonl(dataset, csv_path, compendia_path)
    
    # Step 2: JSONL to normalized JSON
    if step in ["all", "jsonl-to-json"]:
        if not compendia_path.exists():
            print(f"Compendia file not found: {compendia_path}")
            return False
        
        success &= convert_jsonl_to_normalized_json(dataset, compendia_path, normalized_path)
    
    return success


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Consolidated dataset normalization script")
    parser.add_argument("--all", action="store_true", help="Process all datasets")
    parser.add_argument("--dataset", choices=list(DATASET_CONFIGS.keys()), 
                       help="Process specific dataset")
    parser.add_argument("--step", choices=["all", "csv-to-jsonl", "jsonl-to-json"],
                       default="all", help="Which step to run")
    
    args = parser.parse_args()
    
    if not args.all and not args.dataset:
        parser.error("Must specify either --all or --dataset")
    
    datasets = list(DATASET_CONFIGS.keys()) if args.all else [args.dataset]
    
    print(f"Processing datasets: {', '.join(datasets)}")
    print(f"Step: {args.step}")
    
    all_success = True
    for dataset in datasets:
        success = process_dataset(dataset, args.step)
        all_success &= success
        
        if not success:
            print(f"Failed to process {dataset}")
    
    if all_success:
        print(f"\n{'='*60}")
        print("All datasets processed successfully!")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("Some datasets failed to process")
        print(f"{'='*60}")
        sys.exit(1)


if __name__ == "__main__":
    main()
