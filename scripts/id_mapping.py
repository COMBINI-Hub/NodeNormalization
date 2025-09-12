#!/usr/bin/env python3

# python3 scripts/id_mapping.py \
#   --nodes /Users/drshika2/NodeNormalization/nodes.csv \
#   --out-dir /Users/drshika2/NodeNormalization/primekg_full \
#   --endpoint http://127.0.0.1:8000/get_normalized_nodes

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def map_to_curie_with_source(node_id: str, node_type: str, node_name: str, node_source: str) -> Optional[str]:
    """
    Authoritative mapping from PrimeKG nodes.csv data to CURIEs.
    
    Uses the node_source column from PrimeKG's official nodes.csv file
    to determine the correct CURIE prefix for each identifier.

    Returns a CURIE string or None if unknown.
    """

    # Normalize inputs
    node_id = (node_id or "").strip()
    node_type = (node_type or "").strip()
    node_name = (node_name or "").strip()
    node_source = (node_source or "").strip()

    # If already looks like a CURIE, validate and return
    if ":" in node_id:
        prefix, suffix = node_id.split(":", 1)
        # Common PrimeKG prefixes that are valid CURIEs
        valid_prefixes = {
            "NCBIGene", "DRUGBANK", "CHEMBL.COMPOUND", "PUBCHEM.COMPOUND",
            "GO", "UBERON", "REACTOME", "MONDO", "UMLS", "HP", "MESH",
            "OMIM", "DOID", "SNOMEDCT", "NCIT", "HGNC", "UNIPROT", "CTD"
        }
        if prefix.upper() in valid_prefixes:
            return node_id.upper()

    # Map PrimeKG sources to CURIE prefixes
    source_to_prefix = {
        "NCBI": "NCBIGene",
        "DrugBank": "DRUGBANK", 
        "GO": "GO",
        "UBERON": "UBERON",
        "REACTOME": "REACTOME",
        "MONDO": "MONDO",
        "MONDO_grouped": "MONDO",  # Same as MONDO
        "HPO": "HP",
        "CTD": "CTD"
    }

    # Get the appropriate prefix for this source
    prefix = source_to_prefix.get(node_source)
    if not prefix:
        return None

    # Create the CURIE
    return f"{prefix}:{node_id}"




def iter_nodes(csv_path: Path) -> Iterable[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def download_primekg_mappings(output_dir: Path) -> Dict[str, Path]:
    """
    Download PrimeKG mapping tables from Harvard Dataverse.
    Returns dict mapping table names to local file paths.
    """
    import requests
    import zipfile
    
    # PrimeKG Harvard Dataverse DOI
    dataverse_url = "https://dataverse.harvard.edu/api/access/datafile/6180620"
    
    # Download the main PrimeKG file
    kg_file = output_dir / "kg.csv"
    if not kg_file.exists():
        print(f"Downloading PrimeKG from Harvard Dataverse...")
        response = requests.get(dataverse_url, stream=True)
        response.raise_for_status()
        with open(kg_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded {kg_file}")
    
    # For now, return empty dict - would need to parse kg.csv for specific mappings
    # In a full implementation, you'd extract specific mapping tables from the downloaded data
    return {}


def build_compendia_records(rows: Iterable[Dict[str, str]], limit_per_type: Optional[int], mapping_tables: Optional[Dict] = None) -> Tuple[List[dict], Dict[str, int], Dict[str, List[str]]]:
    """
    Convert rows to compendia JSONL objects using authoritative source-based CURIE mapping.
    Uses the node_source column from PrimeKG's official nodes.csv file.
    """
    out: List[dict] = []
    counts: Dict[str, int] = defaultdict(int)
    included_per_type: Dict[str, int] = defaultdict(int)
    unmapped_examples: Dict[str, List[str]] = defaultdict(list)

    # Map PrimeKG node_type values to Biolink categories for compendia
    type_to_biolink: Dict[str, str] = {
        "gene/protein": "biolink:Gene",
        "drug": "biolink:ChemicalEntity",
        "biological_process": "biolink:BiologicalProcess",
        "cellular_component": "biolink:CellularComponent",
        "molecular_function": "biolink:MolecularActivity",
        "anatomy": "biolink:AnatomicalEntity",
        "disease": "biolink:Disease",
        "effect/phenotype": "biolink:PhenotypicFeature",
        "pathway": "biolink:Pathway",
        "exposure": "biolink:NamedThing",
    }

    for row in rows:
        node_id = row.get("node_id", "")
        node_type = row.get("node_type", "")
        node_name = row.get("node_name", "")
        node_source = row.get("node_source", "")

        # enforce per-type sample limit if set
        if limit_per_type is not None and included_per_type[node_type] >= limit_per_type:
            continue

        curie = map_to_curie_with_source(node_id, node_type, node_name, node_source)
        counts[node_type] += 1
        
        if not curie:
            # Track unmapped examples for reporting
            if len(unmapped_examples[node_type]) < 5:  # Keep first 5 examples per type
                unmapped_examples[node_type].append(f"{node_id} ({node_name}) [source: {node_source}]")
            continue

        bl_type = type_to_biolink.get(node_type, "biolink:NamedThing")
        obj = {
            "type": bl_type,
            "identifiers": [{"i": curie, **({"l": node_name} if node_name else {})}],
        }
        out.append(obj)
        included_per_type[node_type] += 1

    return out, dict(counts), dict(unmapped_examples)




def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def normalize_via_api(curies: List[str], endpoint: str, batch_size: int = 100) -> Dict[str, dict]:
    import requests
    import time

    results: Dict[str, dict] = {}
    
    # Use POST endpoint for better performance
    for i in range(0, len(curies), batch_size):
        batch = curies[i:i + batch_size]
        print(f"  Processing batch {i//batch_size + 1}/{(len(curies) + batch_size - 1)//batch_size} ({len(batch)} CURIEs)")
        
        try:
            r = requests.post(
                endpoint,
                json={"curies": batch},
                headers={"Content-Type": "application/json"},
                timeout=60
            )
            r.raise_for_status()
            results.update(r.json())
            
            # Small delay between batches
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"  Error processing batch: {e}")
            # Add null entries for failed batch
            for curie in batch:
                results[curie] = None
    
    return results


def main():
    ap = argparse.ArgumentParser(description="Map PrimeKG nodes.csv to CURIEs and export compendia + normalized CSV + JSON (KG-compatible format)")
    ap.add_argument("--nodes", required=True, help="Path to PrimeKG nodes.csv (with node_source column)")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--limit-per-type", type=int, default=None, help="Max rows per node_type to include (for sampling)")
    ap.add_argument("--emit-compendia", default="primekg_compendia_sample.jsonl", help="Compendia JSONL filename")
    ap.add_argument("--emit-normalized", default="primekg_normalized_sample.csv", help="Normalized CSV filename")
    ap.add_argument("--emit-json", default="primekg_normalized_sample.json", help="Normalized JSON filename (same format as normalized_semmed_sample.json)")
    ap.add_argument("--emit-report", default="mapping_report.txt", help="Mapping report filename")
    ap.add_argument("--endpoint", default="http://127.0.0.1:8000/get_normalized_nodes", help="NodeNorm get_normalized_nodes endpoint")
    ap.add_argument("--batch-size", type=int, default=100, help="Batch size for API calls")
    ap.add_argument("--download-mappings", action="store_true", help="Download PrimeKG mapping tables from Harvard Dataverse")
    ap.add_argument("--skip-normalization", action="store_true", help="Skip NodeNorm API calls (compendia only)")
    args = ap.parse_args()

    nodes_path = Path(args.nodes)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Download mapping tables if requested
    mapping_tables = {}
    if args.download_mappings:
        print("Downloading PrimeKG mapping tables...")
        mapping_tables = download_primekg_mappings(out_dir)

    # Build compendia sample using authoritative source-based mapping
    records, seen_counts, unmapped_examples = build_compendia_records(iter_nodes(nodes_path), args.limit_per_type, mapping_tables)

    comp_path = out_dir / args.emit_compendia
    with open(comp_path, "w", encoding="utf-8") as out:
        for obj in records:
            out.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # Normalize via API (unless skipped)
    norm = {}
    if not args.skip_normalization:
        input_curies = []
        for obj in records:
            ids = obj.get("identifiers", [])
            if ids:
                input_curies.append(ids[0].get("i"))

        if input_curies:
            print(f"Normalizing {len(input_curies)} CURIEs via NodeNorm API...")
            norm = normalize_via_api(input_curies, args.endpoint, args.batch_size)

        # Write normalized CSV
        norm_path = out_dir / args.emit_normalized
        headers = [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "types",
            "equivalent_identifiers",
        ]
        with open(norm_path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(headers)
            for curie in input_curies:
                obj = norm.get(curie) or {}
                pid = (obj.get("id") or {}).get("identifier", "")
                plabel = (obj.get("id") or {}).get("label", "")
                types = ";".join(obj.get("type") or [])
                eqs = obj.get("equivalent_identifiers") or []
                eq_join = ";".join([e.get("identifier", "") for e in eqs])
                w.writerow([curie, pid, plabel, types, eq_join])

        # Write normalized JSON (same format as normalized_semmed_sample.json)
        if norm:
            json_path = out_dir / args.emit_json
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(norm, f, indent=2, ensure_ascii=False)

    # Write mapping report
    report_path = out_dir / args.emit_report
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("PrimeKG ID Mapping Report\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Total nodes processed: {sum(seen_counts.values())}\n")
        f.write(f"Successfully mapped: {len(records)}\n")
        f.write(f"Mapping success rate: {len(records)/sum(seen_counts.values())*100:.1f}%\n\n")
        
        f.write("Counts by node_type:\n")
        for k in sorted(seen_counts.keys()):
            mapped_count = sum(1 for r in records if r.get("type", "").startswith("biolink:"))
            f.write(f"  {k}: {seen_counts[k]} total\n")
        
        f.write("\nUnmapped examples (first 5 per type):\n")
        for node_type, examples in unmapped_examples.items():
            if examples:
                f.write(f"\n{node_type}:\n")
                for ex in examples:
                    f.write(f"  - {ex}\n")

    # Summary to stdout
    mapped = len(records)
    print(f"Wrote compendia: {comp_path} ({mapped} mapped records)")
    if not args.skip_normalization:
        print(f"Wrote normalized CSV: {out_dir / args.emit_normalized} ({len(norm)} normalized)")
        print(f"Wrote normalized JSON: {out_dir / args.emit_json} (same format as normalized_semmed_sample.json)")
    print(f"Wrote mapping report: {report_path}")
    print(f"Mapping success rate: {mapped/sum(seen_counts.values())*100:.1f}%")
    print("\nCounts by node_type (seen in input, not necessarily mapped):")
    for k in sorted(seen_counts.keys()):
        print(f"  {k}: {seen_counts[k]}")


if __name__ == "__main__":
    sys.exit(main())


