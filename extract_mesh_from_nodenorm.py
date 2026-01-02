#!/usr/bin/env python3
"""Extract MESH synonyms for PubChem IDs from NodeNormalization.

This script normalizes PubChem IDs via NodeNormalization and extracts
MESH identifiers from the equivalent_identifiers array. This is much
simpler and faster than querying PubChem directly!
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from pathlib import Path
from typing import Optional, Set
import aiohttp


async def normalize_batch_async(
    session: aiohttp.ClientSession,
    curies: list[str],
    base_url: str = "https://nodenormalization-sri.renci.org",
    *,
    timeout: int = 120,
    retries: int = 3,
) -> dict:
    """Normalize a batch of CURIEs via NodeNormalization asynchronously."""
    url = base_url.rstrip("/") + "/get_normalized_nodes"
    payload = {
        "curies": curies,
        "conflate": True,
        "drug_chemical_conflate": False,
        "description": False,
        "individual_types": False,
    }
    
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={"Accept": "application/json", "Content-Type": "application/json"}
            ) as resp:
                resp.raise_for_status()
                result = await resp.json()
                return result if isinstance(result, dict) else {}
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(1.0 * (2**attempt))
    
    raise RuntimeError(f"Normalization failure after {retries} attempts: {last_err}")


def extract_mesh_from_normalized(normalized_data: dict | None) -> Set[str]:
    """Extract MESH identifiers from NodeNormalization response."""
    mesh_ids = set()
    
    if not normalized_data:
        return mesh_ids
    
    # Check equivalent_identifiers array
    equivalent_ids = normalized_data.get("equivalent_identifiers", [])
    for equiv in equivalent_ids:
        identifier = equiv.get("identifier", "")
        if identifier and identifier.startswith("MESH:"):
            mesh_ids.add(identifier)
    
    return mesh_ids


async def process_batch(
    session: aiohttp.ClientSession,
    batch: list[dict],
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Process a batch of PubChem IDs."""
    async with semaphore:
        # Prepare CURIEs for this batch
        curies = [item["pubchem_curie"] for item in batch]
        
        try:
            # Normalize batch
            norm_results = await normalize_batch_async(session, curies)
            
            # Extract MESH for each
            results = []
            for item in batch:
                curie = item["pubchem_curie"]
                normalized = norm_results.get(curie)
                
                result = {
                    "original_id": item["original_id"],
                    "cid": item["cid"],
                    "pubchem_curie": curie,
                    "normalized": normalized is not None,
                    "normalized_identifier": "",
                    "normalized_label": "",
                    "mesh_descriptors": "",
                    "mesh_count": 0,
                    "found_mesh": False,
                }
                
                if normalized:
                    # Get normalized identifier
                    norm_id = normalized.get("id", {})
                    result["normalized_identifier"] = norm_id.get("identifier", "")
                    result["normalized_label"] = norm_id.get("label", "")
                    
                    # Extract MESH identifiers
                    mesh_ids = extract_mesh_from_normalized(normalized)
                    if mesh_ids:
                        result["found_mesh"] = True
                        result["mesh_count"] = len(mesh_ids)
                        result["mesh_descriptors"] = ",".join(sorted(mesh_ids))
                
                results.append(result)
            
            return results
            
        except Exception as e:
            # Return error results for all items in batch
            return [
                {
                    "original_id": item["original_id"],
                    "cid": item["cid"],
                    "pubchem_curie": item["pubchem_curie"],
                    "normalized": False,
                    "error": str(e),
                    "mesh_descriptors": "",
                    "mesh_count": 0,
                    "found_mesh": False,
                }
                for item in batch
            ]


async def process_pubchem_ids_async(
    pubchem_ids: list[dict],
    output_path: Path,
    *,
    max_concurrent: int = 5,  # Lower for NodeNormalization API
    batch_size: int = 100,  # NodeNormalization handles batches well
):
    """Process PubChem IDs asynchronously via NodeNormalization."""
    
    total = len(pubchem_ids)
    print(f"Processing {total:,} PubChem CIDs via NodeNormalization (max {max_concurrent} concurrent batches)...", file=sys.stderr)
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    stats = {
        "total": total,
        "normalized": 0,
        "found_mesh": 0,
        "no_mesh": 0,
        "errors": 0,
        "processed": 0,
    }
    
    # Open output file
    output_file = output_path.open("w", newline="", encoding="utf-8")
    fieldnames = [
        "original_id", "cid", "pubchem_curie",
        "normalized", "normalized_identifier", "normalized_label",
        "found_mesh", "mesh_count", "mesh_descriptors", "error"
    ]
    writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel-tab", extrasaction="ignore")
    writer.writeheader()
    
    # Process in batches
    async with aiohttp.ClientSession() as session:
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = pubchem_ids[batch_start:batch_end]
            
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            print(f"Processing batch {batch_num}/{total_batches} ({batch_start:,}-{batch_end:,})...", file=sys.stderr)
            
            # Process batch
            results = await process_batch(session, batch, semaphore)
            
            # Write results and update stats
            for result in results:
                stats["processed"] += 1
                
                if result.get("error"):
                    stats["errors"] += 1
                elif result.get("normalized"):
                    stats["normalized"] += 1
                    if result.get("found_mesh"):
                        stats["found_mesh"] += 1
                    else:
                        stats["no_mesh"] += 1
                
                writer.writerow(result)
                output_file.flush()
            
            # Progress update
            if stats["processed"] % 1000 == 0 or batch_end == total:
                pct = stats["processed"] / total * 100
                print(f"  Progress: {stats['processed']:,}/{total:,} ({pct:.1f}%) - Normalized: {stats['normalized']:,}, Found MESH: {stats['found_mesh']:,}", file=sys.stderr)
            
            # Small delay to be respectful to API
            await asyncio.sleep(0.1)
    
    output_file.close()
    
    # Print summary
    print("\n" + "="*60, file=sys.stderr)
    print("MESH EXTRACTION SUMMARY (via NodeNormalization)", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"Total processed: {stats['processed']:,}", file=sys.stderr)
    print(f"Successfully normalized: {stats['normalized']:,} ({stats['normalized']/stats['processed']*100:.1f}%)", file=sys.stderr)
    print(f"Found MESH descriptors: {stats['found_mesh']:,} ({stats['found_mesh']/stats['normalized']*100:.1f}% of normalized)", file=sys.stderr)
    print(f"No MESH found: {stats['no_mesh']:,}", file=sys.stderr)
    print(f"Errors: {stats['errors']:,} ({stats['errors']/stats['processed']*100:.1f}%)", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"\nResults written to: {output_path}", file=sys.stderr)
    
    # Create mapping file
    mapping_path = output_path.with_suffix("").as_posix() + ".mapping.tsv"
    print(f"Creating mapping file: {mapping_path}...", file=sys.stderr)
    
    with output_path.open("r", newline="", encoding="utf-8") as in_f, \
         Path(mapping_path).open("w", newline="", encoding="utf-8") as map_f:
        reader = csv.DictReader(in_f, dialect="excel-tab")
        writer = csv.DictWriter(
            map_f,
            fieldnames=["pubchem_cid", "pubchem_curie", "mesh_descriptor"],
            dialect="excel-tab"
        )
        writer.writeheader()
        
        for row in reader:
            if row.get("found_mesh", "").lower() == "true":
                mesh_list = row["mesh_descriptors"].split(",")
                for mesh_desc in mesh_list:
                    if mesh_desc.strip():
                        writer.writerow({
                            "pubchem_cid": row["cid"],
                            "pubchem_curie": row["pubchem_curie"],
                            "mesh_descriptor": mesh_desc.strip(),
                        })
    
    print(f"Mapping file written to: {mapping_path}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract MESH synonyms for PubChem IDs from NodeNormalization"
    )
    ap.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input file with PubChem CIDs (one per line, or TSV with 'cid' column)",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("pubchem_mesh_from_nodenorm.tsv"),
        help="Output TSV file with MESH synonyms",
    )
    ap.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="Maximum concurrent batch requests (default: 5)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="CURIEs per batch (default: 100)",
    )
    ap.add_argument(
        "--nodenorm-url",
        type=str,
        default="https://nodenormalization-sri.renci.org",
        help="NodeNormalization base URL",
    )
    args = ap.parse_args()
    
    if not args.input.exists():
        print(f"Error: Input file does not exist: {args.input}", file=sys.stderr)
        return 1
    
    # Read PubChem IDs
    pubchem_ids = []
    
    if args.input.suffix == '.tsv':
        with args.input.open("r", newline="", encoding="utf-8") as in_f:
            reader = csv.DictReader(in_f, dialect="excel-tab")
            for row in reader:
                cid_str = row.get("cid") or row.get("CID")
                if cid_str:
                    try:
                        cid = int(cid_str)
                        pubchem_ids.append({
                            "cid": cid,
                            "original_id": row.get("original_id", f"PubChem:CID{cid}"),
                            "pubchem_curie": f"PUBCHEM.COMPOUND:{cid}",
                        })
                    except ValueError:
                        continue
    else:
        # Assume one CID per line
        with args.input.open("r", encoding="utf-8") as in_f:
            for line in in_f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    cid = int(line)
                    pubchem_ids.append({
                        "cid": cid,
                        "original_id": f"PubChem:CID{cid}",
                        "pubchem_curie": f"PUBCHEM.COMPOUND:{cid}",
                    })
                except ValueError:
                    continue
    
    if not pubchem_ids:
        print("Error: No PubChem IDs found in input file", file=sys.stderr)
        return 1
    
    print(f"Found {len(pubchem_ids):,} PubChem IDs to process", file=sys.stderr)
    
    # Run async processing
    asyncio.run(process_pubchem_ids_async(
        pubchem_ids,
        args.output,
        max_concurrent=args.max_concurrent,
        batch_size=args.batch_size,
    ))
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
