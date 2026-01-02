#!/usr/bin/env python3
"""Analyze MESH identifier coverage for normalized PubChem IDs.

This script processes PubChem IDs via NodeNormalization and provides
statistics on MESH identifier coverage.
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
    
    equivalent_ids = normalized_data.get("equivalent_identifiers", [])
    for equiv in equivalent_ids:
        identifier = equiv.get("identifier", "")
        if identifier and identifier.startswith("MESH:"):
            mesh_ids.add(identifier)
    
    return mesh_ids


async def analyze_batch(
    session: aiohttp.ClientSession,
    batch: list[str],
    semaphore: asyncio.Semaphore,
) -> dict:
    """Analyze a batch and return stats."""
    async with semaphore:
        try:
            norm_results = await normalize_batch_async(session, batch)
            
            stats = {
                "total": len(batch),
                "normalized": 0,
                "with_mesh": 0,
                "without_mesh": 0,
                "not_normalized": 0,
            }
            
            for curie in batch:
                normalized = norm_results.get(curie)
                if normalized:
                    stats["normalized"] += 1
                    mesh_ids = extract_mesh_from_normalized(normalized)
                    if mesh_ids:
                        stats["with_mesh"] += 1
                    else:
                        stats["without_mesh"] += 1
                else:
                    stats["not_normalized"] += 1
            
            return stats
            
        except Exception as e:
            return {
                "total": len(batch),
                "error": str(e),
                "normalized": 0,
                "with_mesh": 0,
                "without_mesh": 0,
                "not_normalized": len(batch),
            }


async def analyze_pubchem_ids(
    pubchem_ids: list[int],
    *,
    max_concurrent: int = 5,
    batch_size: int = 100,
    sample_size: Optional[int] = None,
):
    """Analyze MESH coverage for PubChem IDs."""
    
    if sample_size:
        pubchem_ids = pubchem_ids[:sample_size]
    
    total = len(pubchem_ids)
    print(f"Analyzing {total:,} PubChem CIDs for MESH coverage...", file=sys.stderr)
    print(f"Using batch size: {batch_size}, max concurrent: {max_concurrent}", file=sys.stderr)
    
    # Prepare CURIEs
    curies = [f"PUBCHEM.COMPOUND:{cid}" for cid in pubchem_ids]
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    overall_stats = {
        "total": 0,
        "normalized": 0,
        "with_mesh": 0,
        "without_mesh": 0,
        "not_normalized": 0,
        "errors": 0,
    }
    
    # Process in batches
    async with aiohttp.ClientSession() as session:
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = curies[batch_start:batch_end]
            
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            
            if batch_num % 10 == 0 or batch_num == 1:
                print(f"Processing batch {batch_num}/{total_batches} ({batch_start:,}-{batch_end:,})...", file=sys.stderr)
            
            # Analyze batch
            batch_stats = await analyze_batch(session, batch, semaphore)
            
            # Accumulate stats
            overall_stats["total"] += batch_stats.get("total", 0)
            overall_stats["normalized"] += batch_stats.get("normalized", 0)
            overall_stats["with_mesh"] += batch_stats.get("with_mesh", 0)
            overall_stats["without_mesh"] += batch_stats.get("without_mesh", 0)
            overall_stats["not_normalized"] += batch_stats.get("not_normalized", 0)
            if "error" in batch_stats:
                overall_stats["errors"] += 1
            
            # Progress update
            if batch_num % 50 == 0 or batch_end == total:
                pct_norm = overall_stats["normalized"] / overall_stats["total"] * 100 if overall_stats["total"] > 0 else 0
                pct_mesh = overall_stats["with_mesh"] / overall_stats["normalized"] * 100 if overall_stats["normalized"] > 0 else 0
                print(f"  Progress: {overall_stats['total']:,} analyzed | "
                      f"Normalized: {overall_stats['normalized']:,} ({pct_norm:.1f}%) | "
                      f"With MESH: {overall_stats['with_mesh']:,} ({pct_mesh:.1f}% of normalized)", file=sys.stderr)
            
            await asyncio.sleep(0.1)
    
    # Print final summary
    print("\n" + "="*70, file=sys.stderr)
    print("MESH COVERAGE STATISTICS", file=sys.stderr)
    print("="*70, file=sys.stderr)
    print(f"Total PubChem IDs analyzed: {overall_stats['total']:,}", file=sys.stderr)
    print(f"", file=sys.stderr)
    print(f"Normalization Results:", file=sys.stderr)
    print(f"  Successfully normalized: {overall_stats['normalized']:,} ({overall_stats['normalized']/overall_stats['total']*100:.1f}%)", file=sys.stderr)
    print(f"  Not normalized: {overall_stats['not_normalized']:,} ({overall_stats['not_normalized']/overall_stats['total']*100:.1f}%)", file=sys.stderr)
    print(f"", file=sys.stderr)
    print(f"MESH Coverage (of normalized IDs):", file=sys.stderr)
    if overall_stats['normalized'] > 0:
        print(f"  With MESH identifiers: {overall_stats['with_mesh']:,} ({overall_stats['with_mesh']/overall_stats['normalized']*100:.1f}%)", file=sys.stderr)
        print(f"  Without MESH identifiers: {overall_stats['without_mesh']:,} ({overall_stats['without_mesh']/overall_stats['normalized']*100:.1f}%)", file=sys.stderr)
    else:
        print(f"  No normalized IDs to analyze", file=sys.stderr)
    print(f"", file=sys.stderr)
    print(f"Overall MESH Coverage (of all IDs):", file=sys.stderr)
    print(f"  {overall_stats['with_mesh']:,} / {overall_stats['total']:,} = {overall_stats['with_mesh']/overall_stats['total']*100:.1f}%", file=sys.stderr)
    print("="*70, file=sys.stderr)
    
    return overall_stats


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze MESH identifier coverage for normalized PubChem IDs"
    )
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("all_ikraph_pubchem_ids.txt"),
        help="Input file with PubChem CIDs (one per line)",
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
        "--sample",
        type=int,
        default=None,
        help="Only analyze first N CIDs (for quick testing)",
    )
    args = ap.parse_args()
    
    if not args.input.exists():
        print(f"Error: Input file does not exist: {args.input}", file=sys.stderr)
        return 1
    
    # Read PubChem IDs
    pubchem_ids = []
    with args.input.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                cid = int(line)
                pubchem_ids.append(cid)
            except ValueError:
                continue
    
    if not pubchem_ids:
        print("Error: No PubChem IDs found in input file", file=sys.stderr)
        return 1
    
    print(f"Loaded {len(pubchem_ids):,} PubChem IDs from {args.input}", file=sys.stderr)
    
    # Run analysis
    stats = asyncio.run(analyze_pubchem_ids(
        pubchem_ids,
        max_concurrent=args.max_concurrent,
        batch_size=args.batch_size,
        sample_size=args.sample,
    ))
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
