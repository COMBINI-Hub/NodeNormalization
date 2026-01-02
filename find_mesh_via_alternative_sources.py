#!/usr/bin/env python3
"""Find MESH identifiers using alternative sources beyond NodeNormalization.

Methods:
1. UMLS (Unified Medical Language System) - maps PubChem to MESH
2. NCBI E-utilities - query MeSH database directly
3. Enhanced PubChem pug_view extraction - better parsing of MeSH tree structures
4. PubChem synonyms with better MESH pattern matching
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional, Set
import aiohttp
import urllib.parse


async def query_umls_for_mesh(session: aiohttp.ClientSession, pubchem_cid: int) -> Set[str]:
    """Query UMLS for MESH mappings for a PubChem CID.
    
    UMLS has mappings between different vocabularies. We can search for
    PubChem compounds and get their MESH equivalents.
    """
    mesh_ids = set()
    
    # UMLS REST API - search by PubChem CID
    # Note: This requires UMLS API key, but we can try without for testing
    try:
        # Search for PubChem CID in UMLS
        search_url = f"https://uts-ws.nlm.nih.gov/rest/search/current"
        params = {
            "string": f"PUBCHEM.COMPOUND:{pubchem_cid}",
            "sabs": "MSH",  # MeSH source
            "returnIdType": "code",
        }
        
        async with session.get(search_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("result", {}).get("results", [])
                for result in results:
                    ui = result.get("ui", "")
                    if ui and ui.startswith("D"):
                        mesh_ids.add(f"MESH:{ui}")
    except Exception:
        # UMLS requires authentication, skip if not available
        pass
    
    return mesh_ids


async def query_ncbi_eutils_for_mesh(session: aiohttp.ClientSession, pubchem_cid: int) -> Set[str]:
    """Query NCBI E-utilities to find MESH descriptors for a PubChem CID.
    
    Uses NCBI's E-utilities to search MeSH database by PubChem CID.
    """
    mesh_ids = set()
    
    try:
        # First, get the compound name from PubChem
        pubchem_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{pubchem_cid}/property/IUPACName,Title/JSON"
        async with session.get(pubchem_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                data = await resp.json()
                props = data.get("PropertyTable", {}).get("Properties", [{}])[0]
                name = props.get("Title") or props.get("IUPACName", "")
                
                if name:
                    # Search MeSH database by name
                    query = f'"{name}"[MeSH Terms]'
                    esearch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                    params = {
                        "db": "mesh",
                        "term": query,
                        "retmode": "json",
                    }
                    
                    async with session.get(esearch_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp2:
                        if resp2.status == 200:
                            data2 = await resp2.json()
                            uids = data2.get("esearchresult", {}).get("idlist", [])
                            
                            if uids:
                                # Get summaries to extract MeSH UI
                                esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
                                params2 = {
                                    "db": "mesh",
                                    "id": ",".join(str(uid) for uid in uids[:10]),  # Limit to 10
                                    "retmode": "json",
                                }
                                
                                async with session.get(esummary_url, params=params2, timeout=aiohttp.ClientTimeout(total=30)) as resp3:
                                    if resp3.status == 200:
                                        data3 = await resp3.json()
                                        for uid in uids[:10]:
                                            summary = data3.get("result", {}).get(str(uid), {})
                                            mesh_ui = summary.get("ds_meshui", "")
                                            if mesh_ui and mesh_ui.startswith("D"):
                                                mesh_ids.add(f"MESH:{mesh_ui}")
    except Exception:
        pass
    
    return mesh_ids


def extract_mesh_from_pubchem_tree(section: dict, mesh_ids: Set[str]) -> None:
    """Extract MESH descriptor IDs from PubChem's MeSH Tree structure.
    
    The MeSH Tree section contains hierarchical information with descriptor IDs.
    """
    toc = section.get("TOCHeading", "")
    
    if "MeSH" in toc:
        # Look for URL fields that might contain MESH descriptor IDs
        info_list = section.get("Information", [])
        for info in info_list:
            # Check URL field
            url = info.get("URL", "")
            if url:
                # Extract MESH descriptor from URL like /mesh/D001241
                match = re.search(r'/mesh/([DM]\d+)', url, re.IGNORECASE)
                if match:
                    mesh_ids.add(f"MESH:{match.group(1).upper()}")
            
            # Check Value field for descriptor IDs
            value = info.get("Value", {})
            if isinstance(value, dict):
                # Look in StringWithMarkup
                strings = value.get("StringWithMarkup", [])
                for s in strings:
                    text = s.get("String", "")
                    # Look for patterns like D001241 or MESH:D001241
                    matches = re.findall(r'(?:MESH:)?([DM]\d{6,})', text, re.IGNORECASE)
                    for match in matches:
                        mesh_ids.add(f"MESH:{match.upper()}")
                
                # Check Number field
                number = value.get("Number", "")
                if number and re.match(r'^[DM]\d+$', number, re.IGNORECASE):
                    mesh_ids.add(f"MESH:{number.upper()}")
            elif isinstance(value, str):
                matches = re.findall(r'(?:MESH:)?([DM]\d{6,})', value, re.IGNORECASE)
                for match in matches:
                    mesh_ids.add(f"MESH:{match.upper()}")
    
    # Recursively search subsections
    for sub in section.get("Section", []):
        extract_mesh_from_pubchem_tree(sub, mesh_ids)


async def get_pubchem_mesh_enhanced(session: aiohttp.ClientSession, cid: int) -> Set[str]:
    """Enhanced PubChem MESH extraction - better parsing of tree structures."""
    mesh_ids = set()
    
    try:
        url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{cid}/JSON"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                j = await resp.json()
                record = j.get("Record", {})
                sections = record.get("Section", [])
                
                for section in sections:
                    extract_mesh_from_pubchem_tree(section, mesh_ids)
    except Exception:
        pass
    
    return mesh_ids


async def find_mesh_all_methods(
    session: aiohttp.ClientSession,
    cid: int,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Try all alternative methods to find MESH identifiers."""
    result = {
        "cid": cid,
        "pubchem_curie": f"PUBCHEM.COMPOUND:{cid}",
        "found_mesh": False,
        "mesh_descriptors": "",
        "mesh_count": 0,
        "methods_used": [],
    }
    
    all_mesh = set()
    
    # Method 1: Enhanced PubChem pug_view
    async with semaphore:
        try:
            mesh_from_pubchem = await get_pubchem_mesh_enhanced(session, cid)
            if mesh_from_pubchem:
                all_mesh.update(mesh_from_pubchem)
                result["methods_used"].append("pubchem_enhanced")
        except Exception as e:
            result["error_pubchem"] = str(e)[:100]
    
    # Method 2: NCBI E-utilities (slower, so do fewer)
    if not all_mesh:
        async with semaphore:
            try:
                mesh_from_eutils = await query_ncbi_eutils_for_mesh(session, cid)
                if mesh_from_eutils:
                    all_mesh.update(mesh_from_eutils)
                    result["methods_used"].append("ncbi_eutils")
            except Exception as e:
                result["error_eutils"] = str(e)[:100]
    
    # Method 3: UMLS (requires API key, skip for now)
    # if not all_mesh:
    #     mesh_from_umls = await query_umls_for_mesh(session, cid)
    #     if mesh_from_umls:
    #         all_mesh.update(mesh_from_umls)
    #         result["methods_used"].append("umls")
    
    if all_mesh:
        result["found_mesh"] = True
        result["mesh_count"] = len(all_mesh)
        result["mesh_descriptors"] = ",".join(sorted(all_mesh))
    
    return result


async def process_batch_alternative(
    session: aiohttp.ClientSession,
    batch: list[int],
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Process a batch using alternative methods."""
    tasks = [find_mesh_all_methods(session, cid, semaphore) for cid in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle exceptions
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results.append({
                "cid": batch[i],
                "pubchem_curie": f"PUBCHEM.COMPOUND:{batch[i]}",
                "found_mesh": False,
                "error": str(result)[:100],
            })
        else:
            processed_results.append(result)
    
    return processed_results


async def process_large_scale_alternative(
    input_path: Path,
    output_path: Path,
    *,
    batch_size: int = 50,  # Smaller batches for alternative methods
    max_concurrent: int = 5,
):
    """Process using alternative methods."""
    
    # Read PubChem IDs
    pubchem_ids = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                try:
                    pubchem_ids.append(int(line))
                except ValueError:
                    continue
    
    total = len(pubchem_ids)
    print(f"Processing {total:,} PubChem IDs using alternative methods...", file=sys.stderr)
    print(f"Methods: Enhanced PubChem extraction, NCBI E-utilities", file=sys.stderr)
    print(f"Batch size: {batch_size}, max concurrent: {max_concurrent}", file=sys.stderr)
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    stats = {
        "total": total,
        "processed": 0,
        "found_mesh": 0,
        "no_mesh": 0,
    }
    
    # Open output file
    output_file = output_path.open("w", newline="", encoding="utf-8")
    fieldnames = [
        "cid", "pubchem_curie",
        "found_mesh", "mesh_count", "mesh_descriptors", "methods_used",
        "error_pubchem", "error_eutils",
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
            
            if batch_num % 100 == 0 or batch_num == 1:
                print(f"Processing batch {batch_num}/{total_batches} ({batch_start:,}-{batch_end:,})...", file=sys.stderr)
            
            # Process batch
            results = await process_batch_alternative(session, batch, semaphore)
            
            # Write results
            for result in results:
                stats["processed"] += 1
                if result.get("found_mesh"):
                    stats["found_mesh"] += 1
                else:
                    stats["no_mesh"] += 1
                
                writer.writerow(result)
                output_file.flush()
            
            # Progress update
            if batch_num % 200 == 0 or batch_end == total:
                pct = stats["processed"] / total * 100
                print(f"  Progress: {stats['processed']:,}/{total:,} ({pct:.1f}%) | "
                      f"Found MESH: {stats['found_mesh']:,} ({stats['found_mesh']/stats['processed']*100:.1f}%)", file=sys.stderr)
            
            # Rate limiting
            await asyncio.sleep(0.3)
    
    output_file.close()
    
    # Print summary
    print("\n" + "="*70, file=sys.stderr)
    print("ALTERNATIVE METHODS MESH FINDING SUMMARY", file=sys.stderr)
    print("="*70, file=sys.stderr)
    print(f"Total processed: {stats['processed']:,}", file=sys.stderr)
    print(f"Found MESH via alternative methods: {stats['found_mesh']:,} ({stats['found_mesh']/stats['processed']*100:.1f}%)", file=sys.stderr)
    print(f"No MESH found: {stats['no_mesh']:,} ({stats['no_mesh']/stats['processed']*100:.1f}%)", file=sys.stderr)
    print("="*70, file=sys.stderr)
    print(f"\nResults written to: {output_path}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Find MESH identifiers using alternative sources"
    )
    ap.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input file with PubChem CIDs (one per line)",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("pubchem_mesh_via_alternatives.tsv"),
        help="Output TSV file",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="CIDs per batch (default: 50)",
    )
    ap.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="Max concurrent requests (default: 5)",
    )
    ap.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Only process first N CIDs (for testing)",
    )
    args = ap.parse_args()
    
    if not args.input.exists():
        print(f"Error: Input file does not exist: {args.input}", file=sys.stderr)
        return 1
    
    # Apply sample if specified
    if args.sample:
        # Create temporary file with sample
        with args.input.open("r") as f_in, Path(f"{args.input}.sample").open("w") as f_out:
            for i, line in enumerate(f_in):
                if i >= args.sample:
                    break
                f_out.write(line)
        input_path = Path(f"{args.input}.sample")
    else:
        input_path = args.input
    
    asyncio.run(process_large_scale_alternative(
        input_path,
        args.output,
        batch_size=args.batch_size,
        max_concurrent=args.max_concurrent,
    ))
    
    if args.sample:
        Path(f"{args.input}.sample").unlink()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
