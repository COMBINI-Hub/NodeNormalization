#!/usr/bin/env python3
"""Query MeSH data via NCBI E-utilities as an alternative to NLM API.

NCBI has a "mesh" database that can be queried using E-utilities.
This script explores using E-utilities to:
1. Search for MeSH descriptors by ConceptUI
2. Get MeSH descriptor information
3. Find relationships between concepts and descriptors
4. Verify MeSH term status

Reference: https://www.ncbi.nlm.nih.gov/books/NBK25500/
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


_MESH_M_RE = re.compile(r"^MESH:(M\d{7})$")
_MESH_D_RE = re.compile(r"^MESH:(D\d{6})$")


def _http_get_json(url: str, *, timeout_s: int = 60, retries: int = 6, backoff_s: float = 1.0) -> Any:
    """Fetch JSON from URL with retries."""
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "NodeNormalization-MeSH-Eutils/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw)
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(backoff_s * (2**attempt))
    raise RuntimeError(f"HTTP/JSON failure after {retries} attempts: {last_err}")


def esearch_mesh(query: str, *, tool: str = "NodeNormalization", email: str = "devnull@example.org", api_key: Optional[str] = None) -> List[str]:
    """Search NCBI MeSH database and return UIDs.
    
    Args:
        query: Search query (e.g., "M0000025[UID]" or "Abortion, Tubal[MeSH Terms]")
        tool: Tool name for E-utilities
        email: Email for E-utilities
        api_key: Optional NCBI API key
    
    Returns:
        List of UIDs (MeSH descriptor UIDs)
    """
    params = {
        "db": "mesh",
        "term": query,
        "retmode": "json",
        "tool": tool,
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?" + urllib.parse.urlencode(params)
    
    try:
        result = _http_get_json(url, timeout_s=30, retries=3, backoff_s=0.5)
        id_list = result.get("esearchresult", {}).get("idlist", [])
        return [str(uid) for uid in id_list]
    except Exception as e:
        print(f"Error in esearch: {e}", file=sys.stderr)
        return []


def esummary_mesh(uids: List[str], *, tool: str = "NodeNormalization", email: str = "devnull@example.org", api_key: Optional[str] = None) -> Dict[str, Any]:
    """Get summaries for MeSH UIDs.
    
    Args:
        uids: List of MeSH UIDs
        tool: Tool name for E-utilities
        email: Email for E-utilities
        api_key: Optional NCBI API key
    
    Returns:
        Dictionary mapping UID to summary data
    """
    if not uids:
        return {}
    
    params = {
        "db": "mesh",
        "id": ",".join(uids),
        "retmode": "json",
        "tool": tool,
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?" + urllib.parse.urlencode(params)
    
    try:
        result = _http_get_json(url, timeout_s=30, retries=3, backoff_s=0.5)
        return result.get("result", {})
    except Exception as e:
        print(f"Error in esummary: {e}", file=sys.stderr)
        return {}


def find_descriptor_by_conceptui(concept_id: str, *, tool: str = "NodeNormalization", email: str = "devnull@example.org", api_key: Optional[str] = None) -> Optional[Dict]:
    """Try to find MeSH descriptor using ConceptUI via NCBI E-utilities.
    
    Args:
        concept_id: MeSH ConceptUI like "M0000025"
        tool: Tool name for E-utilities
        email: Email for E-utilities
        api_key: Optional NCBI API key
    
    Returns:
        Dictionary with descriptor information if found, None otherwise
    """
    # Try searching by ConceptUI UID
    query1 = f"{concept_id}[UID]"
    uids = esearch_mesh(query1, tool=tool, email=email, api_key=api_key)
    
    if uids:
        summaries = esummary_mesh(uids, tool=tool, email=email, api_key=api_key)
        if summaries:
            # Return first result
            first_uid = uids[0]
            return summaries.get(first_uid, {})
    
    return None


def find_descriptor_by_label(label: str, *, tool: str = "NodeNormalization", email: str = "devnull@example.org", api_key: Optional[str] = None) -> List[Dict]:
    """Search for MeSH descriptors by label using NCBI E-utilities.
    
    Args:
        label: MeSH term label
        tool: Tool name for E-utilities
        email: Email for E-utilities
        api_key: Optional NCBI API key
    
    Returns:
        List of descriptor summaries
    """
    # Try searching by MeSH Terms
    query = f'"{label}"[MeSH Terms]'
    uids = esearch_mesh(query, tool=tool, email=email, api_key=api_key)
    
    if not uids:
        # Try without quotes
        query = f"{label}[MeSH Terms]"
        uids = esearch_mesh(query, tool=tool, email=email, api_key=api_key)
    
    if uids:
        summaries = esummary_mesh(uids, tool=tool, email=email, api_key=api_key)
        results = []
        for uid in uids:
            if uid in summaries:
                results.append(summaries[uid])
        return results
    
    return []


def get_descriptor_info(descriptor_uid: str, *, tool: str = "NodeNormalization", email: str = "devnull@example.org", api_key: Optional[str] = None) -> Optional[Dict]:
    """Get detailed information about a MeSH descriptor by UID.
    
    Args:
        descriptor_uid: MeSH descriptor UID
        tool: Tool name for E-utilities
        email: Email for E-utilities
        api_key: Optional NCBI API key
    
    Returns:
        Descriptor summary dictionary
    """
    summaries = esummary_mesh([descriptor_uid], tool=tool, email=email, api_key=api_key)
    return summaries.get(descriptor_uid)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Query MeSH data via NCBI E-utilities"
    )
    ap.add_argument(
        "--concept-id",
        help="MeSH ConceptUI to search for (e.g., M0000025)",
    )
    ap.add_argument(
        "--label",
        help="MeSH term label to search for",
    )
    ap.add_argument(
        "--descriptor-uid",
        help="MeSH descriptor UID to get info for",
    )
    ap.add_argument(
        "--test-unmapped",
        type=int,
        default=0,
        help="Test on N unmapped ConceptUIs from failures file",
    )
    ap.add_argument(
        "--failures-tsv",
        default="ikraph_failed_to_normalize.mesh_m_to_mesh_d.nodenorm.failures.tsv",
        help="Failures TSV file for testing",
    )
    ap.add_argument(
        "--min-interval-s",
        type=float,
        default=0.34,  # NCBI rate limit: 3 requests/second without API key
        help="Minimum seconds between API calls",
    )
    ap.add_argument(
        "--api-key",
        help="NCBI API key (optional, increases rate limit)",
    )
    args = ap.parse_args()
    
    tool = "NodeNormalization"
    email = "devnull@example.org"
    
    if args.concept_id:
        print(f"Searching for ConceptUI: {args.concept_id}")
        result = find_descriptor_by_conceptui(args.concept_id, tool=tool, email=email, api_key=args.api_key)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print("No descriptor found")
        return 0
    
    if args.label:
        print(f"Searching for label: {args.label}")
        results = find_descriptor_by_label(args.label, tool=tool, email=email, api_key=args.api_key)
        if results:
            print(f"Found {len(results)} descriptor(s):")
            for i, result in enumerate(results, 1):
                print(f"\n--- Result {i} ---")
                print(json.dumps(result, indent=2))
        else:
            print("No descriptors found")
        return 0
    
    if args.descriptor_uid:
        print(f"Getting info for descriptor UID: {args.descriptor_uid}")
        result = get_descriptor_info(args.descriptor_uid, tool=tool, email=email, api_key=args.api_key)
        if result:
            print(json.dumps(result, indent=2))
        else:
            print("Descriptor not found")
        return 0
    
    if args.test_unmapped > 0:
        import csv
        failures_tsv = Path(args.failures_tsv)
        if not failures_tsv.exists():
            print(f"Error: File not found: {failures_tsv}", file=sys.stderr)
            return 1
        
        # Read unmapped ConceptUIs
        unmapped = []
        with failures_tsv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                if row.get("mapping_status", "").strip().lower() == "unmapped":
                    unmapped.append({
                        "original_id": row.get("original_id", "").strip(),
                        "label": row.get("label", "").strip(),
                    })
        
        print(f"Testing {min(args.test_unmapped, len(unmapped))} unmapped ConceptUIs...", file=sys.stderr)
        
        for i, item in enumerate(unmapped[:args.test_unmapped], 1):
            concept_id = item["original_id"].replace("MESH:", "")
            label = item["label"]
            
            print(f"\n[{i}/{min(args.test_unmapped, len(unmapped))}] {item['original_id']}: {label}", file=sys.stderr)
            
            # Try by ConceptUI
            time.sleep(args.min_interval_s)
            result = find_descriptor_by_conceptui(concept_id, tool=tool, email=email, api_key=args.api_key)
            if result:
                print(f"  Found via ConceptUI: {json.dumps(result, indent=2)}")
                continue
            
            # Try by label
            if label:
                time.sleep(args.min_interval_s)
                results = find_descriptor_by_label(label, tool=tool, email=email, api_key=args.api_key)
                if results:
                    print(f"  Found {len(results)} descriptor(s) via label:")
                    for r in results:
                        print(f"    {json.dumps(r, indent=4)}")
                else:
                    print(f"  No descriptors found via label", file=sys.stderr)
            else:
                print(f"  No label available", file=sys.stderr)
        
        return 0
    
    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
