#!/usr/bin/env python3
"""
Analyze the 30 remaining NCBITaxID:* IDs that failed to normalize.

This script queries NCBI Taxonomy E-utilities to determine why these IDs
are not normalizing in NodeNormalization.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
import requests
import xml.etree.ElementTree as ET


_NCBITAXID_RE = re.compile(r"^NCBITaxID:(\d+)$")


def extract_taxid(curie: str) -> Optional[str]:
    """Extract taxon ID from NCBITaxID: prefix."""
    m = _NCBITAXID_RE.match(curie.strip())
    if m:
        return m.group(1)
    return None


def fetch_taxonomy_esummary(
    session: requests.Session,
    taxids: List[str],
    timeout: float = 30.0,
    api_key: Optional[str] = None,
) -> Dict[str, Dict]:
    """Fetch taxonomy summaries using esummary."""
    if not taxids:
        return {}
    
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        'db': 'taxonomy',
        'id': ','.join(taxids),
        'retmode': 'json',
    }
    if api_key:
        params['api_key'] = api_key
    
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        result = data.get('result', {}) if isinstance(data, dict) else {}
        
        out: Dict[str, Dict] = {}
        for tid in taxids:
            rec = result.get(tid)
            out[tid] = rec if isinstance(rec, dict) else {}
        return out
    except Exception as e:
        print(f"Error in esummary for {taxids}: {e}", file=sys.stderr)
        return {}


def fetch_taxonomy_efetch(
    session: requests.Session,
    taxid: str,
    timeout: float = 30.0,
    api_key: Optional[str] = None,
) -> Optional[ET.Element]:
    """Fetch taxonomy record using efetch."""
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        'db': 'taxonomy',
        'id': taxid,
    }
    if api_key:
        params['api_key'] = api_key
    
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        return root.find('Taxon')
    except Exception as e:
        print(f"Error in efetch for {taxid}: {e}", file=sys.stderr)
        return None


def check_nodenorm(curie: str, endpoint: str = "https://nodenormalization-sri.renci.org/get_normalized_nodes") -> Optional[Dict]:
    """Check if a CURIE normalizes in NodeNormalization."""
    # Rewrite NCBITaxID to NCBITaxon
    rewritten = curie.replace("NCBITaxID:", "NCBITaxon:")
    
    params = {"curie": rewritten}
    try:
        r = requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get(rewritten)
    except Exception as e:
        print(f"Error checking NodeNormalization for {curie}: {e}", file=sys.stderr)
        return None


def analyze_taxid(
    session: requests.Session,
    curie: str,
    api_key: Optional[str] = None,
    min_interval_s: float = 0.34,
) -> Dict:
    """Analyze a single taxon ID to determine why it's failing."""
    taxid = extract_taxid(curie)
    if not taxid:
        return {"curie": curie, "error": "Could not extract taxid"}
    
    result = {
        "curie": curie,
        "taxid": taxid,
        "rewritten_curie": f"NCBITaxon:{taxid}",
    }
    
    # Check NodeNormalization
    time.sleep(min_interval_s)
    nodenorm_result = check_nodenorm(curie)
    result["nodenorm_result"] = "normalized" if nodenorm_result else "null"
    
    # Check NCBI Taxonomy esummary
    time.sleep(min_interval_s)
    esummary_records = fetch_taxonomy_esummary(session, [taxid], api_key=api_key)
    esummary = esummary_records.get(taxid, {})
    
    result["esummary"] = {
        "status": esummary.get("status"),
        "error": esummary.get("error"),
        "scientificname": esummary.get("scientificname"),
        "akataxid": esummary.get("akataxid"),
    }
    
    # If esummary failed or returned merged status, try efetch
    if esummary.get("error") == "cannot get document summary" or esummary.get("status") == "merged":
        time.sleep(min_interval_s)
        taxon = fetch_taxonomy_efetch(session, taxid, api_key=api_key)
        
        if taxon is not None:
            current_taxid = (taxon.findtext('TaxId') or '').strip()
            scientific_name = (taxon.findtext('ScientificName') or '').strip()
            rank = (taxon.findtext('Rank') or '').strip()
            
            # Check for AkaTaxIds
            aka_taxids = []
            for aka in taxon.findall('AkaTaxIds/TaxId'):
                aka_taxids.append(aka.text.strip() if aka.text else '')
            
            result["efetch"] = {
                "current_taxid": current_taxid,
                "scientific_name": scientific_name,
                "rank": rank,
                "aka_taxids": aka_taxids,
                "is_merged": current_taxid != taxid or taxid in aka_taxids,
            }
            
            # If merged, check if replacement normalizes
            if result["efetch"]["is_merged"] and current_taxid and current_taxid != taxid:
                time.sleep(min_interval_s)
                replacement_curie = f"NCBITaxon:{current_taxid}"
                replacement_nodenorm = check_nodenorm(replacement_curie)
                result["replacement_nodenorm"] = "normalized" if replacement_nodenorm else "null"
                result["replacement_taxid"] = current_taxid
        else:
            result["efetch"] = {"error": "Failed to fetch"}
    
    # Classify the failure reason
    if result["nodenorm_result"] == "normalized":
        result["failure_reason"] = "Actually normalizes (unexpected)"
    elif result["esummary"].get("status") == "merged" and result["esummary"].get("akataxid"):
        result["failure_reason"] = f"Merged taxid (replacement: {result['esummary']['akataxid']})"
    elif result.get("efetch", {}).get("is_merged"):
        result["failure_reason"] = f"Merged taxid (replacement: {result.get('replacement_taxid', 'unknown')})"
    elif result["esummary"].get("error") == "cannot get document summary" and not result.get("efetch"):
        result["failure_reason"] = "Not found in NCBI Taxonomy (deleted/obsolete)"
    elif result["esummary"].get("error"):
        result["failure_reason"] = f"NCBI Taxonomy error: {result['esummary']['error']}"
    elif not result["esummary"].get("scientificname"):
        result["failure_reason"] = "No scientific name in esummary (possibly invalid)"
    else:
        result["failure_reason"] = "Valid taxid but not in NodeNormalization database"
    
    return result


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze failing NCBITaxID:* IDs"
    )
    ap.add_argument(
        "--input-file",
        default="/Users/drshika2/RTX-KG2/non_normalizing_ikraph_ids.hosted_follow_taxon_merges.txt",
        help="Input file with non-normalizing IDs",
    )
    ap.add_argument(
        "--output-json",
        default="failing_taxon_ids_analysis.json",
        help="Output JSON file with analysis results",
    )
    ap.add_argument(
        "--min-interval-s",
        type=float,
        default=0.34,
        help="Minimum seconds between API calls (default: 0.34 for 3 req/sec)",
    )
    ap.add_argument(
        "--api-key",
        help="NCBI API key (optional, increases rate limit)",
    )
    args = ap.parse_args()
    
    # Read failing NCBITaxID IDs
    input_file = Path(args.input_file)
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        return 1
    
    failing_ids = []
    with input_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("NCBITaxID:"):
                failing_ids.append(line)
    
    print(f"Found {len(failing_ids)} NCBITaxID:* IDs to analyze", file=sys.stderr)
    
    # Analyze each ID
    session = requests.Session()
    results = []
    
    for i, curie in enumerate(failing_ids, 1):
        print(f"[{i}/{len(failing_ids)}] Analyzing {curie}...", file=sys.stderr)
        result = analyze_taxid(session, curie, api_key=args.api_key, min_interval_s=args.min_interval_s)
        results.append(result)
        print(f"  Reason: {result.get('failure_reason', 'unknown')}", file=sys.stderr)
    
    # Save results
    output_file = Path(args.output_json)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nAnalysis complete. Results saved to {output_file}", file=sys.stderr)
    
    # Print summary
    print("\n=== Summary ===", file=sys.stderr)
    reasons = {}
    for result in results:
        reason = result.get("failure_reason", "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
    
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}", file=sys.stderr)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
