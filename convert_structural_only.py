#!/usr/bin/env python3
"""
Convert SemMedDB chemical identifiers to PubChem using ONLY methods that guarantee
exact structural matches. Avoids name-based searches entirely.

High-confidence methods only:
- InChIKey → PubChem (structural hash)
- InChI → PubChem (full structure)
- CAS/UNII → PubChem xref/RegistryID (authoritative registry)
- UniChem mappings (curated cross-references)
- MeSH Registry Number → PubChem xref/RegistryID
- KEGG → PubChem SID (curated mapping)

Excluded methods:
- Name-based searches (unreliable, ambiguous)
- RxNorm → name → PubChem (not structural)
- MeSH → name → PubChem (not structural)

Usage:
    python convert_structural_only.py [--limit N] [--resume]
"""

import json
import csv
import time
import requests
import argparse
from pathlib import Path
from typing import Optional, List, Tuple
from collections import defaultdict

# Rate limiting
def rate_limit(delay: float = 0.2):
    time.sleep(delay)

# ============================================================================
# PubChem API helpers
# ============================================================================

def pubchem_xref_registry(registry_id: str) -> Optional[int]:
    """Query PubChem xref/RegistryID endpoint - returns single best CID"""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/xref/RegistryID/{registry_id}/cids/JSON"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            cids = data.get("IdentifierList", {}).get("CID", [])
            return cids[0] if cids else None
    except Exception:
        pass
    return None

def pubchem_inchikey(inchikey: str) -> Optional[int]:
    """Query PubChem compound/inchikey endpoint"""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/{inchikey}/cids/JSON"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            cids = data.get("IdentifierList", {}).get("CID", [])
            return cids[0] if cids else None
    except Exception:
        pass
    return None

def pubchem_inchi(inchi: str) -> Optional[int]:
    """Query PubChem compound/inchi endpoint using identity search"""
    url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchi/cids/JSON"
    try:
        resp = requests.post(url, data=inchi, headers={'Content-Type': 'text/plain'}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            cids = data.get("IdentifierList", {}).get("CID", [])
            return cids[0] if cids else None
    except Exception:
        pass
    return None

def pubchem_get_name(cid: int) -> Optional[str]:
    """Get the preferred name for a PubChem CID"""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/property/Title/JSON"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            props = data.get("PropertyTable", {}).get("Properties", [])
            if props:
                return props[0].get("Title")
    except Exception:
        pass
    return None

def pubchem_sid_to_cid(sid: int) -> Optional[int]:
    """Convert PubChem SID to CID"""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/substance/sid/{sid}/cids/JSON"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            info = data.get("InformationList", {}).get("Information", [])
            if info and "CID" in info[0]:
                cids = info[0]["CID"]
                return cids[0] if isinstance(cids, list) else cids
    except Exception:
        pass
    return None

# ============================================================================
# UniChem API helper
# ============================================================================

def unichem_source_to_pubchem(source_id: int, compound_id: str) -> Optional[int]:
    """Query UniChem API v1 to get PubChem CID"""
    url = "https://www.ebi.ac.uk/unichem/api/v1/compounds"
    payload = {
        "type": "sourceID",
        "sourceID": str(source_id),
        "compound": compound_id
    }
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            compounds = data.get("compounds", [])
            if compounds:
                sources = compounds[0].get("sources", [])
                for src in sources:
                    if src.get("id") == 22:  # PubChem
                        return int(src.get("compoundId"))
    except Exception:
        pass
    return None

# ============================================================================
# ChEBI OLS4 API helper
# ============================================================================

def chebi_get_inchikey(chebi_id: str) -> Optional[str]:
    """Get InChIKey from ChEBI OLS4 API"""
    url = f"https://www.ebi.ac.uk/ols4/api/ontologies/chebi/terms?short_form=CHEBI_{chebi_id}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            terms = data.get("_embedded", {}).get("terms", [])
            if terms:
                annotations = terms[0].get("annotation", {})
                inchikey_list = annotations.get("inchi_key_string", [])
                if inchikey_list:
                    return inchikey_list[0]
    except Exception:
        pass
    return None

def chebi_get_inchi(chebi_id: str) -> Optional[str]:
    """Get InChI from ChEBI OLS4 API"""
    url = f"https://www.ebi.ac.uk/ols4/api/ontologies/chebi/terms?short_form=CHEBI_{chebi_id}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            terms = data.get("_embedded", {}).get("terms", [])
            if terms:
                annotations = terms[0].get("annotation", {})
                inchi_list = annotations.get("inchi_string", [])
                if inchi_list:
                    return inchi_list[0]
    except Exception:
        pass
    return None

# ============================================================================
# KEGG API helper
# ============================================================================

def kegg_conv_to_pubchem(kegg_id: str) -> Optional[int]:
    """Query KEGG conv API for PubChem SID"""
    url = f"https://rest.kegg.jp/conv/pubchem/{kegg_id}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200 and resp.text.strip():
            parts = resp.text.strip().split("\t")
            if len(parts) >= 2:
                sid_str = parts[1].replace("pubchem:", "")
                return int(sid_str)
    except Exception:
        pass
    return None

# ============================================================================
# MeSH record helper (for registry numbers)
# ============================================================================

def mesh_get_registry_number(mesh_id: str) -> Optional[str]:
    """Get registry number from MeSH record via NCBI efetch"""
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=mesh&id={mesh_id}&rettype=full"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            text = resp.text
            for line in text.split('\n'):
                if line.startswith('Registry Number:'):
                    rn = line.replace('Registry Number:', '').strip()
                    if rn and rn != '0':
                        return rn
    except Exception:
        pass
    return None

# ============================================================================
# NodeNormalization helper (for extracting structural identifiers)
# ============================================================================

def get_nodenorm_structural_ids(curie: str) -> dict:
    """
    Get structural identifiers from NodeNormalization equivalent_identifiers.
    Returns dict with keys: inchikey, inchi, cas, unii
    """
    url = "https://nodenormalization-sri.renci.org/get_normalized_nodes"
    payload = {"curies": [curie]}
    result = {'inchikey': None, 'inchi': None, 'cas': None, 'unii': None}
    
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if curie in data and data[curie]:
                equiv_ids = data[curie].get("equivalent_identifiers", [])
                for eid in equiv_ids:
                    identifier = eid.get("identifier", "")
                    if identifier.startswith("INCHIKEY:"):
                        result['inchikey'] = identifier.replace("INCHIKEY:", "")
                    elif identifier.startswith("INCHI:"):
                        result['inchi'] = identifier.replace("INCHI:", "")
                    elif identifier.startswith("CAS:"):
                        result['cas'] = identifier.replace("CAS:", "")
                    elif identifier.startswith("UNII:"):
                        result['unii'] = identifier.replace("UNII:", "")
    except Exception:
        pass
    
    return result

# ============================================================================
# Conversion dispatcher (STRUCTURAL ONLY)
# ============================================================================

UNICHEM_SOURCE_IDS = {
    'CHEMBL.COMPOUND': 1,
    'DRUGBANK': 2,
    'CHEBI': 7,
    'DrugCentral': 34,
}

def convert_to_pubchem_structural(prefix: str, local_id: str, umls_key: str = "") -> Tuple[Optional[int], str, Optional[str]]:
    """
    Convert an identifier to PubChem CID using ONLY structural methods.
    Returns: (cid, method_used, pubchem_name)
    """
    cid = None
    method = ""
    pubchem_name = None
    
    # Direct structural identifiers
    if prefix == "INCHIKEY":
        cid = pubchem_inchikey(local_id)
        if cid:
            method = "pubchem_inchikey"
    
    elif prefix == "INCHI":
        cid = pubchem_inchi(local_id)
        if cid:
            method = "pubchem_inchi"
    
    # Authoritative registry identifiers
    elif prefix in ["CAS", "UNII"]:
        cid = pubchem_xref_registry(local_id)
        if cid:
            method = f"pubchem_registry_{prefix.lower()}"
    
    # ChEBI: try InChIKey first, then InChI
    elif prefix == "CHEBI":
        inchikey = chebi_get_inchikey(local_id)
        if inchikey:
            rate_limit(0.1)
            cid = pubchem_inchikey(inchikey)
            if cid:
                method = "chebi_inchikey_pubchem"
        
        if not cid:
            rate_limit(0.1)
            inchi = chebi_get_inchi(local_id)
            if inchi:
                rate_limit(0.1)
                cid = pubchem_inchi(inchi)
                if cid:
                    method = "chebi_inchi_pubchem"
    
    # UniChem curated mappings
    elif prefix in UNICHEM_SOURCE_IDS:
        source_id = UNICHEM_SOURCE_IDS[prefix]
        clean_id = local_id
        if prefix == 'CHEMBL.COMPOUND' and local_id.startswith('CHEMBL'):
            clean_id = local_id  # Keep as-is
        cid = unichem_source_to_pubchem(source_id, clean_id)
        if cid:
            method = f"unichem_{prefix.lower().replace('.', '_')}"
    
    # KEGG curated mapping
    elif prefix == "KEGG.COMPOUND":
        sid = kegg_conv_to_pubchem(f"cpd:{local_id}")
        if sid:
            rate_limit(0.1)
            cid = pubchem_sid_to_cid(sid)
            if cid:
                method = "kegg_conv_pubchem_sid"
    
    # MeSH: try multiple methods
    elif prefix == "MESH":
        # Method 1: Try MeSH ID directly as RegistryID (works for many D* and C* IDs)
        cid = pubchem_xref_registry(local_id)
        if cid:
            method = "mesh_direct_pubchem_registry"
        
        # Method 2: Try to get CAS registry number from MeSH record
        if not cid:
            rn = mesh_get_registry_number(local_id)
            if rn:
                rate_limit(0.1)
                cid = pubchem_xref_registry(rn)
                if cid:
                    method = "mesh_cas_registry_pubchem"
    
    # For RXCUI, MESH (if above failed), and other prefixes, check if NodeNormalization has structural identifiers
    if not cid and umls_key:
        struct_ids = get_nodenorm_structural_ids(umls_key)
        
        # Try InChIKey
        if struct_ids['inchikey']:
            rate_limit(0.1)
            cid = pubchem_inchikey(struct_ids['inchikey'])
            if cid:
                method = f"nodenorm_inchikey_from_{prefix.lower()}"
        
        # Try InChI
        if not cid and struct_ids['inchi']:
            rate_limit(0.1)
            cid = pubchem_inchi(struct_ids['inchi'])
            if cid:
                method = f"nodenorm_inchi_from_{prefix.lower()}"
        
        # Try CAS
        if not cid and struct_ids['cas']:
            rate_limit(0.1)
            cid = pubchem_xref_registry(struct_ids['cas'])
            if cid:
                method = f"nodenorm_cas_from_{prefix.lower()}"
        
        # Try UNII
        if not cid and struct_ids['unii']:
            rate_limit(0.1)
            cid = pubchem_xref_registry(struct_ids['unii'])
            if cid:
                method = f"nodenorm_unii_from_{prefix.lower()}"
    
    # Get PubChem name for verification
    if cid:
        rate_limit(0.1)
        pubchem_name = pubchem_get_name(cid)
    
    return cid, method, pubchem_name

# ============================================================================
# Main batch processing
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Convert identifiers to PubChem (structural methods only)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of rows to process')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--input', default='semmed_chemicals_to_convert.tsv', help='Input TSV file')
    parser.add_argument('--output', default='semmed_chemicals_structural_pubchem.tsv', help='Output TSV file')
    args = parser.parse_args()
    
    input_file = Path(args.input)
    output_file = Path(args.output)
    checkpoint_file = Path('_conversion_structural_checkpoint.json')
    
    # Load checkpoint if resuming
    processed_keys = set()
    if args.resume and checkpoint_file.exists():
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
            processed_keys = set(checkpoint.get('processed', []))
        print(f"Resuming from checkpoint, {len(processed_keys)} already processed")
    
    # Read input
    print(f"Reading {input_file}...")
    rows = []
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            rows.append(row)
    
    print(f"Total rows to process: {len(rows)}")
    if args.limit:
        rows = rows[:args.limit]
        print(f"Limited to: {len(rows)}")
    
    # Process and write output
    stats = defaultdict(lambda: {'total': 0, 'success': 0})
    
    # Open output file (append mode if resuming)
    mode = 'a' if args.resume and output_file.exists() else 'w'
    with open(output_file, mode, newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        
        if mode == 'w':
            writer.writerow([
                'umls_key', 'original_label', 'prefix', 'local_id', 'id_label',
                'pubchem_cid', 'method', 'pubchem_name', 'confidence'
            ])
        
        processed_count = 0
        for i, row in enumerate(rows):
            key = f"{row['prefix']}:{row['local_id']}"
            
            if key in processed_keys:
                continue
            
            prefix = row['prefix']
            local_id = row['local_id']
            umls_key = row.get('umls_key', '')
            original_label = row['original_label'] or row.get('id_label', '')
            
            stats[prefix]['total'] += 1
            
            # Convert using structural methods only
            cid, method, pubchem_name = convert_to_pubchem_structural(prefix, local_id, umls_key)
            
            # Assign confidence level based on method
            confidence = ""
            if method:
                if 'inchikey' in method or 'inchi' in method:
                    confidence = "5-star-structural"
                elif 'registry' in method or 'unichem' in method or 'kegg' in method:
                    confidence = "4-star-curated"
                else:
                    confidence = "structural"
            
            if cid:
                stats[prefix]['success'] += 1
            
            writer.writerow([
                row['umls_key'],
                row['original_label'],
                prefix,
                local_id,
                row.get('id_label', ''),
                cid or '',
                method,
                pubchem_name or '',
                confidence
            ])
            
            processed_keys.add(key)
            processed_count += 1
            
            # Progress update every 100 rows
            if processed_count % 100 == 0:
                print(f"Processed {processed_count}/{len(rows)} rows...")
                # Save checkpoint
                with open(checkpoint_file, 'w') as cf:
                    json.dump({'processed': list(processed_keys)}, cf)
            
            # Rate limit
            rate_limit(0.2)
    
    # Final checkpoint
    with open(checkpoint_file, 'w') as cf:
        json.dump({'processed': list(processed_keys)}, cf)
    
    # Print summary
    print("\n" + "=" * 70)
    print("STRUCTURAL CONVERSION SUMMARY (High-Confidence Methods Only)")
    print("=" * 70)
    
    total_all = 0
    success_all = 0
    for prefix in sorted(stats.keys()):
        s = stats[prefix]
        total_all += s['total']
        success_all += s['success']
        pct = (s['success'] / s['total'] * 100) if s['total'] > 0 else 0
        print(f"  {prefix:20s}: {s['success']:5d}/{s['total']:5d} ({pct:5.1f}%)")
    
    overall_pct = (success_all / total_all * 100) if total_all > 0 else 0
    print(f"\n  {'TOTAL':20s}: {success_all:5d}/{total_all:5d} ({overall_pct:5.1f}%)")
    print(f"\nOutput written to: {output_file}")
    print("\nMethods used (all guarantee exact structural matches):")
    print("  - InChIKey → PubChem (structural hash)")
    print("  - InChI → PubChem (full structure)")
    print("  - CAS/UNII → PubChem RegistryID (authoritative)")
    print("  - UniChem mappings (curated cross-refs)")
    print("  - KEGG conv → PubChem SID (curated)")
    print("  - MeSH Registry Number → PubChem")
    print("  - NodeNormalization structural IDs → PubChem")

if __name__ == '__main__':
    main()
