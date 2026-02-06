#!/usr/bin/env python3
"""
Merge PubChem CIDs from structural conversion into normalized SemMedDB data.

This script:
1. Loads the structural conversion results (semmed_chemicals_structural_pubchem.tsv)
2. Loads the normalized SemMedDB data (semmed_normalized_full.json)
3. Adds PubChem CIDs to the equivalent_identifiers array for matching UMLS entries
4. Tracks statistics about additions and updates
5. Saves the updated normalized data
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def load_pubchem_conversions(conversion_file):
    """
    Load PubChem conversions from TSV file.
    
    Returns:
        dict: {umls_key: {pubchem_cid, method, confidence, pubchem_name}}
    """
    conversions = {}
    
    print(f"Loading PubChem conversions from {conversion_file}...")
    
    with open(conversion_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        for row in reader:
            umls_key = row['umls_key']
            pubchem_cid = row.get('pubchem_cid', '').strip()
            
            # Skip entries without PubChem CID
            if not pubchem_cid:
                continue
            
            conversions[umls_key] = {
                'pubchem_cid': pubchem_cid,
                'method': row.get('method', ''),
                'confidence': row.get('confidence', ''),
                'pubchem_name': row.get('pubchem_name', ''),
                'original_label': row.get('original_label', '')
            }
    
    print(f"Loaded {len(conversions)} PubChem conversions")
    return conversions


def merge_pubchem_into_normalized(normalized_data, pubchem_conversions):
    """
    Merge PubChem CIDs into normalized data.
    
    Returns:
        tuple: (updated_data, stats)
    """
    stats = {
        'total_normalized_entries': len(normalized_data),
        'pubchem_conversions_available': len(pubchem_conversions),
        'umls_keys_matched': 0,
        'pubchem_added': 0,
        'pubchem_already_exists': 0,
        'pubchem_updated': 0,
        'missing_umls_keys': 0,
        'by_confidence': defaultdict(int),
        'by_method': defaultdict(int)
    }
    
    missing_keys = []
    
    print("\nMerging PubChem CIDs into normalized data...")
    
    for umls_key, pubchem_info in pubchem_conversions.items():
        pubchem_cid = pubchem_info['pubchem_cid']
        pubchem_identifier = f"PUBCHEM.COMPOUND:{pubchem_cid}"
        
        # Check if UMLS key exists in normalized data
        if umls_key not in normalized_data:
            stats['missing_umls_keys'] += 1
            missing_keys.append(umls_key)
            continue
        
        stats['umls_keys_matched'] += 1
        
        entry = normalized_data[umls_key]
        equiv_ids = entry.get('equivalent_identifiers', [])
        
        # Check if PubChem CID already exists
        existing_pubchem = [
            eq for eq in equiv_ids 
            if eq.get('identifier', '').startswith('PUBCHEM.COMPOUND:')
        ]
        
        if existing_pubchem:
            # Check if it's the same CID
            if any(eq.get('identifier') == pubchem_identifier for eq in existing_pubchem):
                stats['pubchem_already_exists'] += 1
            else:
                # Different CID - add as alternative
                stats['pubchem_added'] += 1
                equiv_ids.append({
                    'identifier': pubchem_identifier,
                    'label': pubchem_info.get('pubchem_name', ''),
                    'type': entry.get('type', ['biolink:SmallMolecule'])[0] if entry.get('type') else 'biolink:SmallMolecule'
                })
        else:
            # No PubChem CID exists - add it
            stats['pubchem_added'] += 1
            
            # Determine the biolink type from existing types
            types = entry.get('type', [])
            biolink_type = 'biolink:SmallMolecule'
            if 'biolink:SmallMolecule' in types:
                biolink_type = 'biolink:SmallMolecule'
            elif 'biolink:ChemicalEntity' in types:
                biolink_type = 'biolink:ChemicalEntity'
            elif types:
                biolink_type = types[0]
            
            equiv_ids.append({
                'identifier': pubchem_identifier,
                'label': pubchem_info.get('pubchem_name', ''),
                'type': biolink_type
            })
        
        # Track by confidence and method
        stats['by_confidence'][pubchem_info['confidence']] += 1
        stats['by_method'][pubchem_info['method']] += 1
    
    print(f"\nMerge complete!")
    print(f"  Matched UMLS keys: {stats['umls_keys_matched']}")
    print(f"  PubChem CIDs added: {stats['pubchem_added']}")
    print(f"  PubChem CIDs already existed: {stats['pubchem_already_exists']}")
    print(f"  Missing UMLS keys: {stats['missing_umls_keys']}")
    
    if missing_keys[:5]:
        print(f"\nExample missing keys: {missing_keys[:5]}")
    
    return normalized_data, stats


def save_results(normalized_data, stats, output_file, stats_file):
    """Save updated normalized data and statistics."""
    
    print(f"\nSaving updated normalized data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saving statistics to {stats_file}...")
    
    # Add timestamp
    stats['timestamp'] = datetime.now().isoformat()
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    print("\nStatistics Summary:")
    print(f"  Total normalized entries: {stats['total_normalized_entries']}")
    print(f"  PubChem conversions available: {stats['pubchem_conversions_available']}")
    print(f"  UMLS keys matched: {stats['umls_keys_matched']}")
    print(f"  PubChem CIDs added: {stats['pubchem_added']}")
    print(f"  PubChem CIDs already existed: {stats['pubchem_already_exists']}")
    print(f"  Missing UMLS keys: {stats['missing_umls_keys']}")
    
    print("\nBy Confidence Level:")
    for confidence, count in sorted(stats['by_confidence'].items()):
        print(f"  {confidence}: {count}")
    
    print("\nTop 10 Methods:")
    for method, count in sorted(stats['by_method'].items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {method}: {count}")


def main():
    # File paths
    base_dir = Path(__file__).parent
    
    conversion_file = base_dir / "semmed_chemicals_structural_pubchem.tsv"
    normalized_file = base_dir / "normalized_nodes" / "semmed" / "semmed_normalized_full.json"
    
    output_file = base_dir / "normalized_nodes" / "semmed" / "semmed_normalized_full_with_pubchem.json"
    stats_file = base_dir / "pubchem_merge_stats.json"
    
    # Load data
    pubchem_conversions = load_pubchem_conversions(conversion_file)
    
    print(f"\nLoading normalized SemMedDB data from {normalized_file}...")
    with open(normalized_file, 'r', encoding='utf-8') as f:
        normalized_data = json.load(f)
    print(f"Loaded {len(normalized_data)} normalized entries")
    
    # Merge
    updated_data, stats = merge_pubchem_into_normalized(normalized_data, pubchem_conversions)
    
    # Save results
    save_results(updated_data, stats, output_file, stats_file)
    
    print("\n✅ PubChem merge complete!")
    print(f"\nOutput files:")
    print(f"  Updated normalized data: {output_file}")
    print(f"  Statistics: {stats_file}")
    
    # Calculate coverage
    coverage = (stats['umls_keys_matched'] / len(pubchem_conversions) * 100) if pubchem_conversions else 0
    print(f"\nCoverage: {stats['umls_keys_matched']} / {len(pubchem_conversions)} ({coverage:.1f}%)")


if __name__ == "__main__":
    main()
