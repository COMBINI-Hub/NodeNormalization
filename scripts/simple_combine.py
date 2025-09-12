#!/usr/bin/env python3
"""
Simple script to combine PrimeKG and SemMed normalized databases.

This is a simplified version for quick combination without advanced features.
"""

import json
import sys
from pathlib import Path

def load_normalized_data(file_path):
    """Load normalized data from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def simple_union_merge(primekg_data, semmed_data):
    """
    Simple union merge - combine all entities from both databases.
    For entities that exist in both, merge their information.
    """
    combined = {}
    
    # Add all PrimeKG entities
    for entity_id, entity_data in primekg_data.items():
        combined[entity_id] = entity_data.copy()
        # Add source tracking
        if 'source_databases' not in combined[entity_id]:
            combined[entity_id]['source_databases'] = ['PrimeKG']
    
    # Add SemMed entities
    for entity_id, entity_data in semmed_data.items():
        if entity_id in combined:
            # Merge existing entity
            existing = combined[entity_id]
            existing['source_databases'].append('SemMed')
            
            # Merge types (remove duplicates)
            existing_types = set(existing['type'])
            existing_types.update(entity_data['type'])
            existing['type'] = list(existing_types)
            
            # Merge equivalent identifiers
            existing_equiv = {eq['identifier'] for eq in existing['equivalent_identifiers']}
            for eq in entity_data['equivalent_identifiers']:
                if eq['identifier'] not in existing_equiv:
                    existing['equivalent_identifiers'].append(eq)
        else:
            # Add new entity
            new_entity = entity_data.copy()
            new_entity['source_databases'] = ['SemMed']
            combined[entity_id] = new_entity
    
    return combined

def main():
    """Main function."""
    if len(sys.argv) != 4:
        print("Usage: python simple_combine.py <primekg_file> <semmed_file> <output_file>")
        sys.exit(1)
    
    primekg_file = sys.argv[1]
    semmed_file = sys.argv[2]
    output_file = sys.argv[3]
    
    print(f"Loading PrimeKG data from {primekg_file}...")
    primekg_data = load_normalized_data(primekg_file)
    print(f"Loaded {len(primekg_data)} entities from PrimeKG")
    
    print(f"Loading SemMed data from {semmed_file}...")
    semmed_data = load_normalized_data(semmed_file)
    print(f"Loaded {len(semmed_data)} entities from SemMed")
    
    print("Combining datasets...")
    combined_data = simple_union_merge(primekg_data, semmed_data)
    print(f"Combined dataset contains {len(combined_data)} entities")
    
    # Count entities by source
    primekg_only = sum(1 for e in combined_data.values() if e['source_databases'] == ['PrimeKG'])
    semmed_only = sum(1 for e in combined_data.values() if e['source_databases'] == ['SemMed'])
    both_sources = sum(1 for e in combined_data.values() if len(e['source_databases']) > 1)
    
    print(f"Entities from PrimeKG only: {primekg_only}")
    print(f"Entities from SemMed only: {semmed_only}")
    print(f"Entities from both sources: {both_sources}")
    
    print(f"Saving combined data to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(combined_data, f, indent=2)
    
    print("Done!")

if __name__ == "__main__":
    main()
