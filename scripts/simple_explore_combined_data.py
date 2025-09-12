#!/usr/bin/env python3
"""
Simple exploration script for combined normalized databases.
Uses only standard library modules - no external dependencies required.
"""

import json
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Set, Any

def load_data(file_path: str) -> Dict[str, Any]:
    """Load data from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def analyze_types(data: Dict[str, Any], name: str) -> Counter:
    """Analyze entity types in dataset."""
    type_counts = Counter()
    for entity in data.values():
        for entity_type in entity.get('type', []):
            type_counts[entity_type] += 1
    
    print(f"\n{name} Type Distribution:")
    for entity_type, count in type_counts.most_common(10):
        print(f"  {entity_type}: {count}")
    
    return type_counts

def simple_union_merge(primekg_data: Dict[str, Any], semmed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Simple union merge of two datasets."""
    combined = {}
    
    # Add all PrimeKG entities
    for entity_id, entity_data in primekg_data.items():
        combined[entity_id] = entity_data.copy()
        combined[entity_id]['source_databases'] = ['PrimeKG']
    
    # Add SemMed entities
    for entity_id, entity_data in semmed_data.items():
        if entity_id in combined:
            # Merge existing entity
            existing = combined[entity_id]
            existing['source_databases'].append('SemMed')
            
            # Merge types
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

def analyze_combined_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze the combined dataset."""
    stats = {
        'total_entities': len(data),
        'primekg_only': 0,
        'semmed_only': 0,
        'both_sources': 0,
        'type_distribution': Counter(),
        'source_distribution': Counter()
    }
    
    for entity in data.values():
        sources = entity.get('source_databases', [])
        
        if sources == ['PrimeKG']:
            stats['primekg_only'] += 1
        elif sources == ['SemMed']:
            stats['semmed_only'] += 1
        else:
            stats['both_sources'] += 1
        
        for source in sources:
            stats['source_distribution'][source] += 1
        
        for entity_type in entity.get('type', []):
            stats['type_distribution'][entity_type] += 1
    
    return stats

def print_ascii_chart(data: Dict[str, int], title: str, max_width: int = 50) -> None:
    """Print a simple ASCII bar chart."""
    if not data:
        return
    
    print(f"\n{title}")
    print("=" * len(title))
    
    max_value = max(data.values())
    scale = max_width / max_value if max_value > 0 else 1
    
    for key, value in data.items():
        bar_length = int(value * scale)
        bar = "█" * bar_length
        print(f"{key:<30} {bar} {value}")

def calculate_quality_metrics(data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate quality metrics for the dataset."""
    metrics = {
        'total_entities': len(data),
        'entities_with_multiple_sources': 0,
        'average_types_per_entity': 0,
        'average_equiv_identifiers_per_entity': 0,
        'type_diversity': 0,
        'identifier_consistency': 0
    }
    
    total_types = 0
    total_equiv = 0
    all_types = set()
    consistent_identifiers = 0
    
    for entity_id, entity in data.items():
        # Multiple sources
        if len(entity.get('source_databases', [])) > 1:
            metrics['entities_with_multiple_sources'] += 1
        
        # Types
        types = entity.get('type', [])
        total_types += len(types)
        all_types.update(types)
        
        # Equivalent identifiers
        equiv = entity.get('equivalent_identifiers', [])
        total_equiv += len(equiv)
        
        # Identifier consistency
        if 'id' in entity and entity['id'].get('identifier') == entity_id:
            consistent_identifiers += 1
    
    metrics['average_types_per_entity'] = total_types / len(data) if data else 0
    metrics['average_equiv_identifiers_per_entity'] = total_equiv / len(data) if data else 0
    metrics['type_diversity'] = len(all_types)
    metrics['identifier_consistency'] = consistent_identifiers / len(data) if data else 0
    
    return metrics

def main():
    """Main function."""
    if len(sys.argv) != 4:
        print("Usage: python simple_explore_combined_data.py <primekg_file> <semmed_file> <output_file>")
        sys.exit(1)
    
    primekg_file = sys.argv[1]
    semmed_file = sys.argv[2]
    output_file = sys.argv[3]
    
    print("=== COMBINING NORMALIZED DATABASES ===")
    print(f"Loading PrimeKG data from {primekg_file}...")
    primekg_data = load_data(primekg_file)
    print(f"Loaded {len(primekg_data)} entities from PrimeKG")
    
    print(f"Loading SemMed data from {semmed_file}...")
    semmed_data = load_data(semmed_file)
    print(f"Loaded {len(semmed_data)} entities from SemMed")
    
    # Analyze original datasets
    primekg_types = analyze_types(primekg_data, "PrimeKG")
    semmed_types = analyze_types(semmed_data, "SemMed")
    
    print("\n=== COMBINING DATASETS ===")
    combined_data = simple_union_merge(primekg_data, semmed_data)
    print(f"Combined dataset contains {len(combined_data)} entities")
    
    # Analyze combined dataset
    stats = analyze_combined_data(combined_data)
    print(f"\n=== COMBINED DATASET STATISTICS ===")
    print(f"Total entities: {stats['total_entities']}")
    print(f"PrimeKG only: {stats['primekg_only']}")
    print(f"SemMed only: {stats['semmed_only']}")
    print(f"Both sources: {stats['both_sources']}")
    
    # Print type distribution
    print_ascii_chart(dict(stats['type_distribution'].most_common(10)), "Top 10 Entity Types")
    
    # Calculate quality metrics
    quality_metrics = calculate_quality_metrics(combined_data)
    print(f"\n=== QUALITY METRICS ===")
    for metric, value in quality_metrics.items():
        if isinstance(value, float):
            print(f"{metric}: {value:.3f}")
        else:
            print(f"{metric}: {value}")
    
    # Save combined data
    print(f"\n=== SAVING DATA ===")
    with open(output_file, 'w') as f:
        json.dump(combined_data, f, indent=2)
    print(f"Combined data saved to {output_file}")
    
    # Save statistics
    stats_file = output_file.replace('.json', '_stats.json')
    with open(stats_file, 'w') as f:
        # Convert Counter objects to regular dicts for JSON serialization
        stats_serializable = {
            'total_entities': stats['total_entities'],
            'primekg_only': stats['primekg_only'],
            'semmed_only': stats['semmed_only'],
            'both_sources': stats['both_sources'],
            'type_distribution': dict(stats['type_distribution']),
            'source_distribution': dict(stats['source_distribution']),
            'quality_metrics': quality_metrics
        }
        json.dump(stats_serializable, f, indent=2)
    print(f"Statistics saved to {stats_file}")
    
    print("\n=== EXPLORATION COMPLETE ===")
    print("You can now:")
    print("1. Use the combined JSON file for further analysis")
    print("2. Run the Jupyter notebook for interactive visualization")
    print("3. Use the validation script to check data quality")

if __name__ == "__main__":
    main()
