#!/usr/bin/env python3
"""
Validation script for combined normalized databases.

This script performs various quality checks on the combined dataset.
"""

import json
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Set, Any

def load_combined_data(file_path: str) -> Dict[str, Any]:
    """Load combined data from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def validate_data_integrity(data: Dict[str, Any]) -> List[str]:
    """Validate basic data integrity."""
    issues = []
    
    for entity_id, entity_data in data.items():
        # Check required fields
        required_fields = ['id', 'equivalent_identifiers', 'type', 'source_databases']
        for field in required_fields:
            if field not in entity_data:
                issues.append(f"Entity {entity_id} missing required field: {field}")
        
        # Check id structure
        if 'id' in entity_data:
            if 'identifier' not in entity_data['id'] or 'label' not in entity_data['id']:
                issues.append(f"Entity {entity_id} has malformed id field")
        
        # Check equivalent_identifiers structure
        if 'equivalent_identifiers' in entity_data:
            for i, equiv in enumerate(entity_data['equivalent_identifiers']):
                if not isinstance(equiv, dict) or 'identifier' not in equiv or 'label' not in equiv:
                    issues.append(f"Entity {entity_id} has malformed equivalent_identifier at index {i}")
        
        # Check types
        if 'type' in entity_data:
            if not isinstance(entity_data['type'], list):
                issues.append(f"Entity {entity_id} has non-list type field")
            else:
                for type_val in entity_data['type']:
                    if not isinstance(type_val, str) or not type_val.startswith('biolink:'):
                        issues.append(f"Entity {entity_id} has invalid type: {type_val}")
    
    return issues

def check_identifier_consistency(data: Dict[str, Any]) -> List[str]:
    """Check for identifier consistency issues."""
    issues = []
    
    for entity_id, entity_data in data.items():
        # Check if entity_id matches the id.identifier
        if 'id' in entity_data and 'identifier' in entity_data['id']:
            if entity_id != entity_data['id']['identifier']:
                issues.append(f"Entity key {entity_id} doesn't match id.identifier {entity_data['id']['identifier']}")
        
        # Check if entity_id appears in equivalent_identifiers
        if 'equivalent_identifiers' in entity_data:
            equiv_ids = [eq['identifier'] for eq in entity_data['equivalent_identifiers']]
            if entity_id not in equiv_ids:
                issues.append(f"Entity {entity_id} not found in its own equivalent_identifiers")
    
    return issues

def analyze_type_distribution(data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze the distribution of entity types."""
    type_counts = Counter()
    source_type_counts = defaultdict(Counter)
    
    for entity_data in data.values():
        if 'type' in entity_data and 'source_databases' in entity_data:
            for entity_type in entity_data['type']:
                type_counts[entity_type] += 1
                for source in entity_data['source_databases']:
                    source_type_counts[source][entity_type] += 1
    
    return {
        'total_types': len(type_counts),
        'type_counts': dict(type_counts),
        'source_type_counts': {k: dict(v) for k, v in source_type_counts.items()}
    }

def analyze_source_overlap(data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze overlap between source databases."""
    source_counts = Counter()
    overlap_analysis = defaultdict(int)
    
    for entity_data in data.values():
        if 'source_databases' in entity_data:
            sources = tuple(sorted(entity_data['source_databases']))
            source_counts[sources] += 1
            
            if len(sources) == 1:
                overlap_analysis['single_source'] += 1
            else:
                overlap_analysis['multi_source'] += 1
                overlap_analysis[f'overlap_{"_".join(sources)}'] += 1
    
    return {
        'source_combinations': {str(k): v for k, v in source_counts.items()},
        'overlap_analysis': dict(overlap_analysis)
    }

def check_duplicate_identifiers(data: Dict[str, Any]) -> List[str]:
    """Check for duplicate identifiers across entities."""
    issues = []
    identifier_to_entities = defaultdict(list)
    
    for entity_id, entity_data in data.items():
        # Check main identifier
        if 'id' in entity_data and 'identifier' in entity_data['id']:
            main_id = entity_data['id']['identifier']
            identifier_to_entities[main_id].append(entity_id)
        
        # Check equivalent identifiers
        if 'equivalent_identifiers' in entity_data:
            for equiv in entity_data['equivalent_identifiers']:
                if 'identifier' in equiv:
                    identifier_to_entities[equiv['identifier']].append(entity_id)
    
    # Find duplicates
    for identifier, entities in identifier_to_entities.items():
        if len(entities) > 1:
            issues.append(f"Identifier {identifier} appears in multiple entities: {entities}")
    
    return issues

def generate_quality_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a comprehensive quality report."""
    report = {
        'total_entities': len(data),
        'validation_issues': [],
        'type_analysis': {},
        'source_analysis': {},
        'duplicate_issues': []
    }
    
    print("Running data integrity checks...")
    report['validation_issues'] = validate_data_integrity(data)
    
    print("Checking identifier consistency...")
    report['validation_issues'].extend(check_identifier_consistency(data))
    
    print("Analyzing type distribution...")
    report['type_analysis'] = analyze_type_distribution(data)
    
    print("Analyzing source overlap...")
    report['source_analysis'] = analyze_source_overlap(data)
    
    print("Checking for duplicate identifiers...")
    report['duplicate_issues'] = check_duplicate_identifiers(data)
    
    return report

def main():
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python validate_combined_data.py <combined_data_file>")
        sys.exit(1)
    
    data_file = sys.argv[1]
    
    print(f"Loading combined data from {data_file}...")
    data = load_combined_data(data_file)
    
    print("Generating quality report...")
    report = generate_quality_report(data)
    
    # Print summary
    print(f"\n=== QUALITY REPORT ===")
    print(f"Total entities: {report['total_entities']}")
    print(f"Validation issues: {len(report['validation_issues'])}")
    print(f"Duplicate identifier issues: {len(report['duplicate_issues'])}")
    
    if report['validation_issues']:
        print(f"\n=== VALIDATION ISSUES ===")
        for issue in report['validation_issues'][:10]:  # Show first 10
            print(f"  - {issue}")
        if len(report['validation_issues']) > 10:
            print(f"  ... and {len(report['validation_issues']) - 10} more")
    
    if report['duplicate_issues']:
        print(f"\n=== DUPLICATE IDENTIFIER ISSUES ===")
        for issue in report['duplicate_issues'][:5]:  # Show first 5
            print(f"  - {issue}")
        if len(report['duplicate_issues']) > 5:
            print(f"  ... and {len(report['duplicate_issues']) - 5} more")
    
    print(f"\n=== TYPE DISTRIBUTION ===")
    type_counts = report['type_analysis']['type_counts']
    sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
    for entity_type, count in sorted_types[:10]:
        print(f"  {entity_type}: {count}")
    
    print(f"\n=== SOURCE OVERLAP ===")
    overlap = report['source_analysis']['overlap_analysis']
    print(f"  Single source entities: {overlap.get('single_source', 0)}")
    print(f"  Multi-source entities: {overlap.get('multi_source', 0)}")
    
    # Save detailed report
    report_file = data_file.replace('.json', '_validation_report.json')
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\nDetailed report saved to {report_file}")

if __name__ == "__main__":
    main()
