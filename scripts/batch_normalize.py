#!/usr/bin/env python3
"""
Batch normalize CURIEs using NodeNorm POST endpoint.
This automatically converts CURIEs to the same format as normalized_semmed_sample.json
"""

import json
import requests
import csv
from pathlib import Path
from typing import List, Dict, Any
import time

def batch_normalize_curies(curies: List[str], endpoint: str = "http://127.0.0.1:8000/get_normalized_nodes", 
                          batch_size: int = 100) -> Dict[str, Any]:
    """
    Normalize a list of CURIEs using NodeNorm POST endpoint.
    
    Args:
        curies: List of CURIEs to normalize
        endpoint: NodeNorm API endpoint
        batch_size: Number of CURIEs to process per batch
    
    Returns:
        Dictionary mapping CURIEs to their normalized data
    """
    result = {}
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(curies), batch_size):
        batch = curies[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(curies) + batch_size - 1)//batch_size} ({len(batch)} CURIEs)")
        
        try:
            response = requests.post(
                endpoint,
                json={"curies": batch},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            
            batch_result = response.json()
            result.update(batch_result)
            
            # Small delay between batches
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error processing batch: {e}")
            # Add null entries for failed batch
            for curie in batch:
                result[curie] = None
    
    return result

def load_curies_from_csv(csv_path: str, limit: int = None) -> List[str]:
    """Load CURIEs from a CSV file."""
    curies = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            curie = row.get('input_curie', '').strip()
            if curie:
                curies.append(curie)
    return curies

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch normalize CURIEs using NodeNorm POST endpoint')
    parser.add_argument('--csv', required=True, help='CSV file with input_curie column')
    parser.add_argument('--output', required=True, help='Output JSON file path')
    parser.add_argument('--endpoint', default='http://127.0.0.1:8000/get_normalized_nodes', 
                       help='NodeNorm API endpoint')
    parser.add_argument('--limit', type=int, help='Limit number of CURIEs to process')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for API calls')
    
    args = parser.parse_args()
    
    print(f"Loading CURIEs from {args.csv}...")
    curies = load_curies_from_csv(args.csv, args.limit)
    print(f"Found {len(curies)} CURIEs to normalize")
    
    print(f"Normalizing CURIEs using {args.endpoint}...")
    result = batch_normalize_curies(curies, args.endpoint, args.batch_size)
    
    # Filter out null results
    valid_results = {k: v for k, v in result.items() if v is not None}
    print(f"Successfully normalized {len(valid_results)} out of {len(curies)} CURIEs")
    
    # Write results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"Wrote normalized results to {output_path}")

if __name__ == "__main__":
    main()
