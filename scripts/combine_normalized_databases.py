#!/usr/bin/env python3
"""
Script to combine PrimeKG and SemMed normalized databases.

This script provides multiple strategies for combining the normalized data from
PrimeKG and SemMed databases, including union, intersection, and conflict resolution approaches.

Author: AI Assistant
Date: 2024
"""

import json
import logging
from typing import Dict, List, Set, Any, Optional, Tuple
from collections import defaultdict
import argparse
from pathlib import Path
import pandas as pd
import networkx as nx
from dataclasses import dataclass, asdict
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class NormalizedEntity:
    """Represents a normalized entity with its identifiers and types."""
    identifier: str
    label: str
    equivalent_identifiers: List[Dict[str, str]]
    types: List[str]
    source_databases: Set[str]
    confidence_score: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format compatible with the original structure."""
        return {
            "id": {
                "identifier": self.identifier,
                "label": self.label
            },
            "equivalent_identifiers": self.equivalent_identifiers,
            "type": self.types,
            "source_databases": list(self.source_databases),
            "confidence_score": self.confidence_score
        }

class DatabaseCombiner:
    """Combines normalized databases with various strategies."""
    
    def __init__(self, primekg_path: str, semmed_path: str):
        """Initialize with paths to normalized databases."""
        self.primekg_path = primekg_path
        self.semmed_path = semmed_path
        self.primekg_data = {}
        self.semmed_data = {}
        self.combined_data = {}
        
    def load_databases(self) -> None:
        """Load both normalized databases."""
        logger.info("Loading PrimeKG normalized data...")
        with open(self.primekg_path, 'r') as f:
            self.primekg_data = json.load(f)
        logger.info(f"Loaded {len(self.primekg_data)} entities from PrimeKG")
        
        logger.info("Loading SemMed normalized data...")
        with open(self.semmed_path, 'r') as f:
            self.semmed_data = json.load(f)
        logger.info(f"Loaded {len(self.semmed_data)} entities from SemMed")
    
    def create_entity_from_normalized(self, entity_data: Dict, source_db: str) -> NormalizedEntity:
        """Create NormalizedEntity from normalized database format."""
        return NormalizedEntity(
            identifier=entity_data["id"]["identifier"],
            label=entity_data["id"]["label"],
            equivalent_identifiers=entity_data["equivalent_identifiers"],
            types=entity_data["type"],
            source_databases={source_db}
        )
    
    def union_merge(self) -> Dict[str, NormalizedEntity]:
        """
        Strategy 1: Union merge - combine all entities from both databases.
        For overlapping entities, merge their information.
        """
        logger.info("Performing union merge...")
        combined = {}
        
        # Add all PrimeKG entities
        for entity_id, entity_data in self.primekg_data.items():
            combined[entity_id] = self.create_entity_from_normalized(entity_data, "PrimeKG")
        
        # Add SemMed entities, merging if they already exist
        for entity_id, entity_data in self.semmed_data.items():
            if entity_id in combined:
                # Merge existing entity
                existing = combined[entity_id]
                existing.source_databases.add("SemMed")
                # Merge types (remove duplicates)
                existing.types = list(set(existing.types + entity_data["type"]))
                # Merge equivalent identifiers
                existing_equiv = {eq["identifier"] for eq in existing.equivalent_identifiers}
                for eq in entity_data["equivalent_identifiers"]:
                    if eq["identifier"] not in existing_equiv:
                        existing.equivalent_identifiers.append(eq)
            else:
                # Add new entity
                combined[entity_id] = self.create_entity_from_normalized(entity_data, "SemMed")
        
        logger.info(f"Union merge resulted in {len(combined)} entities")
        return combined
    
    def intersection_merge(self) -> Dict[str, NormalizedEntity]:
        """
        Strategy 2: Intersection merge - only keep entities that exist in both databases.
        """
        logger.info("Performing intersection merge...")
        primekg_ids = set(self.primekg_data.keys())
        semmed_ids = set(self.semmed_data.keys())
        common_ids = primekg_ids.intersection(semmed_ids)
        
        combined = {}
        for entity_id in common_ids:
            # Use PrimeKG as base and add SemMed information
            entity = self.create_entity_from_normalized(self.primekg_data[entity_id], "PrimeKG")
            entity.source_databases.add("SemMed")
            
            # Merge types from both sources
            semmed_types = self.semmed_data[entity_id]["type"]
            entity.types = list(set(entity.types + semmed_types))
            
            # Merge equivalent identifiers
            existing_equiv = {eq["identifier"] for eq in entity.equivalent_identifiers}
            for eq in self.semmed_data[entity_id]["equivalent_identifiers"]:
                if eq["identifier"] not in existing_equiv:
                    entity.equivalent_identifiers.append(eq)
            
            combined[entity_id] = entity
        
        logger.info(f"Intersection merge resulted in {len(combined)} entities")
        return combined
    
    def confidence_based_merge(self, primekg_weight: float = 0.6) -> Dict[str, NormalizedEntity]:
        """
        Strategy 3: Confidence-based merge - use confidence scores to resolve conflicts.
        """
        logger.info("Performing confidence-based merge...")
        combined = {}
        
        # Calculate confidence scores based on source database and entity properties
        for entity_id, entity_data in self.primekg_data.items():
            entity = self.create_entity_from_normalized(entity_data, "PrimeKG")
            entity.confidence_score = self._calculate_confidence(entity, "PrimeKG", primekg_weight)
            combined[entity_id] = entity
        
        for entity_id, entity_data in self.semmed_data.items():
            if entity_id in combined:
                # Conflict resolution based on confidence
                existing = combined[entity_id]
                new_entity = self.create_entity_from_normalized(entity_data, "SemMed")
                new_entity.confidence_score = self._calculate_confidence(new_entity, "SemMed", 1 - primekg_weight)
                
                if new_entity.confidence_score > existing.confidence_score:
                    # Replace with higher confidence entity
                    new_entity.source_databases = existing.source_databases.union({"SemMed"})
                    combined[entity_id] = new_entity
                else:
                    # Merge information into existing entity
                    existing.source_databases.add("SemMed")
                    existing.types = list(set(existing.types + new_entity.types))
                    existing_equiv = {eq["identifier"] for eq in existing.equivalent_identifiers}
                    for eq in new_entity.equivalent_identifiers:
                        if eq["identifier"] not in existing_equiv:
                            existing.equivalent_identifiers.append(eq)
            else:
                new_entity = self.create_entity_from_normalized(entity_data, "SemMed")
                new_entity.confidence_score = self._calculate_confidence(new_entity, "SemMed", 1 - primekg_weight)
                combined[entity_id] = new_entity
        
        logger.info(f"Confidence-based merge resulted in {len(combined)} entities")
        return combined
    
    def _calculate_confidence(self, entity: NormalizedEntity, source_db: str, weight: float) -> float:
        """Calculate confidence score for an entity."""
        base_confidence = weight
        
        # Boost confidence based on number of equivalent identifiers
        equiv_bonus = min(0.2, len(entity.equivalent_identifiers) * 0.05)
        
        # Boost confidence based on number of types (more specific)
        type_bonus = min(0.1, len(entity.types) * 0.01)
        
        # Boost confidence for certain source databases
        source_bonus = 0.1 if source_db == "PrimeKG" else 0.05
        
        return min(1.0, base_confidence + equiv_bonus + type_bonus + source_bonus)
    
    def type_based_merge(self) -> Dict[str, NormalizedEntity]:
        """
        Strategy 4: Type-based merge - prioritize entities based on their biolink types.
        """
        logger.info("Performing type-based merge...")
        combined = {}
        
        # Type hierarchy for prioritization
        type_hierarchy = {
            "biolink:Gene": 10,
            "biolink:Protein": 9,
            "biolink:ChemicalEntity": 8,
            "biolink:AnatomicalEntity": 7,
            "biolink:Disease": 6,
            "biolink:OrganismTaxon": 5,
            "biolink:NamedThing": 1
        }
        
        def get_type_priority(types: List[str]) -> int:
            """Get the highest priority type for an entity."""
            max_priority = 0
            for entity_type in types:
                for type_name, priority in type_hierarchy.items():
                    if entity_type.startswith(type_name):
                        max_priority = max(max_priority, priority)
            return max_priority
        
        # Add all entities with type-based prioritization
        all_entities = []
        
        for entity_id, entity_data in self.primekg_data.items():
            entity = self.create_entity_from_normalized(entity_data, "PrimeKG")
            entity.confidence_score = get_type_priority(entity.types)
            all_entities.append((entity_id, entity))
        
        for entity_id, entity_data in self.semmed_data.items():
            entity = self.create_entity_from_normalized(entity_data, "SemMed")
            entity.confidence_score = get_type_priority(entity.types)
            all_entities.append((entity_id, entity))
        
        # Group by identifier and select best entity
        entity_groups = defaultdict(list)
        for entity_id, entity in all_entities:
            entity_groups[entity_id].append(entity)
        
        for entity_id, entities in entity_groups.items():
            if len(entities) == 1:
                combined[entity_id] = entities[0]
            else:
                # Select entity with highest type priority
                best_entity = max(entities, key=lambda e: e.confidence_score)
                # Merge source databases
                for entity in entities:
                    best_entity.source_databases.update(entity.source_databases)
                combined[entity_id] = best_entity
        
        logger.info(f"Type-based merge resulted in {len(combined)} entities")
        return combined
    
    def save_combined_data(self, combined_data: Dict[str, NormalizedEntity], output_path: str) -> None:
        """Save combined data to JSON file."""
        logger.info(f"Saving combined data to {output_path}")
        
        # Convert to the original format
        output_data = {}
        for entity_id, entity in combined_data.items():
            output_data[entity_id] = entity.to_dict()
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        logger.info(f"Saved {len(output_data)} entities to {output_path}")
    
    def generate_statistics(self, combined_data: Dict[str, NormalizedEntity]) -> Dict[str, Any]:
        """Generate statistics about the combined dataset."""
        stats = {
            "total_entities": len(combined_data),
            "entities_by_source": defaultdict(int),
            "entities_by_type": defaultdict(int),
            "entities_with_multiple_sources": 0,
            "average_equivalent_identifiers": 0,
            "type_distribution": defaultdict(int)
        }
        
        total_equiv_identifiers = 0
        
        for entity in combined_data.values():
            # Source database statistics
            for source in entity.source_databases:
                stats["entities_by_source"][source] += 1
            
            if len(entity.source_databases) > 1:
                stats["entities_with_multiple_sources"] += 1
            
            # Type statistics
            for entity_type in entity.types:
                stats["entities_by_type"][entity_type] += 1
                # Get base type for distribution
                base_type = entity_type.split(":")[1] if ":" in entity_type else entity_type
                stats["type_distribution"][base_type] += 1
            
            total_equiv_identifiers += len(entity.equivalent_identifiers)
        
        stats["average_equivalent_identifiers"] = total_equiv_identifiers / len(combined_data) if combined_data else 0
        
        # Convert defaultdicts to regular dicts for JSON serialization
        stats["entities_by_source"] = dict(stats["entities_by_source"])
        stats["entities_by_type"] = dict(stats["entities_by_type"])
        stats["type_distribution"] = dict(stats["type_distribution"])
        
        return stats
    
    def create_network_graph(self, combined_data: Dict[str, NormalizedEntity]) -> nx.Graph:
        """Create a network graph from the combined data."""
        G = nx.Graph()
        
        for entity_id, entity in combined_data.items():
            G.add_node(entity_id, 
                      label=entity.label,
                      types=entity.types,
                      sources=list(entity.source_databases),
                      confidence=entity.confidence_score)
            
            # Add edges based on equivalent identifiers
            for equiv in entity.equivalent_identifiers:
                equiv_id = equiv["identifier"]
                if equiv_id != entity_id and equiv_id in combined_data:
                    G.add_edge(entity_id, equiv_id, relationship="equivalent")
        
        return G

def main():
    """Main function to run the database combination."""
    parser = argparse.ArgumentParser(description="Combine normalized PrimeKG and SemMed databases")
    parser.add_argument("--primekg", required=True, help="Path to PrimeKG normalized JSON file")
    parser.add_argument("--semmed", required=True, help="Path to SemMed normalized JSON file")
    parser.add_argument("--output", required=True, help="Output path for combined data")
    parser.add_argument("--strategy", choices=["union", "intersection", "confidence", "type"], 
                       default="union", help="Merging strategy to use")
    parser.add_argument("--primekg-weight", type=float, default=0.6, 
                       help="Weight for PrimeKG in confidence-based merge")
    parser.add_argument("--stats", action="store_true", help="Generate and save statistics")
    parser.add_argument("--graph", action="store_true", help="Generate network graph")
    
    args = parser.parse_args()
    
    # Initialize combiner
    combiner = DatabaseCombiner(args.primekg, args.semmed)
    
    # Load databases
    combiner.load_databases()
    
    # Perform merge based on strategy
    if args.strategy == "union":
        combined_data = combiner.union_merge()
    elif args.strategy == "intersection":
        combined_data = combiner.intersection_merge()
    elif args.strategy == "confidence":
        combined_data = combiner.confidence_based_merge(args.primekg_weight)
    elif args.strategy == "type":
        combined_data = combiner.type_based_merge()
    
    # Save combined data
    combiner.save_combined_data(combined_data, args.output)
    
    # Generate statistics if requested
    if args.stats:
        stats = combiner.generate_statistics(combined_data)
        stats_path = args.output.replace('.json', '_stats.json')
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Statistics saved to {stats_path}")
        
        # Print summary
        print(f"\n=== COMBINATION STATISTICS ===")
        print(f"Total entities: {stats['total_entities']}")
        print(f"Entities with multiple sources: {stats['entities_with_multiple_sources']}")
        print(f"Average equivalent identifiers per entity: {stats['average_equivalent_identifiers']:.2f}")
        print(f"\nEntities by source:")
        for source, count in stats['entities_by_source'].items():
            print(f"  {source}: {count}")
        print(f"\nTop entity types:")
        sorted_types = sorted(stats['type_distribution'].items(), key=lambda x: x[1], reverse=True)
        for entity_type, count in sorted_types[:10]:
            print(f"  {entity_type}: {count}")
    
    # Generate network graph if requested
    if args.graph:
        G = combiner.create_network_graph(combined_data)
        graph_path = args.output.replace('.json', '_graph.gml')
        nx.write_gml(G, graph_path)
        logger.info(f"Network graph saved to {graph_path}")
        
        # Print graph statistics
        print(f"\n=== NETWORK GRAPH STATISTICS ===")
        print(f"Nodes: {G.number_of_nodes()}")
        print(f"Edges: {G.number_of_edges()}")
        print(f"Connected components: {nx.number_connected_components(G)}")
        if G.number_of_nodes() > 0:
            print(f"Average clustering coefficient: {nx.average_clustering(G):.4f}")

if __name__ == "__main__":
    main()
