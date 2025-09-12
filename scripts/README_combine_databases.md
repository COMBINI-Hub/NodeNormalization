# Combining PrimeKG and SemMed Normalized Databases

This directory contains tools and scripts for combining the normalized PrimeKG and SemMed databases. The normalized data has a consistent structure that makes it possible to merge them effectively.

## Overview

After normalization, both databases follow the same JSON structure:
```json
{
  "ENTITY_ID": {
    "id": {
      "identifier": "ENTITY_ID",
      "label": "Entity Label"
    },
    "equivalent_identifiers": [
      {
        "identifier": "ENTITY_ID",
        "label": "Entity Label"
      }
    ],
    "type": [
      "biolink:EntityType",
      "biolink:ParentType"
    ]
  }
}
```

## Available Scripts

### 1. Simple Combination (`simple_combine.py`)

**Purpose**: Quick and straightforward union merge of both databases.

**Usage**:
```bash
python simple_combine.py primekg_normalized_sample.json normalized_semmed_sample.json combined_output.json
```

**Features**:
- Union merge (keeps all entities from both databases)
- Merges information for overlapping entities
- Adds source tracking
- Simple and fast

### 2. Advanced Combination (`combine_normalized_databases.py`)

**Purpose**: Comprehensive combination with multiple strategies and advanced features.

**Usage**:
```bash
# Union merge (default)
python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined_union.json --strategy union

# Intersection merge (only common entities)
python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined_intersection.json --strategy intersection

# Confidence-based merge
python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined_confidence.json --strategy confidence --primekg-weight 0.6

# Type-based merge
python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined_type.json --strategy type

# Generate statistics and network graph
python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined.json --strategy union --stats --graph
```

**Features**:
- Multiple merging strategies
- Confidence scoring
- Type-based prioritization
- Network graph generation
- Comprehensive statistics
- Quality metrics

### 3. Data Validation (`validate_combined_data.py`)

**Purpose**: Validate the quality and integrity of combined datasets.

**Usage**:
```bash
python validate_combined_data.py combined_output.json
```

**Features**:
- Data integrity checks
- Identifier consistency validation
- Duplicate detection
- Type distribution analysis
- Source overlap analysis
- Quality report generation

### 4. Interactive Exploration (`explore_combined_data.ipynb`)

**Purpose**: Jupyter notebook for interactive data exploration and visualization.

**Features**:
- Interactive plots with Plotly
- Network visualization
- Statistical analysis
- Type distribution charts
- Source overlap analysis
- Quality metrics dashboard

## Merging Strategies

### 1. Union Merge
- **Description**: Combines all entities from both databases
- **Use Case**: When you want maximum coverage
- **Overlap Handling**: Merges information for entities present in both databases
- **Result**: Largest possible dataset

### 2. Intersection Merge
- **Description**: Only keeps entities that exist in both databases
- **Use Case**: When you want high-confidence entities
- **Overlap Handling**: Merges information from both sources
- **Result**: Smallest but highest quality dataset

### 3. Confidence-Based Merge
- **Description**: Uses confidence scores to resolve conflicts
- **Use Case**: When you have preferences for certain sources
- **Overlap Handling**: Keeps the entity with higher confidence score
- **Result**: Balanced dataset with quality prioritization

### 4. Type-Based Merge
- **Description**: Prioritizes entities based on their biolink types
- **Use Case**: When certain entity types are more important
- **Overlap Handling**: Keeps the entity with higher type priority
- **Result**: Type-optimized dataset

## Data Structure After Combination

The combined data includes additional metadata:

```json
{
  "ENTITY_ID": {
    "id": {
      "identifier": "ENTITY_ID",
      "label": "Entity Label"
    },
    "equivalent_identifiers": [...],
    "type": [...],
    "source_databases": ["PrimeKG", "SemMed"],
    "confidence_score": 0.85
  }
}
```

## Quality Metrics

The validation script provides several quality metrics:

- **Total entities**: Number of entities in the combined dataset
- **Entities with multiple sources**: Entities present in both databases
- **Average types per entity**: Type richness metric
- **Average equivalent identifiers per entity**: Identifier richness metric
- **Type diversity**: Number of unique types
- **Identifier consistency**: Percentage of consistent identifiers

## Network Analysis

The combination tools can generate network graphs where:
- **Nodes**: Represent entities
- **Edges**: Represent equivalent identifier relationships
- **Node colors**: Indicate source databases
- **Node sizes**: Can represent confidence scores or type priority

## Example Workflow

1. **Basic combination**:
   ```bash
   python simple_combine.py primekg_normalized_sample.json normalized_semmed_sample.json combined_basic.json
   ```

2. **Validate the result**:
   ```bash
   python validate_combined_data.py combined_basic.json
   ```

3. **Advanced combination with statistics**:
   ```bash
   python combine_normalized_databases.py --primekg primekg_normalized_sample.json --semmed normalized_semmed_sample.json --output combined_advanced.json --strategy union --stats --graph
   ```

4. **Interactive exploration**:
   - Open `explore_combined_data.ipynb` in Jupyter
   - Run all cells to generate interactive visualizations

## Dependencies

Install required packages:
```bash
pip install pandas matplotlib seaborn networkx plotly numpy
```

## Output Files

- `combined_*.json`: Combined dataset
- `*_stats.json`: Statistics report
- `*_graph.gml`: Network graph (GML format)
- `*_validation_report.json`: Detailed validation report

## Best Practices

1. **Start with simple combination** to understand the data overlap
2. **Validate the results** to ensure data quality
3. **Choose appropriate strategy** based on your use case
4. **Use interactive exploration** to understand the data better
5. **Consider confidence scores** for production use cases

## Troubleshooting

### Common Issues

1. **Memory errors**: Use the simple combination script for large datasets
2. **Missing dependencies**: Install all required packages
3. **File not found**: Ensure correct file paths
4. **Validation errors**: Check data format consistency

### Performance Tips

1. Use intersection merge for faster processing
2. Limit network graph size for large datasets
3. Process data in chunks if memory is limited
4. Use confidence-based merge for better quality

## Future Enhancements

- Support for additional merging strategies
- Real-time validation during combination
- Advanced conflict resolution algorithms
- Integration with graph databases
- API endpoints for programmatic access
