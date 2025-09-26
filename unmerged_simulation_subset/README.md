# 🧬 Biomedical Knowledge Graph Normalization

This directory contains tools for normalizing and combining four biomedical knowledge graphs: **BioKDE**, **iKraph**, **PrimeKG**, and **SemMedDB**.

## 📊 **Datasets Overview**

| Dataset | Description | Nodes | Edges | Source |
|---------|-------------|-------|-------|--------|
| **BioKDE** | Deep learning-powered search engine and knowledge graph | 1,001 | 2,000 | Allen Brain Atlas |
| **iKraph** | Biomedical knowledge graph with gene relationships | 1,000 | 2,000 | NCBI Gene |
| **PrimeKG** | Multimodal knowledge graph for precision medicine | 1,000 | 2,000 | Mixed sources |
| **SemMedDB** | Semantic database from biomedical literature | 1,001 | 2,000 | UMLS |

## 🚀 **Quick Start**

### **1. Normalize All Datasets**
```bash
# Complete normalization pipeline (CSV → JSONL → Enhanced JSON)
python normalize_datasets.py --all
```

### **2. Normalize Specific Dataset**
```bash
# Process only BioKDE
python normalize_datasets.py --dataset biokde

# Process only PrimeKG
python normalize_datasets.py --dataset primekg
```

### **3. Step-by-Step Processing**
```bash
# Step 1: Convert CSV to JSONL compendia
python normalize_datasets.py --all --step csv-to-jsonl

# Step 2: Convert JSONL to enhanced normalized JSON
python normalize_datasets.py --all --step jsonl-to-json
```

### **4. Combine All Datasets**
```bash
# Merge all normalized datasets with duplicate handling
python combine_all_four_datasets.py \
  --biokde normalized/biokde/biokde_normalized_sample.json \
  --ikraph normalized/ikraph/ikraph_normalized_sample.json \
  --primekg normalized/primekg/primekg_normalized_sample.json \
  --semmeddb normalized/semmeddb/semmeddb_normalized_sample.json \
  --output combined_four_datasets_union.json \
  --strategy union \
  --stats
```

## 📁 **Output Structure**

```
unmerged_simulation_subset/
├── compendia/                          # JSONL compendia files
│   ├── biokde_compendia_sample.jsonl
│   ├── ikraph_compendia_sample.jsonl
│   ├── primekg_compendia_sample.jsonl
│   └── semmeddb_compendia_sample.jsonl
├── normalized/                         # Final normalized JSON files
│   ├── biokde/biokde_normalized_sample.json
│   ├── ikraph/ikraph_normalized_sample.json
│   ├── primekg/primekg_normalized_sample.json
│   └── semmeddb/semmeddb_normalized_sample.json
└── combined_four_datasets_union.json   # Merged graph
```

## 🔧 **Scripts**

### **`normalize_datasets.py`** - Main Normalization Script
- **Purpose**: Converts CSV data to normalized JSON with full Biolink type hierarchies
- **Process**: CSV → JSONL compendia → Enhanced normalized JSON
- **Flags**:
  - `--all`: Process all datasets
  - `--dataset {biokde,ikraph,primekg,semmeddb}`: Process specific dataset
  - `--step {all,csv-to-jsonl,jsonl-to-json}`: Run specific step

### **`jsonl_to_normalized_json.py`** - JSONL to JSON Converter
- **Purpose**: Converts JSONL compendia to enhanced normalized JSON
- **Features**: Uses existing normalized data as lookup table, generates full Biolink type hierarchies

### **`combine_all_four_datasets.py`** - Graph Combination Script
- **Purpose**: Merges all normalized datasets with duplicate handling
- **Strategies**: Union merge (recommended), intersection merge
- **Features**: Statistics generation, source tracking, type hierarchy preservation

## 📈 **Sample Results**

### **Normalization Statistics**
| Dataset | Input Nodes | Mapped CURIEs | Success Rate | Output Entities |
|---------|-------------|---------------|--------------|-----------------|
| **BioKDE** | 1,001 | 915 | 91.4% | 915 |
| **iKraph** | 1,000 | 506 | 50.6% | 506 |
| **PrimeKG** | 1,000 | 1,000 | 100.0% | 1,000 |
| **SemMedDB** | 1,001 | 996 | 99.5% | 996 |
| **Total** | **4,002** | **3,417** | **85.4%** | **3,417** |

### **Combined Graph Statistics**
- **Total Entities**: 3,881 (after duplicate merging)
- **Entities with Multiple Sources**: 37 (0.95%)
- **Unique Entities**: 3,844 (99.05%)
- **Average Equivalent Identifiers**: 1.00 per entity

### **Entity Type Distribution**
- **NamedThing**: 5,444 (100% - base type)
- **BiologicalEntity**: 2,164 (39.7%)
- **MolecularEntity**: 1,998 (36.7%)
- **Gene**: 1,974 (36.2%)
- **GenomicEntity**: 1,974 (36.2%)

## 🎯 **Key Features**

### **✅ Duplicate Handling**
- **Union merge**: Combines all entities, merges overlapping information
- **Type hierarchy merging**: Removes duplicate types, preserves all information
- **Source tracking**: Maintains provenance for all entities

### **✅ Enhanced Type Hierarchies**
- **Full Biolink compliance**: Complete type hierarchies for all entities
- **Semantic enrichment**: Adds parent types based on base entity type
- **Consistent formatting**: Standardized type representation

### **✅ Flexible Processing**
- **Individual or batch processing**: Process one dataset or all at once
- **Step-by-step control**: Run specific parts of the pipeline
- **Error handling**: Graceful failure with detailed error messages

## 🔍 **Technical Details**

### **CURIE Mapping**
- **BioKDE**: Allen Brain Atlas identifiers → `ABA:*`
- **iKraph**: NCBI Gene identifiers → `NCBI:*`
- **PrimeKG**: Numeric IDs → `NCBIGene:*`, `MONDO:*`, `CHEBI:*`
- **SemMedDB**: UMLS CUIDs → `UMLS:*`

### **Type Hierarchy Generation**
- **Known entities**: Uses existing normalized data as lookup
- **New entities**: Generates realistic type hierarchies based on base type
- **Biolink compliance**: Ensures all types follow Biolink model standards

## 📚 **References**

- **BioKDE**: [BioKDE: a Deep Learning-Powered Search Engine and Knowledge Graph](https://www.semanticscholar.org/paper/BioKDE%3A-a-Deep-Learning-Powered-Search-Engine-and-Chung-Zhou/99f50c3adba60aa0cbb64ca7906ef0320615d3d2)
- **iKraph**: [iKraph: A Knowledge Graph for Biomedical Literature Analysis](https://www.biorxiv.org/content/10.1101/2023.10.13.562216v3)
- **PrimeKG**: [PrimeKG: A Prime Number Knowledge Graph](https://arxiv.org/abs/2203.12533)
- **SemMedDB**: [SemMedDB: A PubMed-scale repository of biomedical semantic predications](https://academic.oup.com/bioinformatics/article/28/23/3158/232889)