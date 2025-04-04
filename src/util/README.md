# BGE Import Scripts

This directory contains scripts used to populate the BGE Barcode Metadata Database. These scripts form a data pipeline to ingest taxonomic data, specimen records, and DNA barcodes from various sources into a unified database structure.

## Overview

The BGE system tracks DNA barcodes produced by the BGE project under iBOL Europe. These scripts import data from:

1. Target species lists with taxonomy
2. Taxonomic synonyms
3. BGE specimen data from BOLD workbench projects
4. Public barcode data from BOLD

## Scripts

| Script | Description |
|--------|-------------|
| [`bge_create_barcode_metadata_db.py`](./bge_create_barcode_metadata_db.py) | Creates an empty SQLite database with the required schema |
| [`bge_load_targetlist.py`](./bge_load_targetlist.py) | Imports taxonomic data and builds the hierarchical taxonomic tree |
| [`bge_load_synonyms.py`](./bge_load_synonyms.py) | Imports synonym data for species names |
| [`bge_load_specimens.py`](./bge_load_specimens.py) | Imports specimen data from BGE BOLD workbench projects |
| [`bge_fetch_bold.py`](./bge_fetch_bold.py) | Downloads the latest BOLD public data package |
| [`bge_load_bold.py`](./bge_load_bold.py) | Imports public barcode data from BOLD into the database |
| [`bge_export_appview.py`](./bge_export_appview.py) | Extracts species statistics into a single-pane view TSV file |
| [`bge_update_appview.py`](./bge_update_appview.py) | Uploads species statistics to a SQL Server database |

## Usage

### Setup and Database Creation

```bash
# Create a new empty database
python bge_create_barcode_metadata_db.py -outfile bge-barcode-metadata.db
```

### Data Import Pipeline

The scripts should be run in the following order:

1. **Create Database**
   ```bash
   python bge_create_barcode_metadata_db.py -outfile bge-barcode-metadata.db
   ```

2. **Import Target List (Taxonomy)**
   ```bash
   python bge_load_targetlist.py --db bge-barcode-metadata.db --input Gap_list_all_updated.csv --delimiter ";"
   ```

3. **Import Synonyms**
   ```bash
   python bge_load_synonyms.py --db bge-barcode-metadata.db --input all_specs_and_syn.csv --delimiter ";"
   ```

4. **Import BGE Specimens and Barcodes**
   ```bash
   python bge_load_specimens.py --db bge-barcode-metadata.db --voucher voucher.tsv --taxonomy taxonomy.tsv --lab lab.tsv --out-file addendum.csv
   ```

5. **Download and Import BOLD Data**
   ```bash
   python bge_fetch_bold.py
   python bge_load_bold.py --db bge-barcode-metadata.db --bold-tsv BOLD_Public.*.tsv
   ```

6. **Export Statistics for Application View**
   ```bash
   python bge_export_appview.py --db bge-barcode-metadata.db --output species_stats.tsv
   ```

7. **Upload Statistics to SQL Server (Optional)**
   ```bash
   python bge_update_appview.py --password <your_password> --input species_stats.tsv
   ```

### Automated Pipeline

A shell script [`make_bge_db.sh`](./make_bge_db.sh) is provided to automate the entire process:

```bash
#!/usr/bin/env bash
# Activate your environment first
# conda activate target-list
# export PYTHONPATH=./src/

# Run the script
./make_bge_db.sh
```

## Script Details

### bge_create_barcode_metadata_db.py
Creates an empty SQLite database with tables defined in the ORM classes.

### bge_load_targetlist.py
Imports taxonomic data from a CSV file and builds a hierarchical tree with nested set indexing. This script:
- Parses a CSV containing taxonomic information
- Creates NsrSpecies records
- Builds a complete taxonomic hierarchy in NsrNode from kingdom to species
- Computes left and right indexes for the nested set model

### bge_load_synonyms.py
Imports synonym data from a semicolon-separated file containing canonical names and their synonyms.
- Handles encoding detection and mixed encoding issues
- Cleans taxonomic names
- Generates variants for subgenus notation
- Links synonyms to canonical species names

### bge_load_specimens.py
Imports specimen data from BGE BOLD workbench exports.
- Reads three TSV files (voucher, taxonomy, and lab)
- Joins the data to create specimen records
- Creates barcode records for specimens with sequence data
- Outputs unmapped species to an addendum file

### bge_fetch_bold.py
Downloads the latest BOLD public data package.
- Retrieves the latest datapackage ID from BOLD
- Checks if the package already exists locally
- Downloads the package if needed

### bge_load_bold.py
Imports BOLD public data into the database.
- Processes large files in chunks to manage memory usage
- Filters for COI-5P records
- Links barcodes to species via the taxonomy
- Creates specimen records with a locality of "BOLD"

### bge_export_appview.py
Extracts species statistics into a single-pane view TSV file.
- Computes counts of barcodes and specimens for each species
- Includes subspecies in species counts
- Formats data for application use
- Uses batch processing for better performance

### bge_update_appview.py
Uploads species statistics to a SQL Server database.
- Verifies database schema
- Supports replacing existing data
- Processes data in batches for efficiency

## Common Issues and Troubleshooting

### Memory Management
- The BOLD import can be memory-intensive. Use the `--chunk-size` parameter in `bge_load_bold.py` to adjust the number of rows processed at once.
- For SQLite performance, the scripts configure pragmas like journal mode, cache size, and synchronous mode.

### Encoding Issues
- The `bge_load_synonyms.py` script includes robust handling of mixed encodings in files.
- If you encounter encoding errors, try specifying an encoding with the `--encoding` parameter.

### Missing Species
- Species not found in the taxonomy are logged and, for `bge_load_specimens.py`, saved to an addendum file.
- This addendum can be appended to the target list in future imports.

## Dependencies

These scripts depend on:
- Python 3.7+
- SQLAlchemy
- pandas
- BeautifulSoup4
- Requests
- pyodbc (for SQL Server integration)
- chardet (for encoding detection)