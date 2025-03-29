#!/usr/bin/env bash

# activate the env first:
# conda activate target-list
# export PYTHONPATH=./src/

# create the directory if it does not exist
if [ ! -d "data/sqlite/ignored" ]; then
  mkdir -p data/sqlite/ignored
fi

# remove the file if it exists
rm -f data/sqlite/ignored/arise-barcode-metadata.db

# create the database
python src/arise/barcode/metadata/util/arise_create_barcode_metadata_db.py \
  -outfile data/sqlite/ignored/arise-barcode-metadata.db

# fetch the backbone data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv \
  -o data/input_files/Gap_list_all_updated.csv

# load the backbone data
python src/arise/barcode/metadata/util/bge_load_targetlist.py \
  --db data/sqlite/ignored/arise-barcode-metadata.db \
  --input data/input_files/Gap_list_all_updated.csv \
  --delimiter ";" \
  --log-level DEBUG

# fetch the synonyms data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv \
  -o data/input_files/all_specs_and_syn.csv