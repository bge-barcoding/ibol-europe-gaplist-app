#!/usr/bin/env bash

# activate the env first:
# conda activate target-list
# export PYTHONPATH=./src/

# create the directory if it does not exist
if [ ! -d "data/sqlite/ignored" ]; then
  mkdir -p data/sqlite/ignored
fi

# remove the files if they exist
rm -f data/sqlite/ignored/arise-barcode-metadata.db
rm -f data/input_files/Gap_list_all_updated.csv
rm -f data/input_files/all_specs_and_syn.csv
rm -f data/input_files/taxonomy.tsv
rm -f data/input_files/voucher.tsv

# create the database
python src/arise/barcode/metadata/util/arise_create_barcode_metadata_db.py \
  -outfile data/sqlite/ignored/arise-barcode-metadata.db

# fetch the target list data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv \
  -o data/input_files/Gap_list_all_updated.csv

# load the target list data
python src/arise/barcode/metadata/util/bge_load_targetlist.py \
  --db data/sqlite/ignored/arise-barcode-metadata.db \
  --input data/input_files/Gap_list_all_updated.csv \
  --delimiter ";" \
  --log-level INFO

# fetch the synonyms data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv \
  -o data/input_files/all_specs_and_syn.csv

# load the synonyms data
python src/arise/barcode/metadata/util/bge_load_synonyms.py \
  --db data/sqlite/ignored/arise-barcode-metadata.db \
  --input data/input_files/all_specs_and_syn.csv \
  --delimiter ";" \
  --log-level INFO

# fetch the BOLD taxonomy data (for Species, Identifier) using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/taxonomy.tsv
# Mapping:
# `Species` -> lookup in nsr_synonym table to fetch `species_id`
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/taxonomy.tsv \
  -o data/input_files/taxonomy.tsv

# fetch the BOLD voucher data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/voucher.tsv
# Mapping:
# `Sample ID` -> `sampleid`
# `Museum ID` -> `catalognum` (if empty, use `Field ID`)
# `Institution Storing` -> `institution_storing`
# set locality to BGE
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/voucher.tsv \
  -o data/input_files/voucher.tsv

# fetch the BOLD lab data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/lab.tsv
# Mapping:
# `Sample ID` -> `sampleid`
# `Museum ID` -> `catalognum` (if empty, use `Field ID`)
# `Institution Storing` -> `institution_storing`
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/lab.tsv \
  -o data/input_files/lab.tsv

# load the voucher data
python src/arise/barcode/metadata/util/bge_load_specimens.py \
  --db data/sqlite/ignored/arise-barcode-metadata.db \
  --voucher data/input_files/voucher.tsv \
  --taxonomy data/input_files/taxonomy.tsv \
  --lab data/input_files/lab.tsv \
  --out-file ./addendum.csv \
  --log-level WARNING

# download the BOLD data
python src/arise/barcode/metadata/util/bge_fetch_bold.py
tar -xzf data/input_files/BOLD_Public.*.zip -C data/input_files

# load the BOLD data
python src/arise/barcode/metadata/util/bge_load_bold.py \
  --db data/sqlite/ignored/arise-barcode-metadata.db \
  --bold-tsv data/input_files/BOLD_Public.*.tsv \
  --log-level INFO