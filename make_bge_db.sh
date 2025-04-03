#!/usr/bin/env bash

# activate the env first:
# conda activate target-list
# export PYTHONPATH=./src/

# create the directory if it does not exist
if [ ! -d "data/sqlite/ignored" ]; then
  mkdir -p data/sqlite/ignored
fi

DB=data/sqlite/ignored/bge-barcode-metadata.db
GAPLIST=data/input_files/Gap_list_all_updated.csv
SYNONYMS=data/input_files/all_specs_and_syn.csv
TAXONOMY=data/input_files/taxonomy.tsv
VOUCHER=data/input_files/voucher.tsv
LAB=data/input_files/lab.tsv
ADDENDUM=data/input_files/addendum.csv
BOLD_ZIP='data/input_files/BOLD_Public.*.zip'
BOLD_TSV='data/input_files/BOLD_Public.*.tsv'

# remove the files if they exist
rm -f $DB
rm -f $GAPLIST
rm -f $SYNONYMS
rm -f $TAXONOMY
rm -f $VOUCHER
rm -f $LAB
rm -f $ADDENDUM

# create the database
python src/util/arise_create_barcode_metadata_db.py -outfile $DB

# fetch the target list data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/Gap_list_all_updated.csv -o $GAPLIST

# load the target list data
python src/util/bge_load_targetlist.py --db $DB --input $GAPLIST --delimiter ";" --log-level INFO

# fetch the synonyms data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/all_specs_and_syn.csv -o $SYNONYMS

# load the synonyms data
python src/util/bge_load_synonyms.py --db $DB --input $SYNONYMS --delimiter ";" --log-level INFO

# fetch the BOLD taxonomy data (for Species, Identifier) using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/taxonomy.tsv
# Mapping:
# `Species` -> lookup in nsr_synonym table to fetch `species_id`
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/taxonomy.tsv -o $TAXONOMY

# fetch the BOLD voucher data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/voucher.tsv
# Mapping:
# `Sample ID` -> `sampleid`
# `Museum ID` -> `catalognum` (if empty, use `Field ID`)
# `Institution Storing` -> `institution_storing`
# set locality to BGE
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/voucher.tsv -o $VOUCHER

# fetch the BOLD lab data using curl from:
# https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/lab.tsv
# Mapping:
# `Sample ID` -> `sampleid`
# `Museum ID` -> `catalognum` (if empty, use `Field ID`)
# `Institution Storing` -> `institution_storing`
curl -L https://raw.githubusercontent.com/bge-barcoding/gaplist-data/refs/heads/main/data/bold/lab.tsv -o $LAB

# load the voucher data
python src/util/bge_load_specimens.py --db $DB --voucher $VOUCHER --taxonomy $TAXONOMY --lab $LAB --out-file $ADDENDUM --log-level WARNING

# download the BOLD data
python src/arise/barcode/metadata/util/bge_fetch_bold.py
tar -xzf $BOLD_ZIP -C data/input_files

# load the BOLD data
python src/util/bge_load_bold.py --db $DB --bold-tsv $BOLD_TSV  --log-level INFO
