# activate the env first:
# source venv/bin/activate (or use the conda env)
# export PYTHONPATH=./src/

rm -f data/sqlite/ignored/arise-barcode-metadata.db
python src/arise/barcode/metadata/util/arise_create_barcode_metadata_db.py -outfile data/sqlite/ignored/arise-barcode-metadata.db &&
python src/arise/barcode/metadata/util/arise_load_nsr_backbone.py -db data/sqlite/ignored/arise-barcode-metadata.db &&

# BOLD GLOBAL DATA
echo "Insert BOLD data"
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/BOLD/BOLD_06-Jan-2024_Animalia.tsv.gz -kingdom animalia &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/BOLD/BOLD_06-Jan-2024_Plantae.tsv.gz -kingdom plantae &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/BOLD/BOLD_06-Jan-2024_Fungi.tsv.gz -kingdom fungi &&

# NATURALIS DATA
echo "Insert Klasse data"
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/KLASSE/BCP-Zoo_ARISE_Rutger_20220524_2.txt -kingdom animalia -marker COI-5P &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/KLASSE/BCP-Bot_ARISE_Rutger_ITS1_20220712.txt -kingdom plantae -marker ITS1 &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/KLASSE/BCP-Bot_ARISE_Rutger_ITS_20220712.txt -kingdom plantae -marker ITS &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/KLASSE/BCP-Bot_ARISE_Rutger_matK_20220712.txt -kingdom plantae -marker matK &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/KLASSE/BCP-Bot_ARISE_Rutger_trnL_20220712.txt -kingdom plantae -marker trnL &&
echo "Insert Jorinde data"
python src/arise/barcode/metadata/util/arise_load_Jorinde_excel.py -db data/sqlite/ignored/arise-barcode-metadata.db -marker ITS -excel data/input_files/Fungi\ barcodes\ Jorinde.xlsx -kingdom fungi &&

# ARISE - WI & Naturalis
echo "Insert Naturalis data"
python src/arise/barcode/metadata/util/arise_load_naturalis.py -db data/sqlite/ignored/arise-barcode-metadata.db -tsv data/input_files/ARISE_barcodes_09_01_24.tsv &&
python src/arise/barcode/metadata/util/arise_load_WFBI_excel.py -db data/sqlite/ignored/arise-barcode-metadata.db -excel data/input_files/WI_DutchStrainsWithPublicBarcodes.xlsx &&

# build DB tree
export DBFILE=ignored/arise-barcode-metadata.db &&
docker compose -f docker-compose.yml run dbtree

echo "Done"