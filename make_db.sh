# activate the env first:
# source venv/bin/activate

export PYTHONPATH=./src/

rm data/sqlite/arise-barcode-metadata.db
python src/arise/barcode/metadata/util/arise_create_barcode_metadata_db.py -outfile data/sqlite/arise-barcode-metadata.db &&
python src/arise/barcode/metadata/util/arise_load_backbone.py -db data/sqlite/arise-barcode-metadata.db &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/bold_data_animalia_BE_DE_NL.tsv.gz -kingdom animalia &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/bold_data_plantae_BE_DE_NL.tsv.gz -kingdom plantae &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/bold_data_fungi_BE_DE_NL.tsv.gz -kingdom fungi &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/BCP-Zoo_ARISE_Rutger_20220524_2.txt -kingdom animalia -marker COI-5P &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/BCP-Bot_ARISE_Rutger_ITS1_20220712.txt -kingdom plantae -marker ITS1 &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/BCP-Bot_ARISE_Rutger_ITS_20220712.txt -kingdom plantae -marker ITS &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/BCP-Bot_ARISE_Rutger_matK_20220712.txt -kingdom plantae -marker matK &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/arise-barcode-metadata.db -tsv data/input_files/BCP-Bot_ARISE_Rutger_trnL_20220712.txt -kingdom plantae -marker trnL
export DBFILE=arise-barcode-metadata.db &&
docker-compose -f docker-compose.yml run dbtree
