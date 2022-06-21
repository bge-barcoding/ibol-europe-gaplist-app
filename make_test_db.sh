# activate the env first:
# source venv/bin/activate

export PYTHONPATH=./src/

rm data/sqlite/test.db
python src/arise/barcode/metadata/util/arise_create_barcode_metadata_db.py -outfile data/sqlite/test.db &&
python src/arise/barcode/metadata/util/arise_load_backbone.py -db data/sqlite/test.db -testdb &&
python src/arise/barcode/metadata/util/arise_load_bold.py -db data/sqlite/test.db -tsv data/input_files/bold_data.tsv.gz &&
python src/arise/barcode/metadata/util/arise_load_klasse.py -db data/sqlite/test.db -tsv data/input_files/BCP-Zoo_ARISE_Rutger_20220524_2.txt &&
export DBFILE=test.db &&
docker-compose -f docker-compose.yml run dbtree
