# Data folder

All data that has been collected and reformatted is found in this folder.

## Directory layout:

- [sqlite](sqlite) - database file(s)
- [input_files](input_files) - all data received or downloaded manually

	The input files were moved to Gdrive:

	https://drive.google.com/drive/folders/1K8lnQkQfhRAu4X45hgMoOaiLTM3lZt9O?usp=sharing

	List of files:

	- BOLD/*.tsv.gz - this is a recent export from BOLD's "give me everything" web service endpoint. 2 sets of files are available; filtered to  include all countries or  only european countries. To re download BOLD data use the [fetch_bold_data.py](/src/arise/barcode/metadata/util/fetch_bold_data.py) script

	- KLASSE/*.txt - Gneious/Klasse export ot Naturalis barcodes for Animalia and Plants
	- Fungi barcdes Jorinde.xlsx - Fungi barcode tada from Jorinde. Note: only ITS barcodes in that file are inserted in the DB

