# Data folder

All data that has been collected and reformatted is found in this folder.

## Directory layout:

- [sqlite](sqlite) - database file(s)
- [input_files](input_files) - all data fetch via APIs / received / downloaded manually

	The input files were moved to Gdrive:

	https://drive.google.com/drive/folders/1K8lnQkQfhRAu4X45hgMoOaiLTM3lZt9O?usp=sharing

	List of files on Gdrive:

	- BOLD/*.tsv.gz - this is a recent export from BOLD's "give me everything" web service endpoint. 2 sets of files are available; filtered to include all countries or only european countries. To re download BOLD data use the [fetch_bold_data.py](/src/arise/barcode/metadata/util/fetch_bold_data.py) script
	- KLASSE/*.txt - Gneious/Klasse export ot Naturalis barcodes for Animalia and Plants
	- Fungi barcodes Jorinde.xlsx - Fungi barcode data from Jorinde. Note: only ITS barcodes in that file are inserted in the DB
    - WI_DutchStrainsWithPublicBarcodes.xlsx - Westerdijk Fungi barcode data from Duong Vu (part of ARISE data)
    - Databases:  
      - arise-barcode-metadata_6c.db - Belgium, Denmark, France, Germany, Netherlands and UK barcodes (**DEPRECATED, do not contain the latest DB changes**)
      - arise-barcode-metadata_eu.db - with 50+ European countries
      - arise-barcode-metadata.db - with all countries
      > Other inputs files are systematically inserted in all the .db files

