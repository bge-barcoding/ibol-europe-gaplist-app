# Data folder

All data that has been collected and reformatted is found in this folder.

## Directory layout:

- [sqlite](sqlite) - database file(s)
- [input_files](input_files) - all data fetch via APIs / received / downloaded manually

The input files were moved to Gdrive:

https://drive.google.com/drive/folders/1K8lnQkQfhRAu4X45hgMoOaiLTM3lZt9O?usp=sharing

List of files on Gdrive:

- BOLD/ - this is a recent export from BOLD's datapackage [2023 - 02 - 03]
    - BOLD_Public...tar.gz - data package file Download from https://www.boldsystems.org/index.php/datapackages
    - BOLD_<date>_<kingdom>.tsv.gz - the above file has been parsed and split into 3 files, one per kingdom of interest for the target list. These are the files ingested in the current target list
    - bold_<region>_countries.txt - list of countries used in BOLD "country" col. These files are not used anymore but could help to generate subset versions of the target list.
- KLASSE/*.txt - Geneious/Klasse export ot Naturalis barcodes for Animalia and Plants [Summer 2022]
- Fungi barcodes Jorinde.xlsx - Fungi barcode data from Jorinde. Note: only ITS barcodes in that file are inserted in the DB [August 2022]
- WI_DutchStrainsWithPublicBarcodes.xlsx - Westerdijk Fungi barcode data from Duong Vu (part of ARISE data) [September 2022]
- Databases:
    - arise-barcode-metadata.db - contains all data above. Without filter on specimen sampling location.


