# Using the ARISE barcode metadata DB

## 1. Loading data 

This section describes how to populate the DB with data from various sources. The steps are as follows:

1. Initializing a new database
2. Loading the NSR taxonomic topology and then NSR synonyms
3. Loading barcode data from various sources  
    3.1 Fetching BOLD metadata  
    3.2 Loading the Geneious metadata table  
    3.2 Inserting Jorinde Fungi data  
    3.2 Inserting Westerdijk data
4. Indexing the topology for faster queries
5. Start and use jupyter lab
6. Generate the HTML target list table

### 1. Initializing a new database

A new, empty SQLite database file that implements the database schema as implied by the object-relational mapping
in arise.barcode.metadata.orm.* is generated as follows:

    $ arise_create_barcode_metadata_db.py -outfile arise-barcode-metadata.db

### 2. Loading the backbone topology and synonyms

This operation will load the accepted species names from the NSR into the table nsr_species and the higher taxonomy
in the node table, whose structure is based on [Vos, 2019](https://doi.org/10.1111/2041-210X.13337). This requires an 
empty database file (see 1.1). The contents are fetched from the NSR endpoint for DwC data dumps at: 
http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr - use the script as follows:

    $ arise_load_backbone.py -db <sqlite.db> -endpoint <url>

The terms between pointed brackets are merely placeholders here; the script uses sensible defaults, i.e. the actual
endpoint location and the name of the default output from 1.1

>Notes: 
> - the `-endpoint` argument is actually not used if a file named 'Taxa.txt' is present in
> the current working directory. This local file will be used instead.
> - the database file is updated in place


### 3 Loading Batcoding data
### 3.1 Loading BOLD metadata

In this step we load the output from BOLD's _Full Data Retrieval (Specimen + Sequence)_ web service. It is however better to
specify a cached copy, the format must be TSV.GZ. The operation is as follows:

```commandline
arise_load_bold.py -tsv <file.tsv>
```

BOLD data currently inserted in the database that is used on the target list interface
is filtered on all countries, the files are made available 
[here](https://drive.google.com/drive/folders/1XJpYYg-nF6bs48fvbBAeu3vaoZEOLxHJ) 
for the Animalia, Plantae and Fungi kingdoms.

#### Using data per kingdom

Some clades (e.g. genus name) have homonyms, leading to ambiguous cases, preventing data to be inserted. 
Is it advise to insert BOLD data using the per-kingdom-files in combination with the `-kingdom` argument 
to help targeting the correct taxon:

```commandline
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_eu_Animalia.tsv.gz -kingdom animalia
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_eu_Plantae.tsv.gz -kingdom plantae
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_eu_Fungi.tsv.gz -kingdom fungi
```

### 3.2 Inserting Klasse dump files

To load Naturalis barcoding data exported from Geneious/Klasse, use the arise_load_klasse.py script, example:

```commandline
python arise_load_klasse.py -db <sqlite.db> -tsv data/input_files/BCP-Bot_ARISE_Rutger_ITS1_20220712.txt -kingdom plantae -marker ITS1
```

>Notes
> * Each file contains barcodes for a give marker, so the `-marker` must be specified.
> * Always better specify the kingdom to avoid homonym errors.

### 3.3 Inserting Jorinde Fungi data

The Fungi database produced by Jorinde & collaborators for Naturalis must be inserted via the following command:

```commandline
python arise_load_Jorinde_excel.py -db <sqlite.db> -excel "data/input_files/Fungi barcodes Jorinde.xlsx" -kingdom fungi -marker ITS
```

> Note: The Excel file contain barcode information for other markers than ITS but only ITS barcode are currently
> relevant. This is why `-marker ITS` is applied

### 3.4 Inserting Westerdijk data

The Fungi database provided by Duong Vu for ARISE must be inserted via the following command:

```commandline
python arise_load_WFBI_excel.py -db <sqlite.db> -excel data/input_files/WI_DutchStrainsWithPublicBarcodes.xlsx
```

> Note: The Excel file contain a _kingdom_ column that is used to insert the data
> consequently the argument '-kingdom' is not available for that script.

### 4. Indexing the topology for faster queries

Now the tree topology should be indexed. This is easiest done using the
[Bio::Phylo::Forest::DBTree](https://metacpan.org/pod/Bio::Phylo::Forest::DBTree) package:

```
$ perl -MBio::Phylo::Forest::DBTree -e \
    'Bio::Phylo::Forest::DBTree->connect("arise-barcode-metadata.db")->get_root->_index'
```

Or run it in the container by executing the following command at the root of the project:

```
# specify the input db filename (must be located in /data/sqlite/)
export DBFILE=arise-barcode-metadata.db
docker-compose -f docker-compose.yml run dbtree
```
(docker-compose 1.xx command, try `docker compose -f ...` for [v2.XX](https://github.com/docker/compose))

### 5. Query and visualize the database content using Jupyter Lab

**DEPRECATED: may not work correctly with the recent updates**

Jupyter Lab runs in a docker container, to start it run:

```
NB_UID=`id -u` docker-compose -f docker-compose.yml up jupyter
```

and follow the instructions displayed in the terminal to access the interface.

### 6. Generate the HTML target list interface

To generate the HTML interactive table, run the following command

```
python generate_target_list_html.py -db data/sqlite/arise-barcode-metadata.db
```

The HTML file `target_list.html` located in `/html/`, can be then opened in the web browser.
The HTML table is also available online at: https://arise-biodiversity.gitlab.io/sequencing/arise-target-list/

### Helper script

To build the database in one command, run `sh make_db.sh` in the root of the project. You'll need to activate the virtual environment first.
