# Using the ARISE barcode metadata DB

## 1. Loading data 

This section describes how to populate the DB with data from various sources. The steps are as follows:

1. Initializing a new database
2. Loading the NSR taxonomic topology and then NSR synonyms
3. Generate BOLD data file
4. Loading barcode data from various sources  
    4.1 Load BOLD data  
    4.2 Load Klasse dump files  
    4.3 Load Jorinde Fungi data  
    4.4 Load Westerdijk data  
    4.5 Load latest Naturalis/ARISE data  
5. Indexing the topology for faster queries
6. Query and visualize the database content using Jupyter Lab
7. Generate the HTML interface and tables

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
endpoint location and the name of the default output from 1.

Synonyms are also loaded during the process. Note that synonyms involving infra-specific epithet are ignored.

>Notes: 
> - the `-endpoint` argument is actually not used if a file named 'Taxa.txt' is present in
> the current working directory. This local file will be used instead.
> - the database file is updated in place

#### 2.1 NSR statistics

During this process, statistics are computed and stored in an
independent file named `nsr_backbone_stats_<date>.tsv`. These statistics must be inserted
to this [Google Drive document](https://docs.google.com/spreadsheets/d/1ZbOblN7XOmeet3WeOV_MB-3uBR8_rbvQ8J44duxWBJg/edit#gid=1362329747)
(first sheet - first section: NSR content, the whole column with the stats can be copy/pasted).

### 3. Generate BOLD data files

BOLD data is retrieved by downloading the latest datapackage release file
available at https://www.boldsystems.org/index.php/datapackages. This project does not use
the BOLD (Full Data Retrieval) API anymore.

While the .tsv file inside the data package archive can be directly used with the `load_bold_data.py` script,
users are invited to generate tree subset files, one per kingdom of interest (Animalia, Plantae and Fungi),
using the [generate_bold_gz_files.py](/src/arise/barcode/metadata/util/generate_bold_gz_files.py) script:

```commandline
python src/arise/barcode/metadata/util/generate_bold_gz_files.py <datapackage.tar.gz> <label>
```
with `label` being the date part of the datapackage filename downloaded (DD-MMM-YYY e.g. 24-May-2024).

The script:

- Reports any invalid line found in the datapackage file (it seems there are none, but there were many in the
data retrieved by the API)
- Indicates the number of entries saved in the output files and the number of ignored entries.

The latest downloaded and generated files are described [here](../data/README.md).

### 4. Loading Barcoding data

### 4.1 Loading BOLD data

In this step we load the tree gz.file created by step [3](#3-generate-bold-data-files). 
Each file is loaded with the following command:

```commandline
python src/arise/barcode/metadata/util/arise_load_bold.py -db <sqlite.db> -tsv <file.tsv>
```

> Note: BOLD data currently inserted in the database and displayed on the target list interface
is not filtered on location anymore.  

Again, in principle, every time a new datapackage file from BOLD is used, the gz files created from it
are available saved on GDrive 
[here](https://drive.google.com/drive/folders/1XJpYYg-nF6bs48fvbBAeu3vaoZEOLxHJ) 
for the Animalia, Plantae and Fungi kingdoms only.

> Note: the `arise_load_bold.py` cannot be used to load data retrieve with BOLD API anymore because the names
> of the fields/columns retrieve from the API differ from the data package file.

#### Using data per kingdom

Some clades (e.g. genus name) have homonyms, leading to ambiguous cases, preventing data to be inserted. 
Is it advised to insert BOLD data using the per-kingdom-files in combination with the `-kingdom` argument 
to target the correct taxon during insertion of the data:

```commandlin
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_<date>_Animalia.tsv.gz -kingdom animalia
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_<date>_Plantae.tsv.gz -kingdom plantae
arise_load_bold.py -db <sqlite.db> -tsv data/input_files/BOLD_<date>_Fungi.tsv.gz -kingdom fungi
```

### 4.2 Inserting Klasse dump files

To load Naturalis barcoding data exported from Geneious/Klasse, use the `arise_load_klasse.py` script, example:

```commandline
python src/arise/barcode/metadata/util/arise_load_klasse.py -db <sqlite.db> -tsv data/input_files/BCP-Bot_ARISE_Rutger_ITS1_20220712.txt -kingdom plantae -marker ITS1
```

>Notes
> * Each file contains barcodes for a give marker, so the `-marker` must be specified.
> * Always better to specify the kingdom to avoid "homonym errors".

### 4.3 Inserting Jorinde Fungi data

The Fungi database produced by Jorinde & collaborators for Naturalis must be inserted via the following command:

```commandline
python src/arise/barcode/metadata/util/arise_load_Jorinde_excel.py -db <sqlite.db> -excel "data/input_files/Fungi barcodes Jorinde.xlsx" -kingdom fungi -marker ITS
```

> Note: The Excel file contains barcode information for other markers than ITS but only ITS barcodes are currently
> relevant. This is why `-marker ITS` is applied.

### 4.4 Inserting Westerdijk data

The Fungi database provided by Duong Vu for ARISE must be inserted via the following command:

```commandline
python src/arise/barcode/metadata/util/arise_load_WFBI_excel.py -db <sqlite.db> -excel data/input_files/WI_DutchStrainsWithPublicBarcodes.xlsx
```

> Note: The Excel file contains a _kingdom_ column that is used to insert the data
> consequently the argument '-kingdom' is not available for that script. However, 
> only Fungi barcodes are expected from Westerdijk.

### 4.5 Inserting latest Naturalis/ARISE data

The barcodes/consensuses generated by Nanopore sequencing, for ARISE, 
must be inserted via the following command:

```commandline
python src/arise/barcode/metadata/util/arise_load_naturalis.py -db <sqlite.db> -tsv data/input_files/ARISE_identified_barcodes.tsv
```

> Note: These barcodes have been BLASTed against BOLD (current version on Galaxy, 100%? identity)
> only barcodes identified at the species level and having the same family rank
> than the taxon name assigned to its specimen/sample, are present in that file. 

---
>All files inserted during steps 4.2 to 4.5 are available on Google Drive 
> [here](https://drive.google.com/drive/folders/1K8lnQkQfhRAu4X45hgMoOaiLTM3lZt9O). Again
> check the [Readme](../data/README.md) in /data/ for more information.

### 5. Indexing the topology for faster queries

Now the tree topology should be indexed. This is easiest done using the
[Bio::Phylo::Forest::DBTree](https://metacpan.org/pod/Bio::Phylo::Forest::DBTree) package:

```
$ perl -MBio::Phylo::Forest::DBTree -e \
    'Bio::Phylo::Forest::DBTree->connect("arise-barcode-metadata.db")->get_root->_index'
```

Or run it in the container by executing the following command at the root of the project:

```commandline
# specify the input db filename (it must be located in /data/sqlite/!)
export DBFILE=arise-barcode-metadata.db
docker compose -f docker-compose.yml run dbtree
```
(use `docker-compose` for v1.xx, `docker compose -f ...` is for version [v2.XX](https://github.com/docker/compose))

### 6. Query and visualize the database content using Jupyter Lab

Jupyter Lab runs in a docker container, to start it run:

```commandline
NB_UID=`id -u` docker-compose -f docker-compose.yml up jupyter
```

and follow the instructions displayed in the terminal to access the interface.

### 7. Generate the HTML interface

To generate the HTML interactive table, run the following command:

```commandline
python src/arise/barcode/metadata/util/generate_target_list_html.py -db data/sqlite/arise-barcode-metadata.db --filter-species
```

>Note: the default is now to systematically add the `--filter-species` argument, 
> to not include artefactual or out-of-scope species from the HTML table and the coverage_table.tsv.
> See the --help of the script for more info.

> **Important**: if you wish to update/commit `target_list.html` generated, and update the online interface 
> (read [here](#target-list-interfaces))
> it is important that you update the dates in the `target_list_template.html` as well. Check the commits
> introducing modifications to that file to see what dates should be modified.

The HTML file `target_list.html` located in `/html/`, can be then opened in the web browser.

This script also produces a file `coverage_table_<date>.tsv` in the current working directory. 
This table contains the information
presented on the target list HTML table. i.e. the barcodes count for each taxonomic level.

This table is used by a Naturalis process to update the [official interface](#target-list-interfaces).
It must be sent to this Gdrive location
https://drive.google.com/drive/folders/1K8lnQkQfhRAu4X45hgMoOaiLTM3lZt9O, for the interface to be
updated.

>**Important**: put the file on Gdrive only after having generated and checked 
> the coverage table statistics (see next section)! In principe,
> the coverage table file generated must have a bigger size (disk space used) than the existing/previous
> coverage tables present in the GDrive folder.

#### 7.1 Coverage table statistics

The statistics of the newly generated coverage table can be computed 
with the [dedicated script](/src/arise/barcode/metadata/util/stats_coverage_table.py). Stats are written in a file named
`coverage_table_stats_<date>.tsv`. These statistics must also be inserted in
this [Google Drive document](https://docs.google.com/spreadsheets/d/1ZbOblN7XOmeet3WeOV_MB-3uBR8_rbvQ8J44duxWBJg/edit#gid=1362329747)
(first sheet - second section: Target List content).

Usage:
```commandline
python src/arise/barcode/metadata/util/stats_coverage_table.py coverage_table_new.tsv --old_coverage_table coverage_table_old.tsv
```

It is better to call the script with the additional option `--old_coverage_table <file>` in order to
generate additional statistics about the comparison between the two tables. 
The statistics of this comparison are written at the end of `coverage_table_stats_<date>.tsv` and must
be also inserted in this 
[Google Drive document](https://docs.google.com/spreadsheets/d/1ZbOblN7XOmeet3WeOV_MB-3uBR8_rbvQ8J44duxWBJg/edit#gid=1362329747),
at the end of the table, first sheet.  
(The whole columns 2 and 3 can be just copied and pasted in the Google Drive document) 

>You might get an error when pasting data if a cell from column 3 (the ones containing the list of species name)
> is too long, so truncate or delete the list of species for these problematic cells first. As you can see on the
> GDrive document, the ones that have been deleted in past updates are named "Too Many".


### Target list interfaces

The HTML file generated by [step 7](#7-generate-the-html-interface) can be used locally.  
However, when this file (and when the `main` branch of the repository) is updated and pushed
The Gitlab page interface available at: https://arise-biodiversity.gitlab.io/sequencing/arise-target-list/
is also updated.  

The Gitlab interface is for development only, the official interface can be accessed at 
https://use.arise-biodiversity.nl/targetlist. How to update this interface with the new target-list data is
explained [above](#7-generate-the-html-interface).

### Helper script

To build the database in one command, use the shell script 
provided at the root of the repository: run `bash make_db.sh` in the root of the project. 
You'll need to activate the virtual environment first.

The script is creating the database in the `ignored` folder, this folder is where all the changes happen to the
database when the barcodes are being inserted during the various steps, 
it is named like this because, it is in the `.gitignore` file.
Tracking this file by git may considerably slow down the pipeline and increase the size of the .git folder.
Once the `make_db.sh` is finished, the database file `<name>.db` can be moved one level up in `data/sqlite/`.
(having the file moved there is also a requirement for step [5](#5-indexing-the-topology-for-faster-queries)).

## FAQ

#### What are "ARISE" and "Other" barcodes?

Barcodes are associated with specimen inserted under the 
"Naturalis Biodiversity Center" or "Westerdijk Fungal Biodiversity 
Institute" institution names are flagged as ARISE. 
These barcodes come from five different sources:
   - Legacy data (Klasse files) - 4.2
   - Naturalis Fungi data (Jorinde excel file) - 4.3
   - Westerdijk barcodes - 4.4
   - Nanopore barcodes - 4.5
   - a (small) part of the BOLD barcodes - 4.1  

"Other" are all barcodes that are not ARISE.

#### Why the occ. status can differ from the occ. status on the NSR website

It is not known how the occurrence status (OS) is determined on the NSR website, 
but, for some species, it does not correspond to the OS found in the list 
of NSR species downloaded from the NBA link (`Taxa.txt`).

In the NSR backbone computed in this project, we discard the subspecies, varietas, etc...
Only species remain in the database as the lowest rank.
However, the OSs of the discarded subspecies & Co are considered, 
and they can be used as OS for the species node. The following priority is taken
into account:

0a > 1[ab] > 2[abcd] > 3[abcd] > 4x > 0 > Null

Example: When parsing the `Taxa.txt` file sequentially, if a species entry has an
`empty` (Null) OS, we create the node with that status.
Now, if later during the parsing we found a subspecies entries with OS=`0` we replace the OS.
Again, later during the parsing, we found another subspecies with an occ. status `1a`,
then the `1a` is retained.

## TODO

 - Parse the new version of the 
[legacy data](https://drive.google.com/drive/folders/1BgyCgy0lTKf_qxntvbXMpvhw54RScI6q)
 instead of using the Klasse files.