# Using the ARISE barcode metadata DB

## 1. Loading data 

This section describes how to populate the DB with data from various sources. The steps are as follows:

1. Initializing a new database
2. Loading the NSR taxonomic topology 
3. Loading the NSR synonyms table
4. Fetching BOLD metadata 
5. Indexing the topology for faster queries
6. Loading the Geneious metadata table

### 1.1 Initializing a new database

A new, empty SQLite database file that implements the database schema as implied by the object-relational mapping
in arise.barcode.metadata.orm.* is generated as follows:

    $ arise_create_barcode_metadata_db.py -outfile arise-barcode-metadata.db

### 1.2 Loading the backbone topology

This operation will load the accepted species names from the NSR into the table nsr_species and the higher taxonomy
in the node table, whose structure is based on [Vos, 2019](https://doi.org/10.1111/2041-210X.13337). This requires an 
empty database file (see 1.1). The contents are fetched from the NSR endpoint for DwC data dumps at: 
http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr - use the script as follows:

    $ arise_load_backbone.py -db <sqlite.db> -endpoint <url>

The terms between pointed brackets are merely placeholders here; the script uses sensible defaults, i.e. the actual
endpoint location and the name of the default output from 1.1

**Note that the database file is updated in place.**

### 1.3 Loading the synonyms table

Here we load the set of taxonomic synonyms that is curated by the NSR and available as a separate table dump. The 
purpose of this is improve the matching between the NSR and the other data sources (i.e. internal sequencing and BOLD). 
This table is assumed to have similar characteristics as the canonical names table:

- a preamble to ignore
- a header row, but here we are looking for the columns `"synonym"` and `"taxon"`
- tab-separated records

With these assumptions, the loading is then executed as:

    $ arise_load_synonyms.py -infile <synonyms.csv> -db <sqlite.db>

**Note that we now only match species, so anything below or above that will trigger a 'no match' message**

### 1.4 Loading BOLD metadata

In this step we load the output from BOLD's _Full Data Retrieval (Specimen + Sequence)_ web service. When filtered on
`Netherlands|Belgium|Germany` this output is ±300MB and takes an hour or so to download. The step can also be done
from a cached copy. In either case, the format must be TSV. The operation is as follows:

    $ arise_load_bold.py -tsv <file.tsv>

### 1.5 Indexing the topology for faster queries

Now the tree topology should be indexed. This is easiest done using the
[Bio::Phylo::Forest::DBTree](https://metacpan.org/pod/Bio::Phylo::Forest::DBTree) package:

```
$ perl -MBio::Phylo::Forest::DBTree -e \
    'Bio::Phylo::Forest::DBTree->connect("arise-barcode-metadata.db")->get_root->_index'
```

Or run it in the container by executing the following command at the root of the project:

```
# specify the input db filename (must be located be in /data/sqlite/)
export DBFILE=arise-barcode-metadata.db
docker-compose -f docker-compose.yml run dbtree
```
(docker-compose 1.xx command, try `docker compose -f ...` for [v2.XX](https://github.com/docker/compose))

### 1.6 Query and visualize the database content using Jupyter Lab

Jupyter Lab runs in a docker container, to start it:

```
NB_UID=`id -u` docker-compose -f docker-compose.yml up jupyter
```

and follow the instructions displayed in the terminal.

