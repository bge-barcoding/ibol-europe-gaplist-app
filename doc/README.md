# Using the ARISE barcode metadata DB

## 1. Loading data 

This section describes how to populate the DB with data from various sources. The steps are as follows:

1. Initializing a new database
2. Loading the canonical species table of the Nederlands Soortenregister (NSR)
3. Loading the NSR synonyms table
3. Loading the NSR taxonomic topology 
4. Indexing the topology for faster queries
5. Loading the Geneious metadata table
6. Fetching BOLD metadata 

### 1.1 Initializing a new database

A new, empty SQLite database file that implements the database schema as implied by the object-relational mapping
in arise.barcode.metadata.orm.* is generated as follows:

    $ arise_create_barcode_metadata_db.py -outfile arise-barcode-metadata.db

### 1.2 Loading the canonical species table

This operation will load the accepted names from the NSR into the table nsr_species. This requires an empty database
file (see 1.1) and a dumped table from the NSR. The dumps that the NSR provides are assumed to have the following
characteristics, which **must** all be met:

- zero or more lines of 'preamble', where the taxonomic top level, the included ranks and statuses are listed
- a header row, which must contain at least `scientific_name` and `nsr_id`
- zero or more records to ingest, which must be tab-separated

The script simply skips over the preamble, hoping to come across the scientific name and NSR ID. Once those are found
to be present, the remaining lines are ingested. This is executed as follows:

    $ arise_load_canonical_species.py -infile <table.csv> -db <sqlite.db>

The terms between pointed brackets are merely placeholders here; replace these with the right file names. Note that
the database file is updated in place, though duplicate nsr_id records are not inserted.

