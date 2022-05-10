# ARISE barcode metadata

## Project overview

This project aims to provide tooling for managing DNA barcode metadata to achieve
the following:

1. Ingest on-disk, file-based Dutch species registry 
   ([NSR](https://www.nederlandsesoorten.nl/content/toegang-tot-de-data)) taxonomy as TSV data.
2. Ingest on-disk, file-based data (FASTA) and metadata (CSV/TSV) managed using [Geneious](https://www.geneious.com/).
3. Harvest remote data (FASTA) and metadata accessible through BOLD APIs
4. Filter candidate barcodes by flexible criteria, e.g. provider, geographic origin
5. Integrate data (FASTA/BLAST DB) and metadata (SQL) to enrich sequence records
6. Report and visualize gap analysis of database contents for targeted sequencing (JuPyter)
7. Navigate database contents in tabular form as web view and REST API (Django)

## Project structure

The project is organized following the conventions as outlined in Noble, 2009
[10.1371/journal.pcbi.1000424](https://doi.org/10.1371/journal.pcbi.1000424).
