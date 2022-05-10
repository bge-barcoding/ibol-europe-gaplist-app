# ARISE barcode metadata

## Project overview

This project aims to provide tooling for managing DNA barcode metadata to achieve
the following:

- ingest on-disk, file-based taxonomy TSV data from NSR
- ingest on-disk, file-based data (FASTA) and metadata (CSV/TSV) managed using Geneious
- harvest remote data (FASTA) and metadata accessible through BOLD APIs
- filter candidate barcodes by flexible criteria, e.g. provider, geographic origin
- integrate data (FASTA/BLAST DB) and metadata (SQL) to enrich sequence records
- report and visualize gap analysis of database contents for targeted sequencing (JuPyter)
- navigate database contents in tabular form as web view and REST API (Django)

## Project structure

The project is organized following the conventions as outlined in Noble, 2009
[10.1371/journal.pcbi.1000424](https://doi.org/10.1371/journal.pcbi.1000424).
