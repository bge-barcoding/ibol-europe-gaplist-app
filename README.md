# ARISE barcode metadata

## Project overview

This project aims to provide tooling for managing 
[ARISE](https://www.arise-biodiversity.nl/) DNA barcode metadata to achieve
the following:

1. Ingest on-disk, file-based Dutch species registry 
   ([NSR](https://www.nederlandsesoorten.nl/content/toegang-tot-de-data)) taxonomy 
   as TSV data.
2. Ingest on-disk, file-based data (FASTA) and metadata (CSV/TSV) managed 
   using [Geneious](https://www.geneious.com/).
3. Harvest remote data (FASTA) and metadata accessible through 
   [BOLD APIs](https://www.boldsystems.org/index.php/api_home)
4. Filter candidate barcodes by flexible criteria, e.g. provider, geographic origin
5. Integrate data ([FASTA/BLAST DB](https://www.ncbi.nlm.nih.gov/books/NBK279690/)) 
   and metadata ([SQLite](https://www.sqlite.org/index.html)) to enrich sequence records
6. Report and visualize gap analysis of database contents for targeted sequencing 
   ([JuPyter](https://jupyter.org/))
7. Navigate database contents in tabular form as web view and REST API 
   ([Django](https://www.djangoproject.com/))

## Project structure

The project is organized following the conventions as outlined in Noble, 2009
[10.1371/journal.pcbi.1000424](https://doi.org/10.1371/journal.pcbi.1000424).

## Authors

Bastiaan Anker, Naomi van Es, Rutger Vos. 

(Feel free to add yourself here in any substantial pull requests.)

## License

This source code is made available under the [MIT License](LICENSE).
