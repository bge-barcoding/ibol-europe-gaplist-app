import argparse
from arise_load_bold import fetch_bold_records
import os

if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-outdir', default=".", help="output directory of downloaded file(s)")
    parser.add_argument('-institutions', default="", help="Institutions, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-marker', default="", help="Markers, quoted and pipe-separated: 'a|b|c'")
    # parser.add_argument('-tsv', help="A TSV file produced by Full Data Retrieval (Specimen + Sequence)")
    args = parser.parse_args()

    # countries = ['Belgium', 'Denmark', 'France', 'Germany', 'Netherlands', 'United Kingdom']
    countries = {
                    'Belgium': 'BE',
                    # 'Denmark': 'DK',
                    # 'France': 'FR',
                    'Germany': 'DE',
                    'Netherlands': 'NL',
                    # 'United Kingdom': 'UK'
                }

    # the taxon filtering of bold api works only from phylum names, not kingdom
    # maybe get the phylum from the database?
    kingdoms = {
        'animalia': ['Arthropoda', 'Chordata', 'Mollusca', 'Annelida', 'Echinodermata', 'Cnidaria', 'Bryozoa',
                     'Porifera', 'Nemertea', 'Platyhelminthes', 'Rotifera', 'Nematoda', 'Gastrotricha', 'Tardigrada',
                     'Acanthocephala', 'Ctenophora', 'Hemichordata', 'Cephalorhyncha', 'Nematomorpha', 'Cycliophora',
                     'Chaetognatha', 'Myxozoa'],
        'plantae': ['Tracheophyta', 'Marchantiophyta', 'Bryophyta', 'Rhodophyta', 'Chlorophyta', 'Charophyta', 'Anthocerotophyta'],
        'fungi': ['Basidiomycota', 'Ascomycota', 'Mucoromycota', 'Chytridiomycota', 'Glomeromycota']
    }

    for k, lk in kingdoms.items():
        for c in sorted(countries.keys()):
            out_file = os.path.join(args.outdir, f'bold_{k.replace(" ", "_")}_{countries[c]}.tsv')
            if not os.path.isfile(out_file):
                print("Downloading BOLD data for", k, '|', c)
                fetch_bold_records(c, args.institutions, args.marker, '|'.join(lk), to_file=out_file)
            else:
                print(f'File {out_file} already exists')

