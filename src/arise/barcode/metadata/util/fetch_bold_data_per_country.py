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

    for country in ['Belgium', 'Denmark', 'France', 'Germany', 'Netherlands', 'United Kingdom']:
        out_file = os.path.join(args.outdir, f'bold_{country.replace(" ", "_")}.tsv')
        if not os.path.isfile(out_file):
            print("Downloading BOLD data for", country)
            fetch_bold_records(country, args.institutions, args.marker, to_file=out_file)
        else:
            print(f'File {out_file} already exists')

