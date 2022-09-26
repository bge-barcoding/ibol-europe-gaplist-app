import argparse
from arise_load_bold import fetch_bold_records
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.nsr_node import NsrNode
import gzip

if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('outdir', default=".", help="output directory of downloaded file(s)")
    parser.add_argument('bold_country_file', default="", help="list of bold country")
    parser.add_argument('db', default="arise-barcode-metadata.db",
                        help="Input file: SQLite DB, with NSR taxonomy loaded")
    parser.add_argument('label', help="label for output filename")
    parser.add_argument('--mock', dest='mock', action='store_true', help="do not write data to file")

    args = parser.parse_args()

    countries = set()
    with open(args.bold_country_file, 'r') as fh:
        [countries.add(line.strip()) for line in fh if line.strip()]

    for c in sorted(countries):
        out_file = os.path.join(args.outdir, f'bold_{c}.tsv')
        if not os.path.isfile(out_file):
            print("Downloading BOLD data ", out_file)
            fetch_bold_records(c, "", "", "", to_file=out_file)
        else:
            print(f'File {out_file} already exists')

    # all files were downloaded, merge them per kingdom
    # but the kingdom column does not exist?
    # so use the phylum and try to match it on the NSR taxonomy to get the kingdom

    # fix maybe get the phylum from the database?
    phylum_kingdom_map = {
        'Magnoliophyta': 'Plantae',
        'Zygomycota': 'Fungi',
        'Pteridophyta': 'Plantae',
        'Pinophyta': 'Plantae',
        'Lycopodiophyta': 'Plantae',
        'Kinorhyncha': 'Animalia',
        'Priapulida': 'Animalia',
        'Xenacoelomorpha': 'Animalia',
        'Ginkgophyta': 'Plantae',
        'Gnetophyta': 'Plantae',
        'Streptophyta': 'Plantae',
        'Neocallimastigomycota': 'Fungi',
        'Brachiopoda': 'Animalia',
        'Onychophora': 'Animalia',
        'Placozoa': 'Animalia',
        'Rhombozoa': 'Animalia',
        'Cycadophyta': 'Plantae',
        'Psilophyta': 'Plantae',
    }

    ignored_phylum = {
        'Heterokontophyta': 'protist',
        'Proteobacteria': 'bacteria',
        'Bacteroidetes': 'bacteria',
        'Cyanobacteria': 'bacteria',
        'Pyrrophycophyta': 'chromista',
        'Bacillariophyta': 'chromista',
        'Actinobacteria': 'bacteria',
        'Euryarchaeota': 'archaea',
        'Chlorarachniophyte': 'Cercozoa',
        'Chlorarachniophyta': 'Cercozoa',
        'Ciliophora': 'protist',
        'Planctomycetes': 'bacteria',
        'Riboviria': 'virus',
        'Amoebozoa': 'protist',
        'Chloroflexi': 'bacteria',
        'Firmicutes': 'bacteria',
        'Apicomplexa': 'protist',
        'Unknown': 'unknown',
        'Cryptophyta': 'algae',
        'Haptophyta': 'algae',
        'Glaucophyta': 'algae',
        'Deinococcus-Thermus': 'bacteria',
        'Crenarchaeota': 'archaea',
        'Air': '?',
        'Spirochaetes': 'bacteria',
        'Aquificae': 'bacteria',
        'Marine': '?',
        'Prime': '?',
        'Thaumarchaeota': 'archaea',
        'Euglenida': 'protist?',
        'Terrestrial': '?',
    }

    engine = create_engine(f'sqlite:///{args.db}', echo=False)
    Session = sessionmaker(engine)
    session = Session()

    write_header = False
    with gzip.open('BOLD_%s_Fungi.tsv.gz' % args.label, 'wt', encoding='ISO-8859-1') as fw_f, \
         gzip.open('BOLD_%s_Plantae.tsv.gz' % args.label, 'wt', encoding='ISO-8859-1') as fw_p, \
         gzip.open('BOLD_%s_Animalia.tsv.gz' % args.label, 'wt', encoding='ISO-8859-1') as fw_a:
        for f in os.listdir(args.outdir):
            full_f = os.path.join(args.outdir, f)
            if os.stat(full_f).st_size == 0:
                print("Warning: file %s is empty" % full_f)
                continue
            print("Parsing %s" % full_f)

            df = pd.read_csv(full_f, sep='\t', encoding='ISO-8859-1', error_bad_lines=False, quoting=3)
            df.fillna('', inplace=True)

            num_lines = sum(1 for line in open(full_f, encoding='ISO-8859-1'))
            if num_lines != len(df) + 1:
                print("Warning: Entry parsed:", len(df), ' | Line count:', num_lines)

            if not write_header:
                fw_f.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                fw_p.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                fw_a.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                write_header = True

            if 'kingdom' in df.columns or 'kingdom_name' in df.columns:
                print("file has kingdom col")
                exit()

            for row in df.itertuples(name='Node'):
                if row.phylum_name in ignored_phylum:
                    continue
                if row.phylum_name not in phylum_kingdom_map:
                    nodes = session.query(NsrNode).filter(NsrNode.name == row.phylum_name, NsrNode.rank == 'phylum').all()
                    # print(nodes)
                    if len(nodes) != 1:
                        print("Error: could not find exactly 1 phylum with name '%s'" % row.phylum_name)
                        print('results:', nodes)
                        print('class:', row.class_name, '; order',  row.order_name)
                        print('Manually specify the kingdom for this phylum')
                        exit()
                    kingdom = nodes[0].kingdom
                    phylum_kingdom_map[row.phylum_name] = kingdom
                else:
                    kingdom = phylum_kingdom_map[row.phylum_name]

                if args.mock:
                    continue

                if kingdom == 'Fungi':
                    # df_fungi = df_fungi.append(df.loc[row.Index].to_dict(), ignore_index=True)
                    fw_f.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")
                elif kingdom == 'Plantae':
                    fw_p.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")
                elif kingdom == 'Animalia':
                    fw_a.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")

