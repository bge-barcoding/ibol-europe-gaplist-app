import os
import argparse
import pandas as pd
import gzip
import csv
import tarfile
import codecs

if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('bold_data_package', default=".",
                        help="BOLD data package file (tar.gz), downloadable at https://www.boldsystems.org/index.php/datapackages")
    parser.add_argument('label', help="label for output filename")
    parser.add_argument('--read-only', dest='read_only', action='store_true', help="do not write data to file")
    parser.add_argument('--output-dir', dest='output_dir', help="Location of the output gz files")

    args = parser.parse_args()

    tar = tarfile.open(args.bold_data_package)
    bold_tsv_file = [e for e in tar.getmembers() if e.name.endswith('.tsv')][0]  # hopefully only one .tsv per archive!

    write_header = False
    entry_no_kingdom = 0
    entry_fungi = 0
    entry_plantea = 0
    entry_animalia = 0
    entry_other = 0

    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except:
        print(f"Could not create output directory '{args.output_dir}'")
        exit(1)

    fungi_file = os.path.join(args.output_dir, 'BOLD_%s_Fungi.tsv.gz' % args.label)
    assert not os.path.isfile(fungi_file), f"Fungi file '{fungi_file}' already exist"
    plantae_file = os.path.join(args.output_dir, 'BOLD_%s_Plantae.tsv.gz' % args.label)
    assert not os.path.isfile(fungi_file), f"Plantae file '{plantae_file}' already exist"
    animalia_file = os.path.join(args.output_dir, 'BOLD_%s_Animalia.tsv.gz' % args.label)
    assert not os.path.isfile(fungi_file), f"Animalia file '{animalia_file}' already exist"

    with gzip.open(fungi_file, 'wt', encoding='UTF-8') as fw_f, \
         gzip.open(plantae_file, 'wt', encoding='UTF-8') as fw_p, \
         gzip.open(animalia_file, 'wt', encoding='UTF-8') as fw_a:

        utf8reader = codecs.getreader('UTF-8')
        fp = utf8reader(tar.extractfile(bold_tsv_file))

        bold_tsv_file = None
        for df in pd.read_csv(fp, sep='\t', encoding='UTF-8', error_bad_lines=False,
                              warn_bad_lines=True, quoting=csv.QUOTE_NONE, chunksize=500000):
            df.fillna('', inplace=True)

            if not write_header:
                fw_f.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                fw_p.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                fw_a.write("\t".join([str(e) for e in list(df.columns.values)]) + "\n")
                write_header = True

            for row in df.itertuples(name='Node'):
                if not row.kingdom:
                    entry_no_kingdom += 1
                    continue

                if args.read_only:
                    continue

                if row.kingdom == 'Fungi':
                    fw_f.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")
                    entry_fungi += 1
                elif row.kingdom == 'Plantae':
                    fw_p.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")
                    entry_plantea += 1
                elif row.kingdom == 'Animalia':
                    fw_a.write("\t".join([str(e) for e in list(row)[1:]]) + "\n")
                    entry_animalia += 1
                else:
                    entry_other += 1


    print("Entries with Fungi:", entry_fungi)
    print("Entries with Plantae:", entry_plantea)
    print("Entries with Animalia:", entry_animalia)
    print("Entries without kingdom (ignored):", entry_no_kingdom)
    print("Entries with other (ignored):", entry_other)

