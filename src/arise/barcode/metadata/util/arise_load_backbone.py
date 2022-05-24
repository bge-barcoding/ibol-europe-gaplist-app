import argparse
import io
import pandas as pd
import requests
import zipfile
import os
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.barcode import Barcode
from arise.barcode.metadata.orm.nsr_node import NsrNode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DEFAULT_URL = "http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr"

def download_and_extract(url):
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()


def load_backbone(infile):
    df = pd.read_csv(infile, sep=',')
    df.reset_index(inplace=True)  # make sure indexes pair with number of rows
    df.fillna('', inplace=True)
    node_counter = 3  # magic number because root will be 2 with parent 1
    session.add(NsrNode(id=2, name="All of life", parent=1))

    taxon_level_dict = dict()
    for index, row in df.iterrows():

        # compose binomial name, see if it exists
        binomial = None
        try:
            binomial = row['genus'] + ' ' + row['specificEpithet']
        except:
            print(row)
            exit()
            continue
        nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == binomial).first()
        if nsr_species is None:

            # not seen this before, insert
            nsr_species = NsrSpecies(canonical_name=binomial)
            session.add(nsr_species)
            session.flush()

            # check if already inserted in backbone, presumably by way of another pruned infraspecific epithet
            child = session.query(NsrNode).filter(NsrNode.name == binomial).first()
            if child is None:
                child = NsrNode(id=node_counter, species_id=nsr_species.species_id, name=binomial, rank='species')
                session.add(child)
                session.flush()
                node_counter += 1
            else:

                # we've seen this species before (prob from another subspecies), move onto the next row
                continue

            # iterate over higher levels, see if they already exist
            for level in ['genus', 'family', 'order', 'class', 'phylum', 'kingdom']:
                higher_taxon = row[level]

                if not higher_taxon:
                    print(f'Warning taxon is N/A for level "{level}", index {index}')
                else:
                    if higher_taxon not in taxon_level_dict:
                        taxon_level_dict[higher_taxon] = level
                    elif taxon_level_dict[higher_taxon] != level:
                        print(f'Warning "{higher_taxon}" is level "{level}" but was "{taxon_level_dict[higher_taxon]},'
                              f' index {index}"')

                node = session.query(NsrNode).filter(NsrNode.name == higher_taxon).first()
                if node is None:

                    # instantiate a parent node, graft child onto it, move up the hierarchy
                    parent = NsrNode(id=node_counter, name=higher_taxon, rank=level, parent=2)
                    session.add(parent)
                    session.flush()
                    child.parent = parent.id
                    child = parent
                    node_counter += 1

                else:

                    # we've already reached and instantiated this higher taxon via another path, graft onto that
                    child.parent = node.id
                    break


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-endpoint', default=DEFAULT_URL, help="Input URL: NSR DwC endpoint")
    args = parser.parse_args()

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # make session
    Session = sessionmaker(engine)
    session = Session()

    # download and extract DwC zip file
    url = args.endpoint
    if 'Taxa.txt' not in os.listdir(os.getcwd()):
        download_and_extract(args.endpoint)

    # read 'Taxa.txt' from zip as a data frame
    load_backbone('Taxa.txt')
    session.commit()
