import argparse
import requests, zipfile, io
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.nsr_species import NsrSpecies

default_url = "http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr"

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
parser.add_argument('-endpoint', default=default_url, help="Input URL: NSR DwC endpoint")
args = parser.parse_args()

# create connection/engine to database file
dbfile = args.db
engine = create_engine(f'sqlite:///{dbfile}', echo=False)

# make session
Session = sessionmaker(engine)
session = Session()

# download and extract DwC zip file
url = args.endpoint
r = requests.get(url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall()

# read 'Taxa.txt' from zip as a data frame
df = pd.read_csv('Taxa.txt', sep=',')
df = df.reset_index()  # make sure indexes pair with number of rows
species_counter = 1
node_counter = 3 # magic number because root will be 2 with parent 1
session.add(NsrNode(id=2, name="All of life", parent=1))
for index, row in df.iterrows():

    # compose binomial name, see if it exists
    binomial = None
    try:
        binomial = row['genus'] + ' ' + row['specificEpithet']
    except:
        print(row)
        continue
    nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == binomial).first()
    if nsr_species is None:

        # not seen this before, insert
        nsr_species = NsrSpecies(species_id=species_counter, canonical_name=binomial)
        session.add(nsr_species)

        # check if already inserted in backbone, presumably by way of another pruned infraspecific epithet
        child = session.query(NsrNode).filter(NsrNode.name == binomial).first()
        if child is None:
            child = NsrNode(id=node_counter, species_id=species_counter, name=binomial, rank='species')
            session.add(child)
            node_counter += 1
        else:

            # we've seen this species before (prob from another subspecies), move onto the next row
            continue

        # iterate over higher levels, see if they already exist
        for level in ['genus', 'family', 'order', 'class', 'phylum', 'kingdom']:
            higher_taxon = row[level]
            node = session.query(NsrNode).filter(NsrNode.name == higher_taxon).first()
            if node is None:

                # instantiate a parent node, graft child onto it, move up the hierarchy
                parent = NsrNode(id=node_counter, name=higher_taxon, rank=level, parent=2)
                session.add(parent)
                child.parent = parent.id
                child = parent
                node_counter += 1

            else:

                # we've already reached and instantiated this higher taxon via another path, graft onto that
                child.parent = node.id
                break

        # generate the next primary key
        species_counter += 1

# commit transaction
session.commit()
