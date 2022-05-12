from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.database import Database
from arise.barcode.metadata.orm.marker import Marker
from arise.barcode.metadata.orm.imports import *
from arise.barcode.metadata.orm.barcode import Barcode
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.nsr_node import NsrNode
import argparse

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-outfile', default="arise-barcode-metadata.db", help="Output file: SQLite DB")
args = parser.parse_args()

# create connection/engine to database file
outfile = args.outfile
engine = create_engine(f'sqlite:///{outfile}', echo=False)

# make session
Session = sessionmaker(engine)
session = Session()

# create tables if they do not exist
Base.metadata.create_all(engine)

# commit transaction
session.commit()
