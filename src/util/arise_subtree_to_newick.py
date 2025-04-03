import argparse
from arise.barcode.metadata.orm.nsr_node import NsrNode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-db', default="/Users/rutger/Documents/local-projects/arise-barcode-metadata/data/sqlite/arise-barcode-metadata.db", help="Input file: SQLite DB")
parser.add_argument('-node', default=13, help="Node ID")
args = parser.parse_args()

# create connection/engine to database file
dbfile = args.db
engine = create_engine(f'sqlite:///{dbfile}', echo=False)

# make session
Session = sessionmaker(engine)
session = Session()

# write tree to newick
root = session.query(NsrNode).filter(NsrNode.id == args.node).first()
tree = root.to_ete(session)
print(tree.write(format=1))

