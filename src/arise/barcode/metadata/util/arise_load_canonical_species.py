import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from taxon_parser import TaxonParser, UnparsableNameException

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
parser.add_argument('-infile', default="nsr_species.csv", help="Input file: NSR species table")
args = parser.parse_args()

# create connection/engine to database file
dbfile = args.db
engine = create_engine(f'sqlite:///{dbfile}', echo=False)

# make session
Session = sessionmaker(engine)
session = Session()

# open file handle to NSR TSV file
infile = args.infile
counter = 0
header = []
with open(infile, encoding='latin-1') as file:

    # iterate over lines
    for line in file:
        fields = line.split("\t")

        # found header line
        if '"scientific_name"' in fields and '"nsr_id"' in fields:
            counter = 1
            header = fields
            continue

        # processing records after header
        if counter != 0:
            record = dict(zip(header, fields))
            parser = TaxonParser(record['"scientific_name"'])
            try:
                parsed_name = parser.parse()
                name = parsed_name.canonicalName()
                nsr_id = record['"nsr_id"'].replace('"','')
                species = NsrSpecies(species_id=counter, nsr_id=nsr_id, canonical_name=name)
                session.add(species)
                counter += 1
            except UnparsableNameException as e:
                print("This name does not seem to be a valid taxon name: \n" + record['"scientific_name"'])

# commit transaction
session.commit()





