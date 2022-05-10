import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from taxon_parser import TaxonParser, UnparsableNameException

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
parser.add_argument('-infile', default="nsr_synonym.csv", help="Input file: NSR synonyms table")
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
        if '"synonym"' in fields and '"taxon_nsr_id"' in fields:
            counter = 1
            header = fields
            continue

        # processing records after header
        if counter != 0:
            record = dict(zip(header, fields))
            parser = TaxonParser(record['"synonym"'])
            try:
                parsed_name = parser.parse()
                name = parsed_name.canonicalName()
                nsr_id = record['"taxon_nsr_id"'].replace('"', '')
                hitcounter = 0
                for species in session.query(NsrSpecies).filter(NsrSpecies.nsr_id == nsr_id):
                    species_id = species.species_id
                    synonym = NsrSynonym(synonym_id=counter, nsr_id=nsr_id, synonym_name=name, species_id=species_id)
                    session.add(synonym)
                    counter += 1
                    hitcounter += 1
                if hitcounter > 1:
                    print("More than one hit for " + nsr_id)
            except UnparsableNameException as e:
                print("This name does not seem to be a valid taxon name: \n" + record['"synonym"'])

# commit transaction
session.commit()
