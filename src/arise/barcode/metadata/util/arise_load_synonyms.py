import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from taxon_parser import TaxonParser


def clean_name(name):
    name_parser = TaxonParser(name)
    try:
        parsed = name_parser.parse()
        cleaned = parsed.canonicalNameWithoutAuthorship()
        return cleaned
    except:
        print("This name does not seem to be a valid taxon name: \n" + name)


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
header = []
with open(infile, encoding='latin-1') as file:

    # iterate over lines
    for line in file:
        fields = line.split("\t")

        # found header line
        if '"synonym"' in fields and '"taxon"' in fields:
            header = fields
            continue

        # processing records after header
        if len(header) != 0:
            record = dict(zip(header, fields))
            synonym = clean_name(record['"synonym"'])
            taxon = clean_name(record['"taxon"'])
            hitcounter = 0
            if synonym is not None and taxon is not None:
                for species in session.query(NsrSpecies).filter(NsrSpecies.canonical_name == taxon):
                    species_id = species.species_id
                    synonym = NsrSynonym(synonym_name=synonym, species_id=species_id)
                    session.add(synonym)
                    hitcounter += 1
                if hitcounter > 1:
                    print("More than one hit for " + taxon)
                if hitcounter == 0:
                    print("No hit for " + taxon )

# commit transaction
session.commit()
