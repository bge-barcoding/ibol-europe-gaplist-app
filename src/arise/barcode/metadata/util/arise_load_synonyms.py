import argparse
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.barcode import Barcode
from taxon_parser import TaxonParser


def clean_name(name):
    name_parser = TaxonParser(name)
    try:
        parsed = name_parser.parse()
        cleaned = parsed.canonicalNameWithoutAuthorship()
        return cleaned
    except:
        print("This name does not seem to be a valid taxon name: \n" + name)


def load_synonyms(infile):
    with open(infile, encoding='latin-1') as csvfile:
        for _ in range(4):  # ignore the first 5 lines, but make sure they are invalid
            assert(len(csvfile.readline().split('\t')) < 6)
        reader = csv.DictReader(csvfile, delimiter='\t', quotechar='"')
        for row in reader:
            synonym = clean_name(row['synonym'])
            taxon = clean_name(row['taxon'])
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
                    print("No hit for " + taxon)


if __name__ == '__main__':

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
    load_synonyms(args.infile)

    # commit transaction
    session.commit()
