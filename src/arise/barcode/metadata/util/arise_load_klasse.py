import argparse
import logging
import re
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.marker import Marker
from arise.barcode.metadata.orm.barcode import Barcode
from arise.barcode.metadata.orm.imports import *


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}

    # IEEE specs say NaN's can not be equal, so that's how we do the checks for missing values

    # check if there is a sequence, otherwise nothing to do
    if row['Sequence'] != row['Sequence']:
        logging.debug("Record %s has no sequence, skipping..." % row['label'])
        return None

    if row['Taxon'] == row['Taxon']:
        parts = row['Taxon'].split(' ')

        # if the name has at least two parts it's a (sub)species, which is good
        if len(parts) > 1:
            record['taxon'] = row['Taxon']
        else:

            # taxon names above genus level are managed by Klasse in all caps
            if re.match('^[A-Z]+$', row['Taxon']):
                logging.debug("Taxon %s is higher than genus, skipping" % row['Taxon'])
                return None
            else:

                # it's a genus, which will be grafted as 'Genus sp.'
                record['taxon'] = row['Taxon']
    else:
        logging.debug("Record has no Taxon field, skipping...")
        return None

    # setup the other required fields
    record['catalognum'] = row['label']
    record['institution_storing'] = 'Naturalis Biodiversity Center' # same as in BOLD
    record['identification_provided_by'] = row['det.'] # XXX can be NaN!
    record['external_id'] = row['Name'] # this is the internal FASTA ID / defline managed by Geneious

    # done
    return record


def load_klasse(marker_name, input_file, encoding='utf-8'):
    df = pd.read_csv(input_file, sep='\t', encoding=encoding)
    for index, row in df.iterrows():

        # initialize dict with relevant fields, next row if failed
        record = init_record_fields(row)
        if record is None:
            continue

        # initialize species, next row if failed
        nsr_species = NsrSpecies.match_species(record['taxon'], session)
        if nsr_species is None:
            continue

        # get or create specimen
        specimen = Specimen.match_specimen(nsr_species.species_id, record['catalognum'], record['institution_storing'],
                                  record['identification_provided_by'], session)

        # get or create marker
        marker = Marker.match_marker(marker_name, session)

        # set database field value
        database = DataSource.NATURALIS

        # check if barcode already exists, criteria: 1) DataSource is NATURALIS, 2) marker matches
        for barcode in specimen.barcodes:
            if barcode.marker_id == marker.marker_id:
                logging.warning("Specimen %s already has barcode for marker %s" % (specimen.catalognum, marker_name))
                break

        barcode = Barcode(specimen_id=specimen.specimen_id, database=database, marker_id=marker.marker_id,
                          external_id=record['external_id'])
        logging.info("Inserting %s barcode for specimen %s" % (marker_name, specimen.catalognum))
        session.add(barcode)


if __name__ == '__main__':

    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-marker', default="", help="Marker name using BOLD vocab, e.g. COI-5P")
    parser.add_argument('-tsv', help="A TSV file exported from Klasse using the ARISE template")
    parser.add_argument('--verbose', '-v', action='count', default=1)
    args = parser.parse_args()

    # configure logging
    args.verbose = 70 - (10 * args.verbose) if args.verbose > 0 else 0
    logging.basicConfig(level=args.verbose)

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # load data during session
    Session = sessionmaker(engine)
    session = Session()
    load_klasse(args.marker, args.tsv)
    session.commit()
