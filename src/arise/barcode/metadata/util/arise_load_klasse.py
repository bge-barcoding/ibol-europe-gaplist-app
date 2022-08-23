import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import logging
import re
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.common import DataSource
from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.barcode import Barcode
from orm.specimen import Specimen
from orm.marker import Marker
import loggers

main_logger = logging.getLogger('main')
lk_logger = logging.getLogger('load_klasse')


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}
    # IEEE specs say NaN's can not be equal, so that's how we do the checks for missing values

    # check if there is a sequence, otherwise nothing to do
    if row['Sequence']:
        lk_logger.warning("Record %s has no sequence, skipping..." % row['label'])
        return None

    if row['Taxon']:
        parts = row['Taxon'].split(' ')

        # if the name has at least two parts it's a (sub)species, which is good
        if len(parts) > 1:
            record['taxon'] = ' '.join(parts[:2])
        else:
            # taxon names above genus level are managed by Klasse in all caps
            if re.match('^[A-Z]+$', row['Taxon']):
                lk_logger.warning("Taxon %s is higher than genus, skipping" % row['Taxon'])
                return None
            else:
                # it's a genus, which will be grafted as 'Genus sp.'
                record['taxon'] = row['Taxon']
    else:
        lk_logger.warning("Record has no Taxon field, skipping...")
        return None

    # set up the other required fields
    record['catalognum'] = row['label']
    record['institution_storing'] = 'Naturalis Biodiversity Center'  # same as in BOLD
    record['identification_provided_by'] = row['det.']  # XXX can be NaN!
    record['external_id'] = row['Name']  # this is the internal FASTA ID / defline managed by Geneious
    record['locality'] = row['land'] if row['land'] else 'Unknown'

    return record


def load_klasse(marker_name, kingdom, input_file, encoding='utf-8'):
    df = pd.read_csv(input_file, sep='\t', encoding=encoding)
    df.fillna('', inplace=True)
    specimens_created = 0
    markers_created = 0
    barcodes_created = 0
    incomplete_records = 0
    fail_matching_nsr_species = 0
    for index, row in df.iterrows():

        # initialize dict with relevant fields, next row if failed
        record = init_record_fields(row)
        if record is None:
            incomplete_records += 1
            continue

        # initialize species, continue if failed
        nsr_species = NsrSpecies.match_species(record['taxon'], session, kingdom=kingdom)
        if nsr_species is None:
            fail_matching_nsr_species += 1
            continue

        # get or create specimen
        specimen, created = Specimen.get_or_create_specimen(nsr_species.id,
                                                   record['catalognum'],
                                                   record['institution_storing'],
                                                   record['identification_provided_by'],
                                                   record['locality'],
                                                   session)
        if created:
            specimens_created += 1

        # get or create marker
        marker, created = Marker.get_or_create_marker(marker_name, session)
        if created:
            markers_created += 1

        # check if barcode already exists, criteria: 1) DataSource is NATURALIS, 2) marker matches
        for barcode in specimen.barcodes:
            if barcode.marker_id == marker.id:
                lk_logger.error("Specimen %s already has barcode for marker %s" % (specimen.catalognum, marker_name))
                break

        barcode = Barcode(specimen_id=specimen.id, database=DataSource.NATURALIS, marker_id=marker.id,
                          external_id=record['external_id'])
        barcodes_created += 1
        lk_logger.info("Inserting %s barcode for specimen %s" % (marker_name, specimen.catalognum))
        session.add(barcode)

    main_logger.info(f'{specimens_created=}')
    main_logger.info(f'{markers_created=}')
    main_logger.info(f'{barcodes_created=}')
    main_logger.info(f'{incomplete_records=}')
    main_logger.info(f'{fail_matching_nsr_species=}')


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-marker', choices=['COI-5P', 'ITS1', 'ITS', 'matK', 'trnL'],
                        help="Marker name using BOLD vocab, e.g. COI-5P", required=True)
    parser.add_argument('-kingdom', choices=['animalia', 'plantae', 'fungi'],
                        help="match only species / taxon in the given kingdom")
    parser.add_argument('-tsv', help="A TSV file exported from Klasse using the ARISE template")
    parser.add_argument('--verbose', '-v', action='count', default=1)

    args = parser.parse_args()
    args.verbose = 70 - (10 * args.verbose) if args.verbose > 0 else 0
    [h.addFilter(loggers.LevelFilter(args.verbose)) for h in lk_logger.handlers]

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # load data during session
    Session = sessionmaker(engine)
    session = Session()
    main_logger.info('Load klasse file "%s"', args.tsv)
    load_klasse(args.marker, args.kingdom, args.tsv)
    session.commit()
