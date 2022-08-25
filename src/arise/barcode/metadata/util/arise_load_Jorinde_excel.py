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


def has_unusual_name(taxon):
    ignore_name_pattern = ['?', '(', 'to be checked']
    for el in ignore_name_pattern:
        if el in taxon:
            return True
    return False


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}
    # print(row.__dict__)
    # IEEE specs say NaN's can not be equal, so that's how we do the checks for missing values

    # check if there is a sequence, otherwise nothing to do
    if not row['ITS sequence']:
        lk_logger.warning("Record %s has no ITS sequence, skipping..." % row['Number'])
        return None

    genus = row['Revised genus'] if row['Revised genus'] else row['Genus']
    if genus:
        record['taxon'] = genus
        # try to include the species epithet
        if row['Revised species epithet'] and \
                not has_unusual_name(row['Revised species epithet']):
            record['taxon'] += " " + row['Revised species epithet']
        elif row['original species epithet/field identification'] and \
                not has_unusual_name(row['original species epithet/field identification']):
            record['taxon'] += " " + row['original species epithet/field identification']
    else:
        lk_logger.warning("Record %s has no Taxon field, skipping..." % row['Number'])
        return None

    # set up the other required fields
    record['catalognum'] = row['Herbarium number (collector number if herbarium number not available)']
    record['institution_storing'] = 'Naturalis Biodiversity Center'  # same as in BOLD
    record['identification_provided_by'] = row['Collector (Leg.)']  # XXX can be NaN!
    record['external_id'] = row['Number']
    record['locality'] = row['Country'] if row['Country'] else 'Unknown'

    return record


def load_excel(marker_name, kingdom, input_file):
    df = pd.read_excel(input_file, sheet_name="JNL DNA extraction table", header=1,
                       engine='openpyxl')  # req. openpyxl package to be installed
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
        nsr_species_node = NsrNode.match_species_node(record['taxon'], session, kingdom=kingdom)
        if nsr_species_node is None:
            fail_matching_nsr_species += 1
            continue

        # get or create specimen
        specimen, created = Specimen.get_or_create_specimen(nsr_species_node.species_id,
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
                lk_logger.warning("Specimen %s already has barcode for marker %s" % (specimen.catalognum, marker_name))
                break

        barcode, created = Barcode.get_or_create_barcode(specimen.id, DataSource.NATURALIS, marker.id, None,
                                                         record['external_id'], session)
        if created:
            barcodes_created += 1

    main_logger.info(f'{specimens_created=}')
    main_logger.info(f'{markers_created=}')
    main_logger.info(f'{barcodes_created=}')
    main_logger.info(f'{incomplete_records=}')
    main_logger.info(f'{fail_matching_nsr_species=}')


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-marker', choices=['ITS'],  # currently only suppose to par ITS data in that file
                        help="Marker name using BOLD vocab, e.g. ITS", required=True)
    parser.add_argument('-kingdom', choices=['fungi'],
                        help="match only species / taxon in the given kingdom")
    parser.add_argument('-excel', help="A TSV file exported from Klasse using the ARISE template")
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
    main_logger.info('Load excel file "%s"', args.excel)
    load_excel(args.marker, args.kingdom, args.excel)
    session.commit()
