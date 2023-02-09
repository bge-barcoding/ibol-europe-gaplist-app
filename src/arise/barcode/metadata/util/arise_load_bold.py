import csv
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import urllib.request
import shutil
import logging
import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from orm.common import DataSource, get_specimen_index_dict, get_barcode_index_dict
from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.barcode import Barcode
from orm.specimen import Specimen
from orm.marker import Marker
import loggers

main_logger = logging.getLogger('main')
lbd_logger = logging.getLogger('load_bold')


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}
    if not row['nucraw']:
        return

    # check taxon names
    if row['species']:
        record['taxon'] = row['species']
    else:
        if row['genus']:
            record['taxon'] = row['genus']
        else:
            lbd_logger.warning('Taxonomic identification not specific enough, skip record "%s / %s"' %
                               (row['museumid'], row['sampleid']))
            return

    # check marker name
    if row['marker_code']:
        record['marker'] = row['marker_code']
    else:
        lbd_logger.warning('The marker code is undefined, skip record "%s / %s"' %
                           (row['museumid'], row['sampleid']))
        return

    # we use process ID as external identifier because it can be resolved for inspection, other fields are for specimens
    record['sampleid'] = row['sampleid']
    record['catalognum'] = row['museumid']
    record['institution_storing'] = row['inst']
    record['identification_provided_by'] = row['taxonomist']
    record['locality'] = row['country']
    record['kingdom'] = row['kingdom']

    # distinguish between bold and ncbi
    if row['gb_acs']:
        record['external_id'] = row['gb_acs']
        lbd_logger.info("Record for %s was harvested from NCBI: %s" % (record['taxon'], record['external_id']))
    else:
        record['external_id'] = row['processid']
        lbd_logger.info("Record for %s was submitted directly: %s" % (record['taxon'], record['external_id']))

    return record


# download TSV file from BOLD 'combined' API endpoint
# not used anymore, the project uses BOLD data package instead
# see https://www.boldsystems.org/index.php/datapackages
def fetch_bold_records(geo, institutions, marker, taxon, to_file=None):
    # compose URL
    default_url = "https://www.boldsystems.org/index.php/API_Public/combined?"
    query_string = f'format=tsv&geo={geo}&institutions={institutions}&marker={marker}&taxon={taxon}'
    url = default_url + query_string.replace(' ', '+')

    # we're going to be brave/stupid and just fetch all sequences in one query.
    # For the default geo restriction (NL and surrounding countries?)
    # that means ±200,000 records, ±150MB of data, which is fine
    lbd_logger.info("Going to fetch TSV from %s" % url)

    if to_file is not None:
        with urllib.request.urlopen(url) as response, open(to_file, 'wb') as fw:
            shutil.copyfileobj(response, fw)
        return to_file

    file = urllib.request.urlopen(url)
    lbd_logger.info("Download complete")
    return file


def load_bold(input_file, kingdom=None, encoding='UTF-8'):
    specimens_created = 0
    specimens_existing = 0
    markers_created = 0
    barcodes_created = 0
    barcodes_existing = 0
    incomplete_records = 0
    fail_matching_nsr_species = 0
    unknown_taxon_record_set = set()
    specimen_index_id_dict = get_specimen_index_dict(session, Specimen)
    barcode_index_id_dict = get_barcode_index_dict(session, Barcode)

    for df in pd.read_csv(input_file, sep='\t', encoding=encoding, error_bad_lines=False, warn_bad_lines=True,
                          quoting=csv.QUOTE_NONE, chunksize=500000):
        df.fillna('', inplace=True)

        for index, row in df.iterrows():
            # initialize dict with relevant fields, next row if failed
            record = init_record_fields(row)
            if record is None:
                incomplete_records += 1
                continue

            if record['taxon'] in unknown_taxon_record_set:
                fail_matching_nsr_species += 1
                continue

            # initialize species, next row if failed
            nsr_species_node = NsrNode.match_species_node(record['taxon'], session,
                                                          kingdom=kingdom if kingdom else record['kingdom'])
            if nsr_species_node is None:
                fail_matching_nsr_species += 1
                unknown_taxon_record_set.add(record['taxon'])
                continue

            # get or create specimen
            index = f"{nsr_species_node.species_id}-{record['catalognum']}-{record['institution_storing']}-{record['identification_provided_by']}"
            if index not in specimen_index_id_dict:
                specimen, created = Specimen.get_or_create_specimen(
                    nsr_species_node.species_id, record['sampleid'], record['catalognum'],
                    record['institution_storing'], record['identification_provided_by'], record['locality'],
                    session, fast_insert=True)

                specimen_id = specimen.id
                specimens_created += 1
                specimen_index_id_dict[index] = specimen_id
            else:
                specimen_id = specimen_index_id_dict[index]
                specimens_existing += 1

            # get or create marker
            marker, created = Marker.get_or_create_marker(record['marker'], session)
            if created:
                markers_created += 1

            # set database field value
            database = DataSource.BOLD
            if row['gb_acs']:
                # does it necessary means it was harvested from NCBI?
                database = DataSource.NCBI

            index = f"{specimen_id}-{database}-{marker.id}-{record['external_id']}"
            if index not in barcode_index_id_dict:
                barcode, created = Barcode.get_or_create_barcode(specimen_id, database, marker.id, None,
                                                                 record['external_id'],
                                                                 session, fast_insert=True)
                barcodes_created += 1
                barcode_index_id_dict[index] = barcode.id
            else:
                barcodes_existing += 1

        session.commit()

    main_logger.info(f'{specimens_created=}')
    main_logger.info(f'{specimens_existing=}')
    main_logger.info(f'{markers_created=}')
    main_logger.info(f'{barcodes_created=}')
    main_logger.info(f'{barcodes_existing=}')
    main_logger.info(f'{incomplete_records=}')
    main_logger.info(f'{fail_matching_nsr_species=}')


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute('pragma journal_mode=OFF')
    cursor.execute('PRAGMA synchronous=OFF')
    cursor.execute('PRAGMA cache_size=100000')
    cursor.execute('PRAGMA temp_store = MEMORY')
    cursor.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-geo', default="Belgium|Netherlands|Germany",
                        help="Countries, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-institutions', default="", help="Institutions, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-marker', default="", help="Markers, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-tsv', help="A TSV file produced by Full Data Retrieval (Specimen + Sequence)")
    parser.add_argument('-kingdom', choices=['animalia', 'plantae', 'fungi'],
                        help="match only species / taxon in the given kingdom")
    parser.add_argument('--verbose', '-v', action='count', default=1)

    args = parser.parse_args()
    args.verbose = 70 - (10 * args.verbose) if args.verbose > 0 else 0
    [h.addFilter(loggers.LevelFilter(args.verbose)) for h in lbd_logger.handlers]

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # make session
    Session = sessionmaker(engine)
    session = Session()

    if args.tsv:
        main_logger.info('START load_bold using TSV file "%s"' % args.tsv)
        load_bold(args.tsv, kingdom=args.kingdom, encoding="UTF-8")
    else:
        raise DeprecationWarning()
        # main_logger.info('START load_bold fetch records')
        # file = fetch_bold_records(args.geo, args.institutions, args.marker)
        # load_bold(file, kingdom=args.kingdom)

    session.commit()
