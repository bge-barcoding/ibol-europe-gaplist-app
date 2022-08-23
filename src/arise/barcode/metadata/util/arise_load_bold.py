import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import urllib.request
import shutil
import logging
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
lbd_logger = logging.getLogger('load_bold')


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}
    # IEEE specs say NaN's can not be equal, so that's how we do the checks for missing values

    # check taxon names
    if row['species_name']:
        record['taxon'] = row['species_name']
    else:
        if row['genus_name']:
            record['taxon'] = row['genus_name']
        else:
            lbd_logger.warning('Taxonomic identification not specific enough, skip record "%s"' % row['catalognum'])
            return None

    # check marker name
    if row['markercode']:
        record['marker'] = row['markercode']
    else:
        lbd_logger.warning('The marker code is undefined, skip record "%s"' % row['catalognum'])
        return None

    # we use process ID as external identifier because it can be resolved for inspection, other fields are for specimens
    record['catalognum'] = row['catalognum'] if row['catalognum'] else row['sampleid']
    record['institution_storing'] = row['institution_storing']
    record['identification_provided_by'] = row['identification_provided_by']
    record['locality'] = row['country']

    # distinguish between bold and ncbi
    if row['genbank_accession']:
        record['external_id'] = row['genbank_accession']
        lbd_logger.info("Record for %s was harvested from NCBI: %s" % (record['taxon'], record['external_id']))
    else:
        record['external_id'] = row['processid']
        lbd_logger.info("Record for %s was submitted directly: %s" % (record['taxon'], record['external_id']))

    return record


# download TSV file from BOLD 'combined' API endpoint
def fetch_bold_records(geo, institutions, marker, taxon, to_file=None):
    # compose URL
    default_url = "https://www.boldsystems.org/index.php/API_Public/combined?"
    query_string = f'format=tsv&geo={geo}&institutions={institutions}&marker={marker}&taxon={taxon}'
    url = default_url + query_string.replace(' ', '+')

    # we're going to be brave/stupid and just fetch all sequences in one query. For the default geo restriction
    # that means ±200,000 records, ±150MB of data, which is fine
    lbd_logger.info("Going to fetch TSV from %s" % url)

    if to_file is not None:
        with urllib.request.urlopen(url) as response, open(to_file, 'wb') as fw:
            shutil.copyfileobj(response, fw)
        return to_file

    file = urllib.request.urlopen(url)
    lbd_logger.info("Download complete")
    return file


def load_bold(input_file, kingdom=None, encoding='utf-8'):
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

        # initialize species, next row if failed
        nsr_species_node = NsrNode.match_species_node(record['taxon'], session, kingdom=kingdom)
        if nsr_species_node is None:
            fail_matching_nsr_species += 1
            continue

        # get or create specimen
        specimen, created = Specimen.get_or_create_specimen(
            nsr_species_node.species_id, record['catalognum'], record['institution_storing'],
            record['identification_provided_by'], record['locality'], session)
        if created:
            specimens_created += 1

        # get or create marker
        marker, created = Marker.get_or_create_marker(record['marker'], session)
        if created:
            markers_created += 1

        # set database field value
        database = DataSource.BOLD
        if row['genbank_accession'] == row['genbank_accession']:
            database = DataSource.NCBI

        barcode, created = Barcode.get_or_create_barcode(specimen.id, database, marker.id, None, record['external_id'],
                                                         session)
        if created:
            barcodes_created += 1

    main_logger.info(f'{specimens_created=}')
    main_logger.info(f'{markers_created=}')
    main_logger.info(f'{barcodes_created=}')
    main_logger.info(f'{incomplete_records=}')
    main_logger.info(f'{fail_matching_nsr_species=}')


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
        load_bold(args.tsv, kingdom=args.kingdom, encoding="ISO-8859-1")
    else:
        main_logger.info('START load_bold fetch records')
        file = fetch_bold_records(args.geo, args.institutions, args.marker)
        load_bold(file, kingdom=args.kingdom)
    session.commit()
