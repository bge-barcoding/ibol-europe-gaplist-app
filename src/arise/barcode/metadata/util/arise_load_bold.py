import argparse
import urllib.request
import shutil
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from taxon_parser import TaxonParser
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.marker import Marker
from arise.barcode.metadata.orm.barcode import Barcode
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.imports import *
from sqlalchemy.exc import NoResultFound
from taxon_parser import UnparsableNameException


# initializes a dict with the fields that should go in barcode and specimen table, or None if any of the checks fail
def init_record_fields(row):
    record = {}

    # IEEE specs say NaN's can not be equal, so that's how we do the checks for missing values

    # check taxon names
    if row['species_name'] == row['species_name']:
        record['taxon'] = row['species_name']
    else:
        if row['genus_name'] == row['genus_name']:
            record['taxon'] = row['genus_name']
        else:
            logging.debug("Taxonomic identification not specific enough, skipping")
            return None

    # check marker name
    if row['markercode'] == row['markercode']:
        record['marker'] = row['markercode']
    else:
        logging.debug("The marker code is undefined, skipping")
        return None

    # we use process ID as external identifier because it can be resolved for inspection, other fields are for specimens
    record['catalognum'] = row['catalognum']
    record['institution_storing'] = row['institution_storing']
    record['identification_provided_by'] = row['identification_provided_by']

    # distinguish between bold and ncbi
    if row['genbank_accession'] == row['genbank_accession']:
        record['external_id'] = row['genbank_accession']
        logging.warning("Record for %s was harvested from NCBI: %s" % (record['taxon'], record['external_id']))
    else:
        record['external_id'] = row['processid']
        logging.debug("Record for %s was submitted directly: %s" % (record['taxon'], record['external_id']))

    return record


# download TSV file from BOLD 'combined' API endpoint
def fetch_bold_records(geo, institutions, marker, taxon, to_file=None):
    # compose URL
    default_url = "https://www.boldsystems.org/index.php/API_Public/combined?"
    query_string = f'format=tsv&geo={geo}&institutions={institutions}&marker={marker}&taxon={taxon}'
    url = default_url + query_string.replace(' ', '+')

    # we're going to be brave/stupid and just fetch all sequences in one query. For the default geo restriction
    # that means ±200,000 records, ±150MB of data, which is fine
    logging.info("Going to fetch TSV from %s" % url)

    if to_file is not None:
        with urllib.request.urlopen(url) as response, open(to_file, 'wb') as fw:
            shutil.copyfileobj(response, fw)
        return to_file

    file = urllib.request.urlopen(url)
    logging.info("Download complete")
    return file


def load_bold(input_file, encoding='utf-8'):
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
        specimen = Specimen.get_or_create_specimen(nsr_species.species_id, record['catalognum'], record['institution_storing'],
                                                   record['identification_provided_by'], session)

        # get or create marker
        marker = Marker.get_or_create_marker(record['marker'], session)

        # set database field value
        database = DataSource.BOLD
        if row['genbank_accession'] == row['genbank_accession']:
            database = DataSource.NCBI

        barcode = Barcode(specimen_id=specimen.specimen_id, database=database, marker_id=marker.marker_id,
                          external_id=record['external_id'])
        session.add(barcode)


if __name__ == '__main__':

    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-geo', default="Belgium|Netherlands|Germany",
                        help="Countries, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-institutions', default="", help="Institutions, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-marker', default="", help="Markers, quoted and pipe-separated: 'a|b|c'")
    parser.add_argument('-tsv', help="A TSV file produced by Full Data Retrieval (Specimen + Sequence)")
    parser.add_argument('--verbose', '-v', action='count', default=1)
    args = parser.parse_args()

    # configure logging
    args.verbose = 70 - (10 * args.verbose) if args.verbose > 0 else 0
    logging.basicConfig(level=args.verbose)

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # make session
    Session = sessionmaker(engine)
    session = Session()

    if args.tsv:
        load_bold(args.tsv, encoding="ISO-8859-1")
    else:
        file = fetch_bold_records(args.geo, args.institutions, args.marker)
        load_bold(file)
    session.commit()
