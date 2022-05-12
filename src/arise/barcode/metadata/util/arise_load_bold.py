import argparse
import urllib.request
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
from arise.barcode.metadata.orm.imports import *
from sqlalchemy.exc import NoResultFound
from taxon_parser import UnparsableNameException

# process command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
parser.add_argument('-geo', default="Belgium|Netherlands|Germany", help="Countries, quoted and pipe-separated: 'a|b|c'")
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
    if row['genbank_accession'] == row['genbank_accession'] :
        record['external_id'] = row['genbank_accession']
        logging.warning("Record for %s was harvested from NCBI: %s" % (record['taxon'], record['external_id']))
    else:
        record['external_id'] = row['processid']
        logging.debug("Record for %s was submitted directly: %s" % (record['taxon'], record['external_id']))

    return record


# create data frame either from URL or from static file
def read_tsv_df():
    df = None

    if args.tsv is not None:
        df = pd.read_csv(args.tsv, sep='\t', encoding="ISO-8859-1")
    else:

        # compose URL
        default_url = "https://www.boldsystems.org/index.php/API_Public/combined?"
        query_string = 'format=tsv&geo=' + args.geo + '&institutions=' + args.institutions + '&marker=' + args.marker
        url = default_url + query_string.replace(' ', '+')

        # we're going to be brave/stupid and just fetch all sequences in one query. For the default geo restriction
        # that means ±200,000 records, ±150MB of data, which is fine
        logging.info("Going to fetch TSV from %s" % (url))
        file = urllib.request.urlopen(url)
        logging.info("Download complete")
        df = pd.read_csv(file, sep='\t')

    return df


# find or create species for specimen
# TODO: maybe this should move to nsr_species.py as a reusable factory method
def match_species(taxon):
    # parse species name
    name_parser = TaxonParser(taxon)
    nsr_species = None
    try:
        parsed = name_parser.parse()
        cleaned = parsed.canonicalNameWithoutAuthorship()

        # find exact species match
        nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == cleaned).one()
        logging.info("Matched NSR species %i (PK)" % nsr_species.species_id)
    except NoResultFound:
        try:

            # find genus match
            nsr_node = session.query(NsrNode).filter(NsrNode.name == cleaned, NsrNode.rank == 'genus').one()
            logging.info("No species match but found NSR genus node %i (PK)" % nsr_node.id)

            # find or create sp node
            sp_name = cleaned + ' sp.'
            nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == sp_name).first()
            if nsr_species is None:
                nsr_species = NsrSpecies(canonical_name=sp_name)
                session.add(nsr_species)
                nsr_node = NsrNode(name=sp_name, parent=nsr_node.id, rank='species', species_id=nsr_species.species_id)
                session.add(nsr_node)

        except NoResultFound:
            logging.debug("Taxon %s not found anywhere in NSR topology" % cleaned)
    except AttributeError:
        logging.debug("Problem parsing taxon name %s" % taxon)
    except UnparsableNameException:
        logging.debug("Problem parsing taxon name %s" % taxon)

    return nsr_species


# find or create specimen object
# TODO: maybe this should move to specimen.py as a reusable factory method
def match_specimen(species_id, catalognum, institution_storing, identification_provided_by):
    specimen = session.query(Specimen).filter(Specimen.species_id == species_id, Specimen.catalognum == catalognum,
                                              Specimen.institution_storing == institution_storing,
                                              Specimen.identification_provided_by == identification_provided_by).first()
    if specimen is None:
        specimen = Specimen(species_id=species_id, catalognum=catalognum, institution_storing=institution_storing,
                            identification_provided_by=identification_provided_by)
        session.add(specimen)
    return specimen


# find or create marker object
# TODO: maybe this should move to marker.py as a reusable factory method
def match_marker(marker_name):
    marker = session.query(Marker).filter(Marker.marker_name == marker_name).first()
    if marker is None:
        marker = Marker(marker_name=marker_name)
        session.add(marker)
    return marker


# main function
def main():
    df = read_tsv_df()
    for index, row in df.iterrows():

        # initialize dict with relevant fields, next row if failed
        record = init_record_fields(row)
        if record is None:
            continue

        # initialize species, next row if failed
        nsr_species = match_species(record['taxon'])
        if nsr_species is None:
            continue

        # get or create specimen
        specimen = match_specimen(nsr_species.species_id, record['catalognum'], record['institution_storing'],
                                  record['identification_provided_by'])

        # get or create marker
        marker = match_marker(record['marker'])

        # set database field value
        database = DataSource.BOLD
        if row['genbank_accession'] is not None:
            database = DataSource.NCBI

        barcode = Barcode(specimen_id=specimen.specimen_id, database=database, marker_id=marker.marker_id,
                          external_id=record['external_id'])
        session.add(barcode)
    session.commit()
main()
