import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import io
import pandas as pd
import requests
import zipfile
import os

from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.barcode import Barcode
from orm.specimen import Specimen

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
from collections import defaultdict
import loggers

DEFAULT_URL = "http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr"
main_logger = logging.getLogger('main')
lbb_logger = logging.getLogger('load_backbone')


def download_and_extract(url):
    main_logger.info('download_and_extract URL:', url)
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()


def load_backbone(infile, white_filter=None):
    taxon_levels = ['species', 'genus', 'family', 'order', 't_class', 'phylum', 'kingdom']
    df = pd.read_csv(infile, sep=',')
    df.reset_index(inplace=True)  # make sure indexes pair with number of rows
    df.fillna('', inplace=True)
    df.insert(loc=14, column='species', value='')
    df.rename(columns={'class': 't_class'}, inplace=True)
    node_counter = 3  # magic number because root will be 2 with parent 1
    species_created = 0
    synonyms_created = 0
    session.add(NsrNode(id=2, name="All of life", parent=1, rank='life'))

    # map taxid (DwC) => species id, used to create the synonyms
    taxid_node_dict = dict()
    taxon_homonym_dict = defaultdict(list)

    for row in df.itertuples(name='Entry'):
        if row.taxonomicStatus != "accepted name" and row.taxonomicStatus not in NsrSynonym.taxonomic_status_set:
            lbb_logger.warning('ignore row index %s with taxonomicStatus=%s' % (row.index, row.taxonomicStatus))
            continue

        if row.taxonomicStatus == "accepted name":
            # filter rows using the white_filter
            ignore_entry = False
            for i, level in enumerate(taxon_levels):
                higher_taxon = getattr(row, level).lower()
                # ignore taxon branches using the white lists, if specified
                if white_filter and \
                   level in white_filter and \
                   higher_taxon not in [e.lower() for e in white_filter[level]]:
                    ignore_entry = True
                    break
            if ignore_entry:
                continue

        # compose binomial name, see if it exists
        try:
            row = row._replace(species=row.genus + ' ' + row.specificEpithet)
        except Exception as e:
            lbb_logger.error('Cannot create binomial name for index', row.index)
            lbb_logger.error(e)
            exit()

        if row.taxonomicStatus != "accepted name":
            ref_id = row.acceptedNameUsageId
            # this assumes synonyms & co are at the end of the file
            if ref_id in taxid_node_dict:
                synonym, created = NsrSynonym.insert_synonym(
                    session, row.species, row.taxonomicStatus, taxid_node_dict[ref_id].species_id)
                if created:
                    synonyms_created += 1

            continue

        # create the nsr_node if needed, starting from the full taxonomy
        prev_node = None
        for level in taxon_levels:  # start from species up to kingdom
            higher_taxon = getattr(row, level)
            if not higher_taxon:
                lbb_logger.warning(f'taxon is "N/A" for level "{level}", index {row.index}')

            # create a dict of the full taxonomy, from kingdom up to the current level
            taxonomy = {e: getattr(row, e) for e in taxon_levels[taxon_levels.index(level):]}

            node, created = NsrNode.get_or_create_node(
                session, node_counter, level, 0 if level == 'species' else None, **taxonomy
                # set species_id to 0 to avoid validation error, the correct species_id will be set a few lines below
            )

            if prev_node:
                # default parent is 2 ('life' node) because kingdom nodes will not get the parent updated
                prev_node.parent = node.id
            if not created:
                if level == 'species':
                    lbb_logger.info('species "%s" already in the database' % row.species)
                break

            prev_node = node
            node_counter += 1

            # keep track of taxon for homonyms
            taxon_homonym_dict[higher_taxon.lower()].append(
                {'index': row.index, 'name': higher_taxon, 'level': level, 'taxonomy': taxonomy}
            )

            if level == 'species':
                # create also the species entry
                if row.occurrenceStatus:
                    occurrence_status = row.occurrenceStatus.split(' ')[0]
                    if occurrence_status == '3cE':
                        occurrence_status = '3c'  # fix invalid status
                else:
                    occurrence_status = None
                    lbb_logger.warning('occurrence status is null for species "%s"' % row.species)

                nsr_species = NsrSpecies(canonical_name=row.species, occurrence_status=occurrence_status)
                session.add(nsr_species)
                session.flush()
                species_created += 1

                # update the node species_id
                node.species_id = nsr_species.id
                # keep track of the species created for mapping the synonyms
                taxid_node_dict[row.taxonID] = node

    for k, lst in taxon_homonym_dict.items():
        if len(lst) > 1:
            # {'index': row.index, 'name': higher_taxon, 'level': level, 'taxonomy': d}
            lbb_logger.warning('taxon "%s" is duplicated:' % k)
            for e in lst:
                lbb_logger.warning('%s - "%s" (%s), taxonomy: %s' % (e['index'], e['name'], e['level'], e['taxonomy']))

    main_logger.info('Inserted nodes: %s' % node_counter)
    main_logger.info('Inserted species: %s' % species_created)
    main_logger.info('Inserted synonyms: %s' % synonyms_created)


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-endpoint', default=DEFAULT_URL, help="Input URL: NSR DwC endpoint")
    parser.add_argument('-testdb', action='store_true',
                        help="Create DB using a subset a the taxonomic backbone for testing")
    parser.add_argument('--verbose', '-v', action='count', default=1)

    args = parser.parse_args()
    args.verbose = 70 - (10 * args.verbose) if args.verbose > 0 else 0
    [h.addFilter(loggers.LevelFilter(args.verbose)) for h in lbb_logger.handlers]

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # make session
    Session = sessionmaker(engine)
    session = Session()

    # download and extract DwC zip file
    url = args.endpoint
    if 'Taxa.txt' not in os.listdir(os.getcwd()):
        download_and_extract(args.endpoint)

    filters = None
    main_logger.info('START load_backbone using Taxa.txt')
    if args.testdb:
        filters = {
            # 'order': ['Diptera'],
            'family': ['FRINGILLIDAE', 'Plantaginaceae'],
            # 'genus': ['Platycheirus']
        }
        main_logger.info('taxonomy filter used:', filters)

    # read 'Taxa.txt' from zip as a data frame
    load_backbone('Taxa.txt', white_filter=filters)
    session.commit()
