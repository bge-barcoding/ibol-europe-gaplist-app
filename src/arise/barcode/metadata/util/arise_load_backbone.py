import argparse
import io
import pandas as pd
import requests
import zipfile
import os
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.nsr_species import NsrSpecies
from arise.barcode.metadata.orm.specimen import Specimen
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from arise.barcode.metadata.orm.barcode import Barcode
from arise.barcode.metadata.orm.nsr_node import NsrNode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import loggers
import logging
from collections import defaultdict

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
    session.add(NsrNode(id=2, name="All of life", parent=1, rank='life'))

    # map taxid (DwC) => species id, used to create the synonyms
    taxid_speciesid = dict()
    taxon_homonym_dict = defaultdict(list)

    for row in df.itertuples(name='Entry'):
        if row.taxonomicStatus not in ["accepted name", 'synonym']:
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

        if row.taxonomicStatus == 'synonym':
            ref_id = row.acceptedNameUsageId
            # this assumes synonyms are at the end of the file, i.e. taxid_speciesid is filled
            # do not insert synonym that are identical to the ref name
            if ref_id in taxid_speciesid and taxid_speciesid[ref_id]['canonical_name'] != row.species:
                # TODO check is the couple is already present in the synonym table before insertion
                # duplicates may arise because the subspecies Epithet is trimmed off
                synonym = NsrSynonym(name=row.species, species_id=taxid_speciesid[ref_id]['id'])
                session.add(synonym)
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
                session, id=node_counter, rank=level, species_id=None, **taxonomy
            )

            if prev_node:
                # default parent is 2 ('life' node) because kingdom nodes will not get the parent updated
                prev_node.parent = node.id
            if not created:
                if level == 'species':
                    lbb_logger.warning('species "%s" already in the database' % row.species)
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

                # keep track of the species created for mapping the synonyms
                taxid_speciesid[row.taxonID] = {'id': nsr_species.id, 'canonical_name': row.species}
                # update the node speacies_id
                node.species_id = nsr_species.id

    for k, lst in taxon_homonym_dict.items():
        if len(lst) > 1:
            # {'index': row.index, 'name': higher_taxon, 'level': level, 'taxonomy': d}
            lbb_logger.warning('taxon "%s" is duplicated:' % k)
            for e in lst:
                lbb_logger.warning('%s - "%s" (%s), taxonomy: %s' % (e['index'], e['name'], e['level'], e['taxonomy']))

    main_logger.info('Inserted nodes: %s' % node_counter)
    main_logger.info('Inserted species: %s' % species_created)


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('-endpoint', default=DEFAULT_URL, help="Input URL: NSR DwC endpoint")
    parser.add_argument('-testdb', action='store_true',
                        help="Create DB using a subset a the taxonomic backbone for testing")
    args = parser.parse_args()

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
