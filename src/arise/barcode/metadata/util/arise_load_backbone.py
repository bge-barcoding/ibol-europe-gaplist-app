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

DEFAULT_URL = "http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr"
main_logger = logging.getLogger('main')
lbb_logger = logging.getLogger('load_backbone')

def download_and_extract(url):
    main_logger.info('download_and_extract URL:', url)
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()


def load_backbone(infile, white_filter=None):
    taxon_levels = ['genus', 'family', 'order', 'class', 'phylum', 'kingdom']
    df = pd.read_csv(infile, sep=',')
    df.reset_index(inplace=True)  # make sure indexes pair with number of rows
    df.fillna('', inplace=True)
    node_counter = 3  # magic number because root will be 2 with parent 1
    session.add(NsrNode(id=2, name="All of life", parent=1, rank='life'))

    # dict to keep tract of full taxonomy strings, see details below
    taxonomy_dict = dict()

    # dict to keep tract of taxon name, and detect homonyms
    taxon_level_dict = dict()

    # map taxid (DwC) => species id, used to create the synonyms
    taxid_speciesid = dict()

    for index, row in df.iterrows():
        if row["taxonomicStatus"] not in ["accepted name", 'synonym']:
            lbb_logger.warning('ignore row index %s with taxonomicStatus=%s' % (index, row["taxonomicStatus"]))
            continue

        if row["taxonomicStatus"] == "accepted name":
            # filter rows using the white_filter
            ignore_entry = False
            for level in taxon_levels:
                higher_taxon = row[level].lower()
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
            binomial = row['genus'] + ' ' + row['specificEpithet']
        except:
            lbb_logger.error('cannot create binomial name for index', index)
            exit()

        if row["taxonomicStatus"] == 'synonym':
            ref_id = row.acceptedNameUsageId
            # this assumes synonyms are at the end of the file, i.e. taxid_speciesid is filled
            # do not insert synonym that are identical to the ref name
            if ref_id in taxid_speciesid and taxid_speciesid[ref_id]['canonical_name'] != binomial:
                # TODO check is the couple is already present in the synonym taable before insertion
                # duplicates may arise because the subspecies Epithet is trimmed off
                synonym = NsrSynonym(synonym_name=binomial, species_id=taxid_speciesid[ref_id]['id'])
                session.add(synonym)
            continue

        nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == binomial).all()
        if not nsr_species:
            # not seen this before, insert
            nsr_species = NsrSpecies(canonical_name=binomial)
            session.add(nsr_species)
            session.flush()
            taxid_speciesid[row.taxonID] = {'id': nsr_species.species_id, 'canonical_name': binomial}

            # check if already inserted in backbone, presumably by way of another pruned infraspecific epithet
            child = session.query(NsrNode).filter(NsrNode.name == binomial).first()
            if child is None:
                child = NsrNode(id=node_counter, species_id=nsr_species.species_id, name=binomial, rank='species')
                session.add(child)
                session.flush()
                node_counter += 1
            else:
                # we've seen this species before (prob from another subspecies), move onto the next row
                lbb_logger.warning('species "%s"  already in the database' % binomial)
                continue

            # iterate over higher levels, see if they already exist
            for level in taxon_levels:
                higher_taxon = row[level]

                # compute a string representing the full taxonomy
                #
                ht_full_taxon = '-'.join([row[e] for e in taxon_levels[taxon_levels.index(level):]])

                node = False
                # check if the node already exists by comparing full taxonomy strings
                # this allows to insert node with identical name
                if higher_taxon in taxonomy_dict:
                    if ht_full_taxon in taxonomy_dict[higher_taxon]:
                        # this taxon was already encountered, get the corresponding node
                        node = taxonomy_dict[higher_taxon][ht_full_taxon]
                    else:
                        pass
                        # the taxon name was already encountered, but the full taxonomy is different
                        # = homonym

                if not higher_taxon:
                    lbb_logger.warning(f'taxon is "N/A" for level "{level}", index {index}')
                    # "N/A" are replace with empty string
                    # the nodes are inserted with an empty name
                    # (but if the full taxonomy string exist in the map, the nod is reused)
                    if not node:
                        lbb_logger.warning(f'New taxon "" ("N/A"), level "{level}", index {index} is inserted')
                else:
                    # check for homonym
                    if higher_taxon not in taxon_level_dict:
                        taxon_level_dict[higher_taxon] = level
                    elif taxon_level_dict[higher_taxon] != level:
                        # check if the taxon name was used at another taxonomic level
                        lbb_logger.warning(f'"{higher_taxon}" is level "{level}" but was "{taxon_level_dict[higher_taxon]},'
                              f' index {index}"')

                if not node:
                    # instantiate a parent node, graft child onto it, move up the hierarchy
                    parent = NsrNode(id=node_counter, name=higher_taxon, rank=level, parent=2)
                    session.add(parent)
                    session.flush()
                    child.parent = parent.id
                    child = parent
                    node_counter += 1
                    if parent.name not in taxonomy_dict:
                        taxonomy_dict[parent.name] = {}
                    taxonomy_dict[parent.name][ht_full_taxon] = parent
                    if level == "genus":
                        pass
                        # TRY SKIP THIS
                        # if a new genus is inserted, link the current species to it via the 'genus_id' attribute
                        # FIXME currently it is set only for "[genus] sp." species
                        # nsr_species.genus_id = parent.id
                else:
                    # we've already reached and instantiated this higher taxon via another path, graft onto that
                    child.parent = node.id
                    break
    lbb_logger.warning('Inserted nodes: %s' % node_counter)


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
