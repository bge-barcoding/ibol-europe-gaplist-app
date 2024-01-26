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

from sqlalchemy import create_engine, event, func, or_, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
import logging
from collections import defaultdict
import loggers
from datetime import datetime


DEFAULT_URL = "http://api.biodiversitydata.nl/v2/taxon/dwca/getDataSet/nsr"
main_logger = logging.getLogger('main')
lbb_logger = logging.getLogger('load_backbone')


def download_and_extract(url):
    main_logger.info(f'download_and_extract URL: {url}')
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()


def load_backbone(infile, white_filter=None):
    taxon_levels = ['species', 'genus', 'family', 'order', 't_class', 'phylum', 'kingdom']
    df = pd.read_csv(infile, sep=',')
    df.reset_index(inplace=True)  # make sure indexes pair with number of rows
    df.fillna('', inplace=True)
    # create a column species to store the binomial name
    df.insert(loc=14, column='species', value='')
    # need to rename the attr 'class' otherwise is will clash with reversed keywords, see methode get_or_create_node
    df.rename(columns={'class': 't_class'}, inplace=True)
    node_counter = 3  # magic number because root will be 2 with parent 1
    species_created = 0
    synonyms_created = 0
    session.add(NsrNode(id=2, name="All of life", parent=1, rank='life'))

    # map taxid (DwC) => species id, used to create the synonyms
    taxid_node_dict = dict()
    taxon_homonym_dict = defaultdict(list)
    ignored_entries = 0
    ignored_entries_kingdom = 0
    ignored_synonyms = 0
    existing_species = 0
    existing_synonyms = 0
    occ_status_species_dict = {
        "0": 0, "0a": 0,
        "1": 0, "1a": 0, "1b": 0,
        "2": 0, "2a": 0, "2b": 0, "2c": 0, "2d": 0,
        "3a": 0, "3b": 0, "3c": 0, "3d": 0,
        "4": 0,
        "Null": 0,
    }
    line_count = 0
    # a list to keep to of which species name and ID was added, or discarded (and why)
    # the list is written in a file that is used by the get_clb_issues_priority.py script
    species_name_status = []

    for row in df.itertuples(name='Entry'):
        line_count += 1
        if row.kingdom == "":
            # ignore *many* rows with only genus and species information
            ignored_entries_kingdom += 1
            ignored_entries += 1
            species_name_status.append([row.taxonID, "", "DISCARDED", "NO KINGDOM"])
            continue
        if row.taxonomicStatus != "accepted name" and row.taxonomicStatus not in NsrSynonym.taxonomic_status_set:
            # synonyms
            lbb_logger.warning('ignore row index %s with taxonomicStatus=%s' % (row.index, row.taxonomicStatus))
            ignored_synonyms += 1
            species_name_status.append([row.taxonID, "", "DISCARDED", "IGNORED SYNONYM"])
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
                ignored_entries += 1
                continue

        # compose binomial name
        try:
            row = row._replace(species=row.genus + ' ' + row.specificEpithet)
        except Exception as e:
            lbb_logger.error('Cannot create binomial name for index', row.index)
            lbb_logger.error(e)
            exit()

        if row.taxonomicStatus != "accepted name":
            if row.infraspecificEpithet:
                lbb_logger.warning(f"ignore synonym '{row.species}' with infraspecificEpithet")
                ignored_synonyms += 1
                species_name_status.append([row.taxonID, row.species, "DISCARDED", "IGNORED SYNONYM (INFRA)"])
                continue
            ref_id = row.acceptedNameUsageId
            # this assumes synonyms & co are at the end of the file
            # if red_id in taxid_node_dict, it means the species node has been created,
            # and that the current row is a synomyn/basionym/etc... linked to that node
            if ref_id in taxid_node_dict:
                synonym, created = NsrSynonym.insert_synonym(
                    session, row.species, row.taxonomicStatus, taxid_node_dict[ref_id].species_id)
                if created:
                    synonyms_created += 1
                else:
                    existing_synonyms += 1
            species_name_status.append([row.taxonID, row.species, "ADDED", "AS SYNONYM"])
            continue

        # create the nsr_nodes if the node do not already exist,
        # insert each level as a node, starting from "species"
        prev_node = None
        for level in taxon_levels:  # start from species up to kingdom
            higher_taxon = getattr(row, level)
            if not higher_taxon:
                lbb_logger.warning(f'taxon is "N/A" for level "{level}", index {row.index}')
                print(f'taxon is name is empty for level "{level}", index {row.index}')

            # create a dict of the full taxonomy, from kingdom up to the current level
            # if one level of the taxonomy is different from the existing nodes in the DB,
            # a new node will be created!
            taxonomy = {e: getattr(row, e) for e in taxon_levels[taxon_levels.index(level):]}

            node, created = NsrNode.get_or_create_node(
                session, node_counter, level, 0 if level == 'species' else None, **taxonomy
                # species must have a valid species_id (should be id that references the NsrSpecies table)
                # see nsr_node.validates_fields
                # but the species entry is not yet created, so we use 0 temporary
                # the correct species_id will be set a few lines below once the nsr_species entry is created
            )

            if prev_node:
                # default parent is 2 ('life' node) because kingdom nodes will not get the parent updated
                prev_node.parent = node.id
            if not created:
                if level == 'species':
                    existing_species += 1
                    lbb_logger.info('species "%s" already in the database' % row.species)
                    species_name_status.append([row.taxonID, row.species, "DISCARDED", "ALREADY INSERTED"])
                break

            prev_node = node
            node_counter += 1

            # store each taxon name to detect homonyms
            taxon_homonym_dict[higher_taxon.lower()].append(
                {'index': row.index, 'name': higher_taxon, 'level': level, 'taxonomy': taxonomy}
            )

            if level == 'species':
                # create also the species entry
                if row.occurrenceStatus:
                    occurrence_status = row.occurrenceStatus.split(' ')[0]
                    if occurrence_status == '3cE':
                        occurrence_status = '3c'  # fix invalid status
                    occ_status_species_dict[occurrence_status] += 1
                else:
                    occurrence_status = None
                    occ_status_species_dict["Null"] += 1
                    lbb_logger.warning('occurrence status is null for species "%s"' % row.species)

                nsr_species = NsrSpecies(canonical_name=row.species, occurrence_status=occurrence_status)
                session.add(nsr_species)
                session.flush()
                species_created += 1
                species_name_status.append([row.taxonID, row.species, "ADDED", ""])

                # update the node species_id
                node.species_id = nsr_species.id
                # keep track of the species created for mapping the synonyms
                taxid_node_dict[row.taxonID] = node

    homonyms = 0
    for k, lst in taxon_homonym_dict.items():
        # log the homonyms
        if len(lst) > 1:
            if k:
                homonyms += 1
            # {'index': row.index, 'name': higher_taxon, 'level': level, 'taxonomy': d}
            lbb_logger.warning('taxon "%s" is duplicated:' % k)
            for e in lst:
                lbb_logger.warning('%s - "%s" (%s), taxonomy: %s' % (e['index'], e['name'], e['level'], e['taxonomy']))

    main_logger.info('Inserted nodes: %s' % node_counter)
    main_logger.info('Inserted species: %s' % species_created)
    main_logger.info('Inserted synonyms: %s' % synonyms_created)
    main_logger.info('Ignored entries: %s' % ignored_entries)
    main_logger.info('Ignored synonyms: %s' % ignored_synonyms)
    main_logger.info('Existing species: %s' % existing_species)
    main_logger.info('Existing synonyms: %s' % existing_synonyms)

    ranks_node_counts = {}
    query = session.query(NsrNode.rank, func.count()).group_by(NsrNode.rank)
    for r, c in query.all():
        main_logger.info('Inserted %s %s' % (c, r))
        ranks_node_counts[r] = c
        if r == "species":
            assert c == species_created

    # get additional stats
    species_per_kingdom_dict = \
        {k: c for (k, c) in session.query(
            NsrNode.kingdom, func.count()).where(NsrNode.rank == "species").group_by(NsrNode.kingdom).all()}
    sp_species = session.query(func.count()).where(NsrNode.name.like("% sp.")).first()[0]

    # empty node global + per kingdom
    empty_name_nodes = (session.query(func.count()).
             where(and_(or_(NsrNode.phylum == "", NsrNode.t_class == "", NsrNode.order == "", NsrNode.family == "",
                   NsrNode.order == "", NsrNode.species == ""), NsrNode.name == ""))).first()[0]
    query = (session.query(NsrNode.kingdom, func.count(NsrNode.kingdom)).
             where(and_(or_(NsrNode.phylum == "", NsrNode.t_class == "", NsrNode.order == "", NsrNode.family == "",
                       NsrNode.order == "", NsrNode.species == ""), NsrNode.rank == "species")).group_by(NsrNode.kingdom))
    species_missing_taxo_kingdom = {k: c for k, c in query.all()}

    # undetermined ranks
    nodes_incertae_sedis_taxo = 0
    nodes_unassigned_taxo = 0
    unsure_name_kingdom = {}
    for s in ["Incertae sedis", "[unassigned]"]:
        res = (session.query(func.count()).
                 where(or_(NsrNode.phylum.like(f"%{s}%"), NsrNode.t_class.like(f"%{s}%"), NsrNode.order.like(f"%{s}%"),
                           NsrNode.family.like(f"%{s}%"), NsrNode.order.like(f"%{s}%"), NsrNode.species.like(f"%{s}%"))
                       ))
        if s == "Incertae sedis":
            nodes_incertae_sedis_taxo = res.first()[0]
        else:
            nodes_unassigned_taxo = res.first()[0]

        query = (session.query(NsrNode.kingdom, func.count()).
                 where(and_(or_(NsrNode.phylum.like(f"%{s}%"), NsrNode.t_class.like(f"%{s}%"), NsrNode.order.like(f"%{s}%"),
                           NsrNode.family.like(f"%{s}%"), NsrNode.order.like(f"%{s}%"), NsrNode.species.like(f"%{s}%"))
                       , NsrNode.rank == "species")).group_by(NsrNode.kingdom))
        unsure_name_kingdom[s] = {k: c for k, c in query.all()}

    with open("nsr_backbone_stats_%s.tsv" % "{:%b_%d_%Y}".format(datetime.now()), "w") as st:
        st.write(f"lines_in_file\t{line_count}\n")
        st.write(f"inserted_nodes\t{node_counter}\n")
        for r in ["kingdom", "phylum", "class", "order", "family", "genus", "species"]:
            st.write(f"inserted_{r}\t{ranks_node_counts[r]}\n")
        for k in sorted(species_per_kingdom_dict.keys()):
            st.write(f"inserted_species_{k}\t{species_per_kingdom_dict[k]}\n")
        st.write(f"ignored_entries\t{ignored_entries}\n")
        st.write(f"ignored_entries_empty_kingdom\t{ignored_entries_kingdom}\n")
        st.write(f"existing_species\t{existing_species}\n")

        st.write(f"inserted_synonyms\t{synonyms_created}\n")
        st.write(f"ignored_synonyms\t{ignored_synonyms}\n")
        st.write(f"existing_synonyms\t{existing_synonyms}\n")

        st.write(f"homonyms\t{homonyms}\n")
        st.write(f"nodes_without_name\t{empty_name_nodes}\n")
        for k in sorted(species_per_kingdom_dict.keys()):
            if k in species_missing_taxo_kingdom:
                st.write(f"species_incomplete_taxo_{k}\t{species_missing_taxo_kingdom[k]}\n")
            else:
                st.write(f"species_incomplete_taxo_{k}\t0\n")

        st.write(f"nodes_Incertae_sedis_taxo\t{nodes_incertae_sedis_taxo}\n")
        for k in sorted(species_per_kingdom_dict.keys()):
            if k in unsure_name_kingdom["Incertae sedis"]:
                st.write(f"species_Incertae_sedis_{k}\t{unsure_name_kingdom['Incertae sedis'][k]}\n")
            else:
                st.write(f"species_Incertae_sedis_{k}\t0\n")

        st.write(f"nodes_Unassigned_taxo\t{nodes_unassigned_taxo}\n")
        for k in sorted(species_per_kingdom_dict.keys()):
            if k in unsure_name_kingdom["[unassigned]"]:
                st.write(f"species_Unassigned_{k}\t{unsure_name_kingdom['[unassigned]'][k]}\n")
            else:
                st.write(f"species_Unassigned_{k}\t0\n")

        st.write(f"sp_name_species\t{sp_species}\n")
        for occ, count in occ_status_species_dict.items():
            st.write(f"occ_status_{occ}\t{count}\n")

    with open("species_names_added_status.tsv", "w") as fw:
        fw.write("taxon_id\tspecies_name\tstatus\treason\n")
        for reason in species_name_status:
            fw.write('%s\n' % "\t".join(reason))


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute('pragma journal_mode=OFF')
    cursor.execute('PRAGMA synchronous=OFF')
    cursor.execute('PRAGMA cache_size=100000')
    cursor.execute('PRAGMA temp_store = MEMORY')
    cursor.close()


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
