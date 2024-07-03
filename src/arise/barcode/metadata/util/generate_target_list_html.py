import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import shutil
import pandas as pd
import json
from sqlalchemy import create_engine, func, distinct, union_all, and_
from sqlalchemy.orm import sessionmaker
from orm.common import RANK_ORDER, DataSource
from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.barcode import Barcode
from orm.specimen import Specimen
from compute_barcode_coverage import add_count_features, valid_occ_statuses
from datetime import datetime

rank_hierarchy = RANK_ORDER[1:]


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('--filter-species', action="store_true",
                        help="Do not include in the coverage table"
                             "<genus> sp. species created by the pipeline or present in NSR AND "
                             "species with occurrence status not being 0a, 1 or 2")
    args = parser.parse_args()

    # create connection/engine to database file
    dbfile = args.db
    engine = create_engine(f'sqlite:///{dbfile}', echo=False)

    # load data during session
    Session = sessionmaker(engine)
    session = Session()
    nsr_root = NsrNode.get_root(session)

    max_rank = 'species'
    ete_tree_of_life = nsr_root.to_ete(session, until_rank=max_rank, remove_empty_rank=True,
                                       remove_incertae_sedis_rank=True)

    coverage_table = add_count_features(session, ete_tree_of_life, max_rank,
                                                                 args.filter_species)
    # convert the table to json using pandas
    df = pd.DataFrame(coverage_table, columns=['nsr_id'] + rank_hierarchy +
                                                               ['rank', 'total_sp', 'sp_w_bc', 'total_bc', 'coverage',
                                                                'arise_bc', 'coverage_arise', 'not_arise_bc',
                                                                'coverage_not_arise', 'locality', 'occ_status'])
    # add id column for slickgrid dataview
    df.insert(0, 'id', range(1, 1 + len(df)))
    df = df.fillna("")
    df[['coverage', 'coverage_arise', 'coverage_not_arise']] = \
        df[['coverage', 'coverage_arise', 'coverage_not_arise']].apply(lambda x: round(x, 1))
    shutil.copyfile('html/target_list_template.html', 'html/target_list.html')

    # build a list a distinct localities, remove empty and "Unknown"
    localities = set()
    df['locality'].transform(lambda x: [localities.add(e.strip()) for e in x.split(';')
                                        if e.strip() and e.strip() != "Unknown"])

    # export the coverage table to tsv
    df.to_csv("coverage_table_%s.tsv" % "{:%b_%d_%Y}".format(datetime.now()), sep='\t', quoting=0, index=False)
    # remove the nsr_id column, not needed for the HTML file
    df = df.drop('nsr_id', axis=1)

    # compute the overall completeness
    df_kingdom = df[df['rank'] == 'kingdom']
    # but use only three kingdoms!
    overall_completeness = df_kingdom[df_kingdom.kingdom.isin(['Animalia', 'Plantae', 'Fungi'])]['coverage'].mean()

    html = open('html/target_list.html').read() \
        .replace('##COMPLNES##', '{0:3.1f}'.format(overall_completeness)) \
        .replace('"##LOCALITIES##"', json.dumps(sorted(list(localities)))) \
        .replace('"##DATA##"', df.to_json(orient="records"))

    # get the number of entries/nodes per rank and replace them in the stats modal windons
    for rank in RANK_ORDER[1:]:
        html = html.replace('##%s##' % rank, str(len(df[df['rank'] == rank])))

    # compute stats
    if args.filter_species:
        stats = list(session.query(
            func.count(distinct(Specimen.id)),
            func.count(distinct(Barcode.id)),
            func.count(distinct(Barcode.marker_id)),
        ).join(Barcode, Barcode.specimen_id == Specimen.id)
         .join(NsrSpecies, NsrSpecies.id == Specimen.species_id)
            .filter(
                and_(
                    NsrSpecies.canonical_name.not_like("% sp."),
                    NsrSpecies.occurrence_status.in_(valid_occ_statuses),
                )
            ).one())
        stats += list(session.query(
            func.count(distinct(Specimen.id)),
            func.count(distinct(Barcode.id)),
            func.count(distinct(Barcode.marker_id)),
        ).join(Barcode, Barcode.specimen_id == Specimen.id)
         .join(NsrSpecies, NsrSpecies.id == Specimen.species_id)
                      .filter(
                            and_(
                                NsrSpecies.canonical_name.not_like("% sp."),
                                NsrSpecies.occurrence_status.in_(valid_occ_statuses),
                            )
                        )
                      .filter(Barcode.database.in_([DataSource.NATURALIS, DataSource.WFBI])).one())
    else:
        stats = list(session.query(
            func.count(distinct(Specimen.id)),
            func.count(distinct(Barcode.id)),
            func.count(distinct(Barcode.marker_id)),
        ).join(Barcode).one())
        stats += list(session.query(
            func.count(distinct(Specimen.id)),
            func.count(distinct(Barcode.id)),
            func.count(distinct(Barcode.marker_id)),
        ).join(Barcode).filter(Barcode.database.in_([DataSource.NATURALIS, DataSource.WFBI])).one())

    for v, n in zip(['sc', 'bc', 'mc', 'asc', 'abc', 'amc'], stats):
        html = html.replace('##%s##' % v, str(n))

    with open('html/target_list.html', 'w') as fw:
        fw.write(html)
