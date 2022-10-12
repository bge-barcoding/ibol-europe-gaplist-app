import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import shutil
import pandas as pd
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.common import RANK_ORDER
from orm.nsr_node import NsrNode
import compute_barcode_coverage

rank_hierarchy = RANK_ORDER[1:]


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
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

    coverage_table = compute_barcode_coverage.add_count_features(session, ete_tree_of_life, max_rank)
    # convert the table to json using pandas lib
    df = pd.DataFrame(coverage_table, columns=rank_hierarchy + ['rank', 'total_sp', 'sp_w_bc', 'total_bc', 'coverage',
                                                                'arise_bc', 'coverage_arise', 'not_arise_bc',
                                                                'coverage_not_arise', 'locality', 'occ_status'])
    # add id column for slickgrid dataview
    df.insert(0, 'id', range(1, 1 + len(df)))
    df = df.fillna("")
    df[['coverage', 'coverage_arise', 'coverage_not_arise']] = \
        df[['coverage', 'coverage_arise', 'coverage_not_arise']].apply(lambda x: round(x, 1))
    shutil.copyfile('html/target_list_template.html', 'html/target_list.html')

    # build a list a distinct localities
    localities = set()
    df['locality'].transform(lambda x: [localities.add(e.strip()) for e in x.split(';')
                                        if e.strip() and e.strip() != "Unknown"])

    # compute the overall completeness
    df_kingdom = df[df['rank'] == 'kingdom']
    overall_completeness = df_kingdom[df_kingdom.kingdom.isin(['Animalia', 'Plantae', 'Fungi'])]['coverage'].mean()

    html = open('html/target_list.html').read() \
        .replace('##COMPLNES##', '{0:3.1f}'.format(overall_completeness)) \
        .replace('"##LOCALITIES##"', json.dumps(sorted(list(localities)))) \
        .replace('"##DATA##"', df.to_json(orient="records"))

    with open('html/target_list.html', 'w') as fw:
        fw.write(html)
