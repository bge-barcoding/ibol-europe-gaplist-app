import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
import shutil
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from orm.common import RANK_ORDER
from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.barcode import Barcode
from orm.specimen import Specimen
from orm.marker import Marker

rank_hierarchy = RANK_ORDER[1:]


#
def make_ancestors_list(node, max_rank):
    """
        build an ordered list of taxon name from ancestor nodes
    """
    l = [None] * (rank_hierarchy.index(max_rank) + 1)
    for e in node.get_ancestors():
        if e._up is None or not hasattr(e, 'rank') or e.rank == 'life':
            continue
        l[rank_hierarchy.index(e.rank)] = e.name
    # add also the current node name in the list
    l[rank_hierarchy.index(node.rank)] = node.name
    return l


def get_species_barcode_count():
    """
        get the number of barcode per species id
        count
    """
    from sqlalchemy import case
    query = session.query(
        Specimen.species_id,
        func.count(),
        func.sum(case(
            (Barcode.database == 1, 1),  # Naturalis barcodes
            else_=0
        )),
        func.sum(case(
            (Barcode.database != 1, 1),  # Other barcodes
            else_=0
        ))
    ).join(Barcode).group_by(Specimen.species_id)
    return {e: [ab, nb, ob] for e, ab, nb, ob in query.all()}


def add_features(node, total_sp, sp_with_bc, sp_with_bc_nat, sp_with_bc_not_nat, total_bc, nat_bc, not_nat_bc):
    """
        add multi ete feature to node
        (to replace with ete3.TreeNode.add_features?)
    """
    node.add_feature('total_sp', total_sp)
    node.add_feature('sp_with_bc', sp_with_bc)
    node.add_feature('sp_with_bc_nat', sp_with_bc_nat)
    node.add_feature('sp_with_bc_not_nat', sp_with_bc_not_nat)
    node.add_feature('total_bc', total_bc)
    node.add_feature('nat_bc', nat_bc)
    node.add_feature('not_nat_bc', not_nat_bc)


# does postorder traversel, propagating total species and total barcodes from tips to root
def add_count_features(tree, max_rank, species_bc_dict):
    """
         does postorder traversel, propagating total species and total barcodes from tips to root
         compute the coverage (% of species with barcodes) at each taxon level
         for all barcodes, Naturalis barcodes, non-Naturalis barcodes
    """
    coverage_table = []
    for node in tree.iter_descendants(strategy='postorder'):
        total_sp = 0  # total species
        sp_with_bc = 0  # species with barcode(s)
        sp_with_bc_nat = 0  # species with barcode(s) from Naturalis
        sp_with_bc_not_nat = 0  # species with barcode(s) from Other inst. than Naturalis
        total_bc = 0  # total number of barcodes
        nat_bc = 0  # total number of Naturalis barcodes
        not_nat_bc = 0  # total number of not Naturalis barcodes
        coverage = 0  # percentage of species having at least one barcode
        coverage_nat = 0  # percentage of species with barcode(s) from Nat.
        coverage_not_nat = 0  # etc
        if node.rank == max_rank:
            if max_rank == 'species':
                # special case, species is the max level, meaning the number of species = 1
                total_sp = 1
                nsr_node = session.query(NsrNode).filter(NsrNode.id == node.id).first()
                # remove species without left and right index
                # TODO debug such cases!
                if nsr_node.species_id is None or (nsr_node.left == nsr_node.right and nsr_node.right is None):
                    add_features(node, 0, 0, 0, 0, 0, 0, 0)
                    continue

                if nsr_node.species_id and nsr_node.species_id in species_bc_dict:
                    total_bc, nat_bc, not_nat_bc = species_bc_dict[nsr_node.species_id]
                    sp_with_bc = 1
                    sp_with_bc_nat = 1 if nat_bc else 0
                    sp_with_bc_not_nat = 1 if not_nat_bc else 0
                    coverage = 100
                    coverage_nat = sp_with_bc_nat * 100
                    coverage_not_nat = sp_with_bc_not_nat * 100
            else:
                tips = session.query(NsrNode).filter(NsrNode.id == node.id).first().get_leaves(session).all()
                if len(tips) != 0:
                    total_sp = len(tips)
                    sp_with_bc_nat = 0
                    sp_with_bc_not_nat = 0
                    for tip in tips:
                        if tip.species_id and tip.species_id in species_bc_dict:
                            c_total_bc, c_nat_bc, c_not_nat_bc = species_bc_dict[tip.species_id]
                            total_bc += c_total_bc
                            nat_bc += c_nat_bc
                            not_nat_bc += c_not_nat_bc
                            sp_with_bc += 1
                            sp_with_bc_nat += 1 if c_nat_bc else 0
                            sp_with_bc_not_nat += 1 if c_not_nat_bc else 0

                    coverage = sp_with_bc / len(tips) * 100
                    coverage_nat = sp_with_bc_nat / len(tips) * 100
                    coverage_not_nat = sp_with_bc_not_nat / len(tips) * 100
        else:
            for child in node.get_children():
                total_sp += child.total_sp
                sp_with_bc += child.sp_with_bc
                sp_with_bc_nat += child.sp_with_bc_nat
                sp_with_bc_not_nat += child.sp_with_bc_not_nat
                total_bc += child.total_bc
                nat_bc += child.nat_bc
                not_nat_bc += child.not_nat_bc

            coverage = sp_with_bc / total_sp * 100
            coverage_nat = sp_with_bc_nat / total_sp * 100
            coverage_not_nat = sp_with_bc_not_nat / total_sp * 100

        add_features(node, total_sp, sp_with_bc, sp_with_bc_nat, sp_with_bc_not_nat, total_bc, nat_bc, not_nat_bc)
        if node.name != "All of life":
            coverage_table.append(make_ancestors_list(node, max_rank) +
                                                              [node.rank, total_sp, sp_with_bc,
                                                               total_bc, coverage,
                                                               nat_bc, coverage_nat,
                                                               not_nat_bc, coverage_not_nat])

    # finally also do the tree root itself
    total_sp = 0
    sp_with_bc = 0
    sp_with_bc_nat = 0
    sp_with_bc_not_nat = 0
    total_bc = 0
    nat_bc = 0
    not_nat_bc = 0
    for child in tree.get_children():
        total_sp += child.total_sp
        sp_with_bc += child.sp_with_bc
        sp_with_bc_nat += child.sp_with_bc_nat
        sp_with_bc_not_nat += child.sp_with_bc_not_nat
        total_bc += child.total_bc
        nat_bc += child.nat_bc
        not_nat_bc += child.not_nat_bc

    add_features(tree, total_sp, sp_with_bc, sp_with_bc_nat, sp_with_bc_not_nat, total_bc, nat_bc, not_nat_bc)

    return coverage_table


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
    d = get_species_barcode_count()
    ete_tree_of_life = nsr_root.to_ete(session, until_rank=max_rank, remove_empty_rank=True,
                                       remove_incertae_sedis_rank=True)

    coverage_table = add_count_features(ete_tree_of_life, max_rank, d)
    # convert the ttable to json using pandas lib
    df = pd.DataFrame(coverage_table, columns=rank_hierarchy + ['rank', 'total_sp', 'sp_w_bc', 'total_bc', 'coverage', 'nat_bc',
                                                                'coverage_nat', 'not_nat_bc', 'coverage_not_nat'])
    # add id column for slickgrid dataview
    df.insert(0, 'id', range(1, 1 + len(df)))
    df = df.fillna("")
    df[['coverage', 'coverage_nat', 'coverage_not_nat']] = \
        df[['coverage', 'coverage_nat', 'coverage_not_nat']].apply(lambda x: round(x, 1))
    shutil.copyfile('html/target_list_template.html', 'html/target_list.html')
    html = open('html/target_list.html').read().replace('##DATA##', df.to_json(orient="records"))
    with open('html/target_list.html', 'w') as fw:
        fw.write(html)
