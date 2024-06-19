import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import argparse
from sqlalchemy import case, create_engine, func, and_
from sqlalchemy.orm import sessionmaker
from orm.common import RANK_ORDER, DataSource
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


def get_species_barcode_count(session, filter_species):
    """
        get the number of barcode per species id
    """
    if filter_species:
        query = (session.query(
            Specimen.species_id,
            func.count(),
            func.sum(case(
                (Barcode.database.in_([DataSource.NATURALIS, DataSource.WFBI]), 1),  # ARISE barcodes
                else_=0
            )),
            func.sum(case(
                (Barcode.database.not_in([DataSource.NATURALIS, DataSource.WFBI]), 1),  # Other barcodes
                else_=0
            ))
        ).join(Barcode, Barcode.specimen_id == Specimen.id)
         .join(NsrSpecies, NsrSpecies.id == Specimen.species_id)
            .where(
                and_(
                    NsrSpecies.canonical_name.not_like("% sp."),
                    NsrSpecies.occurrence_status.in_(["0a", "1", "1a", "1b", "2a", "2b", "2c", "2d"]),
                )
            ).group_by(Specimen.species_id))
    else:
        query = session.query(
            Specimen.species_id,
            func.count(),
            func.sum(case(
                (Barcode.database.in_([DataSource.NATURALIS, DataSource.WFBI]), 1),  # ARISE barcodes
                else_=0
            )),
            func.sum(case(
                (Barcode.database.not_in([DataSource.NATURALIS, DataSource.WFBI]), 1),  # Other barcodes
                else_=0
            ))
        ).join(Barcode).group_by(Specimen.species_id)
    return {e: [ab, nb, ob] for e, ab, nb, ob in query.all()}


def get_specimen_locality(session, filter_species):
    fix_locality_dict = {
        'USA': "United State of America",
        'United States': "United State of America",
        'Faeroe Islands': 'Faroe Islands'
    }
    if filter_species:
        query = (session.query(
            Specimen.species_id,
            func.group_concat(Specimen.locality.distinct())
        ).join(NsrSpecies, NsrSpecies.id == Specimen.species_id)
         .where(
            and_(
                NsrSpecies.canonical_name.not_like("% sp."),
                NsrSpecies.occurrence_status.in_(["0a", "1", "1a", "1b", "2a", "2b", "2c", "2d"])
            )
        ).group_by(Specimen.species_id))
    else:
        query = (session.query(
            Specimen.species_id,
            func.group_concat(Specimen.locality.distinct())
        ).join(NsrSpecies).group_by(Specimen.species_id))
    d = dict()
    for e, loc in query.all():
        loc = '; '.join(
            [fix_locality_dict[c] if c in fix_locality_dict else c for c in sorted(loc.split(','))]
        )
        d[e] = loc
    return d


def get_species_occ_status(session, filter_species):
    query = session.query(
        NsrSpecies.id,
        NsrSpecies.occurrence_status
    )
    if filter_species:
        (query.join(NsrSpecies, NsrSpecies.id == Specimen.species_id)
              .where(
                  and_(
                      NsrSpecies.canonical_name.not_like("% sp."),
                      NsrSpecies.occurrence_status.in_(["0a", "1", "1a", "1b", "2a", "2b", "2c", "2d"])
                  )
              )
          )
    return {e: ocs for e, ocs in query.all()}


def add_features(node, total_sp, sp_with_bc, sp_with_bc_arise, sp_with_bc_not_arise, total_bc, arise_bc, not_arise_bc):
    """
        add multi ete feature to node
        (to replace with ete3.TreeNode.add_features?)
    """
    node.add_feature('total_sp', total_sp)
    node.add_feature('sp_with_bc', sp_with_bc)
    node.add_feature('sp_with_bc_arise', sp_with_bc_arise)
    node.add_feature('sp_with_bc_not_arise', sp_with_bc_not_arise)
    node.add_feature('total_bc', total_bc)
    node.add_feature('arise_bc', arise_bc)
    node.add_feature('not_arise_bc', not_arise_bc)


# does postorder traversal, propagating total species and total barcodes from tips to root
def add_count_features(session, tree, max_rank, filter_species) -> list:
    """
    :param session: SQLite session
    :param tree: ETE tree of life
    :param max_rank: the lowest rank level to consider
    :param filter_species: see argument parser description :p
    :return: Coverage table
    """
    species_bc_dict = get_species_barcode_count(session, filter_species)
    specimen_loc_dict = get_specimen_locality(session, filter_species)
    species_occ_status_dict = get_species_occ_status(session, filter_species)

    """
    does postorder traversal, propagating total species and total barcodes from tips to root
    compute the coverage (% of species with barcodes) at each taxon level
    for all barcodes, ARISE barcodes, non-ARISE barcodes
    """
    coverage_table = []
    for node in tree.iter_descendants(strategy='postorder'):
        total_sp = 0  # total species
        sp_with_bc = 0  # species with barcode(s)
        sp_with_bc_arise = 0  # species with barcode(s) from ARISE
        sp_with_bc_not_arise = 0  # species with barcode(s) from inst. other than ARISE
        total_bc = 0  # total number of barcodes
        arise_bc = 0  # total number of ARISE barcodes
        not_arise_bc = 0  # total number of not ARISE barcodes
        coverage = 0  # percentage of species having at least one barcode
        coverage_arise = 0  # percentage of species with barcode(s) from ARISE.
        coverage_not_arise = 0  # etc
        occurrence_status = None
        locality = None
        nsr_id = None
        if node.rank == max_rank:
            if max_rank == 'species':
                # special case, species is the max level, meaning the number of species = 1
                total_sp = 1
                nsr_node = session.query(NsrNode).filter(NsrNode.id == node.id).first()
                nsr_id = nsr_node.nsr_id
                if nsr_node.species_id and nsr_node.species_id in species_occ_status_dict:
                    occurrence_status = species_occ_status_dict[nsr_node.species_id]

                # remove species without left and right index
                # TODO debug such cases!
                if nsr_node.species_id is None or (nsr_node.left == nsr_node.right and nsr_node.right is None):
                    add_features(node, 0, 0, 0, 0, 0, 0, 0)
                    continue

                if filter_species and (
                        nsr_node.name.endswith(" sp.") or
                        occurrence_status not in ["0a", "1", "1a", "1b", "2a", "2b", "2c", "2d"]
                    ):
                    add_features(node, 0, 0, 0, 0, 0, 0, 0)
                    continue

                if nsr_node.species_id and nsr_node.species_id in species_bc_dict:
                    total_bc, arise_bc, not_arise_bc = species_bc_dict[nsr_node.species_id]
                    sp_with_bc = 1
                    sp_with_bc_arise = 1 if arise_bc else 0
                    sp_with_bc_not_arise = 1 if not_arise_bc else 0
                    coverage = 100
                    coverage_arise = sp_with_bc_arise * 100
                    coverage_not_arise = sp_with_bc_not_arise * 100
                if nsr_node.species_id and nsr_node.species_id in specimen_loc_dict:
                    locality = specimen_loc_dict[nsr_node.species_id]

            else:
                tips = session.query(NsrNode).filter(NsrNode.id == node.id).first().get_leaves(session).all()
                if len(tips) != 0:
                    total_sp = len(tips)
                    sp_with_bc_arise = 0
                    sp_with_bc_not_arise = 0
                    for tip in tips:
                        if tip.species_id and tip.species_id in species_bc_dict:
                            c_total_bc, c_arise_bc, c_not_arise_bc = species_bc_dict[tip.species_id]
                            total_bc += c_total_bc
                            arise_bc += c_arise_bc
                            not_arise_bc += c_not_arise_bc
                            sp_with_bc += 1
                            sp_with_bc_arise += 1 if c_arise_bc else 0
                            sp_with_bc_not_arise += 1 if c_not_arise_bc else 0

                    if total_sp != 0:
                        coverage = sp_with_bc / len(tips) * 100
                        coverage_arise = sp_with_bc_arise / len(tips) * 100
                        coverage_not_arise = sp_with_bc_not_arise / len(tips) * 100
        else:
            for child in node.get_children():
                total_sp += child.total_sp
                sp_with_bc += child.sp_with_bc
                sp_with_bc_arise += child.sp_with_bc_arise
                sp_with_bc_not_arise += child.sp_with_bc_not_arise
                total_bc += child.total_bc
                arise_bc += child.arise_bc
                not_arise_bc += child.not_arise_bc

            if total_sp != 0:
                coverage = sp_with_bc / total_sp * 100
                coverage_arise = sp_with_bc_arise / total_sp * 100
                coverage_not_arise = sp_with_bc_not_arise / total_sp * 100

        add_features(node, total_sp, sp_with_bc, sp_with_bc_arise, sp_with_bc_not_arise, total_bc, arise_bc, not_arise_bc)
        if node.name != "All of life":
            coverage_table.append([nsr_id] + make_ancestors_list(node, max_rank) +
                                                              [node.rank, total_sp, sp_with_bc,
                                                               total_bc, coverage,
                                                               arise_bc, coverage_arise,
                                                               not_arise_bc, coverage_not_arise,
                                                               locality, occurrence_status])

    # finally also do the tree root itself
    total_sp = 0
    sp_with_bc = 0
    sp_with_bc_arise = 0
    sp_with_bc_not_arise = 0
    total_bc = 0
    arise_bc = 0
    not_arise_bc = 0
    for child in tree.get_children():
        total_sp += child.total_sp
        sp_with_bc += child.sp_with_bc
        sp_with_bc_arise += child.sp_with_bc_arise
        sp_with_bc_not_arise += child.sp_with_bc_not_arise
        total_bc += child.total_bc
        arise_bc += child.arise_bc
        not_arise_bc += child.not_arise_bc

    add_features(tree, total_sp, sp_with_bc, sp_with_bc_arise, sp_with_bc_not_arise, total_bc, arise_bc, not_arise_bc)

    return coverage_table


if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', default="arise-barcode-metadata.db", help="Input file: SQLite DB")
    parser.add_argument('--filter-species', action="store_true",
                        help="Do not include in the coverage table: "
                             " - <genus> sp. species created by the pipeline or in NSR,"
                             " - species with occurrence status not being 0a, 1 or 2")

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
    add_count_features(session, ete_tree_of_life, max_rank, args.filter_species)
