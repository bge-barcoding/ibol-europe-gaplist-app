from enum import IntEnum
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DataSource(IntEnum):
    NATURALIS = 1
    BOLD = 2
    NCBI = 3
    WFBI = 4
    UNITE = 5


RANK_ORDER = ['life', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']


def get_specimen_index_dict(session, Specimen):
    """
    Hash table of unique index of specimen in the database, formatted as:
        index: specimen_id in DB
    The index is a concatenation of fields [species_id, catalognum, institution_storing, and identification_provided_by]
    The index must be consistent with the query in specimen.py get_or_create_specimen()
    The returned dict is used to test if a specimen is already in the database, avoiding
    querying the database to do the check. This dict is used in many util/ scripts to
    greatly speed up the insertion of input files.
    """
    specimen_data = session.query(Specimen).with_entities(Specimen.id, Specimen.species_id, Specimen.catalognum,
                                                          Specimen.institution_storing,
                                                          Specimen.identification_provided_by).all()
    return {f"{a}-{b}-{c}-{d}": i for i, a, b, c, d in specimen_data}


def get_barcode_index_dict(session, Barcode):
    """
    Hash table of unique index of barcode in the database, formatted as:
        index: barcode_id in DB
    The index is a concatenation of fields [specimen_id, database, marker_id, and external_id]
    The index must be consistent with the query in barcode.py get_or_create_barcode()
    The returned dict is used to test if a specimen is already in the database, avoiding
    to query the database to do the check. This dict is used in many util/ scripts to
    greatly speed up the insertion of input files.
    """
    barcode_data = session.query(Barcode).with_entities(Barcode.id, Barcode.specimen_id, Barcode.database,
                                                        Barcode.marker_id, Barcode.external_id).all()
    return {f"{a}-{b}-{c}-{d}": i for i, a, b, c, d in barcode_data}
