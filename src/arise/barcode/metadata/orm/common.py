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
    specimen_data = session.query(Specimen).with_entities(Specimen.id, Specimen.species_id, Specimen.catalognum,
                                                          Specimen.institution_storing,
                                                          Specimen.identification_provided_by).all()
    return {f"{a}-{b}-{c}-{d}": i for i, a, b, c, d in specimen_data}


def get_barcode_index_dict(session, Barcode):
    barcode_data = session.query(Barcode).with_entities(Barcode.id, Barcode.specimen_id, Barcode.database,
                                                        Barcode.marker_id, Barcode.external_id).all()
    return {f"{a}-{b}-{c}-{d}": i for i, a, b, c, d in barcode_data}
