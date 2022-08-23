
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
