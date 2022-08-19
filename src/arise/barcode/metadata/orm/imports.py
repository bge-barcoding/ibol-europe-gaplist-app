from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint
from enum import IntEnum
Base = declarative_base()

class DataSource(IntEnum):
    NATURALIS = 1
    BOLD = 2
    NCBI = 3
    WFBI = 4
    UNITE = 5


RANK_ORDER = ['life', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
