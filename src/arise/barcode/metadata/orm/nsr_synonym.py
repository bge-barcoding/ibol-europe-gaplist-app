from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import validates, relationship, backref
from sqlalchemy.schema import UniqueConstraint


class NsrSynonym(Base):
    taxonomic_status_set = {
        'synonym', 'basionym', 'nomen nudum', 'misspelled name', 'invalid name'
    }

    __tablename__ = 'nsr_synonym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    species = relationship('NsrSpecies', backref=backref("synonyms", cascade="all, delete"))
    name = Column(String(50), index=True, nullable=False)
    taxonomic_status = Column(String(15), nullable=False)
    species_id = Column(Integer, ForeignKey('nsr_species.id'), nullable=False)

    @validates('taxonomic_status')
    def validate_taxonomic_status(self, key, value):
        if value is not None:
            assert value in self.taxonomic_status_set, "%s is not a valid occurrence status" % value
        return value

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
                         self.name)

