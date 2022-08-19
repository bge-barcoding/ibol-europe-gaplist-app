from arise.barcode.metadata.orm.imports import *


class NsrSynonym(Base):
    __tablename__ = 'nsr_synonym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), index=True)
    species_id = Column(Integer, ForeignKey('nsr_species.id'))
    species = relationship('NsrSpecies', backref=backref("synonyms", cascade="all, delete"))

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
                         self.name)

