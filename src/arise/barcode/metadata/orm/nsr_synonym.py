from arise.barcode.metadata.orm.imports import *


class NsrSynonym(Base):
    __tablename__ = 'nsr_synonym'
    synonym_id = Column(Integer, primary_key=True, autoincrement=True)
    synonym_name = Column(String, index=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))
    species = relationship('NsrSpecies', backref=backref("synonyms", cascade="all, delete"))

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
                         self.synonym_name)

