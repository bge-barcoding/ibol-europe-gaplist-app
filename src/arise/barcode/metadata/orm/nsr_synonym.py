from arise.barcode.metadata.orm.imports import *


class NsrSynonym(Base):
    __tablename__ = 'nsr_synonym'
    synonym_id = Column(Integer, primary_key=True)
    synonym_name = Column(String, index=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))
    nsr_id = Column(String, ForeignKey('nsr_species.nsr_id'))

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
                         self.synonym_name)

