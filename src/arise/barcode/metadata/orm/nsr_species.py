from arise.barcode.metadata.orm.imports import *


class NsrSpecies(Base):
    __tablename__ = 'nsr_species'

    species_id = Column(Integer, primary_key=True)
    nsr_id = Column(String, index=True)
    canonical_name = Column(String, index=True)

    def __repr__(self):
        return "<NsrSpecies(species_name='%s')>" % (
                         self.species_name)







