from arise.barcode.metadata.imports import *


class NaturalisSpecimen(Base):
    __tablename__ = 'naturalis_specimen'
    naturalis_id = Column(String, primary_key=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'),
                        primary_key=True)
    nsrspecies = relationship("NsrSpecies",
                              backref=backref("NaturalisSpecimens",
                                              cascade="all, delete"))

    def __repr__(self):
        return "<NaturalisSpecimen(name='%s')>" % (
                         self.naturalis_id)
