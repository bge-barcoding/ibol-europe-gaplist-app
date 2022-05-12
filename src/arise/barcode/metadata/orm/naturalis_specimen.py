from arise.barcode.metadata.orm.imports import *


class Specimen(Base):
    __tablename__ = 'specimen'
    specimen_id = Column(Integer, primary_key=True)
    catalognum = Column(String)
    institution_storing = Column(String)
    identification_provided_by = Column(String)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'), primary_key=True)

    # relationship
    nsrspecies = relationship("NsrSpecies", backref=backref("Specimen", cascade="all, delete"))

    def __repr__(self):
        return "<Specimen(name='%s')>" % (
                         self.specimen_id)
