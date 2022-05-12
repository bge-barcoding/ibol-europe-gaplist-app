from arise.barcode.metadata.orm.imports import *


class Specimen(Base):
    __tablename__ = 'specimen'
    specimen_id = Column(Integer, primary_key=True, autoincrement=True)
    catalognum = Column(String, index=True)
    institution_storing = Column(String, index=True)
    identification_provided_by = Column(String, index=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))

    # relationship
    nsrspecies = relationship("NsrSpecies", backref=backref("Specimen", cascade="all, delete"))

    def __repr__(self):
        return "<Specimen(name='%s')>" % (
                         self.specimen_id)
