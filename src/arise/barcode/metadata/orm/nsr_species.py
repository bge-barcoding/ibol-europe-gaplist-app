from arise.barcode.metadata.orm.imports import *


class NsrSpecies(Base):
    __tablename__ = 'nsr_species'

    species_id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(String, index=True)

    specimens = relationship('Specimen', backref=backref("nsrspecies", cascade="all, delete"))
    synonymes = relationship('NsrSynonym')

    def __repr__(self):
        return "<NsrSpecies(canonical_name='%s')>" % (
                         self.canonical_name)







