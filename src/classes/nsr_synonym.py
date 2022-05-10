from scripts.classes.imports import *


class NsrSynonym(Base):
    __tablename__ = 'nsr_synonym'
    synonym_id = Column(Integer, primary_key=True)
    synonym_name = Column(String)
    identification_reference = Column(String)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'),
                        primary_key=True)
    nsrspecies = relationship("NsrSpecies",
                              backref=backref("nsr_synonym",
                                              cascade="all, delete"))

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
                         self.synonym_name)





