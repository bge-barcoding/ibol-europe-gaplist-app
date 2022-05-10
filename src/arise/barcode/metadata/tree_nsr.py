from arise.barcode.metadata.imports import *


class TreeNsr(Base):
    __tablename__ = 'tree_nsr'
    tax_id = Column(Integer, primary_key=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'),
                        primary_key=True)
    nsrspecies = relationship("NsrSpecies",
                              backref=backref("tree_nsr",
                                              cascade="all, delete"))
    parent_tax_id = Column(Integer, primary_key=True)
    rank = Column(String)
    name = Column(String)


    def __repr__(self):
        return "<TreeNsr(name='%s')>" % (
                         self.name)