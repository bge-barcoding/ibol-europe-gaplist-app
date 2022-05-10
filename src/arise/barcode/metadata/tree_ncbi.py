from arise.barcode.metadata.imports import *


class TreeNcbi(Base):
    __tablename__ = 'tree_ncbi'
    tax_id = Column(Integer, primary_key=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))
    nsrspecies = relationship("NsrSpecies",
                              backref=backref("tree_ncbi",
                                              cascade="all, delete"))
    parent_tax_id = Column(Integer, primary_key=True)
    rank = Column(String, primary_key=True)
    name = Column(String)


    def __repr__(self):
        return "<TreeNcbi(name='%s')>" % (
                         self.name)
