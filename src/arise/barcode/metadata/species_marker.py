from arise.barcode.metadata.imports import *


class SpeciesMarker(Base):
    __tablename__ = 'species_marker'

    species_marker_id = Column(Integer, primary_key=True)
    sequence_id = Column(Integer)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))
    nsrspecies = relationship("NsrSpecies", backref=backref("SpeciesMarkers", cascade="all, delete"))
    database_id = Column(Integer, ForeignKey('database.database_id'))
    database = relationship("Database", backref=backref("SpeciesMarkers", cascade="all, delete"))
    marker_id = Column(Integer, ForeignKey('marker.marker_id'))
    marker = relationship("Marker", backref=backref("SpeciesMarkers", cascade="all, delete"))
    genbank_accession = Column(String(20))
    bin_uri = Column(String(13))

    def __repr__(self):
        return "<SpeciesMarker(sequence_id='%s')>" % (
                         self.sequence_id)


#IDEA: method(s) that retrieves the sequence of the object using its sequence id and database_id

#IDEA: add species

