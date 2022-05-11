from arise.barcode.metadata.orm.imports import *


class Barcode(Base):
    __tablename__ = 'barcode'

    # auto-incrementing int, primary key
    barcode_id = Column(Integer, primary_key=True)

    # foreign key to species
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))

    # foreign key to database, e.g. BOLD
    database_id = Column(Integer, ForeignKey('database.database_id'))

    # foreign key to marker names list
    marker_id = Column(Integer, ForeignKey('marker.marker_id'))

    # whatever external ID is available, could be:
    # - http://www.boldsystems.org/index.php/Public_RecordView?processid=$ID
    # - http://ncbi.nlm.nih.gov/nuccore/$ID
    # - internal ID that doesn't resolve
    external_id = Column(String)

    def __repr__(self):
        return "<SpeciesMarker(sequence_id='%s')>" % (
                         self.sequence_id)

