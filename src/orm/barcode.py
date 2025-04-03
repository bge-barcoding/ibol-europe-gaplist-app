from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import validates

class Barcode(Base):
    __tablename__ = 'barcode'

    # auto-incrementing int, primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # foreign key to specimen
    specimen_id = Column(Integer, ForeignKey('specimen.id'), nullable=False)

    # data source - use the DataSource enum from imports
    database = Column(Integer, nullable=False)

    # foreign key to marker names list
    marker_id = Column(Integer, ForeignKey('marker.id'), nullable=False)

    # verbatim copy of FAST definition line, to roundtrip ingested data
    defline = Column(String)

    # whatever external ID is available, could be:
    # - http://www.boldsystems.org/index.php/Public_RecordView?processid=$ID
    # - http://ncbi.nlm.nih.gov/nuccore/$ID
    # - internal ID that doesn't resolve
    # - should be indexed, e.g. to check if a barcode from a BOLD data package was already loaded from a container
    external_id = Column(String, nullable=False, index=True)

    # find or create barcode object
    @classmethod
    def get_or_create_barcode(cls, specimen_id, database, marker_id, defline, external_id, session, fast_insert=False):
        created = False
        if not fast_insert:
            barcode = session.query(Barcode).filter(Barcode.specimen_id == specimen_id, Barcode.database == database,
                                                    Barcode.marker_id == marker_id, Barcode.defline == defline,
                                                    Barcode.external_id == external_id).first()
        else:
            barcode = False

        if not barcode:
            barcode = Barcode(
                specimen_id=specimen_id,
                database=database,
                marker_id=marker_id,
                defline=defline,
                external_id=external_id
            )
            session.add(barcode)
            session.flush()
            created = True
        return barcode, created

    @validates('external_id')
    def validate_external_id(self, key, value):
        assert value.strip() != ""
        return value

    def __repr__(self):
        return "<Barcode(id='%s')>" % (
                         self.id)

