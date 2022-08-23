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
    database = Column(Integer, index=True, nullable=False)

    # foreign key to marker names list
    marker_id = Column(Integer, ForeignKey('marker.id'), nullable=False)

    # verbatim copy of FAST definition line, to roundtrip ingested data
    defline = Column(String)

    # whatever external ID is available, could be:
    # - http://www.boldsystems.org/index.php/Public_RecordView?processid=$ID
    # - http://ncbi.nlm.nih.gov/nuccore/$ID
    # - internal ID that doesn't resolve
    external_id = Column(String, nullable=False)

    # find or create barcode object
    @classmethod
    def get_or_create_barcode(cls, specimen_id, database, marker_id, defline, external_id, session):
        created = False
        barcode = session.query(Barcode).filter(Barcode.specimen_id == specimen_id, Barcode.database == database,
                                                Barcode.marker_id == marker_id, Barcode.defline == defline,
                                                Barcode.external_id == external_id).first()
        if barcode is None:
            barcode = Barcode(specimen_id=specimen_id, database=database, marker_id=marker_id, external_id=external_id)
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

