from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey


class Barcode(Base):
    __tablename__ = 'barcode'

    # auto-incrementing int, primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # foreign key to specimen
    specimen_id = Column(Integer, ForeignKey('specimen.id'))

    # data source - use the DataSource enum from imports
    database = Column(Integer, index=True)

    # foreign key to marker names list
    marker_id = Column(Integer, ForeignKey('marker.id'))

    # verbatim copy of FAST definition line, to roundtrip ingested data
    defline = Column(String)

    # whatever external ID is available, could be:
    # - http://www.boldsystems.org/index.php/Public_RecordView?processid=$ID
    # - http://ncbi.nlm.nih.gov/nuccore/$ID
    # - internal ID that doesn't resolve
    external_id = Column(String)

    def __repr__(self):
        return "<Barcode(barcode_id='%s')>" % (
                         self.barcode_id)

