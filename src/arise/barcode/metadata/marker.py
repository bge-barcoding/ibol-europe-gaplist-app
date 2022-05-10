from arise.barcode.metadata.imports import *


class Marker(Base):
    __tablename__ = 'marker'

    marker_id = Column(Integer, primary_key=True)
    marker_name = Column(String)

    def __repr__(self):
        return "<Marker(marker_name='%s')>" % (
            self.marker_name)


