from arise.barcode.metadata.orm.imports import *


class Marker(Base):
    __tablename__ = 'marker'

    marker_id = Column(Integer, primary_key=True, autoincrement=True)
    marker_name = Column(String)

    def __repr__(self):
        return "<Marker(marker_name='%s')>" % (
            self.marker_name)


