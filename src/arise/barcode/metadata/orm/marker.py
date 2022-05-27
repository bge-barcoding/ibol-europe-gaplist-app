from arise.barcode.metadata.orm.imports import *


class Marker(Base):
    __tablename__ = 'marker'

    marker_id = Column(Integer, primary_key=True, autoincrement=True)
    marker_name = Column(String)

    barcodes = relationship('Barcode', backref=backref("marker", cascade="all, delete"))

    # find or create marker object
    @classmethod
    def match_marker(cls, marker_name, session):
        marker = session.query(Marker).filter(Marker.marker_name == marker_name).first()
        if marker is None:
            marker = Marker(marker_name=marker_name)
            session.add(marker)
            session.flush()
        return marker

    def __repr__(self):
        return "<Marker(marker_name='%s')>" % (
            self.marker_name)


