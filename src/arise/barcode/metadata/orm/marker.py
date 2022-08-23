from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship, backref
import logging

m_logger = logging.getLogger('marker')


class Marker(Base):
    __tablename__ = 'marker'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(16), unique=True, nullable=False)

    barcodes = relationship('Barcode', backref=backref("marker", cascade="all, delete"))

    # find or create marker object
    @classmethod
    def get_or_create_marker(cls, name, session):
        created = False
        marker = session.query(Marker).filter(Marker.name == name).first()
        if marker is None:
            marker = Marker(name=name)
            session.add(marker)
            session.flush()
            created = True
            m_logger.info('new marker "%s" created' % name)
        return marker, created

    def __repr__(self):
        return "<Marker(name='%s')>" % (
            self.name)


