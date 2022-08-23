from orm.common import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import validates, relationship, backref


class NsrSpecies(Base):
    occurrence_status_set = {
        '0', '0a', '1', '1a', '1b', '2', '2a', '2b', '2c', '2d', '3a', '3b', '3c', '3d', '4'
    }

    __tablename__ = 'nsr_species'

    id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(String(50), index=True, nullable=False)
    occurrence_status = Column(String(2))

    specimens = relationship('Specimen', backref=backref("nsrspecies", cascade="all, delete"))

    @validates('occurrence_status')
    def validate_occurrence_status(self, key, value):
        if value is not None:
            assert value in self.occurrence_status_set, "%s is not a valid occurrence status" % value
        return value

    def __repr__(self):
        return "<NsrSpecies(canonical_name='%s')>" % (
                         self.canonical_name)







