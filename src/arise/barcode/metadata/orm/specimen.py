from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import validates, relationship, backref
import logging

spm_logger = logging.getLogger('specimen')


locality_set = {
    'Unknown', 'Netherlands', 'Belgium', 'Germany', 'North Sea'
}


class Specimen(Base):
    __tablename__ = 'specimen'
    id = Column(Integer, primary_key=True, autoincrement=True)
    catalognum = Column(String(50), index=True, nullable=False)
    institution_storing = Column(String(50), index=True)
    identification_provided_by = Column(String(50), index=True)
    locality = Column(String(50))
    species_id = Column(Integer, ForeignKey('nsr_species.id'), nullable=False)

    barcodes = relationship('Barcode', backref=backref("specimen", cascade="all, delete"))

    # find or create specimen object
    @classmethod
    def get_or_create_specimen(cls, species_id, catalognum, institution_storing, identification_provided_by,
                               locality,  session):
        created = False
        specimen = \
            session.query(Specimen).filter(Specimen.species_id == species_id, Specimen.catalognum == catalognum,
                                           Specimen.institution_storing == institution_storing,
                                           Specimen.identification_provided_by == identification_provided_by).all()

        if not specimen:
            specimen = Specimen(species_id=species_id, catalognum=catalognum, institution_storing=institution_storing,
                                identification_provided_by=identification_provided_by, locality=locality)
            session.add(specimen)
            session.flush()
            created = True
        elif len(specimen) > 1:
            spm_logger.error('multiple specimen matched the following criteria:')
            spm_logger.error(f'{species_id=}')
            spm_logger.error(f'{catalognum=}')
            spm_logger.error(f'{institution_storing=}')
            spm_logger.error(f'{identification_provided_by=}')
            spm_logger.error(f'{locality=}')
            exit()
        else:
            specimen = specimen[0]
        return specimen, created

    # @validates('locality')
    # def validate_locality(self, key, value):
    #     assert value in locality_set, "%s is not a valid locality, expected in %s" % (value, locality_set)
    #     return value

    def __repr__(self):
        return "<Specimen(id='%s')>" % (
                         self.id)
