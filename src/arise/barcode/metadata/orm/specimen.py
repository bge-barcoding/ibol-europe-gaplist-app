from arise.barcode.metadata.orm.imports import *
import logging

spm_logger = logging.getLogger('specimen')


class Specimen(Base):
    __tablename__ = 'specimen'
    specimen_id = Column(Integer, primary_key=True, autoincrement=True)
    catalognum = Column(String, index=True)
    institution_storing = Column(String, index=True)
    identification_provided_by = Column(String, index=True)
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))

    barcodes = relationship('Barcode', backref=backref("specimen", cascade="all, delete"))

    # find or create specimen object
    @classmethod
    def get_or_create_specimen(cls, species_id, catalognum, institution_storing, identification_provided_by, session):
        created = False
        specimen = session.query(Specimen).filter(Specimen.species_id == species_id, Specimen.catalognum == catalognum,
                                                  Specimen.institution_storing == institution_storing,
                                                  Specimen.identification_provided_by == identification_provided_by).all()
        if not specimen:
            specimen = Specimen(species_id=species_id, catalognum=catalognum, institution_storing=institution_storing,
                                identification_provided_by=identification_provided_by)
            session.add(specimen)
            session.flush()
            created = True
        elif len(specimen) > 1:
            spm_logger.error('multiple specimen matched the following criteria:')
            spm_logger.error(f'{species_id=}')
            spm_logger.error(f'{catalognum=}')
            spm_logger.error(f'{institution_storing=}')
            spm_logger.error(f'{identification_provided_by=}')
            exit()
        else:
            specimen = specimen[0]
        return specimen, created

    def __repr__(self):
        return "<Specimen(name='%s')>" % (
                         self.specimen_id)
