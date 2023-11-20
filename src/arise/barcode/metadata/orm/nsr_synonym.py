from orm.common import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import validates, relationship, backref
from sqlalchemy.schema import UniqueConstraint
import logging

syn_logger = logging.getLogger('synonym')


class NsrSynonym(Base):
    taxonomic_status_set = {
        'synonym', 'basionym', 'nomen nudum', 'misspelled name', 'invalid name'
    }

    __tablename__ = 'nsr_synonym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), index=True, nullable=False)
    taxonomic_status = Column(String(15), nullable=False)
    species_id = Column(Integer, ForeignKey('nsr_species.id'), nullable=False)
    # species = relationship('NsrSpecies', backref=backref("synonyms", cascade="all, delete"))

    __table_args__ = (UniqueConstraint('name', 'species_id', name='uc_synonym'),)

    @classmethod
    def insert_synonym(cls, session, name, taxonomic_status, species_id):
        created = False
        synonym = session.query(NsrSynonym).filter(NsrSynonym.name == name, NsrSynonym.species_id == species_id).first()
        if not synonym:
            # make sure the name of the species is different from NAME before inserting
            synonym = NsrSynonym(name=name, taxonomic_status=taxonomic_status, species_id=species_id)
            session.add(synonym)
            session.flush()
            created = True
            syn_logger.info('new synonym "%s status=%s (species_id=%s)" created' % (name, taxonomic_status, species_id))
        elif taxonomic_status != synonym.taxonomic_status:
            syn_logger.warning('synonym "%s (species_id=%s)" already exists but taxonomic_status "%s" != "%s"' %
                               (name, species_id, synonym.taxonomic_status, taxonomic_status))

        return synonym, created

    @validates('taxonomic_status')
    def validate_taxonomic_status(self, key, value):
        if value is not None:
            assert value in self.taxonomic_status_set, "%s is not a valid occurrence status" % value
        return value

    def __repr__(self):
        return "<NsrSynonym(synonym_name='%s')>" % (
            self.name)
