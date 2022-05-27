from arise.barcode.metadata.orm.imports import *
from taxon_parser import TaxonParser
from arise.barcode.metadata.orm.nsr_node import NsrNode
from sqlalchemy.exc import NoResultFound
from taxon_parser import UnparsableNameException
import logging


class NsrSpecies(Base):
    __tablename__ = 'nsr_species'

    species_id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(String, index=True)

    specimens = relationship('Specimen', backref=backref("nsrspecies", cascade="all, delete"))
#    synonyms = relationship('NsrSynonym', backref=backref("nsrspecies", cascade="all, delete"))

    # find or create species for specimen
    @classmethod
    def match_species(cls, taxon, session):
        # parse species name
        name_parser = TaxonParser(taxon)
        nsr_species = None
        try:
            parsed = name_parser.parse()
            cleaned = parsed.canonicalNameWithoutAuthorship()

            # find exact species match
            nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == cleaned).one()
        except NoResultFound:
            try:

                # find genus match
                nsr_node = session.query(NsrNode).filter(NsrNode.name == cleaned, NsrNode.rank == 'genus').one()

                # find or create sp node
                sp_name = cleaned + ' sp.'
                nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == sp_name).first()
                if nsr_species is None:
                    nsr_species = NsrSpecies(canonical_name=sp_name)
                    session.add(nsr_species)
                    nsr_node = NsrNode(name=sp_name, parent=nsr_node.id, rank='species',
                                       species_id=nsr_species.species_id)
                    session.add(nsr_node)
                    session.flush()

            except NoResultFound:
                logging.debug("Taxon %s not found anywhere in NSR topology" % cleaned)
        except AttributeError:
            logging.debug("Problem parsing taxon name %s" % taxon)
        except UnparsableNameException:
            logging.debug("Problem parsing taxon name %s" % taxon)

        return nsr_species

    def __repr__(self):
        return "<NsrSpecies(canonical_name='%s')>" % (
                         self.canonical_name)







