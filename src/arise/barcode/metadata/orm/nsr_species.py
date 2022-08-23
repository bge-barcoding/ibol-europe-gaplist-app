from taxon_parser import TaxonParser
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import validates
from taxon_parser import UnparsableNameException
import logging
nsm_logger = logging.getLogger('nsr_species_match')
from orm.common import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import validates, relationship, backref


class NsrSpecies(Base):
    occurrence_status_set = {
        '0', '0a', '1', '1a', '1b', '2', '2a', '2b', '2c', '2d', '3a', '3b', '3c', '3d', '4'
    }

    __tablename__ = 'nsr_species'

    id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(String(50), index=True)
    occurrence_status = Column(String(2))
    # need to know the genus id for species with identical names (rare cases involving 'sp.' name )
    genus_id = Column(Integer, ForeignKey('node.id'))

    specimens = relationship('Specimen', backref=backref("nsrspecies", cascade="all, delete"))

    # find or create species for specimen
    @classmethod
    def match_species(cls, taxon, session, kingdom=""):
        # parse species name
        name_parser = TaxonParser(taxon)
        nsr_species = None
        try:
            parsed = name_parser.parse()
            cleaned = parsed.canonicalNameWithoutAuthorship()
            # find exact species match
            nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == cleaned).all()
            nsr_synonyms = session.query(NsrSynonym).filter(NsrSynonym.name == cleaned).all()

            if nsr_species and nsr_synonyms:
                nsm_logger.warning('species name "%s" is also an existing synonym' % cleaned)
            if len(nsr_species) == 1:
                nsr_species = nsr_species[0]
            elif len(nsr_species) > 1:
                # should not be possible, unless when trying to insert 'xxx sp.' species name
                # but this placeholder name should never be provided in real data
                nsm_logger.error('multiple species match using name: "%s"' % cleaned)
                nsm_logger.error('matches:', nsr_species)
                exit()
            else:
                # species not found, check in synonym table
                if len(nsr_synonyms) == 1:
                    nsr_species = nsr_synonyms[0].species
                else:
                    if len(nsr_synonyms) > 1:
                        nsm_logger.warning('Taxon "%s" match multiple synonyms, ignore them' % cleaned)
                    nsr_species = None
                    # search for genus matches
                    # when a genus name is specify instead of a species name
                    # the strategy is (when the genus name is found in our database)
                    # to create a new species node named "[genus] sp."
                    nsr_node = session.query(NsrNode).filter(NsrNode.name == cleaned, NsrNode.rank == 'genus').all()
                    if len(nsr_node) == 0:
                        nsm_logger.warning('Taxon "%s" not found anywhere in NSR topology, node not created' % cleaned)
                    elif len(nsr_node) > 1:
                        # the taxon name is a homonym, if the kingdom is provided, let try to find the correct node
                        if kingdom:
                            kingdom = kingdom.lower()
                            valid_parents = []
                            for node in nsr_node:
                                n = node
                                while n.rank != "kingdom":
                                    n = session.query(NsrNode).filter(NsrNode.id == n.parent).one()
                                if n.name.lower() == kingdom:
                                    valid_parents.append(node)
                            if len(valid_parents) == 1:
                                nsr_node = valid_parents
                            elif len(valid_parents) > 1 or len(valid_parents) == 0:
                                # exit here because we can safely associate the current name to the good node
                                nsm_logger.error('multiple species/genus match using name: "%s"' % cleaned)
                                nsm_logger.error('cannot select the correct one, even using kingdom' % cleaned)
                                exit()
                        else:
                            # might be triggered if argument -kingdom is not use when loading data
                            nsm_logger.error('multiple genus match using name: "%s"' % cleaned)
                            nsm_logger.error('try to specify -kingdom, to remove ambigous nodes' % cleaned)
                            exit()
                    if nsr_node:
                        nsr_node = nsr_node[0]
                        # find or create sp node, but make sure it is linked to the correct species
                        # this allows the creation of homonyms  sp. species nodes
                        # note: such species won't have a status_occurrence
                        sp_name = cleaned + ' sp.'
                        nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == sp_name,
                                                                       NsrSpecies.genus_id == nsr_node.id).all()
                        if len(nsr_species) > 1:
                            # should not be possible
                            nsm_logger.error('Multiple sp. nodes found in database using taxon "%s"' % cleaned)
                            exit()
                        elif len(nsr_species) == 0:
                            nsr_species = NsrSpecies(canonical_name=sp_name, genus_id=nsr_node.id)
                            session.add(nsr_species)
                            nsr_node = NsrNode(name=sp_name, parent=nsr_node.id, rank='species',
                                               species_id=nsr_species.id)
                            session.add(nsr_node)
                            session.flush()
                        else:
                            # sp. node already in database, return it
                            nsr_species = nsr_species[0]
        except AttributeError as e:
            nsm_logger.error('Problem parsing taxon name "%s"' % taxon)
            nsm_logger.error('Exception: %s' % e)
        except UnparsableNameException as e:
            nsm_logger.error('UnparsableNameException with taxon name "%s"' % taxon)

        return nsr_species

    @validates('occurrence_status')
    def validate_occurrence_status(self, key, value):
        if value is not None:
            assert value in self.occurrence_status_set, "%s is not a valid occurrence status" % value
        return value

    def __repr__(self):
        return "<NsrSpecies(canonical_name='%s')>" % (
                         self.canonical_name)







