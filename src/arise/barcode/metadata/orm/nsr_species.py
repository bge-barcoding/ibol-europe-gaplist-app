from arise.barcode.metadata.orm.imports import *
from taxon_parser import TaxonParser
from arise.barcode.metadata.orm.nsr_node import NsrNode
from arise.barcode.metadata.orm.nsr_synonym import NsrSynonym
from sqlalchemy.exc import NoResultFound
from taxon_parser import UnparsableNameException
import logging


class NsrSpecies(Base):
    __tablename__ = 'nsr_species'

    species_id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(String, index=True)
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
            if len(nsr_species) == 1:
                nsr_species = nsr_species[0]
            elif len(nsr_species) > 1:
                # should not be possible, unless when trying to insert 'xxx sp.' species name
                # but this placeholder name should never be provided in real data
                print('Error: multiple species match using name: "%s' % cleaned)
                print(nsr_species)
                exit()
            else:
                # check if synonym
                nsr_synonyms = session.query(NsrSynonym).filter(NsrSynonym.synonym_name == cleaned).all()
                if len(nsr_synonyms) == 1:
                    nsr_species = nsr_synonyms[0].species
                else:
                    nsr_species = None
                    nsr_node = session.query(NsrNode).filter(NsrNode.name == cleaned, NsrNode.rank == 'genus').all()
                    if len(nsr_node) == 0:
                        logging.debug("Taxon %s not found anywhere in NSR topology" % cleaned)
                        print("Taxon %s not found anywhere in NSR topology" % cleaned)
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
                                # exit here because it's going to mess up the barcode data
                                print("Error cannot select the correct node")
                                exit()
                        else:
                            # might be triggered if argument -kingdom is not use when loading data
                            print("Multiple genus named '%s' cannot map to NSR taxonomy" % cleaned)
                            exit()
                    if nsr_node:
                        nsr_node = nsr_node[0]
                        # find or create sp node, but make sure it is linked to the correct species
                        # this allows the creation of multiple identical sp. species
                        sp_name = cleaned + ' sp.'
                        nsr_species = session.query(NsrSpecies).filter(NsrSpecies.canonical_name == sp_name,
                                                                       NsrSpecies.genus_id == nsr_node.id).all()
                        if len(nsr_species) > 1:
                            # should not be possible
                            print("Error: multiple identical species with same genus")
                            exit()
                        elif len(nsr_species) == 0:
                            nsr_species = NsrSpecies(canonical_name=sp_name, genus_id=nsr_node.id)
                            session.add(nsr_species)
                            nsr_node = NsrNode(name=sp_name, parent=nsr_node.id, rank='species',
                                               species_id=nsr_species.species_id)
                            session.add(nsr_node)
                            session.flush()
                        else:
                            nsr_species = nsr_species[0]
        except AttributeError as e:
            logging.debug("Problem parsing taxon name %s" % taxon)
            print("Problem parsing taxon name %s" % taxon)
            print(e)
        except UnparsableNameException:
            logging.debug("Problem parsing taxon name %s" % taxon)
            print("Problem parsing taxon name %s" % taxon)

        return nsr_species

    def __repr__(self):
        return "<NsrSpecies(canonical_name='%s')>" % (
                         self.canonical_name)







