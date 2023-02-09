import logging
from sqlalchemy import Column, Integer, String, Float, ForeignKey, func
from sqlalchemy.orm import validates
from sqlalchemy.schema import UniqueConstraint
from orm.common import Base, RANK_ORDER
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from ete3 import Tree
from taxon_parser import TaxonParser, UnparsableNameException, Rank

nsm_logger = logging.getLogger('nsr_species_match')
# from sqlalchemy.orm import declarative_base

RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}
RANK_ORDER = ['t_class' if e == 'class' else e for e in RANK_ORDER]


class NsrNode(Base):

    # This constructor defines the equivalent of the below schema from DBTree, extended with a few
    # more NSR-specific fields:
    # create table node(
    #   id int not null,
    #   parent int,
    #   left int,
    #   right int,
    #   name varchar(20),
    #   length float,
    #   height float,
    #   primary key(id)
    # );
    # create index parent_idx on node(parent);
    # create index left_idx on node(left);
    # create index right_idx on node(right);
    # create index name_idx on node(name);

    __tablename__ = 'node'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent = Column(Integer, index=True, nullable=False)
    left = Column(Integer, index=True, unique=True)
    right = Column(Integer, index=True, unique=True)
    name = Column(String, index=True, nullable=False)
    length = Column(Float)
    height = Column(Float)

    # taxonomic rank, e.g. 'genus'
    rank = Column(String(16))

    # foreign key to nsr_species table
    species_id = Column(Integer, ForeignKey(NsrSpecies.id))

    # name of parent node for each rank
    kingdom = Column(String(50))
    phylum = Column(String(50))
    t_class = Column('class', String(50))
    order = Column(String(50))
    family = Column(String(50))
    genus = Column(String(50))
    species = Column(String(50))

    __table_args__ = (UniqueConstraint('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species',
                                       name='uc_classification'),)

    @classmethod
    def get_or_create_node(cls, session, id, rank, species_id, kingdom=None, phylum=None, t_class=None,
                           order=None, family=None, genus=None, species=None):
        ranks = locals()
        [ranks.pop(key) for key in list(ranks.keys()) if key not in RANK_ORDER]
        created = False
        q = session.query(NsrNode)
        for _rank in RANK_ORDER[1:]:  # ignore 'life' key
            q = q.filter(getattr(NsrNode, _rank) == ranks[_rank])  # ranks[rank] may be None/NULL
        nodes = q.all()

        if not nodes:
            node = NsrNode(id=id, rank='class' if rank == 't_class' else rank,
                           name=ranks[rank], species_id=species_id, parent=2, **ranks)
            session.add(node)
            session.flush()
            created = True
        else:
            node = nodes[0]
        return node, created


    @classmethod
    def match_species_node(cls, taxon, session, kingdom=""):
        # parse species name
        name_parser = TaxonParser(taxon, rank=Rank.SPECIES)
        nsr_species_node = None
        try:
            parsed = name_parser.parse()
            cleaned = parsed.canonicalNameWithoutAuthorship()

            for pattern in [" var.", " subsp.", " f.", " f.sp. ", " f. sp. ", " nothovar. ", " spec."]:
                if pattern in cleaned:
                    cleaned = cleaned.split(pattern)[0]
                    break

            # find exact species match
            query = session.query(NsrNode).filter(NsrNode.name == cleaned, NsrNode.rank == 'species')
            if kingdom:
                query = query.filter(func.lower(NsrNode.kingdom) == kingdom.lower())
            nsr_species_nodes = query.all()

            if len(nsr_species_nodes) > 1:
                nsm_logger.error('multiple species match using name: "%s"' % cleaned)
                nsm_logger.error('matches:', list(nsr_species_nodes))
                exit()

            # check also synonyms regardless if a species node was found or not
            nsr_synonyms = session.query(NsrSynonym).filter(NsrSynonym.name == cleaned).all()
            if nsr_species_nodes and nsr_synonyms:
                nsm_logger.warning('species name "%s" is also an existing synonym' % cleaned)

            if len(nsr_species_nodes) == 1:
                return nsr_species_nodes[0]

            # species not found, synonym results
            if len(nsr_synonyms) == 1:
                return session.query(NsrNode).filter(NsrNode.species_id == nsr_synonyms[0].species_id).first()

            if len(nsr_synonyms) > 1:
                nsm_logger.warning('Taxon "%s" match multiple synonyms, ignore them' % cleaned)

            # check if the canonical name match a genus sp.
            sp_name = cleaned if cleaned[-4:] == " sp." else cleaned + ' sp.'
            query = session.query(NsrNode).filter(NsrNode.name == sp_name,
                                                  NsrNode.rank == 'species')
            if kingdom:
                query = query.filter(func.lower(NsrNode.kingdom) == kingdom.lower())
            nsr_species_nodes = query.all()

            if len(nsr_species_nodes) == 1:
                return nsr_species_nodes[0]

            if len(nsr_species_nodes) > 1:
                # should not be possible
                nsm_logger.error('Multiple sp. nodes found in database using taxon "%s"' % cleaned)
                exit()

            # check if the canonical name match a genus node, if yes
            # The strategy will to create a new species node named "[genus] sp."
            genus_name = cleaned[:-4] if cleaned[-4:] == " sp." else cleaned
            query = session.query(NsrNode).filter(NsrNode.name == genus_name, NsrNode.rank == 'genus')
            if kingdom:
                query = query.filter(func.lower(NsrNode.kingdom) == kingdom.lower())
            nsr_genus_nodes = query.all()

            if len(nsr_genus_nodes) == 0:
                nsm_logger.info('Taxon "%s" not found anywhere in NSR topology' % cleaned)
                return None

            if len(nsr_genus_nodes) > 1:
                # the taxon name is a homonym
                nsm_logger.error('multiple genus nodes match using name: "%s"' % cleaned)
                if not kingdom:
                    nsm_logger.error('try to specify -kingdom, to remove ambiguity')
                exit()

            nsr_genus_node = nsr_genus_nodes[0]
            nsr_species = NsrSpecies(canonical_name=sp_name)
            session.add(nsr_species)
            session.flush()

            nsr_species_node = NsrNode(name=sp_name, parent=nsr_genus_node.id, rank='species',
                                       species_id=nsr_species.id, kingdom=nsr_genus_node.kingdom,
                                       phylum=nsr_genus_node.phylum, t_class=nsr_genus_node.t_class,
                                       order=nsr_genus_node.order, family=nsr_genus_node.family,
                                       genus=nsr_genus_node.genus, species=sp_name)
            session.add(nsr_species_node)
            # session.flush()

        except AttributeError as e:
            nsm_logger.error('Problem parsing taxon name "%s"' % taxon)
            nsm_logger.error('Exception: %s' % e)
        except UnparsableNameException as e:
            nsm_logger.error('UnparsableNameException with taxon name "%s"' % taxon)

        return nsr_species_node

    @validates('rank', 'species_id')
    def validates_fields(self, key, value):
        if key == 'species_id' and self.rank == 'species':
            assert value is not None
        return value

    # decorators
    @classmethod
    def get_root(cls, session):
        return session.query(NsrNode).filter(NsrNode.id == 2).first()

    def get_parent(self, session):
        return session.query(NsrNode).filter(NsrNode.id == self.parent).first()

    def get_children(self, session):
        return session.query(NsrNode).filter(NsrNode.parent == self.id)

    def get_leaves(self, session):
        return session.query(NsrNode).filter(NsrNode.left > self.left, NsrNode.right < self.right, NsrNode.left == NsrNode.right)

    def get_ancestors(self, session):
        return session.query(NsrNode).filter(NsrNode.left < self.left, NsrNode.right > self.right)

    def to_ete(self, session, until_rank=None, remove_empty_rank=False, remove_incertae_sedis_rank=False):
        if until_rank:
            index_rank = RANK_INDEX[until_rank]
            # set the max rank to None if the rank specified is the lower rank, i.e. 'species'
            until_rank = index_rank if index_rank != len(RANK_ORDER) - 1 else None
        ete_tree = Tree()
        self._recurse_to_ete(session,
                             ete_tree,
                             until_rank=until_rank,
                             remove_empty_rank=remove_empty_rank,
                             remove_incertae_sedis_rank=remove_incertae_sedis_rank)
        return ete_tree

    def _recurse_to_ete(self,
                        session,
                        ete_node,
                        until_rank=None,
                        remove_empty_rank=False,
                        remove_incertae_sedis_rank=False):

        if until_rank is not None and RANK_INDEX[self.rank] > until_rank:
            return

        if (remove_empty_rank and not self.name) or \
                (remove_incertae_sedis_rank and self.name and "Incertae sedis" in self.name):
            # pass the parent node as new node, i.e. skip the current taxon level
            new_node = ete_node
        else:
            new_node = ete_node.add_child(name=self.name)
            new_node.add_feature('rank', self.rank)
            new_node.add_feature('id', self.id)
            new_node.add_feature('rank_index', RANK_INDEX[self.rank])

        for db_child in self.get_children(session):
            db_child._recurse_to_ete(session,
                                     new_node,
                                     until_rank=until_rank,
                                     remove_empty_rank=remove_empty_rank,
                                     remove_incertae_sedis_rank=remove_incertae_sedis_rank)

    @classmethod
    def get_mrca(cls, session, nodes):
        s = sorted(nodes, key=lambda x: x.left)
        return session.query(NsrNode).filter(NsrNode.left < s[0].left, NsrNode.right > s[-1].right).order_by(NsrNode.left).first()

    def __repr__(self):
        return "<NsrNode(name='%s (%s)')>" % (
            (self.name, self.id))
