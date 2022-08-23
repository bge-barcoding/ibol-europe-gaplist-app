import logging
from sqlalchemy import Column, Integer, String, Float, ForeignKey, func
from sqlalchemy.orm import validates
from sqlalchemy.schema import UniqueConstraint
from orm.common import Base, RANK_ORDER
from ete3 import Tree

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
