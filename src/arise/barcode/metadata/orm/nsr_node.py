from arise.barcode.metadata.orm.imports import *
from ete3 import Tree, TreeNode

rank_order = ['life', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
rank_index = {r: i for i, r in enumerate(rank_order)}


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
    parent = Column(Integer, index=True)
    left = Column(Integer, index=True, unique=True)
    right = Column(Integer, index=True, unique=True)
    name = Column(String, index=True)
    length = Column(Float)
    height = Column(Float)

    # taxonomic rank, e.g. 'genus'
    rank = Column(String(16))

    # foreign key to nsr_species table
    species_id = Column(Integer, ForeignKey('nsr_species.id'))

    # relationship to species
#    species = relationship('NsrSpecies', backref=backref("species_id", cascade="all, delete"))

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
            index_rank = rank_index[until_rank]
            # set the max rank to None if the rank specified is the lower rank, i.e. 'species'
            until_rank = index_rank if index_rank != len(rank_order) - 1 else None
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

        if until_rank is not None and rank_index[self.rank] > until_rank:
            return

        if (remove_empty_rank and not self.name) or \
                (remove_incertae_sedis_rank and self.name and "Incertae sedis" in self.name):
            # pass the parent node as new node, i.e. skip the current taxon level
            new_node = ete_node
        else:
            new_node = ete_node.add_child(name=self.name)
            new_node.add_feature('rank', self.rank)
            new_node.add_feature('id', self.id)
            new_node.add_feature('rank_index', rank_index[self.rank])

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
