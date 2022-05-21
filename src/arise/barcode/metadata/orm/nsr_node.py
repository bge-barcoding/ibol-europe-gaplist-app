from arise.barcode.metadata.orm.imports import *
from ete3 import Tree, TreeNode


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
    rank = Column(String)

    # foreign key to nsr_species table
    species_id = Column(Integer, ForeignKey('nsr_species.species_id'))

    # relationship to species
    species = relationship('Species', backref=backref("species_id", cascade="all, delete"))

    # decorators
    @classmethod
    def get_root(cls, session):
        return session.query(NsrNode).filter(NsrNode.id == 2).first()

    def get_parent(self, session):
        return session.query(NsrNode).filter(NsrNode.id == self.parent).first()

    def get_children(self, session):
        return session.query(NsrNode).filter(NsrNode.parent == self.id)

    def get_descendants(self, session):
        return session.query(NsrNode).filter(NsrNode.left > self.left, NsrNode.right < self.right, NsrNode.left == NsrNode.right)

    def get_ancestors(self, session):
        return session.query(NsrNode).filter(NsrNode.left < self.left, NsrNode.right > self.right)

    def to_ete(self, session):
        ete_tree = Tree()
        ete_node = ete_tree.add_child(name=self.name)
        self._recurse_to_ete(session, ete_tree, ete_node)
        return ete_tree

    def _recurse_to_ete(self, session, ete_tree, ete_node):
        for db_child in self.get_children(session):
            ete_child = ete_node.add_child(name=db_child.name)
            db_child._recurse_to_ete(session, ete_tree, ete_child)

    @classmethod
    def get_mrca(cls, session, nodes):
        s = sorted(nodes, key=lambda x: x.left)
        return session.query(NsrNode).filter(NsrNode.left < s[0].left, NsrNode.right > s[-1].right).order_by(NsrNode.left).first()

    def __repr__(self):
        return "<NsrNode(name='%s')>" % (
                         self.id)
