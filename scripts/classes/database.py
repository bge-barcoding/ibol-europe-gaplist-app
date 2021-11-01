from scripts.classes.imports import *

class Database(Base):
    __tablename__ = 'database'

    database_id = Column(Integer, primary_key=True)
    database_name = Column(String)

    def __repr__(self):
        return "<Database(database_name='%s')>" % (
                         self.database_name)