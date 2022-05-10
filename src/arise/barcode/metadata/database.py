from arise.barcode.metadata.imports import *

class Database(Base):
    __tablename__ = 'database'

    database_id = Column(Integer, primary_key=True)
    database_name = Column(String)

    def __repr__(self):
        return "<Database(database_name='%s')>" % (
                         self.database_name)


    #extra table idea: this one to marker_database
    # new one database with info of database (link, full name etc)
