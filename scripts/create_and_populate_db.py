import os
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from scripts.classes.database import Database
from scripts.classes.marker import Marker
from scripts.classes.imports import *
meta = MetaData()
RESULTS_PATH = fileDir = os.path.join(os.path.dirname(
    os.path.realpath('__file__')), '../data/insert_files/')



def tables(engine, csv_file, table_name):
    df = pd.read_csv(RESULTS_PATH + csv_file)
    df.to_sql(
        table_name,
        engine,
        index=False,
        if_exists='replace'
    )

if __name__ == '__main__':
    engine = create_engine(
        'postgresql://postgres:password@localhost:5432/barcodes', echo=True)
    Session = sessionmaker(engine)
    session = Session()

    Base.metadata.create_all(engine)
    tables(engine, "databases.csv", "database")
    all_databases = session.query(Database)
    for i in all_databases:
        print(i)
    tables(engine, "markers.csv", "marker")
    session.commit()
    all_markers = session.query(Marker)
    for i in all_markers:
        print(i)