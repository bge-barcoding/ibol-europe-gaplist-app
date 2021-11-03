# Imports
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from scripts.classes.database import Database
from scripts.classes.marker import Marker
from scripts.classes.imports import *
from scripts.classes.speciesmarker import SpeciesMarker
from scripts.classes.nsrpecies import NsrSpecies
from scripts.classes.tree_ncbi import TreeNcbi
from scripts.classes.nsr_synonym import NsrSynonym

meta = MetaData()

# Set path to csv files used to populate the database
RESULTS_PATH = fileDir = os.path.join(os.path.dirname(
    os.path.realpath('__file__')), '../data/insert_files_banker/')


def populate_tables(engine, csv_file, table_name):
    df = pd.read_csv(RESULTS_PATH + csv_file, sep=",", quotechar='"')
    # Change column "sequenceID" to "sequence_id" (temp)
    # Will be done automatically in data extraction
    if csv_file == "species_markers.csv":
        df = df.rename(columns={"sequenceID": "sequence_id"})
    # Change column first row from NA to UNKNOWN (temp)
    # Will be done automatically in data extraction
    if csv_file == "markers.csv":
        df.iloc[0] = "UNKNOWN"
    # Populate table with csv file contents
    df.to_sql(
        table_name,
        engine,
        index=False,
        if_exists='replace'
    )


def temp_relations_temp():
    # Test if relationships between tables work properly (temp)
    obj = session.query(SpeciesMarker, NsrSpecies)\
        .join(NsrSpecies)\
        .filter(SpeciesMarker.sequence_id == "RMNH.INS.710961@CRS")
    for i in obj:
        print(i.NsrSpecies.species_name + "\t" + i.SpeciesMarker.sequence_id)
    obj = session.query(TreeNcbi, NsrSpecies, SpeciesMarker)\
        .join(NsrSpecies, TreeNcbi.species_id == NsrSpecies.species_id)\
        .join(SpeciesMarker, SpeciesMarker.species_id == NsrSpecies.species_id)\
        .filter(SpeciesMarker.sequence_id=="RMNH.INS.710961@CRS")
    for i in obj:
        print(i.TreeNcbi.name + "\t" + i.NsrSpecies.species_name)
    obj = session.query(NsrSynonym, NsrSpecies, SpeciesMarker)\
        .join(NsrSpecies, NsrSynonym.species_id == NsrSpecies.species_id)\
        .join(SpeciesMarker, NsrSpecies.species_id == SpeciesMarker.species_id)\
        .filter(SpeciesMarker.species_id==3)
    for i in obj:
        print(i.NsrSpecies.species_name + "\t" + i.NsrSynonym.synonym_name + "\t" +
              i.SpeciesMarker.sequence_id)
    session.commit()


if __name__ == '__main__':
    # Make connection to database
    engine = create_engine(
        'postgresql://postgres:password@localhost:5432/barcodes', echo=False)

    # Create session
    Session = sessionmaker(engine)
    session = Session()

    # Drop all tables (temp for test purposes)
    Base.metadata.drop_all(engine, tables=[TreeNcbi.__table__,
                                           SpeciesMarker.__table__,
                                           NsrSynonym.__table__,
                                           NsrSpecies.__table__,
                                           Database.__table__,
                                           Marker.__table__,
                                           ])
    session.commit()

    # Create tables if they do not exist
    Base.metadata.create_all(engine)

    # Populate tables using populate_tables() function
    # dir path?
    populate_tables(engine, "tree_ncbi.csv", "tree_ncbi")
    populate_tables(engine, "tree_nsr.csv", "tree_nsr")
    populate_tables(engine, "species_markers.csv", "species_marker")
    populate_tables(engine, "databases.csv", "database")
    populate_tables(engine, "markers.csv", "marker")
    populate_tables(engine, "nsr_synonyms.csv", "nsr_synonym")
    populate_tables(engine, "nsr_species.csv", "nsr_species")
    session.commit()

    # Test if relationships between tables work properly (temp)
    temp_relations_temp()

