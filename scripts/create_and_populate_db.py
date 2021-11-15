# Imports
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from scripts.classes.database import Database
from scripts.classes.marker import Marker
from scripts.classes.imports import *
from scripts.classes.species_marker import SpeciesMarker
from scripts.classes.nsr_species import NsrSpecies
from scripts.classes.tree_ncbi import TreeNcbi
from scripts.classes.nsr_synonym import NsrSynonym

meta = MetaData()

# Set path to data folder where exports data and the
# database csv files are kept
PATH = fileDir = os.path.join(os.path.dirname(
    os.path.realpath('__file__')), '../data/')


def create_nsr_synonym():
    # *** UPDATE VARIABLE NAMES ***
    # Construct nsr_synonym.csv
    # Merge nsr_export_synonyms (bold_export.py) with nsr_species (backbone.Rmd)
    # Kept when taxon_name and taxon_author in nsr_export_synonyms are the same
    # as species_name and identification_reference in nsr species

    # PATH is ../data/
    df_synonyms = pd.read_csv(PATH + "exports/nsr_export_synoynyms.csv")
    df_species = pd.read_csv(PATH + "insert_files/nsr_species.csv")
    df_joined = pd.merge(df_synonyms, df_species, how="inner",
                         left_on=["taxon_name", "taxon_author"],
                         right_on=["species_name", "identification_reference"])

    # Use dataframe index as synonym_id
    df_joined["synonym_id"] = df_joined.index

    # Keep specific columns and rename identification_reference
    df_joined = df_joined[["synonym_id", "species_id", "synonym_name",
                           "identification_reference_x"]].rename(
        columns={'identification_reference_x': 'identification_reference'})

    # Create nsr_synonyms.csv, 4 columns: synonym_id,
    # species_id, synonym_name and identification_reference
    # PATH is ../data/
    df_joined.to_csv(PATH + "insert_files/nsr_synonym.csv",
                     sep=",", index=False)


def create_database_and_marker():
    # Construct database.csv
    database_data = {'database_name': ['NATURALIS', 'BOLD', 'NCBI',
                                       'WFIB', 'NSR', 'SILVA',
                                       'UNITE']}

    # Convert dictionary to dataframe
    database_df = pd.DataFrame(database_data)

    # Use dataframe index as database_id
    database_df.insert(0, "database_id", database_df.index)

    # Create database.csv, 2 columns: database_id and database_name
    # PATH is ../data/
    database_df.to_csv(PATH + "insert_files/database.csv",
                       sep=",", index=False)

    # Construct marker.csv
    # Done with standard markers (create automatically with export data?)
    marker_data = {'marker_name': ['UNKNOWN', 'COI-3P', 'COI-5P',
                                   '16S', '12S', '18S',
                                   'ITS', 'matK', 'rbcL', 'trnL']}

    # Convert dictionary to dataframe
    marker_df = pd.DataFrame(marker_data)

    # Use dataframe index as marker_id
    marker_df.insert(0, 'marker_id', marker_df.index)

    # Create marker.csv, 2 columns: marker_id and marker_name
    # PATH is ../data/
    marker_df.to_csv(PATH + "insert_files/marker.csv",
                     sep=",", index=False)


def bold_formatting_data(bfd_marker, bfd_species):
    # Load data/exports/bold_match.csv (only needed columns are selected)
    # PATH is ../data/
    df_bold = pd.read_csv(PATH + "exports/bold_match.tsv", sep="\t", usecols=[
        "species_name", "markercode", "sequenceID", "identification_reference"])

    # Rename columns to correspond to database
    df_bold = df_bold.rename(columns={"markercode": "marker_name",
                                      "sequenceID": "sequence_id"})

    # Change sequence_id from float to string
    df_bold["sequence_id"] = df_bold['sequence_id'].astype('Int64').astype(
        'str')

    # Merge bold with nsr_species on species_name and identification_reference
    df_joined = pd.merge(bfd_species, df_bold, how='right',
                         on=["species_name", "identification_reference"])

    # Merge dataframe with marker on marker_name
    df_joined = pd.merge(df_joined, bfd_marker, on="marker_name")

    # Drop unessecary columns and NA rows
    df_bold_species = df_joined.drop(
        ['identification_reference', 'species_name', "marker_name"], axis=1)\
        .dropna()

    # Create new column database_id and give all bold data id 1 (corresponds to
    # database_name BOLD in database.csv)
    df_bold_species.insert(0, "database_id", 1)

    # Change species_id from float to string
    df_bold_species["species_id"] = df_bold_species['species_id'].astype(
        'Int64').astype('str')

    # Return merged and formatted dataframe
    return df_bold_species


def crs_formatting_data(cfd_species, cfd_synonym):
    # Load bold_match.csv (only needed columns are selected)
    df_naturalis = pd.read_csv(PATH + "exports/naturalis.csv")

    # Merge CRS data with nsr_synonyms (with only selected rows) where
    # CRS species column overlaps with nsr_synonyms synonym_name column.
    # Drop the column counts from CRS data
    df_naturalis = pd.merge(df_naturalis, cfd_synonym[['species_id',
                                                       'synonym_name']],
                            right_on="synonym_name", left_on="species",
                            how='left').drop(['counts'], axis=1)

    # Merge dataframe with nsr_species on species_id
    df_naturalis_species = pd.merge(df_naturalis,
                                    cfd_species[['species_id', 'species_name']],
                                    on="species_id", how='left')

    # Put values of column species in species_name if species_name is NA.
    df_naturalis_species['species_name'] = df_naturalis_species[
        'species_name'].combine_first(
        df_naturalis_species['species'])

    # Keep columns species_id and sequence_id, merge nsr_species by species_name
    # to get corresponding species_id
    df_naturalis_species = df_naturalis_species[['sequence_id', 'species_name']]
    df_naturalis_species = pd.merge(df_naturalis_species, cfd_species,
                                    on="species_name")

    # Create new column database_id and give all CRS data id 0 (corresponds to
    # database_name NATURALIS in database.csv)
    df_naturalis_species.insert(1, 'database_id', 0)

    # Create new column marker_id and give all CRS data id 0 (corresponds to
    # database_name UNKNOWN in marker.csv)
    df_naturalis_species.insert(2, 'marker_id', 0)

    # Change species_id from float to string
    df_naturalis_species["species_id"] = df_naturalis_species['species_id']\
        .astype('Int64').astype('str')

    # Select needed coluymns in the right order
    df_naturalis_species = df_naturalis_species[['species_id',
                                                 'database_id',
                                                 'marker_id', "sequence_id"]]
    # Return merged and formatted dataframe
    return df_naturalis_species


def create_species_marker():
    # Load nsr_species.csv (backbone.Rmd), nsr_synonym.csv (bold_export.py),
    # and marker.csv (constructed in this script)
    df_species = pd.read_csv(PATH + "insert_files/nsr_species.csv")
    df_synonym = pd.read_csv(PATH + "insert_files/nsr_synonym.csv")
    df_marker = pd.read_csv(PATH + "insert_files/marker.csv")

    # Use crs_formatting_data() to create dataframe with CRS data
    # Variables needed for this function are the dataframes made with
    # nsr_species.csv and nsr_synonyms.csv
    df_crs = crs_formatting_data(df_species, df_synonym)

    # Use bold_formatting_data() to create dataframe with crs data
    # Variables needed for this function are the dataframes made with
    # marker.csv and nsr_species.csv
    df_bold = bold_formatting_data(df_marker, df_species)

    # Combine the two dataframes to create one
    df_species_markers = df_crs.append(df_bold)

    # Drop rows if the column species_id is empty and drop duplicate rows
    df_species_markers = df_species_markers.dropna().drop_duplicates()

    # Change species_id from string to int
    df_species_markers['species_id'] = df_species_markers['species_id']\
        .astype('float').astype("Int64")

    # Sort dataframe by species_id
    df_species_markers = df_species_markers.sort_values(["species_id"],
                                                        ignore_index=True)

    # Create column species_marker_id and use dataframe index to create
    # the values
    df_species_markers.insert(0, "species_marker_id", df_species_markers.index)

    # Create species_marker.csv, 5 columns: species_marker_id, species_id,
    # database_id, marker_id and sequence_id
    # PATH is ../data/
    df_species_markers.to_csv(PATH + "insert_files/species_marker.csv",
                              sep=",", index=False)


def populate_tables(engine, csv_file, table_name):
    df = pd.read_csv(PATH + "/insert_files/" + csv_file, sep=",", quotechar='"')
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
    obj = session.query(SpeciesMarker, NsrSpecies) \
        .join(NsrSpecies) \
        .filter(SpeciesMarker.sequence_id == "RMNH.INS.710961@CRS")
    for i in obj:
        print(i.NsrSpecies.species_name + "\t" + i.SpeciesMarker.sequence_id)
    obj = session.query(TreeNcbi, NsrSpecies, SpeciesMarker) \
        .join(NsrSpecies, TreeNcbi.species_id == NsrSpecies.species_id) \
        .join(SpeciesMarker, SpeciesMarker.species_id == NsrSpecies.species_id) \
        .filter(SpeciesMarker.sequence_id == "RMNH.INS.710961@CRS")
    for i in obj:
        print(i.TreeNcbi.name + "\t" + i.NsrSpecies.species_name)
    obj = session.query(NsrSynonym, NsrSpecies, SpeciesMarker) \
        .join(NsrSpecies, NsrSynonym.species_id == NsrSpecies.species_id) \
        .join(SpeciesMarker, NsrSpecies.species_id == SpeciesMarker.species_id) \
        .filter(SpeciesMarker.species_id == 3)
    for i in obj:
        print(
            i.NsrSpecies.species_name + "\t" + i.NsrSynonym.synonym_name + "\t" +
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

    # Construct csv files from export data (backbone.Rmd & bold_export.py)
    # Formats it so it can be inserted into the database
    create_nsr_synonym()
    create_database_and_marker()
    create_species_marker()

    # Populate tables using populate_tables() function
    populate_tables(engine, "tree_ncbi.csv", "tree_ncbi")
    populate_tables(engine, "tree_nsr.csv", "tree_nsr")
    populate_tables(engine, "species_marker.csv", "species_marker")
    populate_tables(engine, "database.csv", "database")
    populate_tables(engine, "marker.csv", "marker")
    populate_tables(engine, "nsr_synonym.csv", "nsr_synonym")
    populate_tables(engine, "nsr_species.csv", "nsr_species")
    session.commit()

    # Test if relationships between tables work properly (temp)
    temp_relations_temp()
