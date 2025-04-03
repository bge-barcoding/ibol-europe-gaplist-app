"""
Script to import specimen data into the database schema.
Reads three TSV files (voucher, taxonomy, and lab) and joins them to populate the specimen and barcode tables.
"""

import argparse
import logging
import os
import pandas as pd
import sys
from typing import Dict, List, Optional, Tuple

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.session import close_all_sessions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('specimen_importer')

# Import ORM models
from orm.common import Base, DataSource
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.specimen import Specimen
from orm.barcode import Barcode
from orm.marker import Marker


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import specimen data into the database')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--voucher', type=str, required=True, help='Path to voucher TSV file')
    parser.add_argument('--taxonomy', type=str, required=True, help='Path to taxonomy TSV file')
    parser.add_argument('--lab', type=str, required=True, help='Path to lab TSV file with sequence data')
    parser.add_argument('--delimiter', type=str, default='\t', help='TSV delimiter (default: \\t)')
    parser.add_argument('--out-file', type=str, default='addendum.csv', help='Output CSV file')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set logging level')
    return parser.parse_args()


def setup_database(db_path: str) -> Session:
    """
    Set up database connection and return session.

    :param db_path: Path to SQLite database file
    :return: SQLAlchemy session
    """
    # Close any existing sessions to avoid conflicts
    close_all_sessions()

    # Set up SQLite performance optimizations
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute('pragma journal_mode=OFF')
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.execute('PRAGMA cache_size=100000')
        cursor.execute('PRAGMA temp_store = MEMORY')
        cursor.close()

    # Connect to the database (create if it doesn't exist)
    engine = create_engine(f'sqlite:///{db_path}')

    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    # Create session
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()

    return session


def load_data(voucher_path: str, taxonomy_path: str, lab_path: str, delimiter: str = '\t') -> Tuple[
    pd.DataFrame, pd.DataFrame]:
    """
    Load and join voucher, taxonomy, and lab data from TSV files.

    :param voucher_path: Path to voucher TSV file
    :param taxonomy_path: Path to taxonomy TSV file
    :param lab_path: Path to lab TSV file with sequence data
    :param delimiter: Field delimiter character
    :return: Tuple of (joined_specimen_df, lab_df)
    """
    try:
        # Load voucher data
        voucher_df = pd.read_csv(voucher_path, delimiter=delimiter, low_memory=False)
        logger.info(f"Loaded {len(voucher_df)} records from voucher file: {voucher_path}")

        # Load taxonomy data
        # Set low_memory=False to avoid DtypeWarning for mixed types in column 9
        taxonomy_df = pd.read_csv(taxonomy_path, delimiter=delimiter, low_memory=False)
        logger.info(f"Loaded {len(taxonomy_df)} records from taxonomy file: {taxonomy_path}")

        # Load lab data
        lab_df = pd.read_csv(lab_path, delimiter=delimiter, low_memory=False)
        logger.info(f"Loaded {len(lab_df)} records from lab file: {lab_path}")

        # Join the specimen dataframes on Sample ID
        joined_specimen_df = pd.merge(voucher_df, taxonomy_df, on='Sample ID', how='inner')
        logger.info(f"Joined specimen data contains {len(joined_specimen_df)} records")

        return joined_specimen_df, lab_df

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


def find_species_id_by_name(session: Session, species_name: str) -> Optional[int]:
    """
    Find a species_id by looking up the species name in the nsr_synonym table.

    :param session: SQLAlchemy session
    :param species_name: Species name to look up
    :return: species_id or None if not found
    """
    # First, try to find a direct match in NsrSpecies
    species = session.query(NsrSpecies).filter(
        NsrSpecies.canonical_name == species_name
    ).first()

    if species:
        logger.debug(f"Found direct match for '{species_name}' in NsrSpecies: id={species.id}")
        return species.id

    # If not found, look in the synonyms table
    synonym = session.query(NsrSynonym).filter(
        NsrSynonym.name == species_name
    ).first()

    if synonym and synonym.species_id:
        logger.debug(f"Found synonym match for '{species_name}' in NsrSynonym: species_id={synonym.species_id}")
        return synonym.species_id

    return None


def import_specimens(session: Session, data: pd.DataFrame) -> Tuple[int, int, Dict[str, List[str]], Dict[str, int]]:
    """
    Import specimen data into the database.

    :param session: SQLAlchemy session
    :param data: DataFrame containing joined specimen data
    :return: Tuple of (total_specimens, created_specimens, addendum, specimen_id_map)
    """
    total_specimens = 0
    created_specimens = 0
    specimen_id_map = {}
    addendum = {}
    animal_phyla = { 'Annelida', 'Arthropoda', 'Brachiopoda', 'Bryozoa', 'Chordata', 'Cnidaria', 'Ctenophora',
                     'Echinodermata', 'Mollusca', 'Nematoda', 'Nemertea', 'Platyhelminthes', 'Porifera', 'Rotifera'
                     'Xenacoelomorpha'}

    for _, row in data.iterrows():
        try:
            total_specimens += 1

            # Get taxonomy information
            phylum = row.get('Phylum', '')
            if phylum not in animal_phyla:

                # This is very common, so reduce verbosity
                logger.debug(f"Phylum '{phylum}' are not animals, skipping")
                continue

            # Get species name
            species_name = row.get('Species', '')

            # Get sample ID
            sample_id = row['Sample ID']

            # Check if species name is NaN or empty
            if pd.isna(species_name) or species_name == '':

                # This is very common, e.g. from malaise traps - so reduce verbosity
                logger.debug(f"No species name provided for Sample ID: {sample_id}, skipping")
                continue

            # Convert to string to ensure proper handling, check string for sp. suffix
            species_name = str(species_name).strip()
            if species_name.endswith(' sp.'):
                logger.info(f"Not a true species identification (ends with sp.): {sample_id}, skipping")
                continue

            # Find species_id
            species_id = find_species_id_by_name(session, species_name)
            if not species_id:
                logger.warning(f"Could not find species_id for '{species_name} ({phylum})', skipping {sample_id}")

                # Squash unmapped species names into a dict key, store the lineage for future target list imports
                addendum[species_name] = [
                    phylum if pd.notna(phylum) else '',
                    row.get('Class', '') if pd.notna(row.get('Class', '')) else '',
                    row.get('Order', '') if pd.notna(row.get('Order', '')) else '',
                    row.get('Family', '') if pd.notna(row.get('Family', '')) else '',
                    ';;;;;;;;;;;;;;'
                ]
                continue

            # For catalognum, use Museum ID if available, otherwise use Field ID
            museum_id = row.get('Museum ID')
            field_id = row.get('Field ID')
            catalog_num = museum_id if pd.notna(museum_id) and museum_id else field_id

            # Check if catalog_num is NaN or empty
            if pd.isna(catalog_num) or catalog_num == '':
                catalog_num = sample_id # BGE_00445_D05

            institution_storing = row.get('Institution Storing', '')
            identifier = row.get('Identifier', '')

            # Set locality to 'BGE'. The barcodes that are going to map against the
            # target list from public snapshots but that are from other specimens
            # will be annotated as 'BOLD'.
            locality = 'BGE'

            # Create or get specimen
            specimen, created = Specimen.get_or_create_specimen(
                species_id=species_id,
                sampleid=sample_id,
                catalognum=catalog_num,
                institution_storing=institution_storing,
                identification_provided_by=identifier,
                locality=locality,
                session=session
            )

            if created:
                created_specimens += 1

            # Store specimen id in map for barcode creation
            specimen_id_map[sample_id] = specimen.id

            # Commit every 1000 specimens to avoid large transactions
            if total_specimens % 1000 == 0:
                session.commit()
                logger.info(f"Processed {total_specimens} specimens ({created_specimens} created)")

        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            logger.debug(f"Problematic row: {row}")
            # Continue with next row
            continue

    # Final commit
    session.commit()
    logger.info(f"Total processed: {total_specimens} specimens ({created_specimens} created)")

    return total_specimens, created_specimens, addendum, specimen_id_map


def import_barcodes(session: Session, lab_data: pd.DataFrame, specimen_id_map: Dict[str, int]) -> Tuple[int, int]:
    """
    Import barcode data into the database.

    :param session: SQLAlchemy session
    :param lab_data: DataFrame containing lab data with sequence information
    :param specimen_id_map: Dictionary mapping Sample ID to specimen.id
    :return: Tuple of (total_barcodes, created_barcodes)
    """
    total_barcodes = 0
    created_barcodes = 0

    # Get or create the COI-5P marker once and reuse it
    coi_marker, _ = Marker.get_or_create_marker('COI-5P', session)
    marker_id = coi_marker.id
    logger.info(f"Using marker '{coi_marker.name}' with ID {marker_id}")

    # Use BOLD as the database source
    database = DataSource.BOLD.value

    # Set constant defline
    defline = 'BGE'

    for _, row in lab_data.iterrows():
        try:
            # Get sample ID and process ID
            sample_id = row.get('Sample ID')
            process_id = row.get('Process ID')

            if not process_id or pd.isna(process_id):
                logger.warning(f"Missing Process ID for Sample ID: {sample_id}, skipping barcode creation")
                continue

            # Skip if there's no sequence data
            coi_seq_length = row.get('COI-5P Seq. Length', '0[n]')
            if coi_seq_length == '0[n]':
                logger.debug(f"No COI-5P sequence for Sample ID: {sample_id}, skipping")
                continue

            # Check if we have a specimen id for this sample
            if sample_id not in specimen_id_map:

                # This is probably normal: we don't create a specimen if it doesn't have species identification
                logger.debug(f"No specimen record found for Sample ID: {sample_id}, skipping barcode creation")
                continue

            specimen_id = specimen_id_map[sample_id]

            # Create or get barcode
            barcode, created = Barcode.get_or_create_barcode(
                specimen_id=specimen_id,
                database=database,
                marker_id=marker_id,
                defline=defline,
                external_id=process_id,
                session=session
            )

            total_barcodes += 1
            if created:
                created_barcodes += 1

            # Commit every 1000 barcodes to avoid large transactions
            if total_barcodes % 1000 == 0:
                session.commit()
                logger.info(f"Processed {total_barcodes} barcodes ({created_barcodes} created)")

        except Exception as e:
            logger.error(f"Error processing barcode: {str(e)}")
            logger.debug(f"Problematic row: {row}")
            # Continue with next row
            continue

    # Final commit
    session.commit()
    logger.info(f"Total processed: {total_barcodes} barcodes ({created_barcodes} created)")

    return total_barcodes, created_barcodes


def main() -> None:
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    # Check if input files exist
    for file_path, file_name in [(args.voucher, "Voucher"), (args.taxonomy, "Taxonomy"), (args.lab, "Lab")]:
        if not os.path.exists(file_path):
            logger.error(f"{file_name} file not found: {file_path}")
            sys.exit(1)

    # Set up database session
    session = setup_database(args.db)

    try:
        # Load and join data
        joined_data, lab_data = load_data(args.voucher, args.taxonomy, args.lab, args.delimiter)

        # Import specimens
        total_specimens, created_specimens, addendum, specimen_id_map = import_specimens(session, joined_data)

        # Import barcodes
        total_barcodes, created_barcodes = import_barcodes(session, lab_data, specimen_id_map)

        # Write addendum to CSV file
        if addendum:
            with open(args.out_file, 'w') as f:
                for species_name, lineage in addendum.items():
                    f.write(f"{species_name};{';'.join(lineage)}\n")
            logger.info(f"Wrote {len(addendum)} unmapped species to {args.out_file}")

        logger.info(
            f"Import completed successfully. Processed {total_specimens} specimens, created {created_specimens} new entries.")
        logger.info(
            f"Barcode import completed successfully. Processed {total_barcodes} barcodes, created {created_barcodes} new entries.")

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()