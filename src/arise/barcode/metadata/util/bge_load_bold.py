"""
Script to import BOLD TSV data into the database schema.
Reads the BOLD TSV file, filters for COI-5P records, and populates the specimen and barcode tables.
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
logger = logging.getLogger('bold_importer')

# Import ORM models
from orm.common import Base, DataSource
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.specimen import Specimen
from orm.barcode import Barcode
from orm.marker import Marker


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
        cursor.execute('pragma journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')
        cursor.execute('PRAGMA cache_size=500000')
        cursor.execute('PRAGMA temp_store=MEMORY')
        cursor.execute('PRAGMA mmap_size=30000000000')  # 30GB, adjust based on file size and RAM
        cursor.execute('PRAGMA page_size=8192')  # 8KB pages can be more efficient
        cursor.close()

    # Connect to the database (create if it doesn't exist)
    engine = create_engine(f'sqlite:///{db_path}')

    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    # Create session
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()

    return session


def get_csv_reader(bold_tsv_path: str, delimiter: str = '\t', chunksize: int = 100000):
    """
    Create a CSV reader that processes the data in chunks.

    :param bold_tsv_path: Path to BOLD TSV file
    :param delimiter: Field delimiter character
    :param chunksize: Number of rows to read at a time
    :return: Iterator yielding DataFrame chunks
    """
    try:
        # Create a CSV reader that processes the file in chunks
        csv_reader = pd.read_csv(
            bold_tsv_path,
            delimiter=delimiter,
            low_memory=False,
            chunksize=chunksize
        )
        logger.info(f"Created CSV reader for file: {bold_tsv_path} with chunk size: {chunksize}")
        return csv_reader

    except Exception as e:
        logger.error(f"Error creating CSV reader: {str(e)}")
        raise


def get_existing_barcodes(session: Session) -> Dict[str, int]:
    """
    Get existing barcodes from the database.

    :param session: SQLAlchemy session
    :return: Dictionary mapping external_id (processid) to barcode.id
    """
    barcode_data = session.query(Barcode.external_id, Barcode.id).all()
    barcode_dict = {external_id: barcode_id for external_id, barcode_id in barcode_data}
    logger.info(f"Found {len(barcode_dict)} existing barcodes in the database")
    return barcode_dict


def find_species_id_by_name(session: Session, species_name: str) -> Optional[int]:
    """
    Find a species_id by looking up the species name in the nsr_species and nsr_synonym tables.

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


def initialize_import_resources(session: Session) -> Tuple[Dict[str, int], int, int, str, str]:
    """
    Initialize resources needed for importing BOLD data.

    :param session: SQLAlchemy session
    :return: Tuple of (existing_barcodes, marker_id, database, defline, locality)
    """
    # Get existing barcodes to avoid duplicates
    existing_barcodes = get_existing_barcodes(session)

    # Get or create the COI-5P marker once and reuse it
    coi_marker, _ = Marker.get_or_create_marker('COI-5P', session)
    marker_id = coi_marker.id
    logger.info(f"Using marker '{coi_marker.name}' with ID {marker_id}")

    # Use BOLD as the database source
    database = DataSource.BOLD.value

    # Set constant defline
    defline = 'BOLD'

    # Set constant locality for BOLD data
    locality = 'BOLD'

    return existing_barcodes, marker_id, database, defline, locality


def validate_record(row: pd.Series, existing_barcodes: Dict[str, int], session: Session) -> Tuple[
    bool, Optional[str], Optional[int], Optional[str]]:
    """
    Validate a record from the BOLD TSV file.

    :param row: Pandas Series representing a row from the BOLD TSV file
    :param existing_barcodes: Dictionary of existing barcodes
    :param session: SQLAlchemy session
    :return: Tuple of (is_valid, processid, species_id, sampleid)
    """
    # Get process ID (external_id)
    processid = row.get('processid')
    if pd.isna(processid) or not processid:
        logger.warning(f"Missing processid, skipping record")
        return False, None, None, None

    # Skip if processid already exists in barcode table
    if processid in existing_barcodes:
        logger.debug(f"Processid '{processid}' already exists in barcode table, skipping")
        return False, processid, None, None

    # Get species name
    species_name = row.get('species')
    if pd.isna(species_name) or not species_name:
        logger.debug(f"No species name provided for processid: {processid}, skipping")
        return False, processid, None, None

    # Find species_id
    species_id = find_species_id_by_name(session, species_name)
    if not species_id:
        logger.debug(f"Could not find species_id for '{species_name}', skipping {processid}")
        return False, processid, None, None

    # Get sampleid
    sampleid = row.get('sampleid')
    if pd.isna(sampleid) or not sampleid:
        logger.debug(f"Missing sampleid for processid: {processid}, skipping")
        return False, processid, None, None

    return True, processid, species_id, sampleid


def get_or_create_specimen_for_record(
        row: pd.Series,
        species_id: int,
        sampleid: str,
        locality: str,
        specimen_cache: Dict[str, int],
        session: Session
) -> Tuple[int, bool]:
    """
    Get or create a specimen for a BOLD record.

    :param row: Pandas Series representing a row from the BOLD TSV file
    :param species_id: Species ID to associate with the specimen
    :param sampleid: Sample ID for the specimen
    :param locality: Locality value for the specimen
    :param specimen_cache: Cache of specimen IDs by sampleid
    :param session: SQLAlchemy session
    :return: Tuple of (specimen_id, created)
    """
    # Check cache first
    if sampleid in specimen_cache:
        return specimen_cache[sampleid], False

    # Get field values for specimen
    museumid = row.get('museumid', '')
    if pd.isna(museumid):
        museumid = ''

    institution = row.get('inst', '')
    if pd.isna(institution):
        institution = ''

    identified_by = row.get('identified_by', '')
    if pd.isna(identified_by):
        identified_by = ''

    # Use museum ID as catalog number, if available
    catalognum = museumid if museumid else sampleid

    # Create or get specimen
    specimen, created = Specimen.get_or_create_specimen(
        species_id=species_id,
        sampleid=sampleid,
        catalognum=catalognum,
        institution_storing=institution,
        identification_provided_by=identified_by,
        locality=locality,
        session=session
    )

    specimen_id = specimen.id
    specimen_cache[sampleid] = specimen_id

    return specimen_id, created


def create_barcode_for_record(
        specimen_id: int,
        database: int,
        marker_id: int,
        defline: str,
        processid: str,
        existing_barcodes: Dict[str, int],
        session: Session
) -> bool:
    """
    Create a barcode for a BOLD record.

    :param specimen_id: Specimen ID to associate with the barcode
    :param database: Database value (DataSource enum value)
    :param marker_id: Marker ID to associate with the barcode
    :param defline: Defline value for the barcode
    :param processid: Process ID to use as external_id
    :param existing_barcodes: Dictionary of existing barcodes to update
    :param session: SQLAlchemy session
    :return: Whether a new barcode was created
    """
    barcode, created = Barcode.get_or_create_barcode(
        specimen_id=specimen_id,
        database=database,
        marker_id=marker_id,
        defline=defline,
        external_id=processid,
        session=session
    )

    if created:
        existing_barcodes[processid] = barcode.id

    return created


def process_data_chunk(
        chunk: pd.DataFrame,
        session: Session,
        existing_barcodes: Dict[str, int],
        marker_id: int,
        database: int,
        defline: str,
        locality: str,
        specimen_cache: Dict[str, int],
        stats: Dict[str, int],
        batch_size: int
) -> Dict[str, int]:
    """
    Process a chunk of data from the BOLD TSV file.

    :param chunk: DataFrame chunk from the BOLD TSV file
    :param session: SQLAlchemy session
    :param existing_barcodes: Dictionary of existing barcodes
    :param marker_id: Marker ID to use for barcodes
    :param database: Database value for barcodes
    :param defline: Defline value for barcodes
    :param locality: Locality value for specimens
    :param specimen_cache: Cache of specimen IDs by sampleid
    :param stats: Dictionary of statistics to update
    :param batch_size: Number of records to process before committing
    :return: Updated statistics dictionary
    """
    # Filter for COI-5P records in this chunk
    coi_chunk = chunk[chunk['marker_code'] == 'COI-5P']
    logger.debug(f"Found {len(coi_chunk)} COI-5P records in chunk")

    # Process each row in the dataframe
    for _, row in coi_chunk.iterrows():
        try:
            stats['processed'] += 1

            # Validate record
            is_valid, processid, species_id, sampleid = validate_record(row, existing_barcodes, session)
            if not is_valid:
                stats['skipped'] += 1
                continue

            # Get or create specimen
            specimen_id, specimen_created = get_or_create_specimen_for_record(
                row, species_id, sampleid, locality, specimen_cache, session
            )

            if specimen_created:
                stats['specimens'] += 1

            # Create barcode
            barcode_created = create_barcode_for_record(
                specimen_id, database, marker_id, defline, processid, existing_barcodes, session
            )

            if barcode_created:
                stats['barcodes'] += 1

            # Commit every batch_size records to avoid large transactions
            if stats['processed'] % batch_size == 0:
                session.commit()
                logger.info(
                    f"Processed {stats['processed']} records "
                    f"({stats['skipped']} skipped, {stats['specimens']} specimens created, "
                    f"{stats['barcodes']} barcodes created)"
                )

        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            logger.debug(f"Problematic row: {row}")
            stats['skipped'] += 1
            # Continue with next row
            continue

    return stats


def import_bold_data(session: Session, csv_reader, batch_size: int = 10000) -> Tuple[int, int, int, int]:
    """
    Import BOLD data into the database by processing chunks.

    :param session: SQLAlchemy session
    :param csv_reader: CSV reader yielding DataFrame chunks
    :param batch_size: Number of records to process before committing
    :return: Tuple of (processed_records, skipped_records, created_specimens, created_barcodes)
    """
    # Initialize resources
    existing_barcodes, marker_id, database, defline, locality = initialize_import_resources(session)

    # Initialize statistics
    stats = {
        'processed': 0,
        'skipped': 0,
        'specimens': 0,
        'barcodes': 0
    }

    # Dictionary to cache specimen_id by sampleid to avoid redundant queries
    specimen_cache = {}

    # Process each chunk from the CSV reader
    chunk_num = 0
    for chunk in csv_reader:
        chunk_num += 1
        logger.info(f"Processing chunk {chunk_num}")

        stats = process_data_chunk(
            chunk, session, existing_barcodes, marker_id, database, defline, locality,
            specimen_cache, stats, batch_size
        )

        # Log progress after each chunk
        logger.info(
            f"Finished chunk {chunk_num}. Total processed: {stats['processed']} records "
            f"({stats['skipped']} skipped, {stats['specimens']} specimens created, "
            f"{stats['barcodes']} barcodes created)"
        )

    # Final commit
    session.commit()
    logger.info(
        f"Total processed: {stats['processed']} records "
        f"({stats['skipped']} skipped, {stats['specimens']} specimens created, "
        f"{stats['barcodes']} barcodes created)"
    )

    return stats['processed'], stats['skipped'], stats['specimens'], stats['barcodes']


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import BOLD TSV data into the database')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--bold-tsv', type=str, required=True, help='Path to BOLD TSV file')
    parser.add_argument('--delimiter', type=str, default='\t', help='TSV delimiter (default: \\t)')
    parser.add_argument('--batch-size', type=int, default=10000,
                        help='Batch size for committing transactions (default: 10000)')
    parser.add_argument('--chunk-size', type=int, default=100000,
                        help='Number of rows to read at a time (default: 100000)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set logging level')
    return parser.parse_args()


def main() -> None:
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    # Check if input file exists
    if not os.path.exists(args.bold_tsv):
        logger.error(f"BOLD TSV file not found: {args.bold_tsv}")
        sys.exit(1)

    # Set up database session
    session = setup_database(args.db)

    try:
        # Create CSV reader that processes file in chunks
        csv_reader = get_csv_reader(args.bold_tsv, args.delimiter, args.chunk_size)

        # Import BOLD data
        processed_records, skipped_records, created_specimens, created_barcodes = import_bold_data(
            session, csv_reader, args.batch_size
        )

        logger.info(
            f"Import completed successfully. "
            f"Processed {processed_records} records, "
            f"skipped {skipped_records} records, "
            f"created {created_specimens} specimens, "
            f"created {created_barcodes} barcodes."
        )

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()