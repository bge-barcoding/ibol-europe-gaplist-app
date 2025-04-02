#!/usr/bin/env python3
"""
Script to import BOLD data package into the database schema.
Reads a TSV file from BOLD and populates the specimen and barcode tables.
"""

import argparse
import logging
import os
import pandas as pd
import sys
from typing import Dict, List, Optional, Tuple
from enum import IntEnum

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
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
# Note: These imports assume the script is in the same directory as the ORM modules
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
    parser = argparse.ArgumentParser(description='Import BOLD data into the database')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--bold-tsv', type=str, required=True, help='Path to BOLD TSV file')
    parser.add_argument('--delimiter', type=str, default='\t', help='TSV delimiter (default: \\t)')
    parser.add_argument('--marker', type=str, default='COI-5P',
                        help='Target marker to import (default: COI-5P)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set logging level')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of records to process before committing (default: 100)')
    parser.add_argument('--chunk-size', type=int, default=0,
                        help='Process file in chunks of this size (default: 0, process entire file)')
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


def load_bold_data(file_path: str, delimiter: str = '\t', target_marker: str = 'COI-5P') -> pd.DataFrame:
    """
    Load BOLD data from TSV file and filter for target marker.

    :param file_path: Path to BOLD TSV file
    :param delimiter: Field delimiter character
    :param target_marker: Marker code to filter for
    :return: DataFrame with filtered BOLD data
    """
    try:
        # Load data with error handling for inconsistent fields
        logger.info(f"Loading BOLD data from {file_path}...")

        # First try with the C engine for speed
        try:
            bold_df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                low_memory=False,
                comment=None,
                na_values=['None', 'NA', ''],
                on_bad_lines='warn'  # Just warn about bad lines in newer pandas
            )
            logger.info("Successfully loaded data using C engine")
        except Exception as e:
            logger.warning(f"C engine failed, falling back to Python engine: {str(e)}")
            # Fall back to Python engine if C engine fails
            bold_df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                engine='python',
                on_bad_lines='skip',
                quotechar='"',
                escapechar='\\',
                na_values=['None', 'NA', '']
            )
            logger.info("Successfully loaded data using Python engine")

        logger.info(f"Loaded {len(bold_df)} records from BOLD file")

        # Filter for target marker
        if 'marker_code' in bold_df.columns:
            filtered_df = bold_df[bold_df['marker_code'] == target_marker]
            logger.info(f"Filtered to {len(filtered_df)} records with marker code '{target_marker}'")
        else:
            logger.warning(f"Column 'marker_code' not found in BOLD data. Using all records.")
            filtered_df = bold_df

        return filtered_df

    except Exception as e:
        logger.error(f"Error loading BOLD data: {str(e)}")
        raise


def find_species_id_by_name(session: Session, species_name: str, subspecies_name: str = None) -> Optional[int]:
    """
    Find a species_id by looking up the species name in the nsr_species table
    or through the nsr_synonym table. If subspecies is provided, try that first.

    :param session: SQLAlchemy session
    :param species_name: Species name to look up
    :param subspecies_name: Subspecies name to try first (if provided)
    :return: species_id or None if not found
    """
    # Clean up the species name
    if pd.isna(species_name) or not species_name or species_name == 'None':
        return None

    species_name = species_name.strip()

    # If subspecies is provided, try that first
    if subspecies_name and not pd.isna(subspecies_name) and subspecies_name != 'None':
        subspecies_name = subspecies_name.strip()
        # Try direct match for subspecies
        subspecies = session.query(NsrSpecies).filter(
            NsrSpecies.canonical_name == subspecies_name
        ).first()

        if subspecies:
            logger.debug(f"Found direct match for subspecies '{subspecies_name}' in NsrSpecies: id={subspecies.id}")
            return subspecies.id

        # Try synonym match for subspecies
        subspecies_synonym = session.query(NsrSynonym).filter(
            NsrSynonym.name == subspecies_name
        ).first()

        if subspecies_synonym and subspecies_synonym.species_id:
            logger.debug(
                f"Found synonym match for subspecies '{subspecies_name}' in NsrSynonym: species_id={subspecies_synonym.species_id}")
            return subspecies_synonym.species_id

    # If subspecies not found or not provided, try species
    # First, try to find a direct match in NsrSpecies
    species = session.query(NsrSpecies).filter(
        NsrSpecies.canonical_name == species_name
    ).first()

    if species:
        logger.debug(f"Found direct match for species '{species_name}' in NsrSpecies: id={species.id}")
        return species.id

    # If not found, look in the synonyms table
    synonym = session.query(NsrSynonym).filter(
        NsrSynonym.name == species_name
    ).first()

    if synonym and synonym.species_id:
        logger.debug(f"Found synonym match for species '{species_name}' in NsrSynonym: species_id={synonym.species_id}")
        return synonym.species_id

    return None


def get_or_create_marker(session: Session, marker_name: str = 'COI-5P') -> Tuple[int, bool]:
    """
    Get or create the marker record.

    :param session: SQLAlchemy session
    :param marker_name: Name of the marker
    :return: Tuple of (marker_id, is_created)
    """
    marker, marker_created = Marker.get_or_create_marker(marker_name, session)

    if marker_created:
        logger.info(f"Created new marker '{marker_name}' with id={marker.id}")
    else:
        logger.info(f"Using existing marker '{marker_name}' with id={marker.id}")

    return marker.id, marker_created


def validate_record(row: pd.Series, idx: int) -> Tuple[bool, Optional[str]]:
    """
    Validate that a record has the necessary fields.

    :param row: DataFrame row
    :param idx: Row index for error reporting
    :return: Tuple of (is_valid, error_message)
    """
    # Check for required fields
    process_id = row.get('processid')
    if pd.isna(process_id) or not process_id or process_id == 'None':
        return False, f"Missing process ID at row {idx}"

    species_name = row.get('species')
    if pd.isna(species_name) or not species_name or species_name == 'None':
        return False, f"Missing species name for process ID {process_id}"

    return True, None


def get_or_create_specimen(session: Session, row: pd.Series, species_id: int) -> Tuple[Specimen, bool]:
    """
    Get an existing specimen or create a new one.

    :param session: SQLAlchemy session
    :param row: DataFrame row with specimen data
    :param species_id: Species ID to associate with specimen
    :return: Tuple of (specimen, is_new)
    """
    # Extract specimen fields
    process_id = row.get('processid')
    sample_id = row.get('sampleid', '')
    museum_id = row.get('museumid', '')
    institution = row.get('inst', '')
    identified_by = row.get('identified_by', '')

    # Handle 'None' values
    sample_id = '' if sample_id == 'None' or pd.isna(sample_id) else sample_id
    museum_id = '' if museum_id == 'None' or pd.isna(museum_id) else museum_id
    institution = '' if institution == 'None' or pd.isna(institution) else institution
    identified_by = '' if identified_by == 'None' or pd.isna(identified_by) else identified_by

    # Use catalog_num as museumid if available, otherwise process_id
    catalog_num = museum_id if museum_id else sample_id

    # Check if specimen with this catalog_num already exists
    existing_specimen = session.query(Specimen).filter(
        Specimen.catalognum == catalog_num
    ).first()

    if existing_specimen:
        # Use existing specimen
        logger.debug(f"Using existing specimen with catalognum={catalog_num}")
        return existing_specimen, False

    # Create new specimen
    specimen, created = Specimen.get_or_create_specimen(
        species_id=species_id,
        sampleid=sample_id if sample_id else process_id,
        catalognum=catalog_num,
        institution_storing=institution,
        identification_provided_by=identified_by,
        locality='BOLD',  # As specified in requirements
        session=session
    )

    if created:
        logger.debug(f"Created new specimen for {process_id}")

    return specimen, created


def create_barcode(
        session: Session, specimen: Specimen, marker_id: int, process_id: str, species_name: str
) -> Tuple[Barcode, bool]:
    """
    Create a barcode record.

    :param session: SQLAlchemy session
    :param specimen: Specimen object
    :param marker_id: Marker ID
    :param process_id: BOLD process ID
    :param species_name: Species name for defline
    :return: Tuple of (barcode, is_created)
    """
    # Create defline
    defline = f"BOLD|{process_id}|{species_name}"

    # Create barcode record
    barcode, created = Barcode.get_or_create_barcode(
        specimen_id=specimen.id,
        database=DataSource.BOLD.value,  # From enum in orm.common
        marker_id=marker_id,
        defline=defline,
        external_id=process_id,  # As specified in requirements
        session=session
    )

    return barcode, created


def process_bold_data(session: Session, data: pd.DataFrame, batch_size: int = 100) -> Dict:
    """
    Process BOLD data and import into the database.

    :param session: SQLAlchemy session
    :param data: DataFrame containing BOLD data
    :param batch_size: Number of records to process before committing
    :return: Statistics dictionary
    """
    # Get or create the marker
    marker_id, _ = get_or_create_marker(session)

    # Track statistics
    stats = {
        'total_records': 0,
        'processed_records': 0,
        'skipped_records': 0,
        'new_specimens': 0,
        'existing_specimens': 0,
        'new_barcodes': 0,
        'errors': {}
    }

    # Process records
    for idx, row in data.iterrows():
        try:
            stats['total_records'] += 1

            # Validate record
            is_valid, error_msg = validate_record(row, idx)
            if not is_valid:
                logger.info(error_msg)
                stats['skipped_records'] += 1
                continue

            # Extract key data
            process_id = row.get('processid')
            species_name = row.get('species')
            subspecies_name = row.get('subspecies')

            # Find species ID (trying subspecies first if available)
            species_id = find_species_id_by_name(session, species_name, subspecies_name)
            if not species_id:
                # Skip records without a species match
                stats['skipped_records'] += 1
                logger.debug(f"Species not found in target list: {species_name}")
                continue

            # Get or create specimen
            specimen, is_new_specimen = get_or_create_specimen(session, row, species_id)

            if is_new_specimen:
                stats['new_specimens'] += 1
            else:
                stats['existing_specimens'] += 1

            # Create barcode record
            _, is_new_barcode = create_barcode(
                session, specimen, marker_id, process_id, species_name
            )

            if is_new_barcode:
                stats['new_barcodes'] += 1

            stats['processed_records'] += 1

            # Commit in batches
            if stats['processed_records'] % batch_size == 0:
                session.commit()
                logger.info(f"Processed {stats['processed_records']} records "
                            f"({stats['new_specimens']} new specimens, "
                            f"{stats['new_barcodes']} new barcodes)")

        except Exception as e:
            error_msg = str(e)
            if error_msg not in stats['errors']:
                stats['errors'][error_msg] = 1
            else:
                stats['errors'][error_msg] += 1

            logger.error(f"Error processing row {idx}: {error_msg}")
            stats['skipped_records'] += 1
            continue

    # Final commit
    session.commit()
    logger.info(f"Total processed: {stats['processed_records']} records "
                f"({stats['new_specimens']} new specimens, "
                f"{stats['new_barcodes']} new barcodes, "
                f"{stats['skipped_records']} skipped)")

    return stats


def report_errors(errors: Dict[str, int]) -> None:
    """
    Report errors encountered during processing.

    :param errors: Dictionary of error messages and counts
    """
    if not errors:
        return

    logger.warning("Encountered the following errors during processing:")
    for error_msg, count in errors.items():
        logger.warning(f"  {error_msg} ({count} occurrences)")


def process_chunk(chunk: pd.DataFrame, session: Session, marker_id: int, batch_size: int) -> Dict:
    """
    Process a chunk of BOLD data.

    :param chunk: DataFrame chunk to process
    :param session: SQLAlchemy session
    :param marker_id: ID of the marker
    :param batch_size: Batch size for commits
    :return: Statistics dictionary for this chunk
    """
    # Initialize stats for this chunk
    chunk_stats = {
        'total_records': 0,
        'processed_records': 0,
        'skipped_records': 0,
        'new_specimens': 0,
        'existing_specimens': 0,
        'new_barcodes': 0,
        'errors': {}
    }

    # Process records in chunk
    for idx, row in chunk.iterrows():
        try:
            chunk_stats['total_records'] += 1

            # Validate record
            is_valid, error_msg = validate_record(row, idx)
            if not is_valid:
                logger.info(error_msg)
                chunk_stats['skipped_records'] += 1
                continue

            # Extract key data
            process_id = row.get('processid')
            species_name = row.get('species')
            subspecies_name = row.get('subspecies')

            # Find species ID (trying subspecies first if available)
            species_id = find_species_id_by_name(session, species_name, subspecies_name)
            if not species_id:
                # Skip records without a species match
                chunk_stats['skipped_records'] += 1
                logger.debug(f"Species not found in target list: {species_name}")
                continue

            # Get or create specimen
            specimen, is_new_specimen = get_or_create_specimen(session, row, species_id)

            if is_new_specimen:
                chunk_stats['new_specimens'] += 1
            else:
                chunk_stats['existing_specimens'] += 1

            # Create barcode record
            _, is_new_barcode = create_barcode(
                session, specimen, marker_id, process_id, species_name
            )

            if is_new_barcode:
                chunk_stats['new_barcodes'] += 1

            chunk_stats['processed_records'] += 1

            # Commit in batches
            if chunk_stats['processed_records'] % batch_size == 0:
                session.commit()
                logger.info(f"Processed {chunk_stats['processed_records']} records in current chunk "
                            f"({chunk_stats['new_specimens']} new specimens, "
                            f"{chunk_stats['new_barcodes']} new barcodes)")

        except Exception as e:
            error_msg = str(e)
            if error_msg not in chunk_stats['errors']:
                chunk_stats['errors'][error_msg] = 1
            else:
                chunk_stats['errors'][error_msg] += 1

            logger.error(f"Error processing row {idx}: {error_msg}")
            chunk_stats['skipped_records'] += 1
            continue

    # Final commit for this chunk
    session.commit()

    return chunk_stats


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
        # Process in chunks or all at once based on args
        if args.chunk_size > 0:
            logger.info(f"Processing BOLD data in chunks of {args.chunk_size} records...")

            # Get marker ID once
            marker_id, _ = get_or_create_marker(session)

            # Initialize combined stats
            total_stats = {
                'total_records': 0,
                'processed_records': 0,
                'skipped_records': 0,
                'new_specimens': 0,
                'existing_specimens': 0,
                'new_barcodes': 0,
                'errors': {}
            }

            # Read and process in chunks - try with C engine first
            try:
                chunk_iter = pd.read_csv(
                    args.bold_tsv,
                    delimiter=args.delimiter,
                    chunksize=args.chunk_size,
                    low_memory=False,
                    na_values=['None', 'NA', ''],
                    on_bad_lines='warn'
                )
                logger.info("Using C engine for chunk processing")
            except Exception as e:
                logger.warning(f"C engine failed for chunking, falling back to Python engine: {str(e)}")
                chunk_iter = pd.read_csv(
                    args.bold_tsv,
                    delimiter=args.delimiter,
                    chunksize=args.chunk_size,
                    engine='python',
                    on_bad_lines='skip',
                    quotechar='"',
                    escapechar='\\',
                    na_values=['None', 'NA', '']
                )
                logger.info("Using Python engine for chunk processing")

            for i, chunk in enumerate(chunk_iter):
                logger.info(f"Processing chunk {i + 1}...")

                # Filter for target marker
                if 'marker_code' in chunk.columns:
                    filtered_chunk = chunk[chunk['marker_code'] == args.marker]
                    logger.info(f"Filtered chunk to {len(filtered_chunk)} records with marker code '{args.marker}'")
                else:
                    filtered_chunk = chunk

                # Process this chunk
                chunk_stats = process_chunk(filtered_chunk, session, marker_id, args.batch_size)

                # Update combined stats
                for key in ['total_records', 'processed_records', 'skipped_records',
                            'new_specimens', 'existing_specimens', 'new_barcodes']:
                    total_stats[key] += chunk_stats[key]

                # Combine errors
                for error_msg, count in chunk_stats['errors'].items():
                    if error_msg in total_stats['errors']:
                        total_stats['errors'][error_msg] += count
                    else:
                        total_stats['errors'][error_msg] = count

                logger.info(f"Completed chunk {i + 1}. Running totals: {total_stats['processed_records']} "
                            f"records processed, {total_stats['new_barcodes']} new barcodes")

            stats = total_stats
        else:
            # Process entire file at once
            logger.info("Processing entire BOLD file at once...")
            bold_data = load_bold_data(args.bold_tsv, args.delimiter, args.marker)
            stats = process_bold_data(session, bold_data, args.batch_size)

        # Report any errors
        if 'errors' in stats:
            report_errors(stats.pop('errors'))

        # Report statistics
        logger.info(f"Import completed successfully.")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()