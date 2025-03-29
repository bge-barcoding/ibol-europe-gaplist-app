"""
Script to import specimen data into the database schema.
Reads two TSV files (voucher and taxonomy) and joins them to populate the specimen table.
"""

import argparse
import logging
import os
import pandas as pd
import sys
from typing import Dict, List, Optional, Tuple

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
logger = logging.getLogger('specimen_importer')

# Import ORM models
from orm.common import Base
from orm.nsr_species import NsrSpecies
from orm.nsr_synonym import NsrSynonym
from orm.specimen import Specimen
from orm.barcode import Barcode


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import specimen data into the database')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--voucher', type=str, required=True, help='Path to voucher TSV file')
    parser.add_argument('--taxonomy', type=str, required=True, help='Path to taxonomy TSV file')
    parser.add_argument('--delimiter', type=str, default='\t', help='TSV delimiter (default: \\t)')
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

    # Connect to the database
    engine = create_engine(f'sqlite:///{db_path}')

    # Create session
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()

    return session


def load_data(voucher_path: str, taxonomy_path: str, delimiter: str = '\t') -> pd.DataFrame:
    """
    Load and join voucher and taxonomy data from TSV files.

    :param voucher_path: Path to voucher TSV file
    :param taxonomy_path: Path to taxonomy TSV file
    :param delimiter: Field delimiter character
    :return: Joined DataFrame
    """
    try:
        # Load voucher data
        voucher_df = pd.read_csv(voucher_path, delimiter=delimiter, low_memory=False)
        logger.info(f"Loaded {len(voucher_df)} records from voucher file: {voucher_path}")

        # Load taxonomy data
        # Set low_memory=False to avoid DtypeWarning for mixed types in column 9
        taxonomy_df = pd.read_csv(taxonomy_path, delimiter=delimiter, low_memory=False)
        logger.info(f"Loaded {len(taxonomy_df)} records from taxonomy file: {taxonomy_path}")

        # Join the dataframes on Sample ID
        joined_df = pd.merge(voucher_df, taxonomy_df, on='Sample ID', how='inner')
        logger.info(f"Joined data contains {len(joined_df)} records")

        return joined_df

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


def import_specimens(session: Session, data: pd.DataFrame) -> Tuple[int, int]:
    """
    Import specimen data into the database.

    :param session: SQLAlchemy session
    :param data: DataFrame containing joined specimen data
    :return: Tuple of (total_specimens, created_specimens)
    """
    total_specimens = 0
    created_specimens = 0
    animal_phyla = { 'Annelida', 'Arthropoda', 'Brachiopoda', 'Bryozoa', 'Chordata', 'Cnidaria', 'Ctenophora',
                     'Echinodermata', 'Mollusca', 'Nematoda', 'Nemertea', 'Platyhelminthes', 'Porifera', 'Rotifera'
                     'Xenacoelomorpha'}

    for _, row in data.iterrows():
        try:
            total_specimens += 1

            # Get taxonomy information
            phylum = row.get('Phylum', '')
            if phylum not in animal_phyla:
                logger.info(f"Phylum '{phylum}' are not animals, skipping")
                continue

            # Get species name
            species_name = row.get('Species', '')

            # Get sample ID
            sample_id = row['Sample ID']

            # Check if species name is NaN or empty
            if pd.isna(species_name) or species_name == '':
                logger.info(f"No species name provided for Sample ID: {sample_id}, skipping")
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

            # Set locality to 'BGE' as required
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

            # Commit every 100 specimens to avoid large transactions
            if total_specimens % 100 == 0:
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

    return total_specimens, created_specimens


def main() -> None:
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    # Check if input files exist
    if not os.path.exists(args.voucher):
        logger.error(f"Voucher file not found: {args.voucher}")
        sys.exit(1)
    if not os.path.exists(args.taxonomy):
        logger.error(f"Taxonomy file not found: {args.taxonomy}")
        sys.exit(1)

    # Set up database session
    session = setup_database(args.db)

    try:
        # Load and join data
        joined_data = load_data(args.voucher, args.taxonomy, args.delimiter)

        # Import specimens
        total, created = import_specimens(session, joined_data)

        logger.info(f"Import completed successfully. Processed {total} specimens, created {created} new entries.")

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()