"""
Script to extract species statistics into a single-pane view TSV file.
For each species, provides taxonomy information and counts of barcodes and specimens.
Optimized for better performance.
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Set, Tuple

from sqlalchemy import create_engine, Engine, event, func, and_, or_, not_, select
from sqlalchemy.orm import sessionmaker, Session, aliased
from sqlalchemy.orm.session import close_all_sessions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('species_stats_extractor')

# Import ORM models
from orm.common import Base
from orm.nsr_species import NsrSpecies
from orm.nsr_node import NsrNode
from orm.specimen import Specimen
from orm.barcode import Barcode


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Extract species statistics to a TSV file')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--output', type=str, default='species_stats.tsv', help='Output TSV file path')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Number of species to process in a batch (default: 500)')
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
        cursor.execute('pragma journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')
        cursor.execute('PRAGMA cache_size=500000')  # Increased cache size
        cursor.execute('PRAGMA temp_store=MEMORY')
        cursor.execute('PRAGMA mmap_size=30000000000')  # 30GB, adjust based on system RAM
        cursor.close()

    # Connect to the database with optimized settings
    engine = create_engine(f'sqlite:///{db_path}', pool_pre_ping=True, pool_size=10, max_overflow=20)

    # Create session
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()

    return session


def get_species_nodes(session: Session, offset: int = 0, limit: int = None) -> List[Tuple[NsrSpecies, NsrNode]]:
    """
    Get species entries with their corresponding node information, with pagination.

    :param session: SQLAlchemy session
    :param offset: Query offset for pagination
    :param limit: Query limit for pagination
    :return: List of tuples (species, node)
    """
    # Join nsr_species with node where node.species_id = nsr_species.id
    query = session.query(NsrSpecies, NsrNode) \
        .join(NsrNode, NsrNode.species_id == NsrSpecies.id) \
        .filter(NsrNode.rank == 'species')

    # Apply pagination if limit is provided
    if limit is not None:
        query = query.offset(offset).limit(limit)

    return query.all()


def find_subspecies_ids(session: Session, species_node: NsrNode) -> List[int]:
    """
    Find subspecies IDs for a given species node.

    :param session: SQLAlchemy session
    :param species_node: The species node to find subspecies for
    :return: List of subspecies IDs
    """
    # If left == right, there are no subspecies
    if species_node.left == species_node.right:
        return []

    # Find all nodes where parent = species_node.id and get their species_id
    subspecies = session.query(NsrNode.species_id) \
        .filter(NsrNode.parent == species_node.id) \
        .all()

    return [subsp[0] for subsp in subspecies]


def get_barcode_and_specimen_counts_optimized(
        session: Session,
        species_ids: List[int]
) -> Dict[int, Tuple[int, int, int]]:
    """
    Get the counts of BGE barcodes, other barcodes, and collected specimens for multiple species.
    This optimized version processes multiple species at once to reduce database queries.

    :param session: SQLAlchemy session
    :param species_ids: List of species IDs to process
    :return: Dictionary mapping species_id to (arise_barcodes, other_barcodes, collected)
    """
    if not species_ids:
        return {}

    results = {species_id: (0, 0, 0) for species_id in species_ids}

    # Create aliased tables for complex queries
    SpecimenAlias = aliased(Specimen)
    BarcodeAlias = aliased(Barcode)

    # Get all specimens for these species
    specimens = session.query(
        SpecimenAlias.id,
        SpecimenAlias.species_id,
        SpecimenAlias.locality
    ).filter(
        SpecimenAlias.species_id.in_(species_ids)
    ).all()

    # Create mapping of specimen_id to species_id
    specimen_to_species = {s.id: s.species_id for s in specimens}
    specimen_ids = list(specimen_to_species.keys())

    # Get BGE locality specimens
    bge_specimen_ids = [s.id for s in specimens if s.locality == 'BGE']

    if not specimen_ids:
        return results

    # Get all barcodes for these specimens
    barcodes = session.query(
        BarcodeAlias.specimen_id,
        BarcodeAlias.defline
    ).filter(
        BarcodeAlias.specimen_id.in_(specimen_ids)
    ).all()

    # Count barcodes by type and species
    barcoded_specimen_ids = set()
    for barcode in barcodes:
        specimen_id = barcode.specimen_id
        species_id = specimen_to_species[specimen_id]
        barcoded_specimen_ids.add(specimen_id)

        arise_count, other_count, collected_count = results[species_id]

        if barcode.defline == 'BGE':
            results[species_id] = (arise_count + 1, other_count, collected_count)
        elif barcode.defline == 'BOLD':
            results[species_id] = (arise_count, other_count + 1, collected_count)

    # Count BGE specimens without barcodes
    for specimen_id in bge_specimen_ids:
        if specimen_id not in barcoded_specimen_ids:
            species_id = specimen_to_species[specimen_id]
            arise_count, other_count, collected_count = results[species_id]
            results[species_id] = (arise_count, other_count, collected_count + 1)

    return results


def process_species_batch(
        session: Session,
        species_nodes: List[Tuple[NsrSpecies, NsrNode]]
) -> List[Dict]:
    """
    Process a batch of species and collect their statistics.

    :param session: SQLAlchemy session
    :param species_nodes: List of (species, node) tuples to process
    :return: List of dictionaries with species statistics
    """
    results = []

    # First, collect all species IDs and their subspecies IDs
    all_species_ids = []
    species_to_subspecies = {}

    for species, node in species_nodes:
        # Get subspecies for this species
        subspecies_ids = find_subspecies_ids(session, node)

        # Store mapping from species to subspecies
        species_to_subspecies[species.id] = subspecies_ids

        # Add all IDs to the list for batch processing
        all_species_ids.append(species.id)
        all_species_ids.extend(subspecies_ids)

    # Get counts for all species and subspecies in one batch
    all_counts = get_barcode_and_specimen_counts_optimized(session, all_species_ids)

    # Process results for each species
    for species, node in species_nodes:
        try:
            # Get subspecies IDs
            subspecies_ids = species_to_subspecies[species.id]

            # Get counts for the species itself
            species_counts = all_counts.get(species.id, (0, 0, 0))
            arise_barcodes, other_barcodes, collected = species_counts

            # Add counts for all subspecies
            for subsp_id in subspecies_ids:
                subsp_counts = all_counts.get(subsp_id, (0, 0, 0))
                arise_barcodes += subsp_counts[0]
                other_barcodes += subsp_counts[1]
                collected += subsp_counts[2]

            # Calculate total (sum of barcodes only, not including collected specimens)
            species_total = arise_barcodes + other_barcodes

            # Create result entry
            result = {
                'Kingdom': node.kingdom,
                'Phylum': node.phylum,
                'Class': node.t_class,  # Using t_class as per ORM mapping
                'Order': node.order,
                'Family': node.family,
                'Genus': node.genus,
                'Species': node.species,
                'SpeciesTotal': species_total,
                'AriseBarcodes': arise_barcodes,
                'OtherBarcodes': other_barcodes,
                'Collected': collected
            }

            results.append(result)

        except Exception as e:
            logger.error(f"Error processing species {species.canonical_name}: {str(e)}")
            continue

    return results


def extract_species_stats(session: Session, batch_size: int = 500) -> List[Dict]:
    """
    Extract species statistics for all species using batch processing.

    :param session: SQLAlchemy session
    :param batch_size: Number of species to process in a batch
    :return: List of dictionaries with species statistics
    """
    all_results = []

    # Count total species
    total_species = session.query(func.count(NsrSpecies.id)).scalar()
    logger.info(f"Found {total_species} total species to process")

    # Process species in batches
    offset = 0
    while True:
        # Get a batch of species
        species_batch = get_species_nodes(session, offset, batch_size)
        if not species_batch:
            break

        logger.info(f"Processing batch of {len(species_batch)} species (offset: {offset})")

        # Process the batch
        batch_results = process_species_batch(session, species_batch)
        all_results.extend(batch_results)

        # Update offset for next batch
        offset += batch_size
        logger.info(f"Completed batch. Processed {len(all_results)}/{total_species} species so far")

    return all_results


def write_results_to_tsv(results: List[Dict], output_path: str) -> None:
    """
    Write results to a TSV file.

    :param results: List of dictionaries with species statistics
    :param output_path: Path to output TSV file
    """
    # Define column order
    columns = [
        'Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species',
        'SpeciesTotal', 'AriseBarcodes', 'OtherBarcodes', 'Collected'
    ]

    try:
        with open(output_path, 'w') as f:
            # Write header
            f.write('\t'.join(columns) + '\n')

            # Write data
            for result in results:
                line = '\t'.join(str(result.get(col, '')) for col in columns)
                f.write(line + '\n')

        logger.info(f"Successfully wrote {len(results)} results to {output_path}")

    except Exception as e:
        logger.error(f"Error writing results to TSV: {str(e)}")
        raise


def main() -> None:
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    # Check if database file exists
    if not os.path.exists(args.db):
        logger.error(f"Database file not found: {args.db}")
        sys.exit(1)

    # Set up database session
    session = setup_database(args.db)

    try:
        # Extract species statistics
        logger.info(f"Extracting species statistics with batch size {args.batch_size}...")
        results = extract_species_stats(session, args.batch_size)

        # Write results to TSV
        logger.info(f"Writing {len(results)} species entries to {args.output}")
        write_results_to_tsv(results, args.output)

        logger.info("Extraction completed successfully.")

    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()