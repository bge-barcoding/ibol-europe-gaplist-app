"""
Script to extract species statistics into a single-pane view TSV file.
For each species, provides taxonomy information and counts of barcodes and specimens.
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Set, Tuple

from sqlalchemy import create_engine, Engine, event, func, and_, or_, not_
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
        cursor.execute('PRAGMA cache_size=100000')
        cursor.execute('PRAGMA temp_store=MEMORY')
        cursor.close()

    # Connect to the database
    engine = create_engine(f'sqlite:///{db_path}')

    # Create session
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()

    return session


def get_species_nodes(session: Session) -> List[Tuple[NsrSpecies, NsrNode]]:
    """
    Get all species entries with their corresponding node information.

    :param session: SQLAlchemy session
    :return: List of tuples (species, node)
    """
    # Join nsr_species with node where node.species_id = nsr_species.id
    query = session.query(NsrSpecies, NsrNode)\
        .join(NsrNode, NsrNode.species_id == NsrSpecies.id)\
        .filter(NsrNode.rank == 'species')
    
    return query.all()


def find_subspecies_nodes(session: Session, species_node: NsrNode) -> List[NsrNode]:
    """
    Find all subspecies nodes for a given species node.

    :param session: SQLAlchemy session
    :param species_node: The species node to find subspecies for
    :return: List of subspecies nodes
    """
    # If left == right, there are no subspecies
    if species_node.left == species_node.right:
        return []
    
    # Find all nodes where parent = species_node.id
    subspecies = session.query(NsrNode)\
        .filter(NsrNode.parent == species_node.id)\
        .all()
    
    return subspecies


def get_barcode_and_specimen_counts(
    session: Session, 
    species_id: int, 
    subspecies_ids: List[int]
) -> Tuple[int, int, int]:
    """
    Get the counts of BGE barcodes, other barcodes, and collected specimens.

    :param session: SQLAlchemy session
    :param species_id: Species ID
    :param subspecies_ids: List of subspecies IDs
    :return: Tuple of (arise_barcodes, other_barcodes, collected)
    """
    # All relevant IDs
    all_ids = [species_id] + subspecies_ids
    
    # Get specimen IDs for this species and its subspecies
    specimen_query = session.query(Specimen.id)\
        .filter(Specimen.species_id.in_(all_ids))
    
    # Add locality condition for the "Collected" count
    specimen_with_locality_query = specimen_query.filter(Specimen.locality == 'BGE')
    
    # Get specimen IDs with barcodes
    specimen_with_barcode_query = session.query(Barcode.specimen_id.distinct())\
        .filter(Barcode.specimen_id.in_(specimen_query.subquery()))
    
    # Count BGE barcodes
    arise_barcodes = session.query(func.count(Barcode.id))\
        .filter(
            Barcode.specimen_id.in_(specimen_query.subquery()),
            Barcode.defline == 'BGE'
        ).scalar()
    
    # Count other barcodes (BOLD)
    other_barcodes = session.query(func.count(Barcode.id))\
        .filter(
            Barcode.specimen_id.in_(specimen_query.subquery()),
            Barcode.defline == 'BOLD'
        ).scalar()
    
    # Count specimens without barcodes
    collected = session.query(func.count(Specimen.id))\
        .filter(
            Specimen.id.in_(specimen_with_locality_query.subquery()),
            not_(Specimen.id.in_(specimen_with_barcode_query.subquery()))
        ).scalar()
    
    return arise_barcodes, other_barcodes, collected


def extract_species_stats(session: Session) -> List[Dict]:
    """
    Extract species statistics for all species.

    :param session: SQLAlchemy session
    :return: List of dictionaries with species statistics
    """
    results = []
    total_species = session.query(func.count(NsrSpecies.id)).scalar()
    
    # Get all species with their nodes
    species_nodes = get_species_nodes(session)
    logger.info(f"Processing {len(species_nodes)} species entries (out of {total_species} total)")
    
    for i, (species, node) in enumerate(species_nodes, 1):
        if i % 100 == 0:
            logger.info(f"Processed {i}/{len(species_nodes)} species")
        
        try:
            # Get subspecies for this species
            subspecies_nodes = find_subspecies_nodes(session, node)
            subspecies_ids = [subnode.species_id for subnode in subspecies_nodes] if subspecies_nodes else []
            
            # Get barcode and specimen counts
            arise_barcodes, other_barcodes, collected = get_barcode_and_specimen_counts(
                session, species.id, subspecies_ids
            )
            
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
        
        logger.info(f"Successfully wrote results to {output_path}")
    
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
        logger.info("Extracting species statistics...")
        results = extract_species_stats(session)
        
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
