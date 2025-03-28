#!/usr/bin/env python3
"""
Script to import taxonomic data into the database schema.
Builds a hierarchical taxonomic tree and computes nested set indexes.
"""

import argparse
import csv
import logging
import os
import sys
from typing import Dict, List, Optional, Tuple, Set

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
logger = logging.getLogger('taxonomy_importer')

# Import ORM models
from orm.common import Base
from orm.nsr_species import NsrSpecies
from orm.nsr_node import NsrNode
from orm.barcode import Barcode
from orm.specimen import Specimen


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import taxonomic data into the database')
    parser.add_argument('--db', type=str, required=True, help='Path to SQLite database file')
    parser.add_argument('--input', type=str, required=True, help='Path to input CSV file')
    parser.add_argument('--delimiter', type=str, default=';', help='CSV delimiter (default: ;)')
    parser.add_argument('--encoding', type=str, help='Force specific file encoding (e.g., latin-1, utf-8)')
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

    # No need to create tables as they should already exist
    return session


def read_csv_data(file_path: str, delimiter: str = ';', forced_encoding: str = None) -> List[Dict[str, str]]:
    """
    Read and parse CSV input file.

    :param file_path: Path to input CSV file
    :param delimiter: CSV delimiter character
    :param forced_encoding: Optional specific encoding to use
    :return: List of dictionaries representing rows
    """
    # If encoding is specified, use only that one
    if forced_encoding:
        encodings = [forced_encoding]
    else:
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']

    last_error = None
    for encoding in encodings:
        try:
            data = []
            with open(file_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    # Skip rows that only have the family name
                    if row.get('species') and '.' not in row['species'] and row.get('Phylum'):
                        data.append(row)

            logger.info(f"Read {len(data)} valid records from {file_path} using {encoding} encoding")
            return data
        except UnicodeDecodeError as e:
            last_error = e
            logger.warning(f"Failed to read file with {encoding} encoding, trying next...")
        except KeyError as e:
            logger.error(f"CSV file has incorrect headers. Expected 'species' and 'Phylum' columns. Error: {e}")
            raise ValueError(f"CSV file has incorrect headers. Expected 'species' and 'Phylum' columns.")

    # If all encodings fail
    logger.error(f"Unable to read {file_path} with any of the attempted encodings: {encodings}")
    logger.error(f"Last error: {last_error}")
    raise ValueError(f"Unable to read {file_path} with any of the attempted encodings: {encodings}")


def extract_genus(species_name: str) -> str:
    """
    Extract genus name from species binomial.

    :param species_name: Full species name
    :return: Genus name
    """
    # Handle subgenus notation if present
    if '(' in species_name and ')' in species_name:
        genus_part = species_name.split('(')[0].strip()
    else:
        genus_part = species_name.split(' ')[0].strip()

    return genus_part


def create_initial_nodes(session: Session) -> Tuple[NsrNode, NsrNode]:
    """
    Create or get root and Animalia nodes.

    :param session: SQLAlchemy session
    :return: Tuple of (root_node, animalia_node)
    """
    # Check for root node
    root_node = session.query(NsrNode).filter(NsrNode.id == 1).first()

    if not root_node:
        logger.info("Creating root node")
        root_node = NsrNode(id=1, name='root', parent=0, rank='life')
        session.add(root_node)
        session.flush()

    # Check for Animalia node
    animalia_node = session.query(NsrNode).filter(
        NsrNode.name == 'Animalia',
        NsrNode.rank == 'kingdom'
    ).first()

    if not animalia_node:
        logger.info("Creating Animalia node")
        animalia_node = NsrNode(
            name='Animalia',
            parent=root_node.id,
            rank='kingdom',
            kingdom='Animalia'
        )
        session.add(animalia_node)
        session.flush()

    return root_node, animalia_node


def get_or_create_taxonomic_node(
        session: Session,
        name: str,
        rank: str,
        parent_id: int,
        species_id: Optional[int] = None,
        kingdom: Optional[str] = None,
        phylum: Optional[str] = None,
        t_class: Optional[str] = None,
        order: Optional[str] = None,
        family: Optional[str] = None,
        genus: Optional[str] = None,
        species: Optional[str] = None
) -> Dict:
    """
    Look up or create a node at a specific taxonomic level.

    :param session: SQLAlchemy session
    :param name: Taxonomic name
    :param rank: Taxonomic rank
    :param parent_id: ID of parent node
    :param species_id: Link to nsr_species table for species rank
    :param kingdom: Kingdom name
    :param phylum: Phylum name
    :param t_class: Class name
    :param order: Order name
    :param family: Family name
    :param genus: Genus name
    :param species: Species name
    :return: Dictionary with node information
    """
    # Build query using ORM
    query = session.query(NsrNode).filter(NsrNode.rank == rank)

    if kingdom:
        query = query.filter(NsrNode.kingdom == kingdom)
    if phylum:
        query = query.filter(NsrNode.phylum == phylum)
    if t_class:
        query = query.filter(getattr(NsrNode, 't_class') == t_class)
    if order:
        query = query.filter(getattr(NsrNode, 'order') == order)
    if family:
        query = query.filter(NsrNode.family == family)
    if genus:
        query = query.filter(NsrNode.genus == genus)
    if species:
        query = query.filter(NsrNode.species == species)

    # Check if node exists
    node = query.first()

    if not node:
        logger.debug(f"Creating {rank}: {name}")

        # Create node
        node_data = {
            "name": name,
            "parent": parent_id,
            "rank": rank
        }

        if species_id is not None:
            node_data["species_id"] = species_id
        if kingdom:
            node_data["kingdom"] = kingdom
        if phylum:
            node_data["phylum"] = phylum
        if t_class:
            node_data["t_class"] = t_class
        if order:
            node_data["order"] = order
        if family:
            node_data["family"] = family
        if genus:
            node_data["genus"] = genus
        if species:
            node_data["species"] = species

        node = NsrNode(**node_data)
        session.add(node)
        session.flush()

    # Return node as dictionary
    return {
        "id": node.id,
        "name": node.name,
        "rank": node.rank,
        "parent": node.parent
    }


def process_record(
        session: Session,
        record: Dict[str, str],
        animalia_node: Dict,
        species_map: Dict[str, int]
) -> None:
    """
    Process a single taxonomic record and build the tree.

    :param session: SQLAlchemy session
    :param record: Record dictionary from CSV
    :param animalia_node: Animalia node dictionary
    :param species_map: Map of species names to species_id
    """
    species_name = record['species'].strip()
    genus_name = extract_genus(species_name)

    # Define the taxonomic hierarchy
    taxon_levels = [
        {'rank': 'phylum', 'db_field': 'phylum', 'csv_field': 'Phylum', 'value': record['Phylum'].strip()},
        {'rank': 'class', 'db_field': 't_class', 'csv_field': 'Class', 'value': record['Class'].strip()},
        {'rank': 'order', 'db_field': 'order', 'csv_field': 'Order', 'value': record['Order'].strip()},
        {'rank': 'family', 'db_field': 'family', 'csv_field': 'Family', 'value': record['Family'].strip()},
        {'rank': 'genus', 'db_field': 'genus', 'csv_field': None, 'value': genus_name},
        {'rank': 'species', 'db_field': 'species', 'csv_field': None, 'value': species_name}
    ]

    # Start with kingdom Animalia
    parent_id = animalia_node['id']
    classification = {'kingdom': 'Animalia'}

    # Process each level in the taxonomic hierarchy
    for level in taxon_levels:
        # Skip if value is empty
        if not level['value']:
            continue

        # Add to classification dictionary
        classification[level['db_field']] = level['value']

        # For species level, get the species_id
        species_id = None
        if level['rank'] == 'species':
            species_id = species_map.get(species_name)
            logger.info(f"Inserting species: {species_name}")

        # Get or create node
        node = get_or_create_taxonomic_node(
            session=session,
            name=level['value'],
            rank=level['rank'],
            parent_id=parent_id,
            species_id=species_id,
            **classification
        )

        # Update parent_id for next level
        parent_id = node['id']


def get_or_create_species(session: Session, data: List[Dict[str, str]]) -> Dict[str, int]:
    """
    Populate nsr_species table and return mapping of species names to IDs.

    :param session: SQLAlchemy session
    :param data: List of record dictionaries
    :return: Dictionary mapping species names to species IDs
    """
    species_map = {}

    for record in data:
        species_name = record['species'].strip()

        # Check if species already exists using ORM
        species = session.query(NsrSpecies).filter(
            NsrSpecies.canonical_name == species_name
        ).first()

        if not species:
            logger.debug(f"Creating species: {species_name}")
            # Create species record
            species = NsrSpecies(canonical_name=species_name)
            session.add(species)
            session.flush()
        else:
            raise ValueError(f"Species already exists: {species_name}")

        species_map[species_name] = species.id
        if len(species_map) % 1000 == 0:
            logger.info(f"Processed {len(species_map)} species")

    logger.info(f"Processed {len(species_map)} species")
    return species_map


def compute_tree_indexes(session: Session) -> None:
    """
    Compute left and right indexes for the tree using nested set model.

    :param session: SQLAlchemy session
    """
    logger.info("Computing tree indexes")

    # Get root node using ORM
    root_node = session.query(NsrNode).filter(NsrNode.id == 1).first()
    if not root_node:
        logger.error("Root node not found")
        return

    # Compute indexes recursively
    counter = [1]  # Use list to allow modification in nested function

    def traverse(node_id: int) -> int:
        """
        Recursively traverse tree and set left/right indexes.

        :param node_id: Current node ID
        :return: Next index after processing
        """
        # Get current node using ORM
        node = session.query(NsrNode).filter(NsrNode.id == node_id).first()
        if not node:
            return counter[0]

        # Set left index (pre-order)
        node.left = counter[0]
        counter[0] += 1

        # Process children
        children = session.query(NsrNode).filter(NsrNode.parent == node_id).all()

        if not children:
            # Leaf node - left equals right
            node.right = node.left
        else:
            for child in children:
                traverse(child.id)

            # Set right index (post-order)
            node.right = counter[0]
            counter[0] += 1

        return counter[0]

    # Start traversal from root
    traverse(root_node.id)
    session.commit()

    logger.info(f"Computed tree indexes up to {counter[0]}")


def main() -> None:
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Set up database session
    session = setup_database(args.db)

    try:
        # Read CSV data
        data = read_csv_data(args.input, args.delimiter, args.encoding)

        # Create initial nodes
        root_node, animalia_node = create_initial_nodes(session)

        # Process species records
        species_map = get_or_create_species(session, data)

        # Build taxonomic tree
        for record in data:
            process_record(session, record, animalia_node, species_map)

        # Compute tree indexes
        compute_tree_indexes(session)

        # Commit changes
        session.commit()
        logger.info("Import completed successfully")

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()