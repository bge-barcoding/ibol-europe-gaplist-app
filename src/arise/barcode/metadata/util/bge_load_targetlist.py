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

from sqlalchemy import create_engine, func
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

# Import only what we need directly
from orm.common import Base
from orm.nsr_species import NsrSpecies
from orm.nsr_node import NsrNode


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
    # Use direct SQL queries to avoid relationship loading
    result = session.execute("SELECT id, name, parent, rank FROM node WHERE id = 1").first()

    if not result:
        logger.info("Creating root node")
        root_node = NsrNode(id=1, name='root', parent=0, rank='life')
        session.add(root_node)
        session.flush()
    else:
        root_id, root_name, root_parent, root_rank = result
        root_node = NsrNode(id=root_id, name=root_name, parent=root_parent, rank=root_rank)

    # Check for Animalia node
    result = session.execute(
        "SELECT id, name, parent, rank, kingdom FROM node WHERE name = 'Animalia' AND rank = 'kingdom'").first()

    if not result:
        logger.info("Creating Animalia node")
        animalia_node = NsrNode(
            name='Animalia',
            parent=root_node.id,
            rank='kingdom',
            kingdom='Animalia'
        )
        session.add(animalia_node)
        session.flush()
    else:
        anim_id, anim_name, anim_parent, anim_rank, anim_kingdom = result
        animalia_node = NsrNode(id=anim_id, name=anim_name, parent=anim_parent,
                                rank=anim_rank, kingdom=anim_kingdom)

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
    # Build query conditions for SQL
    conditions = ["rank = ?"]
    params = [rank]

    if kingdom:
        conditions.append("kingdom = ?")
        params.append(kingdom)
    if phylum:
        conditions.append("phylum = ?")
        params.append(phylum)
    if t_class:
        conditions.append("\"class\" = ?")
        params.append(t_class)
    if order:
        conditions.append("\"order\" = ?")
        params.append(order)
    if family:
        conditions.append("family = ?")
        params.append(family)
    if genus:
        conditions.append("genus = ?")
        params.append(genus)
    if species:
        conditions.append("species = ?")
        params.append(species)

    # Build the query
    query = f"SELECT id FROM node WHERE {' AND '.join(conditions)}"

    # Check if node exists
    result = session.execute(query, params).first()

    if not result:
        logger.debug(f"Creating {rank}: {name}")

        # Build columns and values for insert
        columns = ["name", "parent", "rank"]
        values = [name, parent_id, rank]

        if species_id is not None:
            columns.append("species_id")
            values.append(species_id)
        if kingdom:
            columns.append("kingdom")
            values.append(kingdom)
        if phylum:
            columns.append("phylum")
            values.append(phylum)
        if t_class:
            columns.append("\"class\"")
            values.append(t_class)
        if order:
            columns.append("\"order\"")
            values.append(order)
        if family:
            columns.append("family")
            values.append(family)
        if genus:
            columns.append("genus")
            values.append(genus)
        if species:
            columns.append("species")
            values.append(species)

        # Execute insert
        placeholders = ", ".join(["?" for _ in values])
        insert_query = f"INSERT INTO node ({', '.join(columns)}) VALUES ({placeholders}) RETURNING id"
        result = session.execute(insert_query, values).first()

        if not result:
            # Fallback if RETURNING is not supported
            insert_query = f"INSERT INTO node ({', '.join(columns)}) VALUES ({placeholders})"
            session.execute(insert_query, values)
            result = session.execute("SELECT last_insert_rowid()").first()

        node_id = result[0]
    else:
        node_id = result[0]

    # Return node as dictionary
    return {"id": node_id, "name": name, "rank": rank, "parent": parent_id}


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

        # Check if species already exists using direct SQL
        result = session.execute(
            "SELECT id FROM nsr_species WHERE canonical_name = ?",
            (species_name,)
        ).first()

        if not result:
            logger.debug(f"Creating species: {species_name}")
            # Insert species record
            result = session.execute(
                "INSERT INTO nsr_species (canonical_name) VALUES (?) RETURNING id",
                (species_name,)
            ).first()

            if not result:
                # Fallback if RETURNING is not supported
                session.execute(
                    "INSERT INTO nsr_species (canonical_name) VALUES (?)",
                    (species_name,)
                )
                result = session.execute("SELECT last_insert_rowid()").first()

            species_id = result[0]
        else:
            species_id = result[0]

        species_map[species_name] = species_id

    logger.info(f"Processed {len(species_map)} species")
    return species_map


def compute_tree_indexes(session: Session) -> None:
    """
    Compute left and right indexes for the tree using nested set model.

    :param session: SQLAlchemy session
    """
    logger.info("Computing tree indexes")

    # Get root node using direct SQL
    result = session.execute("SELECT id FROM node WHERE id = 1").first()
    if not result:
        logger.error("Root node not found")
        return

    root_id = result[0]

    # Compute indexes recursively
    counter = [1]  # Use list to allow modification in nested function

    def traverse(node_id: int) -> int:
        """
        Recursively traverse tree and set left/right indexes.

        :param node_id: Current node ID
        :return: Next index after processing
        """
        # Get current node using direct SQL
        node_result = session.execute("SELECT id FROM node WHERE id = ?", (node_id,)).first()
        if not node_result:
            return counter[0]

        # Set left index (pre-order)
        session.execute("UPDATE node SET \"left\" = ? WHERE id = ?", (counter[0], node_id))
        counter[0] += 1

        # Process children
        children_results = session.execute("SELECT id FROM node WHERE parent = ?", (node_id,)).fetchall()

        if not children_results:
            # Leaf node - left equals right
            session.execute("UPDATE node SET \"right\" = \"left\" WHERE id = ?", (node_id,))
        else:
            for child_result in children_results:
                traverse(child_result[0])

            # Set right index (post-order)
            session.execute("UPDATE node SET \"right\" = ? WHERE id = ?", (counter[0], node_id))
            counter[0] += 1

        return counter[0]

    # Start traversal from root
    traverse(root_id)
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