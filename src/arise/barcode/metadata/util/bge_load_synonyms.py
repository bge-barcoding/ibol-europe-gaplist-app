"""
Script to import synonym data into the database schema.
Reads a semicolon-separated file with canonical names and their synonyms.
"""

import argparse
import csv
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Set, Tuple

import chardet
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
logger = logging.getLogger('synonym_importer')

# Import ORM models
from orm.common import Base
from orm.nsr_species import NsrSpecies
from orm.nsr_node import NsrNode
from orm.nsr_synonym import NsrSynonym
from orm.specimen import Specimen
from orm.barcode import Barcode


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import synonym data into the database')
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

    return session


def read_synonym_data(
        file_path: str,
        delimiter: str = ';',
        forced_encoding: str = None,
        confidence_threshold: float = 0.7
) -> List[List[str]]:
    """
    Read and parse input file with canonical names and synonyms.
    Handles mixed encodings on a field-by-field basis.

    :param file_path: Path to input file
    :param delimiter: Field delimiter character
    :param forced_encoding: Optional specific encoding to use
    :param confidence_threshold: Minimum confidence for encoding detection
    :return: List of lines, each containing a list of names
    """
    # Define default encodings to try if detection fails
    fallback_encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']
    delimiter_byte = delimiter.encode('ascii')  # Since delimiter is ASCII, this is safe

    # If encoding is specified, use only that one for the whole file
    if forced_encoding:
        try:
            with open(file_path, 'r', encoding=forced_encoding) as f:
                data = []
                for line in f:
                    # Split by delimiter and strip whitespace
                    names = [name.strip() for name in line.strip().split(delimiter)]
                    # Filter out empty strings
                    names = [name for name in names if name]
                    if names:  # Only add non-empty lines
                        data.append(names)

            logger.info(f"Read {len(data)} records from {file_path} using {forced_encoding} encoding")
            return data
        except UnicodeDecodeError as e:
            logger.warning(f"Failed to read file with {forced_encoding} encoding: {e}")
            logger.info("Falling back to mixed encoding detection")

    # If no forced encoding or it failed, try mixed encoding approach
    try:
        with open(file_path, 'rb') as f:
            binary_content = f.read()

        # Split binary content into lines
        if b'\r\n' in binary_content:
            binary_lines = binary_content.split(b'\r\n')
        else:
            binary_lines = binary_content.split(b'\n')

        data = []
        line_count = 0

        for binary_line in binary_lines:
            line_count += 1
            if not binary_line:  # Skip empty lines
                continue

            process_line(binary_line, confidence_threshold, data, delimiter_byte, fallback_encodings, line_count)

        logger.info(f"Read {len(data)} records from {file_path} using mixed encoding detection")
        return data

    except Exception as e:
        logger.error(f"Error reading file with mixed encoding approach: {str(e)}")
        raise ValueError(f"Unable to read {file_path} with mixed encoding approach: {str(e)}")


def process_line(binary_line, confidence_threshold, data, delimiter_byte, fallback_encodings, line_count):
    """
    Process a single line of binary data, attempting to decode it with various encodings.

    :param binary_line: Binary line to process
    :param confidence_threshold: Minimum confidence for encoding detection
    :param data: List to append decoded fields
    :param delimiter_byte: Byte representation of the delimiter
    :param fallback_encodings: List of fallback encodings to try
    :param line_count: Current line number for logging
    """
    # Split line by delimiter
    binary_fields = binary_line.split(delimiter_byte)
    decoded_fields = []
    for binary_field in binary_fields:
        if not binary_field:  # Skip empty fields
            continue

        # Use chardet to detect encoding
        detection = chardet.detect(binary_field)
        detected_encoding = detection['encoding']
        confidence = detection['confidence']

        if detected_encoding and confidence >= confidence_threshold:
            try:
                decoded_field = binary_field.decode(detected_encoding).strip()
                logger.debug(
                    f"Line {line_count}, Field decoded with {detected_encoding} (confidence: {confidence:.2f})")
            except UnicodeDecodeError:
                logger.debug(
                    f"Line {line_count}, Failed to decode with detected encoding {detected_encoding}, trying fallbacks")
                decoded_field = None
        else:
            logger.debug(f"Line {line_count}, Low detection confidence ({confidence:.2f}), trying fallbacks")
            decoded_field = None

        # If detection failed or had low confidence, try fallback encodings
        if decoded_field is None:
            for encoding in fallback_encodings:
                try:
                    decoded_field = binary_field.decode(encoding).strip()
                    logger.debug(f"Line {line_count}, Field decoded with fallback {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

        # If all encodings failed, use 'replace' error handler with utf-8
        if decoded_field is None:
            decoded_field = binary_field.decode('utf-8', errors='replace').strip()
            logger.warning(
                f"Line {line_count}, Field decoded with utf-8 and 'replace' after all attempts failed")

        if decoded_field:
            decoded_fields.append(decoded_field)
    if decoded_fields:  # Only add non-empty lines
        data.append(decoded_fields)


def clean_taxonomic_name(name: str) -> str:
    """
    Clean a taxonomic name by removing various modifiers and symbols.

    :param name: Original taxonomic name
    :return: Cleaned taxonomic name
    """
    # Define patterns to remove
    patterns = [
        r' f\. ',
        r' var\. ',
        r' cf\. ',
        r' \[.+\] ',
        r' group',
        r'_group',
        r' aggr\.',
        r' agg;',
        r' sp\.',
        r' ssp\.',
        r' form ',
        r' cfr\. ',
        r' aff\. ',
        r' pr\. ',
        r' gr\. ',
        r' s\. lato',
        r' s\.l\.',
        r' sl\.',
        r' s\.s\.',
        r' parth\.',
        r'"',
        r' \?',
        r','
    ]

    # Apply each pattern
    cleaned_name = name
    for pattern in patterns:
        cleaned_name = re.sub(pattern, ' ', cleaned_name)

    # Normalize whitespace
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()

    return cleaned_name


def process_subgenus_variants(name: str) -> Set[str]:
    """
    Generate variants for names with subgenus notation.

    For names like "Genus (Subgenus) species", generates:
    - "Genus species"
    - "Genus subgenus"

    :param name: Original species name
    :return: Set of name variants
    """
    # First clean the name
    clean_name = clean_taxonomic_name(name)
    variants = {clean_name}

    if '(' in clean_name and ')' in clean_name:
        # Extract parts
        genus_part = clean_name.split('(')[0].strip()
        subgenus_part = clean_name.split('(')[1].split(')')[0].strip()
        species_part = clean_name.split(')')[-1].strip()

        # Create variants
        variants.add(f"{genus_part} {species_part}".strip())
        variants.add(f"{genus_part} {subgenus_part}".strip())

    return variants


def build_synonym_map(data: List[List[str]]) -> Dict[str, Set[str]]:
    """
    Build a map of canonical names to their synonyms.

    :param data: List of lines, each containing a list of names
    :return: Dictionary mapping canonical names to sets of synonyms
    """
    synonym_map = {}

    for line in data:
        if not line:
            continue

        # Clean the canonical name
        canonical_name = clean_taxonomic_name(line[0])
        if not canonical_name:
            continue

        # Clean and collect all synonyms from the line
        synonyms = set()
        for name in line:
            cleaned_name = clean_taxonomic_name(name)
            if cleaned_name:
                synonyms.add(cleaned_name)

        # Add subgenus variants
        all_variants = set()
        for name in synonyms:
            all_variants.update(process_subgenus_variants(name))

        # Update synonym map
        if canonical_name in synonym_map:
            synonym_map[canonical_name].update(all_variants)
        else:
            synonym_map[canonical_name] = all_variants

    logger.info(f"Built synonym map with {len(synonym_map)} canonical names")
    return synonym_map


def get_species_id(session: Session, canonical_name: str) -> Optional[int]:
    """
    Get species ID for a canonical name.

    :param session: SQLAlchemy session
    :param canonical_name: Canonical species name
    :return: Species ID or None if not found
    """
    species = session.query(NsrSpecies).filter(
        NsrSpecies.canonical_name == canonical_name
    ).first()

    if species:
        return species.id
    else:
        logger.warning(f"Canonical name not found in nsr_species: {canonical_name}")
        return None


def get_node_id(session: Session, species_id: int) -> Optional[int]:
    """
    Get node ID for a species ID.

    :param session: SQLAlchemy session
    :param species_id: Species ID
    :return: Node ID or None if not found
    """
    node = session.query(NsrNode).filter(
        NsrNode.species_id == species_id,
        NsrNode.rank == 'species'
    ).first()

    if node:
        return node.id
    else:
        logger.warning(f"Species ID {species_id} not found in nsr_node")
        return None


def insert_synonyms(
        session: Session,
        synonym_map: Dict[str, Set[str]]
) -> Tuple[int, int]:
    """
    Insert synonyms into nsr_synonym table.

    :param session: SQLAlchemy session
    :param synonym_map: Dictionary mapping canonical names to sets of synonyms
    :return: Tuple of (total_synonyms, created_synonyms)
    """
    total_synonyms = 0
    created_synonyms = 0

    for canonical_name, synonyms in synonym_map.items():
        # Get species_id for canonical name
        species_id = get_species_id(session, canonical_name)
        if not species_id:
            continue

        # Get node_id for species_id
        node_id = get_node_id(session, species_id)
        if not node_id:
            continue

        # Insert each synonym
        for synonym in synonyms:
            total_synonyms += 1

            # Create a new synonym directly
            synonym_obj = session.query(NsrSynonym).filter(
                NsrSynonym.name == synonym,
                NsrSynonym.node_id == node_id
            ).first()

            if not synonym_obj:
                synonym_obj = NsrSynonym(
                    name=synonym,
                    node_id=node_id,
                    species_id=species_id
                )
                session.add(synonym_obj)
                created_synonyms += 1
                logger.debug(f'Created new synonym "{synonym}" for species_id={species_id}')

            # Commit every 1000 synonyms to avoid large transactions
            if total_synonyms % 1000 == 0:
                session.commit()
                logger.info(f"Processed {total_synonyms} synonyms ({created_synonyms} created)")

    # Final commit
    session.commit()
    logger.info(f"Total processed: {total_synonyms} synonyms ({created_synonyms} created)")

    return total_synonyms, created_synonyms


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
        # Read synonym data
        data = read_synonym_data(args.input, args.delimiter, args.encoding)

        # Build synonym map
        synonym_map = build_synonym_map(data)

        # Insert synonyms
        total, created = insert_synonyms(session, synonym_map)

        logger.info(f"Import completed successfully. Processed {total} synonyms, created {created} new entries.")

    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()