"""
Script to upload species statistics from a TSV file to a SQL Server database.
Supports verification of database schema and full replacement of existing data.
"""

import argparse
import csv
import datetime
import logging
import os
import sys
from typing import Dict, List, Set, Optional

import pyodbc  # Using pyodbc for SQL Server connectivity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('species_stats_uploader')


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Upload species statistics to SQL Server database')
    parser.add_argument('--server', type=str, default='sql-prod-targetlist-bge.database.windows.net',
                       help='SQL Server hostname (default: sql-prod-targetlist-bge.database.windows.net)')
    parser.add_argument('--database', type=str, default='targetlist-prod',
                       help='Database name (default: targetlist-prod)')
    parser.add_argument('--user', type=str, default='BGE_DBWriter',
                       help='Database username (default: BGE_DBWriter)')
    parser.add_argument('--password', type=str, required=True,
                       help='Database password')
    parser.add_argument('--table', type=str, default='TargetList',
                       help='Target table name (default: TargetList)')
    parser.add_argument('--input', type=str, required=True,
                       help='Input TSV file path')
    parser.add_argument('--verify', action='store_true',
                       help='Verify database schema without modifying data')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set logging level')
    return parser.parse_args()


def create_connection(server: str, database: str, username: str, password: str) -> pyodbc.Connection:
    """
    Create a connection to the SQL Server database.

    :param server: Server hostname
    :param database: Database name
    :param username: Database username
    :param password: Database password
    :return: Database connection
    """
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        connection = pyodbc.connect(connection_string)
        logger.info(f"Successfully connected to {database} on {server}")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise


def verify_table_schema(connection: pyodbc.Connection, table_name: str) -> bool:
    """
    Verify that the table schema contains the expected columns.

    :param connection: Database connection
    :param table_name: Name of the table to verify
    :return: True if the schema is valid, False otherwise
    """
    required_columns = {
        'Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species',
        'SpeciesTotal', 'AriseBarcodes', 'OtherBarcodes', 'Collected',
        'DateCreated', 'DateModified'
    }
    
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        # Check if all required columns exist
        missing_columns = required_columns - existing_columns
        if missing_columns:
            logger.error(f"Missing required columns in {table_name}: {', '.join(missing_columns)}")
            return False
        
        logger.info(f"Table schema for {table_name} verified successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error verifying table schema: {str(e)}")
        return False


def read_tsv_data(tsv_path: str) -> List[Dict[str, str]]:
    """
    Read data from TSV file.

    :param tsv_path: Path to TSV file
    :return: List of dictionaries with column names as keys
    """
    try:
        data = []
        with open(tsv_path, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                data.append(row)
        
        logger.info(f"Read {len(data)} rows from {tsv_path}")
        return data
    
    except Exception as e:
        logger.error(f"Error reading TSV file: {str(e)}")
        raise


def clear_table(connection: pyodbc.Connection, table_name: str) -> None:
    """
    Clear all data from the table.

    :param connection: Database connection
    :param table_name: Name of the table to clear
    """
    try:
        cursor = connection.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        connection.commit()
        
        # Get the number of affected rows
        cursor.execute(f"SELECT @@ROWCOUNT")
        deleted_rows = cursor.fetchone()[0]
        
        logger.info(f"Cleared {deleted_rows} rows from {table_name}")
    
    except Exception as e:
        logger.error(f"Error clearing table: {str(e)}")
        connection.rollback()
        raise


def upload_data(connection: pyodbc.Connection, table_name: str, data: List[Dict[str, str]], batch_size: int = 1000) -> None:
    """
    Upload data to the database table.

    :param connection: Database connection
    :param table_name: Name of the table to upload to
    :param data: List of dictionaries with column names as keys
    :param batch_size: Number of rows to insert in a single batch
    """
    try:
        now = datetime.datetime.now()
        cursor = connection.cursor()
        
        # Column names for the INSERT statement, explicitly listing the columns we want to insert
        columns = [
            'Kingdom', 'Phylum', 'Class', '[Order]', 'Family', 'Genus', 'Species',
            'SpeciesTotal', 'AriseBarcodes', 'OtherBarcodes', 'Collected',
            'DateCreated', 'DateModified'
        ]
        
        # Parameter placeholders for the INSERT statement
        placeholders = ", ".join(["?"] * len(columns))
        
        # SQL INSERT statement
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Process data in batches
        total_rows = len(data)
        for i in range(0, total_rows, batch_size):
            batch = data[i:i+batch_size]
            rows_to_insert = []
            
            for row in batch:
                # Fix column name for 'Order' which is a reserved word in SQL Server
                order_value = row['Order']
                
                # Map values to the columns in the correct order
                row_values = [
                    row['Kingdom'],
                    row['Phylum'],
                    row['Class'],
                    order_value,
                    row['Family'],
                    row['Genus'],
                    row['Species'],
                    int(row['SpeciesTotal']),
                    int(row['AriseBarcodes']),
                    int(row['OtherBarcodes']),
                    int(row['Collected']),
                    now,
                    now
                ]
                
                rows_to_insert.append(row_values)
            
            # Execute batch insert
            cursor.executemany(insert_sql, rows_to_insert)
            connection.commit()
            
            logger.info(f"Inserted batch {i//batch_size + 1}/{(total_rows-1)//batch_size + 1} ({len(batch)} rows)")
        
        logger.info(f"Total of {total_rows} rows inserted into {table_name}")
    
    except Exception as e:
        logger.error(f"Error uploading data: {str(e)}")
        connection.rollback()
        raise


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

    try:
        # Create database connection
        connection = create_connection(args.server, args.database, args.user, args.password)
        
        # Verify table schema
        if not verify_table_schema(connection, args.table):
            logger.error("Table schema verification failed")
            sys.exit(1)
        
        # If only verifying, exit here
        if args.verify:
            logger.info("Verification completed successfully")
            sys.exit(0)
        
        # Read data from TSV file
        data = read_tsv_data(args.input)
        
        # Clear existing data from the table
        clear_table(connection, args.table)
        
        # Upload new data
        upload_data(connection, args.table, data)
        
        logger.info("Data upload completed successfully")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    
    finally:
        if 'connection' in locals():
            connection.close()


if __name__ == "__main__":
    main()
