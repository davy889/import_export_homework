import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import argparse
from sqlalchemy.exc import OperationalError, IntegrityError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(db_user, db_password, db_host, db_port, db_name):
    """Create a database connection using provided parameters."""
    return create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def export_to_csv(engine, table_name, csv_file_path):
    """Export data from a PostgreSQL table to a CSV file."""
    try:
        df = pd.read_sql_table(table_name, engine)
        df.drop(columns=['id'], errors='ignore', inplace=True)  # Drop 'id' column if it exists
        df.to_csv(csv_file_path, index=False)
        logger.info(f"Data exported successfully to {csv_file_path}.")
    except Exception as e:
        logger.error(f"Error exporting data: {e}")

def import_from_csv(engine, table_name, csv_file_path):
    """Import data from a CSV file into a PostgreSQL table."""
    try:
        with engine.connect() as connection:
            connection.execute(text(f"DELETE FROM {table_name};"))  # Clear the table
            connection.commit()
            logger.info("Table cleared successfully.")

        df = pd.read_csv(csv_file_path)
        df.drop(columns=['id'], errors='ignore', inplace=True)  # Drop 'id' column if it exists
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Data uploaded successfully! {len(df)} records imported.")
    except FileNotFoundError:
        logger.error(f"The file {csv_file_path} was not found.")
    except pd.errors.EmptyDataError:
        logger.error("The file is empty.")
    except pd.errors.ParserError:
        logger.error("The file could not be parsed.")
    except IntegrityError as e:
        logger.error(f"Integrity error during import: {e}")
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
    except Exception as e:
        logger.error(f"Error importing data: {e}")

def main():
    parser = argparse.ArgumentParser(description='Export or import data to/from a PostgreSQL table.')
    parser.add_argument('operation', choices=['export', 'import'], help='Specify whether to export or import data.')
    parser.add_argument('table_name', help='The name of the PostgreSQL table.')
    parser.add_argument('--csv_file', help='The path to the CSV file (required for import).', required=False)
    parser.add_argument('--db_user', required=True, help='Database user name.')
    parser.add_argument('--db_password', required=True, help='Database password.')
    parser.add_argument('--db_host', default='localhost', help='Database host (default: localhost).')
    parser.add_argument('--db_port', default='5432', help='Database port (default: 5432).')
    parser.add_argument('--db_name', required=True, help='Database name.')

    args = parser.parse_args()

    # Create a database connection
    engine = get_db_connection(args.db_user, args.db_password, args.db_host, args.db_port, args.db_name)

    if args.operation == 'export':
        export_to_csv(engine, args.table_name, f"{args.table_name}.csv")
    elif args.operation == 'import':
        if not args.csv_file:
            logger.error("CSV file path is required for import.")
            return
        import_from_csv(engine, args.table_name, args.csv_file)

if __name__ == '__main__':
    main()
