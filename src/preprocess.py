import os
import argparse
import multiprocessing
from pathlib import Path
from dotenv import load_dotenv
import logging

from database_utils.db_values.preprocess import make_db_lsh
from database_utils.db_catalog.preprocess import make_db_context_vec_db
from database_utils.preprocess_postgres_utils import get_postgres_unique_values, postgres_connect

load_dotenv(override=True)
NUM_WORKERS = 1

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def worker_initializer(db_id: str, args: argparse.Namespace):
    """
    Initializes the worker to create LSH and context vectors for a given database ID.
    
    Args:
        db_id (str): The database ID.
        args (argparse.Namespace): The command line arguments.
    """
    db_directory_path = Path(args.db_root_directory) / db_id

    if args.db_type == 'postgres':
        db_config = {
            'dbname': args.db_name,
            'user': args.db_user,
            'password': args.db_password,
            'host': args.db_host,
            'port': args.db_port
        }
        connection = postgres_connect(db_config)
        unique_values = get_postgres_unique_values(connection)
        preprocessed_path = db_directory_path / "preprocessed"
        preprocessed_path.mkdir(parents=True, exist_ok=True)
        
        make_db_lsh(str(preprocessed_path), 
                    unique_values=unique_values,
                    signature_size=args.signature_size, 
                    n_gram=args.n_gram, 
                    threshold=args.threshold,
                    verbose=args.verbose)
       
        make_db_context_vec_db(db_directory_path,
                               use_value_description=args.use_value_description,
                               unique_values=unique_values)
        
        connection.close()
    else:
        logging.info(f"Creating LSH for {db_id}")
        make_db_lsh(db_directory_path, 
                    signature_size=args.signature_size, 
                    n_gram=args.n_gram, 
                    threshold=args.threshold,
                    verbose=args.verbose)
        logging.info(f"LSH for {db_id} created.")
        logging.info(f"Creating context vectors for {db_id}")
        make_db_context_vec_db(db_directory_path,
                            use_value_description=args.use_value_description)
        logging.info(f"Context vectors for {db_id} created.")

if __name__ == '__main__':
    # Setup argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--db_type', type=str, default="sqlite", help="Type of database")
    args_parser.add_argument('--db_root_directory', type=str, required=True, help="Root directory of the databases")
    args_parser.add_argument('--signature_size', type=int, default=20, help="Size of the MinHash signature")
    args_parser.add_argument('--n_gram', type=int, default=3, help="N-gram size for the MinHash")
    args_parser.add_argument('--threshold', type=float, default=0.01, help="Threshold for the MinHash LSH")
    args_parser.add_argument('--db_id', type=str, default='all', help="Database ID or 'all' to process all databases")
    args_parser.add_argument('--verbose', type=bool, default=True, help="Enable verbose logging")
    args_parser.add_argument('--use_value_description', type=bool, default=True, help="Include value descriptions")

    # PostgreSQL specific arguments
    args_parser.add_argument("--db_name", help="PostgreSQL database name")
    args_parser.add_argument("--db_user", help="PostgreSQL user")
    args_parser.add_argument("--db_password", help="PostgreSQL password")
    args_parser.add_argument("--db_host", default="localhost", help="PostgreSQL host")
    args_parser.add_argument("--db_port", default=5432, type=int, help="PostgreSQL port")

    args = args_parser.parse_args()

    if args.db_id == 'all':
        with multiprocessing.Pool(NUM_WORKERS) as pool:
            for db_id in os.listdir(args.db_root_directory):
                pool.apply_async(worker_initializer, args=(db_id, args))
            pool.close()
            pool.join()
    else:
        worker_initializer(args.db_id, args)

    logging.info("Preprocessing is complete.")
