import argparse
import json
from datetime import datetime
from typing import Any, Dict, List

from runner.run_manager import RunManager

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the pipeline with the specified configuration.")
    parser.add_argument('--data_mode', type=str, required=True, help="Mode of the data to be processed.")
    parser.add_argument('--data_path', type=str, required=True, help="Path to the data file.")
    parser.add_argument('--pipeline_nodes', type=str, required=True, help="Pipeline nodes configuration.")
    parser.add_argument('--pipeline_setup', type=str, required=True, help="Pipeline setup in JSON format.")
    parser.add_argument('--use_checkpoint', action='store_true', help="Flag to use checkpointing.")
    parser.add_argument('--checkpoint_nodes', type=str, required=False, help="Checkpoint nodes configuration.")
    parser.add_argument('--checkpoint_dir', type=str, required=False, help="Directory for checkpoints.")
    parser.add_argument('--log_level', type=str, default='warning', help="Logging level.")
    
    # Add database configuration arguments
    parser.add_argument('--db_type', type=str, required=True, choices=['sqlite', 'postgres'], help="Type of database to use.")
    parser.add_argument('--db_path', type=str, help="Path to the SQLite database file (for SQLite).")
    parser.add_argument('--db_name', type=str, help="Name of the PostgreSQL database (for PostgreSQL).")
    parser.add_argument('--db_user', type=str, help="Username for PostgreSQL database (for PostgreSQL).")
    parser.add_argument('--db_password', type=str, help="Password for PostgreSQL database (for PostgreSQL).")
    parser.add_argument('--db_host', type=str, default='localhost', help="Host for PostgreSQL database (for PostgreSQL).")
    parser.add_argument('--db_port', type=int, default=5432, help="Port for PostgreSQL database (for PostgreSQL).")
    
    args = parser.parse_args()

    args.run_start_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    
    if args.use_checkpoint:
        print('Using checkpoint')
        if not args.checkpoint_nodes:
            raise ValueError('Please provide the checkpoint nodes to use checkpoint')
        if not args.checkpoint_dir:
            raise ValueError('Please provide the checkpoint path to use checkpoint')

    elif args.db_type == 'postgres' and (not args.db_name or not args.db_user or not args.db_password):
        raise ValueError('Please provide db_name, db_user, and db_password for PostgreSQL database')

    return args


def load_dataset(data_path: str) -> List[Dict[str, Any]]:
    """
    Loads the dataset from the specified path.

    Args:
        data_path (str): Path to the data file.

    Returns:
        List[Dict[str, Any]]: The loaded dataset.
    """
    with open(data_path, 'r') as file:
        dataset = json.load(file)
    return dataset

# import logging
# logging.basicConfig(level=logging.INFO)

def main():
    """
    Main function to run the pipeline with the specified configuration.
    """
    args = parse_arguments()
    dataset = load_dataset(args.data_path)

    run_manager = RunManager(args)
    run_manager.initialize_tasks(dataset)
    run_manager.run_tasks()
    run_manager.generate_sql_files()

if __name__ == '__main__':
    main()
