import logging
from typing import List, Dict

from database_utils.db_config import get_db_config

def get_db_all_tables(db_path: str) -> List[str]:
    """
    Retrieves all table names from the database.
    
    Args:
        db_path (str): The path to the database file.
        
    Returns:
        List[str]: A list of table names.
    """

    try:
        db_config = get_db_config()
        if db_config.db_type == 'sqlite':
            raw_table_names = db_config.execute_query(query="SELECT name FROM sqlite_master WHERE type='table';", db_path=db_path)
            return [table[0].replace('\"', '').replace('`', '') for table in raw_table_names if table[0] != "sqlite_sequence"]
        elif db_config.db_type == 'postgres':
            raw_table_names = db_config.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            return [table[0] for table in raw_table_names]
        else:
            raise ValueError(f"Unsupported database type: {db_config.db_type}")
    except Exception as e:
        logging.error(f"Error in get_db_all_tables: {e}")
        raise e

def get_table_all_columns(db_path: str, table_name: str) -> List[str]:
    """
    Retrieves all column names for a given table.
    
    Args:
        db_path (str): The path to the database file.
        table_name (str): The name of the table.
        
    Returns:
        List[str]: A list of column names.
    """
    try:
        db_config = get_db_config()
        if db_config.db_type == 'sqlite':
            table_info_rows = db_config.execute_query(query=f"PRAGMA table_info(`{table_name}`);", db_path=db_path)
            return [row[1].replace('\"', '').replace('`', '') for row in table_info_rows]
        elif db_config.db_type == 'postgres':
            query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = '{table_name}';
            """
            table_info_rows = db_config.execute_query(query=query)
            return [row[0] for row in table_info_rows]
        else:
            raise ValueError(f"Unsupported database type: {db_config.db_type}")
    except Exception as e:
        logging.error(f"Error in get_table_all_columns: {e}\nTable: {table_name}")
        raise e

def get_db_schema(db_path: str) -> Dict[str, List[str]]:
    """
    Retrieves the schema of the database.
    
    Args:
        db_path (str): The path to the database file.
        
    Returns:
        Dict[str, List[str]]: A dictionary mapping table names to lists of column names.
    """
    try:
        table_names = get_db_all_tables(db_path)
        return {table_name: get_table_all_columns(db_path, table_name) for table_name in table_names}
    except Exception as e:
        logging.error(f"Error in get_db_schema: {e}")
        raise e
