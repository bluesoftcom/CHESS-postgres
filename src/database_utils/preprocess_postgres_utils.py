import psycopg2
from psycopg2.extras import DictCursor

import logging
from typing import Any, Dict, List, Union

def postgres_connect(db_config: Dict[str, str]):
    """
    Establishes a connection to a PostgreSQL database.

    Args:
        db_config (Dict[str, str]): A dictionary containing database connection parameters.

    Returns:
        connection: A PostgreSQL database connection object.
    """
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error while connecting to PostgreSQL: {error}")
        raise

def execute_postgres_query(connection, query: str, fetch: Union[str, int] = "all") -> Any:
    """
    Executes a PostgreSQL query and fetches results.

    Args:
        connection: A PostgreSQL database connection object.
        query (str): The SQL query to execute.
        fetch (Union[str, int]): How to fetch the results. Options are "all", "one", or an integer.

    Returns:
        Any: The fetched results based on the fetch argument.

    Raises:
        Exception: If an error occurs during query execution.
    """
    try:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            if fetch == "all":
                return cursor.fetchall()
            elif fetch == "one":
                return cursor.fetchone()
            elif isinstance(fetch, int):
                return cursor.fetchmany(fetch)
            else:
                raise ValueError("Invalid fetch argument. Must be 'all', 'one', or an integer.")
    except Exception as e:
        logging.error(f"Error in execute_postgres_query: {e}\nQuery: {query}")
        raise e

def get_postgres_unique_values(connection, exclude_columns: List[str] = []) -> Dict[str, Dict[str, List[str]]]:
    """
    Retrieves unique text values from the PostgreSQL database excluding specified columns.

    Args:
        connection: A PostgreSQL database connection object.
        exclude_columns (List[str]): A list of column names to exclude.

    Returns:
        Dict[str, Dict[str, List[str]]]: A dictionary containing unique values for each table and column.
    """
    unique_values: Dict[str, Dict[str, List[str]]] = {}

    # Get all table names
    table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    tables = [table[0] for table in execute_postgres_query(connection, table_query)]

    for table_name in tables:
        logging.info(f"Processing {table_name}")
        
        # Get column information
        column_query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' AND table_schema = 'public'
        """
        columns = execute_postgres_query(connection, column_query)
        
        table_values: Dict[str, List[str]] = {}
        
        for column in columns:
            column_name, data_type = column['column_name'], column['data_type']
            
            if (column_name.lower() in [col.lower() for col in exclude_columns] or
                any(keyword in column_name.lower() for keyword in ["_id", " id", "url", "email", "web", "time", "phone", "date", "address"]) or
                column_name.endswith("Id") or
                data_type not in ['character varying', 'text']):
                continue

            # Check average length and count of unique values
            length_query = f"""
                SELECT AVG(LENGTH(unique_values)), COUNT(unique_values)
                FROM (
                    SELECT DISTINCT "{column_name}" AS unique_values
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                ) AS subquery
            """
            result = execute_postgres_query(connection, length_query, fetch="one")
            avg_length, count_distinct = result['avg'], result['count']

            if avg_length is None or count_distinct == 0:
                continue

            logging.info(f"Column: {column_name}, avg_length: {avg_length}, count_distinct: {count_distinct}")
            
            if ("name" in column_name.lower() and count_distinct * avg_length < 5000000) or (count_distinct * avg_length < 2000000 and avg_length < 25):
                logging.info(f"Fetching distinct values for {column_name}")
                values_query = f'SELECT DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL'
                values = [str(value[0]) for value in execute_postgres_query(connection, values_query)]
                logging.info(f"Number of different values: {len(values)}")
                table_values[column_name] = values
        
        unique_values[table_name] = table_values

    return unique_values