import random
import sqlite3
import psycopg2
from psycopg2.extras import DictCursor

class DBConfig:
    def __init__(self, db_type, db_path=None, **kwargs):
        self.db_type = db_type
        self.db_path = db_path
        self.postgres_config = kwargs

    def get_connection(self):
        if self.db_type == 'sqlite':
            return sqlite3.connect(self.db_path)
        elif self.db_type == 'postgres':
            return psycopg2.connect(**self.postgres_config, cursor_factory=DictCursor)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def execute_query(self, query:str, fetch='all', db_path=None):
        if self.db_type == 'sqlite' and db_path:
            self.db_path = db_path
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            if fetch == 'all':
                return cur.fetchall()
            elif fetch == 'one':
                return cur.fetchone()
            elif fetch == "random":
                samples = cur.fetchmany(10)
                return random.choice(samples) if samples else []
            elif isinstance(fetch, int):
                return cur.fetchmany(fetch)
            else:
                raise ValueError("Invalid fetch argument. Must be 'all', 'one', or an integer.")

db_config = None

def init_db_config(db_type, db_path=None, **kwargs):
    global db_config
    db_config = DBConfig(db_type, db_path, **kwargs)

def get_db_config():
    if db_config is None:
        raise ValueError("Database configuration not initialized. Call init_db_config first.")
    return db_config