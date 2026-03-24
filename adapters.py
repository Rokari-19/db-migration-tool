import psycopg2
import sqlite3

class BaseAdapter:
    def connect(self):
        raise NotImplementedError

    def fetch_all(self, table_name):
        raise NotImplementedError

    def insert(self, table_name, data):
        raise NotImplementedError
    

class PostgresAdapter(BaseAdapter):
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(**self.config)

    def fetch_all(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]

    def insert(self, table_name, data):
        cursor = self.conn.cursor()

        for row in data:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (full_name, email_address)
                VALUES (%s, %s)
                """,
                (row["full_name"], row["email_address"])
            )

        self.conn.commit()
        

class SQLiteAdapter(BaseAdapter):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)

    def fetch_all(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]

    def insert(self, table_name, data):
        cursor = self.conn.cursor()

        for row in data:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (full_name, email_address)
                VALUES (?, ?)
                """,
                (row["full_name"], row["email_address"])
            )

        self.conn.commit()