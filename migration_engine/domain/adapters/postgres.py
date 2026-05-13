import psycopg2
from .base import BaseAdapter


class PostgresAdapter(BaseAdapter):
    placeholder = "%s"

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
