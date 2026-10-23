import sqlite3
from .base import BaseAdapter


class SQLiteAdapter(BaseAdapter):
    placeholder = "?"

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

    def fetch_schema(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()

        type_mapping = {
            "INTEGER": "BIGINT",
            "REAL": "DOUBLE PRECISION",
            "TEXT": "TEXT",
            "BLOB": "BYTEA",
            "NUMERIC": "NUMERIC",
        }

        schema = {}
        for _, name, col_type, not_null, _, _ in rows:
            sqlite_type = (col_type or "TEXT").upper()
            base_type = sqlite_type.split("(")[0]
            schema[name] = {
                "type": type_mapping.get(base_type, "TEXT"),
                "nullable": not bool(not_null),
            }
        return schema

    def list_tables(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        return [row[0] for row in cursor.fetchall()]
