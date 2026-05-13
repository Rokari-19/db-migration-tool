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

    def fetch_schema(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT column_name, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        rows = cursor.fetchall()

        type_mapping = {
            "int2": "SMALLINT",
            "int4": "INTEGER",
            "int8": "BIGINT",
            "float4": "REAL",
            "float8": "DOUBLE PRECISION",
            "numeric": "NUMERIC",
            "bool": "BOOLEAN",
            "varchar": "VARCHAR(255)",
            "text": "TEXT",
            "timestamp": "TIMESTAMP",
            "timestamptz": "TIMESTAMPTZ",
            "date": "DATE",
            "json": "JSON",
            "jsonb": "JSONB",
        }

        schema = {}
        for name, udt_name, is_nullable in rows:
            schema[name] = {
                "type": type_mapping.get(udt_name, "TEXT"),
                "nullable": is_nullable == "YES",
            }
        return schema
