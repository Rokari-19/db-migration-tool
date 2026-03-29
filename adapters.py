import psycopg2
import sqlite3


class BaseAdapter:
    placeholder = "%s"  # Default

    def connect(self):
        raise NotImplementedError

    def fetch_all(self, table_name):
        raise NotImplementedError

    def insert(self, table_name, data):
        if not data:
            return

        # Dynamically get columns from the first dictionary
        columns = data[0].keys()
        cols_str = ", ".join(columns)
        placeholders = ", ".join([self.placeholder] * len(columns))

        sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        cursor = self.conn.cursor()
        for row in data:
            cursor.execute(sql, tuple(row.values()))
        self.conn.commit()


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

    def insert(self, table_name, data):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                full_name VARCHAR (255) NOT NULL,
                email_address VARCHAR (255) NOT NULL
            );
            """
        )
        for row in data:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (full_name, email_address)
                VALUES (%s, %s)
                """,
                (row["full_name"], row["email_address"])
            )

        self.conn.commit()
        print(f"Loaded {len(data)} records")


class SQLiteAdapter(BaseAdapter):
    placeholder = "%s"

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

    def create_table_from_data(self, table_name, data):
        """Dynamically creates a table based on dict keys."""
        if not data:
            return
        cols = ", ".join([f"{k} TEXT" for k in data[0].keys()])
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")

    def insert(self, table_name, data):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                full_name VARCHAR (255) NOT NULL,
                email_address VARCHAR (255) NOT NULL
            );
            """
        )
        for row in data:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (full_name, email_address)
                VALUES (?, ?)
                """,
                (row["full_name"], row["email_address"])
            )

        self.conn.commit()
