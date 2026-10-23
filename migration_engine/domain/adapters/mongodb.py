from pymongo import MongoClient

from .base import BaseAdapter


class MongoDBAdapter(BaseAdapter):
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.db = None

    def connect(self):
        uri = self.config.get("uri")
        if uri:
            self.conn = MongoClient(uri)
        else:
            self.conn = MongoClient(
                host=self.config.get("host", "localhost"),
                port=int(self.config.get("port", 27017)),
                username=self.config.get("username") or None,
                password=self.config.get("password") or None,
                tls=self.config.get("ssl_mode") in {"require", "verify-full", "verify-ca", True},
            )

        database = self.config.get("database") or self.config.get("dbname")
        if not database:
            raise ValueError("MongoDB requires a database name")
        self.db = self.conn[database]

    def fetch_all(self, table_name):
        documents = self.db[table_name].find()
        rows = []
        for doc in documents:
            doc.pop("_id", None)
            rows.append(doc)
        return rows

    def fetch_schema(self, table_name):
        docs = list(self.db[table_name].find().limit(100))
        if not docs:
            return {}

        columns = set()
        for doc in docs:
            columns.update(key for key in doc.keys() if key != "_id")

        schema = {}
        for col in columns:
            values = [doc.get(col) for doc in docs]
            schema[col] = {
                "type": self._infer_type_from_data(values),
                "nullable": any(v is None for v in values),
            }
        return schema

    def list_tables(self):
        return sorted(self.db.list_collection_names())

    def insert(self, table_name, data, schema=None):
        if not data:
            return
        self.db[table_name].insert_many(data)

    def close(self):
        if self.conn:
            self.conn.close()
