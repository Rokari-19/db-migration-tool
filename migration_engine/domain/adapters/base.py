class BaseAdapter:
    placeholder = "%s"

    def connect(self):
        raise NotImplementedError

    def fetch_all(self, table_name):
        raise NotImplementedError

    def insert(self, table_name, data):
        if not data:
            return
        columns = list(data[0].keys())
        cols_str = ", ".join(columns)
        placeholders = ", ".join([self.placeholder] * len(columns))
        sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        cursor = self.conn.cursor()
        for row in data:
            cursor.execute(sql, tuple(row.get(c) for c in columns))
        self.conn.commit()

    def close(self):
        if getattr(self, "conn", None):
            self.conn.close()
