class BaseAdapter:
    placeholder = "%s"

    def connect(self):
        raise NotImplementedError

    def fetch_all(self, table_name):
        raise NotImplementedError

    def fetch_schema(self, table_name):
        raise NotImplementedError

    def list_tables(self):
        raise NotImplementedError

    def map_schema_for_target(self, source_schema, column_mapping):
        mapped = {}
        if not source_schema:
            return mapped
        for source_column, target_column in (column_mapping or {}).items():
            if source_column in source_schema:
                mapped[target_column] = source_schema[source_column]
        return mapped

    def _infer_type_from_data(self, values):
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            return "TEXT"

        if all(isinstance(v, bool) for v in non_null_values):
            return "BOOLEAN"
        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null_values):
            return "BIGINT"
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null_values):
            return "DOUBLE PRECISION"

        return "TEXT"

    def _build_table_schema(self, data, schema=None):
        columns = list(data[0].keys())
        table_schema = {}
        for column in columns:
            column_values = [row.get(column) for row in data]
            declared_type = (schema or {}).get(column, {}).get("type")
            is_nullable = (schema or {}).get(column, {}).get("nullable")

            table_schema[column] = {
                "type": declared_type or self._infer_type_from_data(column_values),
                "nullable": any(value is None for value in column_values)
                if is_nullable is None
                else bool(is_nullable),
            }
        return table_schema

    def insert(self, table_name, data, schema=None):
        if not data:
            return

        table_schema = self._build_table_schema(data, schema=schema)
        columns = list(table_schema.keys())
        cols_str = ", ".join(columns)
        placeholders = ", ".join([self.placeholder] * len(columns))
        cursor = self.conn.cursor()
        sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        create_cols = ",\n".join(
            [
                f"{col} {table_schema[col]['type']}"
                + ("" if table_schema[col]["nullable"] else " NOT NULL")
                for col in columns
            ]
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {create_cols}
            );
            """
        )
        for row in data:
            cursor.execute(sql, tuple(row.get(c) for c in columns))
        self.conn.commit()

    def close(self):
        if getattr(self, "conn", None):
            self.conn.close()
