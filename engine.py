from adapters import PostgresAdapter, SQLiteAdapter
import os, sys
from dotenv import load_dotenv

load_dotenv()

# ---------- CONNECTIONS ----------
# def connect_db(db_name, user, password, host, port):
#     return psycopg2.connect(
#         dbname=db_name,
#         user=user,
#         password=password,
#         host=host,
#         port=port
#     )


# # ---------- EXTRACT ----------
# def extract(source_conn, table_name):
#     cursor = source_conn.cursor()
#     cursor.execute(f"SELECT * FROM {table_name};")

#     columns = [desc[0] for desc in cursor.description]
#     rows = cursor.fetchall()

#     data = [dict(zip(columns, row)) for row in rows]

#     print(f"Extracted {len(data)} records")
#     return data


# ---------- TRANSFORM ----------
def transform(data, column_mapping, drop_nones=True):
    transformed = []
    
    for row in data:
        new_row = {}
        is_valid = True
        
        for source_col, target_col in column_mapping.items():
            print(source_col, target_col)
            value = row.get(source_col)
            print(value)
            
            # Validation logic: if a mapped field is missing and we can't have NULLs
            if value is None and drop_nones:
                is_valid = False
                # print(f"Validity: {is_valid}")
                break 
            
            new_row[target_col] = value
        
        if is_valid:
            transformed.append(new_row)

    print(f"Transformation complete: {len(transformed)} rows ready.")
    return transformed


# # ---------- LOAD ----------
# def load(target_conn, table_name, data):
#     cursor = target_conn.cursor()
#     cursor.execute(
#         f"""
#         CREATE TABLE {table_name} (
#             full_name VARCHAR (255) NOT NULL,
#             email_address VARCHAR (255) NOT NULL
#         );
#         """
#     )

#     for row in data:
#         cursor.execute(
#             f"""
#             INSERT INTO {table_name} (full_name, email_address)
#             VALUES (%s, %s)
#             """,
#             (row["full_name"], row["email_address"])
#         )

#     target_conn.commit()
#     print(f"Loaded {len(data)} records")


# ---------- RUN PIPELINE ----------
# TODO: make run command dynamic and reversible (manually choosing source & tagret)
import argparse


def run(source, target, old_table, new_table, column_mapping):
    if source == "postgres" and target == "sqlite":
        source = PostgresAdapter(
            {
                "dbname": os.getenv("DB_NAME"),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASS"),
                "host": os.getenv("HOST"),
                "port": os.getenv("DB_PORT"),
            }
        )

        target = SQLiteAdapter("db.sqlite3")
    else:
        source = SQLiteAdapter("db.sqlite3")

        target = PostgresAdapter(
            {
                "dbname": os.getenv("DB_NAME"),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASS"),
                "host": os.getenv("HOST"),
                "port": os.getenv("DB_PORT"),
            }
        )

    source.connect()
    target.connect()

    data = source.fetch_all(old_table)
    transformed = transform(data, column_mapping)
    # print(transformed)
    target.insert(new_table, transformed)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="simple ETL data script")
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--target", type=str, required=True)
    parser.add_argument("--old_table", type=str, required=True)
    parser.add_argument("--new_table", type=str, required=True)
    parser.add_argument(
        "--column_mapping",
        type=str,
        required=True,
        help='JSON string like \'{"name": "full_name", "email": "email_address"}\'',
    )
    import json

    args = parser.parse_args()
    source = args.source
    target = args.target
    old_table = args.old_table
    new_table = args.new_table

    column_mapping = json.loads(args.column_mapping)

    run(source, target, old_table, new_table, column_mapping)
    """
    run command:
    python engine.py 
    --source postgres
    --target sqlite
    --old_table users
    --new_table users
    --column_mapping '{"customer_name": "full_name", "customer_email": "email_address"}'
    """
