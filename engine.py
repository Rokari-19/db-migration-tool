import psycopg2


# ---------- CONNECTIONS ----------
def connect_db(db_name, user, password, host, port):
    return psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )


# ---------- EXTRACT ----------
def extract(source_conn, table_name):
    cursor = source_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    data = [dict(zip(columns, row)) for row in rows]
    
    print(f"Extracted {len(data)} records")
    return data


# ---------- TRANSFORM ----------
def transform(data):
    transformed = []

    for row in data:
        new_row = {
            "full_name": row.get("name"),   # mapping
            "email_address": row.get("email")
        }
        transformed.append(new_row)

    print("Transformation complete")
    return transformed


# ---------- LOAD ----------
def load(target_conn, table_name, data):
    cursor = target_conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
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

    target_conn.commit()
    print(f"Loaded {len(data)} records")


# ---------- RUN PIPELINE ----------
def run():
    source = connect_db("source", "postgres", "7toxicLamps", "localhost", "5432")
    target = connect_db("target", "postgres", "7toxicLamps", "localhost", "5432")

    data = extract(source, "users")
    transformed = transform(data)
    load(target, "customers", transformed)


if __name__ == "__main__":
    run()