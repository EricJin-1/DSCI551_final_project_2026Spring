import csv
from pathlib import Path

import duckdb
import mysql.connector
from pymongo import MongoClient

# Config file
CONFIG_FILE = "dataset_config.txt"


def read_config(config_file):
    """Read configuration"""
    config = {}

    with open(config_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()

    return config


def load_duckdb(config):
    """Create the DuckDB table directly from the CSV file"""
    csv_file = config["csv_file"]
    duckdb_file = config["duckdb_file"]

    conn = duckdb.connect(duckdb_file)

    conn.execute("DROP TABLE IF EXISTS sales_data")

    conn.execute(f"""
        CREATE TABLE sales_data AS
        SELECT *
        FROM read_csv_auto('{csv_file}', HEADER=True)
    """)

    conn.close()

    print("DuckDB loaded")


def load_mysql(config):
    """Create the MySQL table"""
    csv_file = config["csv_file"]

    mysql_config = {
        "host": config["mysql_host"],
        "port": int(config["mysql_port"]),
        "user": config["mysql_user"],
        "password": config["mysql_password"],
        "database": config["mysql_database"]
    }

    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS sales_data")

    cursor.execute("""
        CREATE TABLE sales_data (
            transaction_id INT PRIMARY KEY,
            transaction_date DATE,
            store_id INT,
            product_id INT,
            category VARCHAR(50),
            quantity INT,
            unit_price DECIMAL(10,2),
            sales_amount DECIMAL(12,2)
        )
    """)

    insert_sql = """
        INSERT INTO sales_data (
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            category,
            quantity,
            unit_price,
            sales_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch_size = int(config["mysql_batch_size"])
    rows = []

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows.append((
                int(row["transaction_id"]),
                row["transaction_date"],
                int(row["store_id"]),
                int(row["product_id"]),
                row["category"],
                int(row["quantity"]),
                float(row["unit_price"]),
                float(row["sales_amount"])
            ))

            if len(rows) >= batch_size:
                cursor.executemany(insert_sql, rows)
                conn.commit()
                rows = []

        if rows:
            cursor.executemany(insert_sql, rows)
            conn.commit()

    cursor.close()
    conn.close()

    print("MySQL loaded")


def load_mongodb(config):
    """Create the MongoDB collection"""
    csv_file = config["csv_file"]

    client = MongoClient(
        host=config["mongo_host"],
        port=int(config["mongo_port"])
    )

    db = client[config["mongo_database"]]
    collection = db[config["mongo_collection"]]

    collection.drop()

    batch_size = int(config["mongo_batch_size"])
    batch = []

    with open(csv_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            batch.append({
                "transaction_id": int(row["transaction_id"]),
                "transaction_date": row["transaction_date"],
                "store_id": int(row["store_id"]),
                "product_id": int(row["product_id"]),
                "category": row["category"],
                "quantity": int(row["quantity"]),
                "unit_price": float(row["unit_price"]),
                "sales_amount": float(row["sales_amount"])
            })

            if len(batch) >= batch_size:
                collection.insert_many(batch)
                batch = []

        if batch:
            collection.insert_many(batch)

    client.close()

    print("MongoDB loaded")


def main():
    """Load the same CSV dataset into DuckDB, MySQL, and MongoDB"""
    config_path = Path(CONFIG_FILE)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")

    config = read_config(config_path)

    load_duckdb(config)
    load_mysql(config)
    load_mongodb(config)


if __name__ == "__main__":
    main()
