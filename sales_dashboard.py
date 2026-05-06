import json
import time
from datetime import datetime
from pathlib import Path

import duckdb
import mysql.connector
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Application configuration and shared table schema
CONFIG_FILE = "config.txt"
TABLE_NAME = "sales_data"
REQUIRED_COLUMNS = [
    "transaction_id",
    "transaction_date",
    "store_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "sales_amount",
]
UPDATE_COLUMNS = [column for column in REQUIRED_COLUMNS if column != "transaction_id"]


class InputValidationError(ValueError):
    """Raised when user input fails application level validation"""
    pass


def load_config(path: str = CONFIG_FILE) -> dict:
    """Read config file"""
    config = {}
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with file_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()

    return config


# Load connection settings once when the Streamlit app starts
# Default values are used when optional keys are not defined in config.txt
CONFIG = load_config()

DUCKDB_FILE = CONFIG.get("duckdb_file", "sales.duckdb")

MYSQL_CONFIG = {
    "host": CONFIG.get("mysql_host", "127.0.0.1"),
    "port": int(CONFIG.get("mysql_port", "3306")),
    "user": CONFIG.get("mysql_user", "root"),
    "password": CONFIG.get("mysql_password", ""),
    "database": CONFIG.get("mysql_database", "sales_project"),
}

MONGO_CONFIG = {
    "host": CONFIG.get("mongo_host", "127.0.0.1"),
    "port": int(CONFIG.get("mongo_port", "27017")),
    "database": CONFIG.get("mongo_database", "sales_db"),
    "collection": CONFIG.get("mongo_collection", TABLE_NAME),
}

# Streamlit page setup
st.set_page_config(page_title="Sales Analysis Dashboard", layout="wide")
st.title("Sales Analysis Dashboard")


@st.cache_resource
def get_duckdb_conn():
    """Create and cache the DuckDB connection used by the app"""
    return duckdb.connect(DUCKDB_FILE, read_only=False)


@st.cache_resource
def get_mysql_conn():
    """Create and cache the MySQL connection used by the app"""
    return mysql.connector.connect(**MYSQL_CONFIG)


@st.cache_resource
def get_mongo_collection():
    """Create and cache the MongoDB collection handle"""
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        serverSelectionTimeoutMS=3000,
    )
    db = client[MONGO_CONFIG["database"]]
    return db[MONGO_CONFIG["collection"]]


# DuckDB is used as the primary metadata source for filters and date range checks
duck_conn = get_duckdb_conn()


def try_get_mysql_conn():
    """Return a usable MySQL connection or a user facing error message"""
    try:
        conn = get_mysql_conn()
        if not conn.is_connected():
            conn.reconnect()
        return conn, None
    except Exception as e:
        return None, str(e)


def try_get_mongo_collection():
    """Return a usable MongoDB collection or a user facing error message"""
    try:
        collection = get_mongo_collection()
        collection.database.client.admin.command("ping")
        return collection, None
    except Exception as e:
        return None, str(e)


def normalize_text(text) -> str:
    """Convert stored newline escape sequences into readable text"""
    if text is None:
        return ""
    return str(text).replace("\\n", "\n")


@st.cache_data
def get_metadata():
    """Load date range and filter values from DuckDB for sidebar controls"""
    min_date, max_date = duck_conn.execute(
        f"SELECT MIN(transaction_date), MAX(transaction_date) FROM {TABLE_NAME}"
    ).fetchone()

    categories = [
        row[0]
        for row in duck_conn.execute(
            f"""
            SELECT DISTINCT category
            FROM {TABLE_NAME}
            WHERE category IS NOT NULL
            ORDER BY category
            """
        ).fetchall()
    ]

    stores = [
        str(row[0])
        for row in duck_conn.execute(
            f"""
            SELECT DISTINCT store_id
            FROM {TABLE_NAME}
            WHERE store_id IS NOT NULL
            ORDER BY store_id
            """
        ).fetchall()
    ]

    return min_date, max_date, categories, stores


def parse_date(value: str):
    """Parse an optional date string from the sidebar input"""
    return datetime.strptime(value, "%Y-%m-%d").date() if value else None


def validate_dates(start_date, end_date, min_date, max_date):
    """Validate user provided date filters against the dataset range"""
    if start_date and start_date < min_date:
        raise ValueError(f"Start date must be on or after {min_date}")
    if end_date and end_date > max_date:
        raise ValueError(f"End date must be on or before {max_date}")
    if start_date and end_date and start_date > end_date:
        raise ValueError("Start date cannot be later than end date")


def sql_quote(value) -> str:
    """Escape a value for direct SQL string construction"""
    return "'" + str(value).replace("'", "''") + "'"


def sql_value(value):
    """Format a Python value for use in generated SQL statements"""
    if isinstance(value, str):
        return sql_quote(value)
    return str(value)


def build_sql_in_list_str(values) -> str:
    """Build a quoted SQL IN list for string values"""
    return ", ".join(sql_quote(v) for v in values)


def build_sql_in_list_int(values) -> str:
    """Build a numeric SQL IN list for integer values"""
    return ", ".join(str(int(v)) for v in values)


def build_where_clause(start_date, end_date, categories, store_ids) -> str:
    """Create the SQL WHERE clause from selected filters"""
    conditions = []

    if start_date:
        conditions.append(f"transaction_date >= DATE '{start_date}'")
    if end_date:
        conditions.append(f"transaction_date <= DATE '{end_date}'")
    if categories:
        conditions.append(f"category IN ({build_sql_in_list_str(categories)})")
    if store_ids:
        conditions.append(f"store_id IN ({build_sql_in_list_int(store_ids)})")

    return f"WHERE {' AND '.join(conditions)}" if conditions else ""


def get_query(query_type: str, where_clause: str) -> str:
    """Return the SQL template that matches the selected query type"""
    if query_type == "Sales by Store":
        return f"""
SELECT
    store_id,
    SUM(sales_amount) AS total_revenue
FROM {TABLE_NAME}
{where_clause}
GROUP BY store_id
ORDER BY total_revenue DESC, store_id
""".strip()

    if query_type == "Sales by Category":
        return f"""
SELECT
    category,
    SUM(sales_amount) AS total_revenue
FROM {TABLE_NAME}
{where_clause}
GROUP BY category
ORDER BY total_revenue DESC, category
""".strip()

    if query_type == "Sales Trend":
        return f"""
SELECT
    transaction_date,
    SUM(sales_amount) AS total_revenue
FROM {TABLE_NAME}
{where_clause}
GROUP BY transaction_date
ORDER BY transaction_date
""".strip()

    if query_type == "All Row":
        return f"""
SELECT *
FROM {TABLE_NAME}
{where_clause}
ORDER BY transaction_id
""".strip()

    raise ValueError("Unsupported query type")


def run_duckdb(query: str):
    """Run a query in DuckDB and collect result, timing, and EXPLAIN ANALYZE output"""
    start = time.perf_counter()
    result_df = duck_conn.execute(query).df()
    elapsed = time.perf_counter() - start

    analyze_df = duck_conn.execute(f"EXPLAIN ANALYZE {query}").df()
    explain_text = ""

    if not analyze_df.empty and analyze_df.shape[1] > 1:
        explain_text = normalize_text(analyze_df.iloc[0, 1])

    return result_df, elapsed, explain_text


def format_mysql_explain(explain_df: pd.DataFrame) -> str:
    """Format MySQL EXPLAIN rows into readable step based text"""
    if explain_df.empty:
        return "No EXPLAIN output."

    lines = []
    for idx, row in explain_df.iterrows():
        lines.append(f"Step {idx + 1}")
        for column in explain_df.columns:
            lines.append(f"  {column}: {row[column]}")
        lines.append("")

    return "\n".join(lines).strip()


def run_mysql(query: str, conn):
    """Run a query in MySQL and collect result, timing, and EXPLAIN output"""
    cursor = conn.cursor()
    try:
        start = time.perf_counter()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        elapsed = time.perf_counter() - start

        result_df = pd.DataFrame(rows, columns=columns)

        cursor.execute(f"EXPLAIN {query}")
        explain_rows = cursor.fetchall()
        explain_columns = [desc[0] for desc in cursor.description] if cursor.description else []
        explain_df = pd.DataFrame(explain_rows, columns=explain_columns)

        return result_df, elapsed, format_mysql_explain(explain_df)
    finally:
        cursor.close()


def build_mongo_match(start_date, end_date, categories, store_ids):
    """Create the MongoDB match stage from selected filters"""
    match_stage = {}

    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter["$gte"] = str(start_date)
        if end_date:
            date_filter["$lte"] = str(end_date)
        match_stage["transaction_date"] = date_filter

    if categories:
        match_stage["category"] = {"$in": list(categories)}

    if store_ids:
        match_stage["store_id"] = {"$in": [int(x) for x in store_ids]}

    return match_stage


def build_mongo_pipeline(query_type, start_date, end_date, categories, store_ids):
    """Build the MongoDB aggregation pipeline for the selected query type"""
    pipeline = []
    match_stage = build_mongo_match(start_date, end_date, categories, store_ids)

    if match_stage:
        pipeline.append({"$match": match_stage})

    if query_type == "Sales by Store":
        pipeline.extend(
            [
                {"$group": {"_id": "$store_id", "total_revenue": {"$sum": "$sales_amount"}}},
                {"$sort": {"total_revenue": -1, "_id": 1}},
            ]
        )
    elif query_type == "Sales by Category":
        pipeline.extend(
            [
                {"$group": {"_id": "$category", "total_revenue": {"$sum": "$sales_amount"}}},
                {"$sort": {"total_revenue": -1, "_id": 1}},
            ]
        )
    elif query_type == "Sales Trend":
        pipeline.extend(
            [
                {"$group": {"_id": "$transaction_date", "total_revenue": {"$sum": "$sales_amount"}}},
                {"$sort": {"_id": 1}},
            ]
        )
    elif query_type == "All Row":
        pipeline.append({"$sort": {"transaction_id": 1}})
    else:
        raise ValueError("Unsupported query type")

    return pipeline


def empty_result_frame(query_type: str) -> pd.DataFrame:
    """Return an empty result table with the expected output columns"""
    if query_type == "Sales by Store":
        return pd.DataFrame(columns=["store_id", "total_revenue"])
    if query_type == "Sales by Category":
        return pd.DataFrame(columns=["category", "total_revenue"])
    if query_type == "Sales Trend":
        return pd.DataFrame(columns=["transaction_date", "total_revenue"])
    if query_type == "All Row":
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return pd.DataFrame()


def format_mongo_result(query_type: str, docs):
    """Convert MongoDB aggregation results into the dashboard result format"""
    if not docs:
        return empty_result_frame(query_type)

    if query_type == "All Row":
        df = pd.DataFrame(docs)
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])
        return df

    rows = []
    for doc in docs:
        if query_type == "Sales by Store":
            rows.append({"store_id": doc.get("_id"), "total_revenue": doc.get("total_revenue")})
        elif query_type == "Sales by Category":
            rows.append({"category": doc.get("_id"), "total_revenue": doc.get("total_revenue")})
        elif query_type == "Sales Trend":
            rows.append({"transaction_date": doc.get("_id"), "total_revenue": doc.get("total_revenue")})

    return pd.DataFrame(rows)


def format_mongo_explain(explain_doc) -> str:
    """Serialize MongoDB explain output for display"""
    if not explain_doc:
        return "No EXPLAIN output."

    try:
        return json.dumps(explain_doc, indent=2, default=str)
    except Exception:
        return str(explain_doc)


def run_mongodb(query_type, start_date, end_date, categories, store_ids, collection):
    """Run a MongoDB aggregation and collect result, timing, and explain output"""
    pipeline = build_mongo_pipeline(query_type, start_date, end_date, categories, store_ids)

    try:
        start = time.perf_counter()
        docs = list(collection.aggregate(pipeline, allowDiskUse=True))
        elapsed = time.perf_counter() - start
        result_df = format_mongo_result(query_type, docs)

        explain_doc = collection.database.command(
            {
                "explain": {
                    "aggregate": collection.name,
                    "pipeline": pipeline,
                    "cursor": {},
                },
                "verbosity": "executionStats",
            }
        )

        return result_df, elapsed, format_mongo_explain(explain_doc)

    except PyMongoError as e:
        raise RuntimeError(f"MongoDB query failed: {e}") from e


def render_database_block(title: str, df: pd.DataFrame, elapsed: float, explain_text: str):
    """Render one database result section in the dashboard"""
    st.subheader(title)
    st.dataframe(df, use_container_width=True)
    st.write(f"Execution time: {elapsed:.6f} seconds")
    st.subheader("EXPLAIN")
    st.code(explain_text)


def _is_blank(value) -> bool:
    """Check whether a form value should be treated as empty"""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return str(value).strip() == ""


def parse_required_int(column: str, value) -> int:
    """Parse a required integer input and raise a validation error if invalid"""
    if _is_blank(value):
        raise InputValidationError(f"{column} is required and cannot be empty.")
    try:
        return int(str(value).strip())
    except ValueError as e:
        raise InputValidationError(f"{column} must be an integer.") from e


def parse_optional_int(column: str, value):
    """Parse an optional integer input and return None when blank"""
    if _is_blank(value):
        return None
    try:
        return int(str(value).strip())
    except ValueError as e:
        raise InputValidationError(f"{column} must be an integer.") from e


def parse_required_float(column: str, value) -> float:
    """Parse a required numeric input and raise a validation error if invalid"""
    if _is_blank(value):
        raise InputValidationError(f"{column} is required and cannot be empty.")
    try:
        return float(str(value).strip())
    except ValueError as e:
        raise InputValidationError(f"{column} must be a number.") from e


def parse_optional_float(column: str, value):
    """Parse an optional numeric input and return None when blank"""
    if _is_blank(value):
        return None
    try:
        return float(str(value).strip())
    except ValueError as e:
        raise InputValidationError(f"{column} must be a number.") from e


def parse_required_date(column: str, value) -> str:
    """Parse a required date input using YYYY-MM-DD format"""
    if _is_blank(value):
        raise InputValidationError(f"{column} is required and cannot be empty.")
    try:
        return str(datetime.strptime(str(value).strip(), "%Y-%m-%d").date())
    except ValueError as e:
        raise InputValidationError(f"{column} must use YYYY-MM-DD format.") from e


def parse_optional_date(column: str, value):
    """Parse an optional date input using YYYY-MM-DD format"""
    if _is_blank(value):
        return None
    try:
        return str(datetime.strptime(str(value).strip(), "%Y-%m-%d").date())
    except ValueError as e:
        raise InputValidationError(f"{column} must use YYYY-MM-DD format.") from e


def parse_required_text(column: str, value) -> str:
    """Parse a required text input and reject blank values."""
    if _is_blank(value):
        raise InputValidationError(f"{column} is required and cannot be empty.")
    return str(value).strip()


def parse_optional_text(column: str, value):
    """Parse an optional text input and return None when blank"""
    if _is_blank(value):
        return None
    return str(value).strip()


def limit_records(records: list, operation: str):
    """Enforce the row limit for write operations"""
    if not records:
        raise InputValidationError(f"At least one row is required for {operation.lower()}.")
    if len(records) > 10:
        raise InputValidationError(f"{operation} supports at most 10 rows at a time.")


def validate_unique_transaction_ids(transaction_ids: list[int]):
    """Reject duplicated transaction ids in a single user input batch"""
    seen = set()
    duplicates = []
    for transaction_id in transaction_ids:
        if transaction_id in seen:
            duplicates.append(transaction_id)
        seen.add(transaction_id)
    if duplicates:
        duplicate_text = ", ".join(str(x) for x in sorted(set(duplicates)))
        raise InputValidationError(
            f"transaction_id must be unique within the input. Duplicated value(s): {duplicate_text}.")


def active_editor_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only data editor rows that contain at least one filled value."""
    if df.empty:
        return df
    mask = df.apply(lambda row: any(not _is_blank(row.get(column)) for column in REQUIRED_COLUMNS), axis=1)
    return df.loc[mask].copy()


def build_insert_records_from_df(df: pd.DataFrame) -> list[dict]:
    """Validate insert editor input and convert it into record dictionaries"""
    df = active_editor_rows(df)
    records = []

    for idx, row in df.iterrows():
        row_no = idx + 1
        try:
            records.append(
                {
                    "transaction_id": parse_required_int(f"row {row_no} transaction_id", row.get("transaction_id")),
                    "transaction_date": parse_required_date(f"row {row_no} transaction_date",
                                                            row.get("transaction_date")),
                    "store_id": parse_required_int(f"row {row_no} store_id", row.get("store_id")),
                    "product_id": parse_required_int(f"row {row_no} product_id", row.get("product_id")),
                    "category": parse_required_text(f"row {row_no} category", row.get("category")),
                    "quantity": parse_required_int(f"row {row_no} quantity", row.get("quantity")),
                    "unit_price": parse_required_float(f"row {row_no} unit_price", row.get("unit_price")),
                    "sales_amount": parse_required_float(f"row {row_no} sales_amount", row.get("sales_amount")),
                }
            )
        except InputValidationError:
            raise

    limit_records(records, "Insert")
    validate_unique_transaction_ids([record["transaction_id"] for record in records])
    return records


def build_update_records_from_df(df: pd.DataFrame) -> list[dict]:
    """Validate update editor input and convert it into update dictionaries"""
    df = active_editor_rows(df)
    records = []

    for idx, row in df.iterrows():
        row_no = idx + 1
        transaction_id = parse_required_int(f"row {row_no} transaction_id", row.get("transaction_id"))
        updates = {
            "transaction_date": parse_optional_date(f"row {row_no} transaction_date", row.get("transaction_date")),
            "store_id": parse_optional_int(f"row {row_no} store_id", row.get("store_id")),
            "product_id": parse_optional_int(f"row {row_no} product_id", row.get("product_id")),
            "category": parse_optional_text(f"row {row_no} category", row.get("category")),
            "quantity": parse_optional_int(f"row {row_no} quantity", row.get("quantity")),
            "unit_price": parse_optional_float(f"row {row_no} unit_price", row.get("unit_price")),
            "sales_amount": parse_optional_float(f"row {row_no} sales_amount", row.get("sales_amount")),
        }
        updates = {column: value for column, value in updates.items() if value is not None}

        if not updates:
            raise InputValidationError(f"row {row_no} must provide at least one column to update.")

        records.append({"transaction_id": transaction_id, "updates": updates})

    limit_records(records, "Update")
    validate_unique_transaction_ids([record["transaction_id"] for record in records])
    return records


def parse_transaction_ids(value: str) -> list[int]:
    """Parse delete input into a validated list of transaction ids"""
    if _is_blank(value):
        raise InputValidationError("transaction_id is required")

    parts = [part.strip() for part in str(value).replace("\n", ",").split(",")]
    parts = [part for part in parts if part]

    if not parts:
        raise InputValidationError("transaction_id is required")

    ids = [parse_required_int("transaction_id", part) for part in parts]

    if len(ids) > 10:
        raise InputValidationError("Delete supports at most 10 transaction_id values at a time.")
    validate_unique_transaction_ids(ids)
    return ids


def build_insert_sql(records: list[dict]) -> str:
    """Build a SQL INSERT statement for one or more records"""
    columns = ", ".join(REQUIRED_COLUMNS)
    values = []
    for record in records:
        values.append("(" + ", ".join(sql_value(record[column]) for column in REQUIRED_COLUMNS) + ")")
    return f"INSERT INTO {TABLE_NAME} ({columns}) VALUES\n" + ",\n".join(values)


def build_update_sql(record: dict) -> str:
    """Build a SQL UPDATE statement for one record"""
    transaction_id = record["transaction_id"]
    updates = record["updates"]
    assignments = ", ".join(f"{column} = {sql_value(value)}" for column, value in updates.items())
    return f"UPDATE {TABLE_NAME} SET {assignments} WHERE transaction_id = {transaction_id}"


def build_delete_sql(transaction_ids: list[int]) -> str:
    """Build a SQL DELETE statement for one or more transaction ids"""
    id_list = ", ".join(str(transaction_id) for transaction_id in transaction_ids)
    return f"DELETE FROM {TABLE_NAME} WHERE transaction_id IN ({id_list})"


def get_duckdb_explain(statement: str) -> str:
    """Return DuckDB EXPLAIN output for a write statement"""
    try:
        explain_df = duck_conn.execute(f"EXPLAIN {statement}").df()
        if not explain_df.empty and explain_df.shape[1] > 1:
            return normalize_text(explain_df.iloc[0, 1])
        return explain_df.to_string(index=False)
    except Exception as e:
        return f"DuckDB EXPLAIN failed: {e}"


def get_duckdb_explain_for_statements(statements: list[str]) -> str:
    """Return DuckDB EXPLAIN output for multiple write statements"""
    explain_parts = []
    for idx, statement in enumerate(statements, start=1):
        explain_parts.append(f"Statement {idx}\n{get_duckdb_explain(statement)}")
    return "\n\n".join(explain_parts)


def get_mysql_insert_explain(statement: str, conn) -> str:
    """Return MySQL EXPLAIN output for an insert statement when supported"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN {statement}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        explain_df = pd.DataFrame(rows, columns=columns)
        return format_mysql_explain(explain_df)
    except Exception as e:
        return f"MySQL EXPLAIN for INSERT is unavailable in this environment: {e}"
    finally:
        cursor.close()


def get_mysql_write_note(operation: str, statement: str, conn) -> tuple[str, str]:
    """Return the MySQL detail label and explanatory text for a write operation"""
    if operation == "Insert":
        return "EXPLAIN", get_mysql_insert_explain(statement, conn)
    return "Note", f"MySQL does not support EXPLAIN for {operation.lower()} operations in this module."


def get_mongo_insert_note() -> str:
    """Return the MongoDB note for insert operation"""
    return "MongoDB does not support EXPLAIN for insert operations."


def get_mongo_update_explain(record: dict, collection) -> str:
    """Return MongoDB explain output for one update operation"""
    command = {
        "explain": {
            "update": collection.name,
            "updates": [
                {
                    "q": {"transaction_id": record["transaction_id"]},
                    "u": {"$set": record["updates"]},
                    "multi": False,
                    "upsert": False,
                }
            ],
        },
        "verbosity": "executionStats",
    }
    try:
        return format_mongo_explain(collection.database.command(command))
    except Exception as e:
        return f"MongoDB EXPLAIN for update failed: {e}"


def get_mongo_delete_explain(transaction_ids: list[int], collection) -> str:
    """Return MongoDB explain output for delete operation"""
    command = {
        "explain": {
            "delete": collection.name,
            "deletes": [
                {
                    "q": {"transaction_id": {"$in": transaction_ids}},
                    "limit": 0,
                }
            ],
        },
        "verbosity": "executionStats",
    }
    try:
        return format_mongo_explain(collection.database.command(command))
    except Exception as e:
        return f"MongoDB EXPLAIN for delete failed: {e}"


def get_mongo_update_explain_for_records(records: list[dict], collection) -> str:
    """Return MongoDB explain output for multiple update records"""
    explain_parts = []
    for idx, record in enumerate(records, start=1):
        explain_parts.append(f"Statement {idx}\n{get_mongo_update_explain(record, collection)}")
    return "\n\n".join(explain_parts)


def duckdb_existing_ids(transaction_ids: list[int]) -> set[int]:
    """Find transaction ids that already exist in DuckDB"""
    placeholders = ", ".join("?" for _ in transaction_ids)
    rows = duck_conn.execute(
        f"SELECT transaction_id FROM {TABLE_NAME} WHERE transaction_id IN ({placeholders})",
        transaction_ids,
    ).fetchall()
    return {int(row[0]) for row in rows}


def mysql_existing_ids(transaction_ids: list[int], conn) -> set[int]:
    """Find transaction ids that already exist in MySQL"""
    placeholders = ", ".join(["%s"] * len(transaction_ids))
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT transaction_id FROM {TABLE_NAME} WHERE transaction_id IN ({placeholders})",
            tuple(transaction_ids),
        )
        return {int(row[0]) for row in cursor.fetchall()}
    finally:
        cursor.close()


def mongo_existing_ids(transaction_ids: list[int], collection) -> set[int]:
    """Find transaction ids that already exist in MongoDB"""
    docs = collection.find({"transaction_id": {"$in": transaction_ids}}, {"transaction_id": 1, "_id": 0})
    return {int(doc["transaction_id"]) for doc in docs}


def check_all_connections():
    """Confirm that MySQL and MongoDB are reachable before a write operation"""
    mysql_conn, mysql_error = try_get_mysql_conn()
    mongo_collection, mongo_error = try_get_mongo_collection()

    errors = []
    if mysql_error:
        errors.append(f"MySQL connection failed: {mysql_error}")
    if mongo_error:
        errors.append(f"MongoDB connection failed: {mongo_error}")

    if errors:
        raise RuntimeError("\n".join(errors))

    return mysql_conn, mongo_collection


def validate_insert_not_duplicate(records: list[dict], mysql_conn, mongo_collection):
    """Ensure inserted transaction ids do not already exist in any database"""
    transaction_ids = [record["transaction_id"] for record in records]
    existing_by_db = {
        "DuckDB": duckdb_existing_ids(transaction_ids),
        "MySQL": mysql_existing_ids(transaction_ids, mysql_conn),
        "MongoDB": mongo_existing_ids(transaction_ids, mongo_collection),
    }

    messages = []
    for db_name, existing in existing_by_db.items():
        if existing:
            values = ", ".join(str(x) for x in sorted(existing))
            messages.append(f"{db_name}: {values}")

    if messages:
        raise InputValidationError("transaction_id already exists. " + "; ".join(messages) + ".")


def validate_existing_records(transaction_ids: list[int], mysql_conn, mongo_collection):
    """Ensure update or delete targets exist in all three databases"""
    expected = set(transaction_ids)
    existing_by_db = {
        "DuckDB": duckdb_existing_ids(transaction_ids),
        "MySQL": mysql_existing_ids(transaction_ids, mysql_conn),
        "MongoDB": mongo_existing_ids(transaction_ids, mongo_collection),
    }

    messages = []
    for db_name, existing in existing_by_db.items():
        missing = expected - existing
        if missing:
            values = ", ".join(str(x) for x in sorted(missing))
            messages.append(f"{db_name}: {values}")

    if messages:
        raise InputValidationError("transaction_id does not exist. " + "; ".join(messages) + ".")


def execute_duckdb_insert(records: list[dict]):
    """Execute an insert in DuckDB and return timing and explain details"""
    statement = build_insert_sql(records)
    explain_text = get_duckdb_explain(statement)
    start = time.perf_counter()
    duck_conn.execute(statement)
    elapsed = time.perf_counter() - start
    return elapsed, explain_text, len(records), statement


def execute_duckdb_updates(records: list[dict]):
    """Execute updates in DuckDB and return timing and explain details"""
    statements = [build_update_sql(record) for record in records]
    explain_text = get_duckdb_explain_for_statements(statements)
    start = time.perf_counter()
    for statement in statements:
        duck_conn.execute(statement)
    elapsed = time.perf_counter() - start
    return elapsed, explain_text, len(records), "\n".join(statements)


def execute_duckdb_delete(transaction_ids: list[int]):
    """Execute delete in DuckDB and return timing and explain details"""
    statement = build_delete_sql(transaction_ids)
    explain_text = get_duckdb_explain(statement)
    start = time.perf_counter()
    duck_conn.execute(statement)
    elapsed = time.perf_counter() - start
    return elapsed, explain_text, len(transaction_ids), statement


def execute_mysql_statement(operation: str, statement: str, affected_count_hint: int, conn):
    """Execute a single MySQL write statement with commit and rollback handling"""
    details_label, details = get_mysql_write_note(operation, statement, conn)
    cursor = conn.cursor()
    try:
        start = time.perf_counter()
        cursor.execute(statement)
        conn.commit()
        elapsed = time.perf_counter() - start
        affected = cursor.rowcount if cursor.rowcount != -1 else affected_count_hint
        return elapsed, details_label, details, affected
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def execute_mysql_update_records(records: list[dict], conn):
    """Execute one or more MySQL update statements as a transaction"""
    statements = [build_update_sql(record) for record in records]
    details_label, details = get_mysql_write_note("Update", statements[0], conn)
    cursor = conn.cursor()
    try:
        start = time.perf_counter()
        affected = 0
        for statement in statements:
            cursor.execute(statement)
            if cursor.rowcount != -1:
                affected += cursor.rowcount
        conn.commit()
        elapsed = time.perf_counter() - start
        return elapsed, details_label, details, affected
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def execute_mongo_insert(records: list[dict], collection):
    """Insert records into MongoDB and return timing details"""
    details = get_mongo_insert_note()
    try:
        start = time.perf_counter()
        result = collection.insert_many(records, ordered=True)
        elapsed = time.perf_counter() - start
        return elapsed, "Note", details, len(result.inserted_ids)
    except PyMongoError as e:
        raise RuntimeError(f"MongoDB insert failed: {e}") from e


def execute_mongo_updates(records: list[dict], collection):
    """Update MongoDB records and return timing and explain details"""
    details = get_mongo_update_explain_for_records(records, collection)
    try:
        start = time.perf_counter()
        affected = 0
        for record in records:
            result = collection.update_one(
                {"transaction_id": record["transaction_id"]},
                {"$set": record["updates"]},
            )
            affected += result.modified_count
        elapsed = time.perf_counter() - start
        return elapsed, "EXPLAIN", details, affected
    except PyMongoError as e:
        raise RuntimeError(f"MongoDB update failed: {e}") from e


def execute_mongo_delete(transaction_ids: list[int], collection):
    """Delete MongoDB records and return timing and explain details"""
    details = get_mongo_delete_explain(transaction_ids, collection)
    try:
        start = time.perf_counter()
        result = collection.delete_many({"transaction_id": {"$in": transaction_ids}})
        elapsed = time.perf_counter() - start
        return elapsed, "EXPLAIN", details, result.deleted_count
    except PyMongoError as e:
        raise RuntimeError(f"MongoDB delete failed: {e}") from e


def append_write_result(results: list, database: str, operation: str, affected_rows: int, elapsed: float,
                        details_label: str, details: str):
    """Append one database write result to the shared summary list"""
    results.append(
        {
            "database": database,
            "operation": operation,
            "affected_rows": affected_rows,
            "execution_time_seconds": elapsed,
            "details_label": details_label,
            "details": details,
        }
    )


def run_write_operation(operation: str, records: list[dict] | None = None, transaction_ids: list[int] | None = None):
    """Run the selected write operation across DuckDB, MySQL, and MongoDB"""
    mysql_conn, mongo_collection = check_all_connections()
    results = []

    if operation == "Insert":
        validate_insert_not_duplicate(records, mysql_conn, mongo_collection)

        duck_elapsed, duck_explain, duck_affected, insert_statement = execute_duckdb_insert(records)
        append_write_result(results, "DuckDB", operation, duck_affected, duck_elapsed, "EXPLAIN", duck_explain)

        mysql_elapsed, mysql_label, mysql_details, mysql_affected = execute_mysql_statement(
            operation,
            insert_statement,
            len(records),
            mysql_conn,
        )
        append_write_result(results, "MySQL", operation, mysql_affected, mysql_elapsed, mysql_label, mysql_details)

        mongo_elapsed, mongo_label, mongo_details, mongo_affected = execute_mongo_insert(records, mongo_collection)
        append_write_result(results, "MongoDB", operation, mongo_affected, mongo_elapsed, mongo_label, mongo_details)

    elif operation == "Update":
        ids = [record["transaction_id"] for record in records]
        validate_existing_records(ids, mysql_conn, mongo_collection)

        duck_elapsed, duck_explain, duck_affected, _ = execute_duckdb_updates(records)
        append_write_result(results, "DuckDB", operation, duck_affected, duck_elapsed, "EXPLAIN", duck_explain)

        mysql_elapsed, mysql_label, mysql_details, mysql_affected = execute_mysql_update_records(records, mysql_conn)
        append_write_result(results, "MySQL", operation, mysql_affected, mysql_elapsed, mysql_label, mysql_details)

        mongo_elapsed, mongo_label, mongo_details, mongo_affected = execute_mongo_updates(records, mongo_collection)
        append_write_result(results, "MongoDB", operation, mongo_affected, mongo_elapsed, mongo_label, mongo_details)

    elif operation == "Delete":
        validate_existing_records(transaction_ids, mysql_conn, mongo_collection)
        delete_statement = build_delete_sql(transaction_ids)

        duck_elapsed, duck_explain, duck_affected, _ = execute_duckdb_delete(transaction_ids)
        append_write_result(results, "DuckDB", operation, duck_affected, duck_elapsed, "EXPLAIN", duck_explain)

        mysql_elapsed, mysql_label, mysql_details, mysql_affected = execute_mysql_statement(
            operation,
            delete_statement,
            len(transaction_ids),
            mysql_conn,
        )
        append_write_result(results, "MySQL", operation, mysql_affected, mysql_elapsed, mysql_label, mysql_details)

        mongo_elapsed, mongo_label, mongo_details, mongo_affected = execute_mongo_delete(transaction_ids,
                                                                                         mongo_collection)
        append_write_result(results, "MongoDB", operation, mongo_affected, mongo_elapsed, mongo_label, mongo_details)

    else:
        raise ValueError("Unsupported write operation.")

    get_metadata.clear()
    return results


def render_write_results(results):
    """Render write operation timing, affected rows, and details"""
    summary_df = pd.DataFrame(
        [
            {
                "Database": item["database"],
                "Operation": item["operation"],
                "Affected Rows": item["affected_rows"],
                "Execution Time": f"{item['execution_time_seconds']:.6f} seconds",
            }
            for item in results
        ]
    )

    st.subheader("Write Operation Summary")
    st.dataframe(summary_df, use_container_width=True)

    for item in results:
        st.subheader(f"{item['database']} {item['details_label']}")
        st.write(f"Execution time: {item['execution_time_seconds']:.6f} seconds")
        if item["details_label"] == "Note":
            st.info(item["details"])
        else:
            st.code(item["details"])


def empty_editor_df() -> pd.DataFrame:
    """Create a fixed size blank table for insert and update input"""
    return pd.DataFrame([{column: "" for column in REQUIRED_COLUMNS} for _ in range(10)])


def current_editor_df(editor_key: str, fallback_df: pd.DataFrame) -> pd.DataFrame:
    """Recover the current Streamlit data editor state as a DataFrame"""
    state = st.session_state.get(editor_key)

    if isinstance(state, pd.DataFrame):
        return state.copy()

    if not isinstance(state, dict):
        return fallback_df.copy()

    df = empty_editor_df()

    for row_idx, changes in state.get("edited_rows", {}).items():
        try:
            idx = int(row_idx)
        except (TypeError, ValueError):
            continue
        if 0 <= idx < len(df) and isinstance(changes, dict):
            for column, value in changes.items():
                if column in df.columns:
                    df.at[idx, column] = value

    added_rows = state.get("added_rows", [])
    if added_rows:
        rows = []
        for row in added_rows:
            if isinstance(row, dict):
                rows.append({column: row.get(column, "") for column in REQUIRED_COLUMNS})
        if rows:
            df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

    deleted_rows = state.get("deleted_rows", [])
    if deleted_rows:
        drop_indexes = []
        for row_idx in deleted_rows:
            try:
                drop_indexes.append(int(row_idx))
            except (TypeError, ValueError):
                continue
        if drop_indexes:
            df = df.drop(index=[idx for idx in drop_indexes if idx in df.index]).reset_index(drop=True)

    return df


def render_write_form(operation: str):
    """Render the insert, update, or delete form selected in the sidebar"""
    if operation == "Insert":
        st.subheader("Insert New Records")
        st.caption("Fill 1 to 10 rows. Every column is required for each inserted row.")
        insert_df = st.data_editor(
            empty_editor_df(),
            num_rows="fixed",
            use_container_width=True,
            key="insert_editor",
        )

        if st.button("Insert into All Databases", type="primary", key="insert_button"):
            records = build_insert_records_from_df(current_editor_df("insert_editor", insert_df))
            results = run_write_operation("Insert", records=records)
            st.success(f"{len(records)} record(s) inserted into all three databases.")
            render_write_results(results)

    elif operation == "Update":
        st.subheader("Update Existing Records")
        st.caption("Fill 1 to 10 rows. transaction_id is required. Leave other fields empty to keep existing values.")
        update_df = st.data_editor(
            empty_editor_df(),
            num_rows="fixed",
            use_container_width=True,
            key="update_editor",
        )

        if st.button("Update in All Databases", type="primary", key="update_button"):
            records = build_update_records_from_df(current_editor_df("update_editor", update_df))
            results = run_write_operation("Update", records=records)
            st.success(f"{len(records)} record(s) updated in all three databases.")
            render_write_results(results)

    elif operation == "Delete":
        st.subheader("Delete Existing Records")
        st.caption("Enter up to 10 transaction_id values separated by commas or line breaks.")
        transaction_id_input = st.text_area("transaction_id values", key="delete_transaction_ids")

        if st.button("Delete from All Databases", type="primary", key="delete_button"):
            transaction_ids = parse_transaction_ids(transaction_id_input)
            results = run_write_operation("Delete", transaction_ids=transaction_ids)
            st.success(f"{len(transaction_ids)} record(s) deleted from all three databases.")
            render_write_results(results)


# Initialize dashboard metadata before rendering the sidebar controls
min_date, max_date, categories, stores = get_metadata()

if min_date is None or max_date is None:
    st.error(f"The {TABLE_NAME} table is empty.")
    st.stop()

st.info(f"Available date range: {min_date} to {max_date}")

module = st.sidebar.radio(
    "Module",
    ["Query Analysis", "Data Management"],
)

# Query Analysis compares read queries, execution times, and execution details
if module == "Query Analysis":
    st.sidebar.header("Query Controls")

    db_mode = st.sidebar.selectbox(
        "Database mode",
        ["DuckDB", "MySQL", "MongoDB", "All"],
    )

    query_type = st.sidebar.selectbox(
        "Query template",
        ["Sales by Store", "Sales by Category", "Sales Trend", "All Row"],
    )

    start_input = st.sidebar.text_input("Start date (YYYY-MM-DD)", "")
    end_input = st.sidebar.text_input("End date (YYYY-MM-DD)", "")

    selected_categories = st.sidebar.multiselect(
        "Category filter",
        categories,
        default=[],
    )

    selected_stores = st.sidebar.multiselect(
        "Store filter",
        stores,
        default=[],
    )

    run = st.sidebar.button("Run Query", type="primary")

    if run:
        try:
            start_date = parse_date(start_input)
            end_date = parse_date(end_input)
            validate_dates(start_date, end_date, min_date, max_date)

            where_clause = build_where_clause(
                start_date,
                end_date,
                selected_categories,
                selected_stores,
            )
            query = get_query(query_type, where_clause)

            st.subheader("SQL Query")
            st.code(query, language="sql")

            if db_mode == "DuckDB":
                result_df, elapsed, explain_text = run_duckdb(query)
                render_database_block("DuckDB Result", result_df, elapsed, explain_text)

            elif db_mode == "MySQL":
                mysql_conn, mysql_error = try_get_mysql_conn()
                if mysql_error:
                    st.error(f"MySQL connection failed: {mysql_error}")
                else:
                    result_df, elapsed, explain_text = run_mysql(query, mysql_conn)
                    render_database_block("MySQL Result", result_df, elapsed, explain_text)

            elif db_mode == "MongoDB":
                mongo_collection, mongo_error = try_get_mongo_collection()
                if mongo_error:
                    st.error(f"MongoDB connection failed: {mongo_error}")
                else:
                    result_df, elapsed, explain_text = run_mongodb(
                        query_type,
                        start_date,
                        end_date,
                        selected_categories,
                        selected_stores,
                        mongo_collection,
                    )
                    render_database_block("MongoDB Result", result_df, elapsed, explain_text)

            else:
                mysql_conn, mysql_error = try_get_mysql_conn()
                mongo_collection, mongo_error = try_get_mongo_collection()

                if mysql_error:
                    st.error(f"MySQL connection failed: {mysql_error}")
                if mongo_error:
                    st.error(f"MongoDB connection failed: {mongo_error}")

                if not mysql_error and not mongo_error:
                    col1, col2, col3 = st.columns(3)

                    duck_df, duck_time, duck_explain = run_duckdb(query)
                    mysql_df, mysql_time, mysql_explain = run_mysql(query, mysql_conn)
                    mongo_df, mongo_time, mongo_explain = run_mongodb(
                        query_type,
                        start_date,
                        end_date,
                        selected_categories,
                        selected_stores,
                        mongo_collection,
                    )

                    with col1:
                        render_database_block("DuckDB Result", duck_df, duck_time, duck_explain)

                    with col2:
                        render_database_block("MySQL Result", mysql_df, mysql_time, mysql_explain)

                    with col3:
                        render_database_block("MongoDB Result", mongo_df, mongo_time, mongo_explain)

        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Query execution failed: {e}")

# Data Management applies the same write operation to all three databases
else:
    st.sidebar.header("Data Management Controls")

    write_operation = st.sidebar.selectbox(
        "Write operation",
        ["Insert", "Update", "Delete"],
    )

    st.warning(
        "This module writes the same operation to DuckDB, MySQL, and MongoDB. "
        "transaction_id is required for insert, update, and delete. Insert requires every column. "
        "Update only changes the columns that are filled in."
    )

    try:
        render_write_form(write_operation)
    except InputValidationError as e:
        st.error(str(e))
    except Exception as e:
        st.error(f"Write operation failed: {e}")