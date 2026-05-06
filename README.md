# Sales Analysis Dashboard

This project builds an interactive Streamlit application to compare how DuckDB, MySQL, and MongoDB behave under the same sales analysis workload. The application simulates a realistic sales analysis scenario, where users examine store revenue, category performance, sales trends, and record level data operations. DuckDB is used as the core analytical database, while MySQL represents a traditional row based relational system and MongoDB represents a flexible document oriented database.

The workflow is:

1. Generate the synthetic sales dataset.
2. Install and start the required database systems.
3. Load the same dataset into DuckDB, MySQL, and MongoDB.
4. Start the Streamlit dashboard.
5. Use the application to perform sales analysis, compare query execution, execution plans, and basic data management behavior.

## Project Files

```text
project_folder/
├── generate_dataset.py
├── load_dataset.py
├── sales_dashboard.py
├── dataset_config.txt
├── config.txt
├── requirements.txt
├── sales_data.csv
├── sales.duckdb
└── README.md
```

## File Overview

| File | Purpose |
|---|---|
| `generate_dataset.py` | Generates `sales_data.csv`, a synthetic retail sales dataset. |
| `load_dataset.py` | Loads the CSV dataset into DuckDB, MySQL, and MongoDB. |
| `sales_dashboard.py` | Runs the Streamlit application. |
| `dataset_config.txt` | Configuration file used by the dataset loading script. |
| `config.txt` | Configuration file used by the Streamlit dashboard. |
| `requirements.txt` | Lists the Python packages required to run the data loading scripts and the Streamlit application. |
| `sales_data.csv` | Generated source dataset. |
| `sales.duckdb` | DuckDB database file created after loading data. |
| `README.md` | Provides setup instructions, running steps, and application usage notes. |

## Run Instructions

Clone the GitHub repository with the folder name `sales_project`:

```bash
git clone https://github.com/EricJin-1/DSCI551_final_project_2026Spring.git sales_project
```

This command clones the repository into a local folder named `sales_project`, which matches the folder name used in the commands below. If you clone the repository without the final `sales_project` argument, Git will use the repository name as the folder name, `DSCI551_final_project_2026Spring`. In that case, either replace `sales_project` with `DSCI551_final_project_2026Spring` in the `cd` commands below, or rename the folder manually.

All commands should be run from inside the project folder.

For example, if the folder is on your Desktop:

```bash
cd ~/dir/sales_project
```

## Python Requirements

This project is recommended to run with Python 3.10 or later. To keep the project dependencies isolated from other Python projects, it is recommended to use a Conda environment.

If Conda is not installed, install Miniconda or Anaconda first. After installation, restart the terminal and check whether Conda is available:

```bash
conda --version
```

Create a new Conda environment for this project:

```bash
conda create -n sales_project python=3.10
```

Activate the environment before installing packages or running scripts:

```bash
conda activate sales_project
```

After the environment is activated, install the required Python packages from `requirements.txt`:

```bash
pip install -r requirements.txt
```

All following Python and Streamlit commands should be run after activating this environment.

## Database Setup

### DuckDB

DuckDB does not require a separate server. The Python package is enough:

```bash
pip install duckdb
```

When the loading script runs, it creates the local database file:

```text
sales.duckdb
```

### MySQL

Install MySQL with Homebrew:

```bash
brew install mysql
brew services start mysql
```

Check that MySQL is running:

```bash
brew services list
```

Create the database used by this project:

```bash
mysql -u root -p
```

Then run:

```sql
CREATE DATABASE sales_project;
EXIT;
```

The script will create the `sales_data` table automatically.

### MongoDB

Install MongoDB Community Edition with Homebrew:

```bash
brew tap mongodb/brew
brew install mongodb-community
brew services start mongodb-community
```

Check that MongoDB is running:

```bash
brew services list
```

MongoDB does not require manual database or collection creation. The loading script creates `sales_db.sales_data` automatically when data is inserted.

## Configuration Files

### `dataset_config.txt`

This file is used by `load_dataset.py`.

```text
# Source dataset
csv_file=sales_data.csv

# DuckDB
duckdb_file=sales.duckdb

# MySQL
mysql_host=127.0.0.1
mysql_port=3306
mysql_user=root
mysql_password=your_mysql_password
mysql_database=sales_project
mysql_batch_size=5000

# MongoDB
mongo_host=127.0.0.1
mongo_port=27017
mongo_database=sales_db
mongo_collection=sales_data
mongo_batch_size=10000
```

If your local MySQL root account has no password, leave it empty:

```text
mysql_password=
```

### `config.txt`

This file is used by `sales_dashboard.py`.

```text
# DuckDB
duckdb_file=sales.duckdb

# MySQL
mysql_host=127.0.0.1
mysql_port=3306
mysql_user=root
mysql_password=your_mysql_password
mysql_database=sales_project

# MongoDB
mongo_host=127.0.0.1
mongo_port=27017
mongo_database=sales_db
mongo_collection=sales_data
```

## Run the Project

Run all commands from the project root directory.

### 1. Generate the Dataset

```bash
python generate_dataset.py
```

This creates:

```text
sales_data.csv
```

### 2. Load the Dataset into the Databases

Make sure MySQL and MongoDB are running, and make sure the MySQL database `sales_project` already exists.

```bash
python load_dataset.py
```

Expected output:

```text
DuckDB loaded
MySQL loaded
MongoDB loaded
```

After this step:

| System | Data Location |
|---|---|
| DuckDB | `sales.duckdb`, table `sales_data` |
| MySQL | database `sales_project`, table `sales_data` |
| MongoDB | database `sales_db`, collection `sales_data` |

### 3. Start the Streamlit Dashboard

```bash
streamlit run sales_dashboard.py
```

Streamlit should open the application in the browser. If it does not open automatically, use the local URL shown in the terminal, usually:

```text
http://localhost:8501
```

## How to Use the Application

The dashboard has two main modules in the sidebar.

### Query Analysis

Use this module to compare analytical query behavior across DuckDB, MySQL, and MongoDB.

1. Select a database mode: `DuckDB`, `MySQL`, `MongoDB`, or `All`.
2. Select a query template:
   - `Sales by Store`: total revenue grouped by store id.
   - `Sales by Category`: total revenue grouped by product category.
   - `Sales Trend`: total revenue grouped by transaction date.
   - `All Row`: full row access ordered by transaction id.
3. Optionally apply filters:
   - start date
   - end date
   - category
   - store id
4. Click `Run Query`.

The dashboard displays:

- the generated SQL query,
- the query result,
- execution time,
- EXPLAIN or execution analysis output.

When `All` is selected, the same logical query is run across all three systems for direct comparison.

#### Reproducing the Query Analysis Results in the Report

To reproduce the query analysis runs shown in the report, keep the database mode as `All` so the same logical query is executed on DuckDB, MySQL, and MongoDB.

Use the following settings in the sidebar:

| Report Run | Database Mode | Query Template | Filters |
|---|---|---|---|
| Sales by Store result | `All` | `Sales by Store` | Leave date, category, and store filters empty. |
| Sales by Category result | `All` | `Sales by Category` | Set category to `Grocery` and store id to `5`. |
| Sales Trend result | `All` | `Sales Trend` | Set Date Range from '2024-01-01' to '2024-05-01'|
| All Row result | `All` | `All Row` | Leave date, category, and store filters empty. |

After each run, scroll down to compare the result tables, execution time, and EXPLAIN or execution analysis output for the three databases.

The report also discusses selected index behavior. To reproduce that part more closely, create the store id index before running the related query analysis cases:

```sql
CREATE INDEX idx_sales_store_id ON sales_data(store_id);
```

Run the SQL command above in MySQL after selecting the `sales_project` database. 

For MongoDB, run:

```
use sales_db
db.sales_data.createIndex({ store_id: 1 })
```


### Data Management

Use this module to compare insert, update, and delete behavior across the three databases.

Supported operations:

| Operation | How to Use |
|---|---|
| Insert | Fill 1 to 10 complete rows. Every column is required. |
| Update | Fill 1 to 10 rows. `transaction_id` is required, and only filled columns are updated. |
| Delete | Enter up to 10 `transaction_id` values separated by commas or line breaks. |

After entering values for `Insert`, `Update`, or `Delete`, wait about one second and click outside the input box before pressing the operation button. This allows Streamlit to register the latest input values before the write operation runs.

Each write operation is applied to DuckDB, MySQL, and MongoDB. The dashboard then shows affected rows, execution time, and available EXPLAIN or execution details.

#### Reproducing the Data Management Results in the Report

To reproduce the insert, update, and delete runs shown in the report, select `Data Management` in the sidebar and run the operations below. Each operation is applied to DuckDB, MySQL, and MongoDB.

| Report Run | Write Operation | How to Reproduce |
|---|---|---|
| Insert behavior | `Insert` | Fill one to ten complete new rows. Every column is required. Use transaction ids that do not already exist in the dataset. |
| Update with changed values | `Update` | Enter one to ten existing transaction ids and change at least one field value. Click `Update in All Databases`. |
| Repeated update with identical values | `Update` | Run the same update again without changing the values. This reproduces the unchanged update case discussed in the report. |
| Delete existing records | `Delete` | Enter one to ten existing transaction ids separated by commas or line breaks. Click `Delete from All Databases`. |
| Repeated delete | `Delete` | Run the same delete again. This reproduces the zero matched row case after the records have already been removed. |

For consistent results, reload the original dataset before starting a new reproduction round:

```bash
python load_dataset.py
```

This resets the DuckDB file, the MySQL table, and the MongoDB collection to the same source dataset before the dashboard operations are tested again.


## Notes

Before running any script, first move into the project folder. If you used the recommended clone command above, the folder is named `sales_project`. If you cloned without specifying a folder name, use the repository folder name instead.

For example, if the folder is on your Desktop and named `sales_project`:

```bash
cd ~/Desktop/sales_project
```

- `sales.duckdb` is created in the current working directory.
- MySQL and MongoDB store data inside their own database services, not inside the project folder.
- DuckDB may temporarily create a `.wal` file such as `sales.duckdb.wal`. This is normal and should not be manually edited.
- The project uses relative paths, so it is recommended to run all scripts from the project root directory.
- Running `load_dataset.py` again replaces the existing imported data in all three systems.

## Troubleshooting

### `Config file not found`

Make sure `dataset_config.txt` or `config.txt` is in the same folder where the script is being run.

### `sales_data.csv not found`

Run:

```bash
python generate_dataset.py
```

### MySQL connection failed

Check that MySQL is running:

```bash
brew services list
```

Confirm that the database exists:

```sql
CREATE DATABASE sales_project;
```

Also check the username and password in both config files.

### MongoDB connection failed

Check that MongoDB is running:

```bash
brew services list
```

Start it if needed:

```bash
brew services start mongodb-community
```
