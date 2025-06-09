# 🔌 Generic JDBC Connector Using PySpark

This project provides a robust, extensible JDBC connector for interacting with relational databases like SQL Server, MySQL, and PostgreSQL using PySpark. It offers a clean abstraction layer (`DatabaseConnector`) and a full implementation (`GenericJDBCConnector`) to simplify database operations using Spark DataFrames.

---

## 📁 Project Structure

```
.
├── config.cfg                # Configuration for database connection and JDBC driver
├── database_connector.py     # Abstract base class defining Spark-based DB operations
├── generic_jdbc_connector.py # Full implementation using PySpark JDBC
├── test.py                   # End-to-end test and usage demo
└── drivers/                  # JDBC driver .jar files (already included)
```

---

## ✅ Key Features

- Abstract interface for all JDBC operations using PySpark
- Handles:
  - Reading data (as Spark DataFrames)
  - Streaming data in chunks
  - Inserting single rows
  - Performing bulk upserts using MERGE
  - Updating data conditionally
  - Executing raw SQL statements
  - Calling stored procedures with parameters
- Multi-database support via config: SQL Server, MySQL, PostgreSQL

---

## ⚙️ Setup Instructions

---

## 🛠 prerequisites

- Python 3.8
- Java 8 or higher
- Apache Spark 3.2.2

---

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

> Ensure Java 8+ and Apache Spark(3.2.2) are installed and available in your system's PATH(JAVA_HOME and SPARK_HOME).

---

## 🧩 Configuration

Edit the `config.cfg` file to specify your default DB and credentials:

```ini
[DEFAULT]
DEFAULT_DB_TYPE = SQLSERVER
DRIVERS_FOLDER_NAME = drivers

[SQLSERVER]
DB_URL = jdbc:sqlserver://localhost:1433;databaseName=your_db
DB_USER = your_username
DB_PASSWORD = your_password
JDBC_DRIVER_CLASS = com.microsoft.sqlserver.jdbc.SQLServerDriver
JDBC_DRIVER_FILENAME = mssql-jdbc-12.10.0.jre11.jar

[MYSQL]
DB_URL = jdbc:mysql://localhost:3306/your_db
DB_USER = your_username
DB_PASSWORD = your_password
JDBC_DRIVER_CLASS = com.mysql.cj.jdbc.Driver
JDBC_DRIVER_FILENAME = mysql-connector-j-9.3.0.jar

[POSTGRES]
DB_URL = jdbc:postgresql://localhost:5432/your_db
DB_USER = your_username
DB_PASSWORD = your_password
JDBC_DRIVER_CLASS = org.postgresql.Driver
JDBC_DRIVER_FILENAME = postgresql-42.7.6.jar
```

> JDBC `.jar` drivers must be placed in the `drivers/` folder. This folder is already have drivers included.

---

## 🚀 Running the Test Script

Run the `test.py` script to verify functionality:

```bash
python test.py
```

This will:

1. Create a table called `TestTable`
2. Insert and update data using Spark
3. Perform a bulk upsert using a Spark DataFrame
4. Read and stream data in chunks
5. Call a stored procedure (`sp_UpdateAgeByName`)
6. Delete a row and drop the table

All results will be printed using `.show()` on Spark DataFrames.

---

## 🙌 Notes

- All operations are based on **PySpark DataFrames**, not Pandas.
- Ideal for data engineering, ETL pipelines, and batch jobs.
- The project is fully extensible to support additional databases or driver options.
