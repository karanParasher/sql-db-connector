import os
import sys
import configparser
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import the GenericJDBCConnector class
from generic_jdbc_connector import GenericJDBCConnector

# --- IMPORTANT: Configure PySpark Python executable paths before SparkSession creation ---
python_executable_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_executable_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable_path
print(f"Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to: {python_executable_path}")
# --- End PySpark Python config ---

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config_file_path = "config.cfg"
    config.read(config_file_path)

    DB_TYPE = config.get('DEFAULT', 'DEFAULT_DB_TYPE', fallback='SQLSERVER')
    DRIVERS_FOLDER_NAME = config.get('DEFAULT', 'DRIVERS_FOLDER_NAME', fallback='drivers')
    DRIVERS_FOLDER_PATH = "drivers"

    try:
        db_config = config[DB_TYPE]
        DB_URL = db_config['DB_URL']
        DB_USER = db_config['DB_USER']
        DB_PASSWORD = db_config['DB_PASSWORD']
        JDBC_DRIVER_CLASS = db_config['JDBC_DRIVER_CLASS']
        JDBC_DRIVER_FILENAME = db_config['JDBC_DRIVER_FILENAME']

        JDBC_DRIVER_PATH = os.path.join(DRIVERS_FOLDER_PATH, JDBC_DRIVER_FILENAME)

    except KeyError as e:
        print(f"Error: Section or key '{e}' not found in 'config.cfg' for DB_TYPE '{DB_TYPE}'.")
        print("Please ensure your 'config.cfg' is correctly set up and all required keys are present.")
        sys.exit(1)

    print(f"\n--- Testing with {DB_TYPE} ---")
    print(f"Using JDBC Driver Path: {JDBC_DRIVER_PATH}")
    print(f"Connecting to URL: {DB_URL}")

    try:
        with GenericJDBCConnector(DB_URL, DB_USER, DB_PASSWORD, JDBC_DRIVER_CLASS, JDBC_DRIVER_PATH) as db:
            spark_session = db.spark # Get the SparkSession for creating DataFrames

            # --- Test Execute (Create Table) ---
            print("\n--- Test Execute (Create Table) ---")
            if DB_TYPE == "SQLSERVER":
                db.execute("""
                    IF OBJECT_ID('TestTable', 'U') IS NOT NULL
                        DROP TABLE TestTable;
                """)
                db.execute("""
                    CREATE TABLE TestTable (
                        ID INT PRIMARY KEY IDENTITY(1,1),
                        Name VARCHAR(100),
                        Age INT,
                        City VARCHAR(100),
                        CreatedOn VARCHAR(40) DEFAULT GETDATE()
                    );
                """)
            elif DB_TYPE == "MYSQL":
                db.execute("""
                    DROP TABLE IF EXISTS TestTable;
                    CREATE TABLE TestTable (
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        Name VARCHAR(100),
                        Age INT,
                        City VARCHAR(100),
                        CreatedOn VARCHAR(40) DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            elif DB_TYPE == "POSTGRES":
                db.execute("""
                    DROP TABLE IF EXISTS TestTable;
                    CREATE TABLE TestTable (
                        ID SERIAL PRIMARY KEY,
                        Name VARCHAR(100),
                        Age INT,
                        City VARCHAR(100),
                        CreatedOn TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            print("TestTable created (or recreated).")

            # --- Test Insert ---
            print("\n--- Test Insert ---")
            db.insert('TestTable', {'Name': 'Alice', 'Age': 30, 'City': 'New York'})
            db.insert('TestTable', {'Name': 'Bob', 'Age': 24, 'City': 'London'})

            if DB_TYPE == "SQLSERVER":
                df_after_insert = db.read("SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable")
            else:
                df_after_insert = db.read("SELECT * FROM TestTable")

            if df_after_insert.count() > 0:
                print("Data after initial inserts:")
                # Spark DataFrames do not have sort_values directly. Use orderBy.
                df_after_insert.orderBy('ID').show()
            else:
                print("No data after initial inserts to display.")

            # --- Test Update ---
            print("\n--- Test Update ---")
            db.update('TestTable', {'Age': 31}, "Name = 'Alice'")

            if DB_TYPE == "SQLSERVER":
                df_after_update = db.read("SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable WHERE Name = 'Alice'")
            else:
                df_after_update = db.read("SELECT * FROM TestTable WHERE Name = 'Alice'")

            if df_after_update.count() > 0:
                print("Data after update:")
                df_after_update.orderBy('ID').show()
            else:
                print("No data after update for Alice to display.")

            # --- Test Bulk Upsert ---
            print("\n--- Test Bulk Upsert ---")
            # Create a Spark DataFrame directly
            data_to_upsert = [
                Row(Name='Alice', Age=32, City='New York'),
                Row(Name='Charlie', Age=28, City='Paris'),
                Row(Name='David', Age=35, City='Berlin')
            ]
            # Define schema for creating DataFrame
            schema = StructType([
                StructField("Name", StringType(), True),
                StructField("Age", IntegerType(), True),
                StructField("City", StringType(), True)
            ])
            df_upsert_spark = spark_session.createDataFrame(data_to_upsert, schema=schema)

            db.bulk_upsert('TestTable', df_upsert_spark, on_cols=['Name'])

            if DB_TYPE == "SQLSERVER":
                df_after_bulk_upsert = db.read("SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable")
            else:
                df_after_bulk_upsert = db.read("SELECT * FROM TestTable")

            if df_after_bulk_upsert.count() > 0:
                print("Data after bulk upsert:")
                df_after_bulk_upsert.orderBy('ID').show()
            else:
                print("No data after bulk upsert to display.")

            # --- Test Stream Read ---
            print("\n--- Test Stream Read ---")
            stream_read_query = "SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable" if DB_TYPE == "SQLSERVER" else "SELECT * FROM TestTable"
            for i, chunk_df in enumerate(db.stream_read(stream_read_query, chunk_size=2)):
                if chunk_df.count() > 0:
                    print(f"\nStreamed Chunk {i+1}:")
                    chunk_df.show()
                else:
                    print(f"\nStreamed Chunk {i+1}: (Empty)")
                if i >= 1: # Limit to 2 chunks for brevity
                    break

            # --- Test Call Procedure ---
            print("\n--- Test Call Procedure ---")
            if DB_TYPE == "SQLSERVER":
                db.execute("""
                    IF OBJECT_ID('sp_UpdateAgeByName', 'P') IS NOT NULL
                        DROP PROCEDURE sp_UpdateAgeByName;
                """)
                db.execute("""
                    CREATE PROCEDURE sp_UpdateAgeByName
                        @Name VARCHAR(100),
                        @NewAge INT
                    AS
                    BEGIN
                        UPDATE TestTable SET Age = @NewAge WHERE Name = @Name;
                    END;
                """)
            elif DB_TYPE == "MYSQL":
                db.execute("""
                    DROP PROCEDURE IF EXISTS sp_UpdateAgeByName;
                    CREATE PROCEDURE sp_UpdateAgeByName(IN p_name VARCHAR(100), IN p_new_age INT)
                    BEGIN
                        UPDATE TestTable SET Age = p_new_age WHERE Name = p_name;
                    END;
                """)
            elif DB_TYPE == "POSTGRES":
                db.execute("""
                    DROP FUNCTION IF EXISTS sp_UpdateAgeByName(varchar, int);
                    CREATE FUNCTION sp_UpdateAgeByName(p_name VARCHAR(100), p_new_age INT)
                    RETURNS void
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        UPDATE TestTable SET Age = p_new_age WHERE Name = p_name;
                    END;
                    $$;
                """)
            print(f"Stored procedure 'sp_UpdateAgeByName' created for {DB_TYPE}.")

            db.call_procedure('sp_UpdateAgeByName', ('Alice', 40))

            if DB_TYPE == "SQLSERVER":
                df_proc_updated = db.read("SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable WHERE Name = 'Alice'")
            else:
                df_proc_updated = db.read("SELECT * FROM TestTable WHERE Name = 'Alice'")

            if df_proc_updated.count() > 0:
                print("Data after calling procedure:")
                df_proc_updated.show()
            else:
                print("No data after procedure call to display.")

            # --- Test Execute (Delete Table) ---
            print("\n--- Test Execute (Delete Table) ---")
            db.execute("DELETE FROM TestTable WHERE Name = 'Bob'")

            if DB_TYPE == "SQLSERVER":
                df_after_delete = db.read("SELECT ID, Name, Age, City, CAST(CreatedOn AS VARCHAR(50)) AS CreatedOn FROM TestTable")
            else:
                df_after_delete = db.read("SELECT * FROM TestTable")

            if df_after_delete.count() > 0:
                print("Data after delete:")
                df_after_delete.orderBy('ID').show()
            else:
                print("No data after delete to display.")

            # --- Test Execute (Drop Table) ---
            print("\n--- Test Execute (Drop Table) ---")
            db.execute("DROP TABLE TestTable")
            print("TestTable dropped.")

    except Exception as e:
        print(f"An error occurred during testing: {e}")
        import traceback
        traceback.print_exc()
