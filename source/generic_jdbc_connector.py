import os
from typing import List, Dict, Any, Generator, Tuple
from pyspark.sql import SparkSession, DataFrame

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options=--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--conf spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED pyspark-shell"
)

from database_connector import DatabaseConnector

class GenericJDBCConnector(DatabaseConnector):
    """
    A generic class to handle database operations using PySpark's JDBC capabilities.
    Supports various databases (MySQL, SQL Server, PostgreSQL, etc.) by specifying
    the correct JDBC URL, driver, and driver path. All operations now work
    exclusively with PySpark DataFrames.
    """

    def __init__(self, db_url: str, db_user: str, db_password: str,
                 jdbc_driver_class: str, jdbc_driver_path: str):
        """
        Initializes the GenericJDBCConnector.

        Args:
            db_url (str): The full JDBC connection URL (e.g., "jdbc:sqlserver://localhost:1433;databaseName=mydb").
            db_user (str): The username for authentication.
            db_password (str): The password for authentication.
            jdbc_driver_class (str): The fully qualified class name of the JDBC driver
                                     (e.g., "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                     "com.mysql.cj.jdbc.Driver", "org.postgresql.Driver").
            jdbc_driver_path (str): The absolute path to the JDBC driver .jar file(s).
                                    Can be a single path or a comma-separated list of paths.
        """
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.jdbc_driver_class = jdbc_driver_class
        self.jdbc_driver_path = jdbc_driver_path
        self.spark = None

    def connect(self) -> None:
        """
        Initializes or retrieves the SparkSession configured for JDBC.
        """
        try:
            if not os.path.exists(self.jdbc_driver_path):
                print(f"ERROR: JDBC driver JAR not found at: {self.jdbc_driver_path}")
                raise FileNotFoundError(f"JDBC driver JAR missing: {self.jdbc_driver_path}")

            print(f"Attempting to initialize SparkSession with JDBC driver from: {self.jdbc_driver_path}")

            self.spark = SparkSession.builder \
                .appName("Generic JDBC Connector") \
                .config("spark.jars", self.jdbc_driver_path) \
                .config("spark.driver.extraClassPath", self.jdbc_driver_path) \
                .config("spark.executor.extraClassPath", self.jdbc_driver_path) \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
            print("SparkSession initialized and JDBC driver configured.")
        except Exception as e:
            print(f"Error initializing SparkSession or configuring JDBC driver: {e}")
            raise

    def close(self) -> None:
        """
        Stops the SparkSession.
        """
        if self.spark:
            self.spark.stop()
            self.spark = None
            print("SparkSession stopped.")

    def _get_jdbc_options(self) -> Dict[str, str]:
        """Helper to get common JDBC options."""
        return {
            "url": self.db_url,
            "user": self.db_user,
            "password": self.db_password,
            "driver": self.jdbc_driver_class
        }

    def read(self, query: str) -> DataFrame: # Return type is Spark DataFrame
        """
        Executes a SELECT query and returns the result as a PySpark DataFrame.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return self.spark.createDataFrame([], schema=[])

        try:
            df = self.spark.read.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", f"({query}) AS custom_query") \
                .load()
            return df
        except Exception as e:
            print(f"Error reading from database with Spark: {e}")
            raise # Re-raise the exception

    def stream_read(self, query: str, chunk_size: int = 1000) -> Generator[DataFrame, None, None]:
        """
        Executes a SELECT query and streams results in chunks as PySpark DataFrames.
        Note: PySpark's JDBC read does not directly support 'fetchsize' for true streaming
        in the sense of returning partial Spark DataFrames in a generator.
        This implementation will load the full data and then yield chunks from it.
        For very large datasets, consider external streaming mechanisms or
        more advanced Spark connectors.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            # Load the full DataFrame (Spark's JDBC read typically loads all data unless specific options are used)
            df_spark = self.spark.read.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", f"({query}) AS custom_query") \
                .load()

            # Convert to RDD and then partition for "chunking"
            # This simulates chunking from a loaded Spark DataFrame
            rows = df_spark.collect() # Collects all data to driver memory, can be an issue for very large datasets
            for i in range(0, len(rows), chunk_size):
                chunk_rows = rows[i:i + chunk_size]
                yield self.spark.createDataFrame(chunk_rows, df_spark.schema)

        except Exception as e:
            print(f"Error streaming from database with Spark: {e}")
            raise # Re-raise the exception

    def insert(self, table_name: str, data: Dict[str, Any]) -> None:
        """
        Inserts a single row (represented as a dictionary) into the specified table.
        Converts the dictionary to a Spark DataFrame for insertion.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            # Create a Spark DataFrame from the single dictionary row
            df_spark = self.spark.createDataFrame([data])

            df_spark.write.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", table_name) \
                .mode("append") \
                .save()
            print(f"Successfully inserted 1 row into {table_name}.")
        except Exception as e:
            print(f"Error inserting into {table_name} with Spark: {e}")
            raise

    def bulk_upsert(self, table_name: str, df: DataFrame, on_cols: List[str]) -> None:
        """
        Performs a bulk upsert (INSERT or UPDATE) operation using Spark.
        This will leverage Spark's DataFrameWriter for bulk writes and potential temporary tables/merge.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        if df.count() == 0: # Check if Spark DataFrame is empty
            print("DataFrame is empty. No bulk upsert performed.")
            return

        # Staging table name
        # Use a more robust way to get a unique identifier, e.g., current timestamp
        from datetime import datetime
        staging_table_name = f"spark_staging_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        try:
            print(f"Writing data to staging table: {staging_table_name}")
            df.write.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", staging_table_name) \
                .mode("overwrite") \
                .save()

            columns = ', '.join([f'"{col}"' for col in df.columns])
            match_conditions = ' AND '.join([f'T."{col}" = S."{col}"' for col in on_cols])
            update_assignments = ', '.join([f'T."{col}" = S."{col}"' for col in df.columns if col not in on_cols])
            insert_values = ', '.join([f'S."{col}"' for col in df.columns])

            merge_sql = f"""
            MERGE INTO "{table_name}" AS T
            USING "{staging_table_name}" AS S
            ON ({match_conditions})
            WHEN MATCHED THEN
                UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN
                INSERT ({columns})
                VALUES ({insert_values});
            """

            # Execute the MERGE statement
            self.execute(merge_sql)
            print(f"Bulk upserted {df.count()} rows into {table_name} using MERGE via staging table.")

        except Exception as e:
            print(f"Error during bulk upsert for {table_name}: {e}")
            raise
        finally:
            # Clean up the staging table
            try:
                self.execute(f"DROP TABLE \"{staging_table_name}\"")
                print(f"Staging table {staging_table_name} dropped.")
            except Exception as e:
                print(f"Error dropping staging table {staging_table_name}: {e}")

    def update(self, table_name: str, set_data: Dict[str, Any], where_clause: str) -> None:
        """
        Updates rows in the specified table.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        # Prepare set statements for SQL
        set_statements = []
        for col, value in set_data.items():
            if isinstance(value, str):
                set_statements.append(f'"{col}" = \'{value}\'') # Enclose string values in single quotes
            else:
                set_statements.append(f'"{col}" = {value}')
        set_clause = ', '.join(set_statements)

        sql_statement = f"UPDATE \"{table_name}\" SET {set_clause} WHERE {where_clause}"
        try:
            self.execute(sql_statement)
            print(f"Successfully updated rows in {table_name}.")
        except Exception as e:
            print(f"Error updating {table_name}: {e}")
            raise

    def execute(self, sql_statement: str, params: Tuple[Any, ...] = ()) -> None:
        """
        Executes a generic SQL statement (e.g., CREATE, ALTER, DELETE, DROP, CALL PROCEDURE).
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            jdbcm = self.spark._jvm.java.sql.DriverManager
            conn = jdbcm.getConnection(self.db_url, self.db_user, self.db_password)

            if params:
                pstmt = conn.prepareStatement(sql_statement)
                for i, param in enumerate(params):
                    if isinstance(param, int):
                        pstmt.setInt(i + 1, param)
                    elif isinstance(param, float):
                        pstmt.setDouble(i + 1, param)
                    elif isinstance(param, str):
                        pstmt.setString(i + 1, param)
                    elif isinstance(param, bool):
                        pstmt.setBoolean(i + 1, param)
                    else:
                        pstmt.setObject(i + 1, param)
                pstmt.executeUpdate()
                pstmt.close()
            else:
                stmt = conn.createStatement()
                stmt.execute(sql_statement)
                stmt.close()

            conn.close()
            print(f"Successfully executed SQL statement: {sql_statement[:50]}...")
        except Exception as e:
            print(f"Error executing SQL statement: {e}")
            raise

    def call_procedure(self, procedure_name: str, params: Tuple[Any, ...] = ()) -> None:
        """
        Calls a stored procedure.
        """
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        placeholders = ', '.join(['?' for _ in params])
        call_sql = f"{{CALL {procedure_name}({placeholders})}}" if params else f"{{CALL {procedure_name}}}"

        try:
            jdbcm = self.spark._jvm.java.sql.DriverManager
            conn = jdbcm.getConnection(self.db_url, self.db_user, self.db_password)

            cstmt = conn.prepareCall(call_sql)
            for i, param in enumerate(params):
                if isinstance(param, int):
                    cstmt.setInt(i + 1, param)
                elif isinstance(param, float):
                    cstmt.setDouble(i + 1, param)
                elif isinstance(param, str):
                    cstmt.setString(i + 1, param)
                elif isinstance(param, bool):
                    cstmt.setBoolean(i + 1, param)
                else:
                    cstmt.setObject(i + 1, param)

            cstmt.execute()
            cstmt.close()
            conn.close()
            print(f"Successfully called procedure '{procedure_name}'.")
        except Exception as e:
            print(f"Error calling procedure '{procedure_name}': {e}")
            raise

    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit point, ensures connection is closed."""
        self.close()