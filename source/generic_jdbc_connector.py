import os
import sys # Import sys to get the current interpreter path
import pandas as pd
from typing import List, Dict, Any, Generator, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import configparser # Import the configparser module

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options=--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--conf spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED pyspark-shell"
) # To supress the uneccesary warning which making noise

from database_connector import DatabaseConnector

class GenericJDBCConnector(DatabaseConnector):
    """
    A generic class to handle database operations using PySpark's JDBC capabilities.
    Supports various databases (MySQL, SQL Server, PostgreSQL, etc.) by specifying
    the correct JDBC URL, driver, and driver path.
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

    def connect(self):
        """
        Initializes or retrieves the SparkSession configured for JDBC.
        """
        try:
            # Ensure the driver JAR path is accessible
            if not os.path.exists(self.jdbc_driver_path):
                print(f"ERROR: JDBC driver JAR not found at: {self.jdbc_driver_path}")
                raise FileNotFoundError(f"JDBC driver JAR missing: {self.jdbc_driver_path}")

            print(f"Attempting to initialize SparkSession with JDBC driver from: {self.jdbc_driver_path}")
            
            # Using 'spark.jars' is often more robust for adding external JARs
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

    def close(self):
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

    def read(self, query: str) -> pd.DataFrame:
       
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return pd.DataFrame()
        try:
            # For "dbtable", query must be a valid SQL subquery wrapped in parentheses
            # and given an alias.
            df = self.spark.read.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", f"({query}) AS custom_query") \
                .load()
            return df.toPandas()
        except Exception as e:
            print(f"Error reading from database with Spark: {e}")
            return pd.DataFrame()

    def stream_read(self, query: str, chunk_size: int = 1000) -> Generator[pd.DataFrame, None, None]:
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            df_spark = self.spark.read.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", f"({query}) AS custom_query") \
                .option("fetchsize", chunk_size) \
                .load()

            df_pandas = df_spark.toPandas()
            for i in range(0, len(df_pandas), chunk_size):
                yield df_pandas.iloc[i:i + chunk_size]

        except Exception as e:
            print(f"Error streaming from database with Spark: {e}")
            return

    def insert(self, table_name: str, data: Dict[str, Any]):
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            pdf = pd.DataFrame([data])
            df_spark = self.spark.createDataFrame(pdf.to_dict(orient='records'))

            df_spark.write.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", table_name) \
                .mode("append") \
                .save()
            print(f"Successfully inserted 1 row into {table_name}.")
        except Exception as e:
            print(f"Error inserting into {table_name} with Spark: {e}")
            raise

    def bulk_upsert(self, table_name: str, df: pd.DataFrame, on_cols: List[str]):
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        if df.empty:
            print("DataFrame is empty. No bulk upsert performed.")
            return

        df_spark = self.spark.createDataFrame(df.to_dict(orient='records'))
        
        # Staging table name
        staging_table_name = f"spark_staging_{table_name}_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
        try:
            print(f"Writing data to staging table: {staging_table_name}")
            df_spark.write.format("jdbc").options(**self._get_jdbc_options()) \
                .option("dbtable", staging_table_name) \
                .mode("overwrite") \
                .save()
 
            columns = ', '.join([f'"{col}"' for col in df.columns]) # Quoting columns for safety
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
            print(f"Bulk upserted {len(df)} rows into {table_name} using MERGE via staging table.")

        except Exception as e:
            print(f"Error during bulk upsert for {table_name}: {e}")
            raise # Re-raise the exception after printing
        finally:
            # Clean up the staging table
            try:
                self.execute(f"DROP TABLE \"{staging_table_name}\"") # Quote table name for safety
                print(f"Staging table {staging_table_name} dropped.")
            except Exception as e:
                print(f"Error dropping staging table {staging_table_name}: {e}")


    def update(self, table_name: str, set_data: Dict[str, Any], where_clause: str):
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        set_statements = ', '.join([f'"{col}" = {repr(value)}' if isinstance(value, str) else f'"{col}" = {value}'
                                    for col, value in set_data.items()])
        
        sql_statement = f"UPDATE \"{table_name}\" SET {set_statements} WHERE {where_clause}"
        try:
            self.execute(sql_statement)
            print(f"Successfully updated rows in {table_name}.")
        except Exception as e:
            print(f"Error updating {table_name}: {e}")
            raise # Re-raise

    def execute(self, sql_statement: str, params: Tuple[Any, ...] = ()):
        if not self.spark:
            print("SparkSession not initialized. Please call connect() first.")
            return

        try:
            # Access the Java JDBC API through PySpark's JVM bridge
            jdbcm = self.spark._jvm.java.sql.DriverManager
            conn = jdbcm.getConnection(self.db_url, self.db_user, self.db_password)
            
            if params:
                # Use PreparedStatement for parameterized queries
                pstmt = conn.prepareStatement(sql_statement)
                for i, param in enumerate(params):
                    # Basic type mapping; extend as needed
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
            raise # Re-raise the exception

    def call_procedure(self, procedure_name: str, params: Tuple[Any, ...] = ()):
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
                # Basic type mapping; extend as needed
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
            raise # Re-raise the exception

    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, ensures connection is closed."""
        self.close()