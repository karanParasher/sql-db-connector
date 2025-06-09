from abc import ABC, abstractmethod
from typing import List, Dict, Any, Generator, Tuple
from pyspark.sql import DataFrame

class DatabaseConnector(ABC):
    """
    Abstract Base Class (ABC) for database connectors.
    Defines a generic interface for various database operations,
    isolating backend logic. Operations are now designed to work
    exclusively with PySpark DataFrames.
    """

    @abstractmethod
    def connect(self):
        """Connection to the database."""
        pass

    @abstractmethod
    def close(self):
        """Closes the database connection."""
        pass

    @abstractmethod
    def read(self, query: str) -> DataFrame:
        """
        Executes a SELECT query and returns the result as a PySpark DataFrame.
        """
        pass

    @abstractmethod
    def stream_read(self, query: str, chunk_size: int = 1000) -> Generator[DataFrame, None, None]:
        """
        Executes a SELECT query and streams results in chunks as PySpark DataFrames.
        """
        pass

    @abstractmethod
    def insert(self, table_name: str, data: Dict[str, Any]):
        """
        Inserts a single row (represented as a dictionary) into the specified table.
        This method will convert the dictionary to a Spark DataFrame internally.
        """
        pass

    @abstractmethod
    def bulk_upsert(self, table_name: str, df: DataFrame, on_cols: List[str]):
        """
        Performs a bulk upsert (INSERT or UPDATE) operation using Spark.
        This will leverage Spark's DataFrameWriter for bulk writes and potential temporary tables/merge.
        """
        pass

    @abstractmethod
    def update(self, table_name: str, set_data: Dict[str, Any], where_clause: str):
        """
        Updates rows in the specified table.
        """
        pass

    @abstractmethod
    def execute(self, sql_statement: str, params: Tuple[Any, ...] = ()):
        """
        Executes a generic SQL statement (e.g., CREATE, ALTER, DELETE, DROP, CALL PROCEDURE).
        """
        pass

    @abstractmethod
    def call_procedure(self, procedure_name: str, params: Tuple[Any, ...] = ()):
        """
        Calls a stored procedure.
        """
        pass
