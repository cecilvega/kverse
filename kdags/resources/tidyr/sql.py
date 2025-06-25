import os
import polars as pl
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, Iterator, Dict, Any, List
from urllib.parse import quote_plus
import dagster as dg
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.pool import NullPool
import pyodbc


class SQLDatabase:
    """Class for reading data from Azure SQL Database with Polars integration."""

    def __init__(self, context: dg.AssetExecutionContext = None, connection_string: str = None):
        """
        Initialize SQL Database connection.

        Parameters
        ----------
        context : dg.AssetExecutionContext, optional
            Dagster context for logging
        connection_string : str, optional
            Connection string for Azure SQL Database. If not provided,
            will look for environment variable AZURE_SQL_CONNECTION_STRING
        """
        self.context_check = isinstance(context, dg.AssetExecutionContext)
        self.context = context

        # Get connection string from parameter or environment
        if connection_string:
            self._conn_str = connection_string
        else:
            self._conn_str = os.environ.get("AZURE_SQL_CONNECTION_STRING")

        if not self._conn_str:
            raise ValueError(
                "No connection string provided. Set AZURE_SQL_CONNECTION_STRING environment variable or pass connection_string parameter."
            )

        # Parse connection string and create SQLAlchemy engine
        self._engine = self._create_engine()

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine from connection string."""
        # If it's already a SQLAlchemy URL format
        if self._conn_str.startswith(("mssql://", "mssql+pyodbc://")):
            return create_engine(self._conn_str, poolclass=NullPool)

        # Parse Azure SQL connection string format
        conn_dict = {}
        for item in self._conn_str.strip(";").split(";"):
            if "=" in item:
                key, value = item.split("=", 1)
                conn_dict[key.strip()] = value.strip()

        # Build ODBC connection string
        server = conn_dict.get("Server", conn_dict.get("Data Source", ""))
        database = conn_dict.get("Database", conn_dict.get("Initial Catalog", ""))
        username = conn_dict.get("User ID", conn_dict.get("UID", ""))
        password = conn_dict.get("Password", conn_dict.get("PWD", ""))

        # Handle server format (remove tcp: prefix if present)
        if server.startswith("tcp:"):
            server = server[4:]

        # Build driver string - try to detect available SQL Server ODBC driver
        driver = conn_dict.get("Driver", "")
        if not driver:
            # Try to find available SQL Server ODBC driver
            drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
            if drivers:
                # Prefer newer versions
                driver = sorted(drivers)[-1]
            else:
                driver = "ODBC Driver 17 for SQL Server"  # Default fallback

        # Build connection URL for SQLAlchemy
        params = {
            "driver": driver,
            "TrustServerCertificate": "yes",
            "Connection Timeout": "30",
        }

        # Add any additional parameters from original connection string
        for key in ["Encrypt", "TrustServerCertificate", "Connection Timeout", "ApplicationIntent"]:
            if key in conn_dict:
                params[key] = conn_dict[key]

        # URL encode the password to handle special characters
        password_encoded = quote_plus(password)

        # Build the connection URL
        query_string = ";".join([f"{k}={v}" for k, v in params.items()])
        connection_url = (
            f"mssql+pyodbc://{username}:{password_encoded}@{server}/{database}?driver={driver}&{query_string}"
        )

        return create_engine(connection_url, poolclass=NullPool)

    def read_tibble(
        self,
        query: str,
        *,
        iter_batches: bool = False,
        batch_size: Optional[int] = None,
        schema_overrides: Optional[Dict[str, pl.DataType]] = None,
        execute_options: Optional[Dict[str, Any]] = None,
    ) -> Union[pl.DataFrame, Iterator[pl.DataFrame]]:
        """
        Read SQL query results into a Polars DataFrame.

        Parameters
        ----------
        query : str
            SQL query to execute
        iter_batches : bool, default False
            Return an iterator of DataFrames for memory-efficient processing
        batch_size : int, optional
            Size of each batch when iter_batches is True
        schema_overrides : dict, optional
            Dictionary mapping column names to Polars dtypes
        execute_options : dict, optional
            Additional options passed to the query execution

        Returns
        -------
        pl.DataFrame or Iterator[pl.DataFrame]
            Query results as Polars DataFrame(s)
        """
        if self.context_check:
            self.context.log.info(f"Executing SQL query: {query[:100]}...")

        try:
            result = pl.read_database(
                query=query,
                connection=self._engine,
                iter_batches=iter_batches,
                batch_size=batch_size,
                schema_overrides=schema_overrides,
                execute_options=execute_options,
            )

            if not iter_batches and self.context_check:
                self.context.log.info(f"Query returned {result.height} rows, {result.width} columns")

            return result

        except Exception as e:
            if self.context_check:
                self.context.log.error(f"Error executing query: {str(e)}")
            raise

    def read_table(
        self,
        table_name: str,
        schema: str = None,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pl.DataFrame:
        """
        Read an entire table or filtered subset into a DataFrame.

        Parameters
        ----------
        table_name : str
            Name of the table to read
        schema : str, optional
            Schema name (defaults to dbo if not specified)
        columns : list of str, optional
            Specific columns to select (defaults to all columns)
        where : str, optional
            WHERE clause conditions (without the WHERE keyword)
        limit : int, optional
            Maximum number of rows to return
        **kwargs
            Additional arguments passed to read_tibble

        Returns
        -------
        pl.DataFrame
            Table data as Polars DataFrame
        """
        # Build the query
        if schema:
            table_ref = f"[{schema}].[{table_name}]"
        else:
            table_ref = f"[{table_name}]"

        if columns:
            columns_str = ", ".join([f"[{col}]" for col in columns])
        else:
            columns_str = "*"

        query = f"SELECT {columns_str} FROM {table_ref}"

        if where:
            query += f" WHERE {where}"

        if limit:
            query += f" TOP {limit}"

        return self.read_tibble(query, **kwargs)

    def read_fluid_hours(self, hours_ago: int = 24 * 365 * 2) -> pl.DataFrame:
        """
        Read fluid hours data from FACT_FLUID_SAMPLE table.

        This is a refactored version of your example function using Polars.

        Parameters
        ----------
        hours_ago : int, default 17520 (2 years)
            Number of hours to look back from current time

        Returns
        -------
        pl.DataFrame
            Fluid sample data
        """
        from_datetime = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime("%Y-%m-%d")

        columns = [
            "FLUID_SAMPLE_ID",
            "SITE_ID",
            "MACHINE_ID",
            "COMPONENT_ID",
            "POSITION_ID",
            "FLUID_UNIQUE_NUMBER",
            "SAMPLE_DATE",
            "LOAD_DATE",
            "SAMPLE_TYPE",
            "FLUID_HOURS",
            "MACHINE_HOURS",
            "COMPONENT_HOURS",
        ]

        query = f"""
        SELECT {', '.join(columns)} 
        FROM ISCAA_KCC.FACT_FLUID_SAMPLE 
        WHERE SAMPLE_DATE > '{from_datetime}' 
        AND SAMPLE_TYPE = 'Normal' 
        AND LOAD_DATE IS NOT NULL
        """

        return self.read_tibble(query.strip())

    def read_with_params(self, query: str, params: Dict[str, Any], **kwargs) -> pl.DataFrame:
        """
        Execute a parameterized query safely using SQLAlchemy's parameter binding.

        Parameters
        ----------
        query : str
            SQL query with named parameters (e.g., :param_name)
        params : dict
            Dictionary of parameter values
        **kwargs
            Additional arguments passed to read_tibble

        Returns
        -------
        pl.DataFrame
            Query results

        Examples
        --------
        >>> db = SQLDatabase()
        >>> df = db.read_with_params(
        ...     "SELECT * FROM users WHERE age > :min_age AND city = :city",
        ...     {"min_age": 25, "city": "Seattle"}
        ... )
        """
        execute_options = kwargs.pop("execute_options", {})
        execute_options["parameters"] = params

        return self.read_tibble(
            query=text(query),  # Use SQLAlchemy text() for parameterized queries
            execute_options=execute_options,
            **kwargs,
        )

    def read_chunked(self, query: str, chunk_column: str, chunk_size: int = 100000, **kwargs) -> Iterator[pl.DataFrame]:
        """
        Read large tables in chunks based on a numeric ID column.

        Parameters
        ----------
        query : str
            Base SQL query (should include WHERE clause if needed)
        chunk_column : str
            Column name to use for chunking (should be numeric and indexed)
        chunk_size : int, default 100000
            Number of rows per chunk
        **kwargs
            Additional arguments passed to read_tibble

        Yields
        ------
        pl.DataFrame
            Data chunks
        """
        # First get the range of the chunk column
        base_query = query.lower()
        if "where" in base_query:
            range_query = f"{query} AND {chunk_column} IS NOT NULL"
        else:
            range_query = f"{query} WHERE {chunk_column} IS NOT NULL"

        # Get min and max values
        minmax_query = f"SELECT MIN({chunk_column}) as min_val, MAX({chunk_column}) as max_val FROM ({range_query}) t"
        bounds = self.read_tibble(minmax_query)

        if bounds.is_empty() or bounds["min_val"][0] is None:
            return

        min_val = bounds["min_val"][0]
        max_val = bounds["max_val"][0]

        # Read in chunks
        current = min_val
        while current <= max_val:
            chunk_end = current + chunk_size

            if "where" in base_query:
                chunk_query = f"{query} AND {chunk_column} >= {current} AND {chunk_column} < {chunk_end}"
            else:
                chunk_query = f"{query} WHERE {chunk_column} >= {current} AND {chunk_column} < {chunk_end}"

            chunk_df = self.read_tibble(chunk_query, **kwargs)

            if not chunk_df.is_empty():
                yield chunk_df

            current = chunk_end

    def get_table_info(self, table_name: str, schema: str = None) -> pl.DataFrame:
        """
        Get column information for a table.

        Parameters
        ----------
        table_name : str
            Name of the table
        schema : str, optional
            Schema name (defaults to all schemas)

        Returns
        -------
        pl.DataFrame
            DataFrame with column information
        """
        query = """
                SELECT c.TABLE_SCHEMA, \
                       c.TABLE_NAME, \
                       c.COLUMN_NAME, \
                       c.DATA_TYPE, \
                       c.IS_NULLABLE, \
                       c.CHARACTER_MAXIMUM_LENGTH, \
                       c.NUMERIC_PRECISION, \
                       c.NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = :table_name \
                """

        params = {"table_name": table_name}

        if schema:
            query += " AND c.TABLE_SCHEMA = :schema"
            params["schema"] = schema

        query += " ORDER BY c.ORDINAL_POSITION"

        return self.read_with_params(query, params)

    def test_connection(self) -> bool:
        """
        Test the database connection.

        Returns
        -------
        bool
            True if connection is successful
        """
        try:
            result = self.read_tibble("SELECT 1 as test")
            return not result.is_empty()
        except Exception as e:
            if self.context_check:
                self.context.log.error(f"Connection test failed: {str(e)}")
            return False

    @property
    def engine(self) -> Engine:
        """Get the underlying SQLAlchemy engine."""
        return self._engine

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - dispose of the engine."""
        self._engine.dispose()
