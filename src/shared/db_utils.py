"""
Shared Database Utilities
=========================
Centralized database connection management for all monitoring functions.
Used by ADF Monitor, Databricks Monitor, and Recheck Monitor.
"""

import pyodbc
import os
import logging


# --- Database Configuration ---
SQL_SERVER = os.environ.get("SQLSERVER_SERVER")
SQL_DATABASE = os.environ.get("SQLSERVER_DATABASE")
SQL_USERNAME = os.environ.get("SQLSERVER_USERNAME")
SQL_PASSWORD = os.environ.get("SQLSERVER_PASSWORD")


def get_db_connection() -> pyodbc.Connection:
    """Establishes and returns a connection to the Azure SQL Server database.

    Uses Active Directory Password authentication with credentials from
    environment variables. Connection string uses ODBC Driver 17.

    Returns:
        pyodbc.Connection: Active database connection.

    Raises:
        pyodbc.Error: If the connection cannot be established.
    """
    try:
        conn = pyodbc.connect(
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"Authentication=ActiveDirectoryPassword"
        )
        return conn
    except pyodbc.Error as ex:
        logging.error(f"Database connection failed: {str(ex)}")
        raise
