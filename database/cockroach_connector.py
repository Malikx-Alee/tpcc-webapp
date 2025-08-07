"""
CockroachDB Database Connector - STUDY IMPLEMENTATION SKELETON
Participants will implement this connector to integrate with CockroachDB

This file contains TODO items that participants need to complete during the study.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class CockroachConnector(BaseDatabaseConnector):
    """
    CockroachDB database connector for TPC-C application

    Participants will implement connection management and query execution
    for CockroachDB during the UX study.
    """

    def __init__(self):
        """
        Initialize CockroachDB connection

        TODO: Implement CockroachDB connection initialization
        - Read configuration from environment variables
        - Set up PostgreSQL-compatible connection to CockroachDB
        - Configure connection parameters and SSL settings
        - Handle CockroachDB-specific connection requirements

        Environment variables to use:
        - COCKROACH_CONNECTION_STRING: PostgreSQL connection string for CockroachDB
        """
        super().__init__()
        self.provider_name = "CockroachDB"

        # Initialize CockroachDB connection
        self.connection = None

        # Read configuration from environment
        self.connection_string = os.getenv("COCKROACH_CONNECTION_STRING")

        # Validate required configuration
        if not self.connection_string:
            logger.error("COCKROACH_CONNECTION_STRING environment variable is required")
            raise ValueError("COCKROACH_CONNECTION_STRING environment variable is required")

        # Initialize PostgreSQL client and connection
        try:
            logger.info(f"Initializing CockroachDB connection to: {self._mask_connection_string()}")
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=10
            )
            # Set autocommit for CockroachDB compatibility
            self.connection.autocommit = True
            logger.info("✅ CockroachDB connection initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize CockroachDB connection: {str(e)}")
            self.connection = None
            raise

    def _mask_connection_string(self) -> str:
        """Mask sensitive information in connection string for logging"""
        if not self.connection_string:
            return "None"
        # Replace password with asterisks
        import re
        masked = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', self.connection_string)
        return masked

    def test_connection(self) -> bool:
        """
        Test connection to CockroachDB database

        TODO: Implement connection testing
        - Test connection to CockroachDB cluster
        - Execute a simple query to verify connectivity
        - Return True if successful, False otherwise
        - Log connection status for study data collection

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if not self.connection:
                logger.warning("No connection available for testing")
                return False

            # Test connection to CockroachDB cluster
            with self.connection.cursor() as cursor:
                # Execute a simple query to verify connectivity
                start_time = time.time()
                cursor.execute("SELECT 1 as test_value")
                result = cursor.fetchone()
                end_time = time.time()

                # Log connection status for study data collection
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                logger.info(f"✅ CockroachDB connection test successful (response time: {response_time:.2f}ms)")
                logger.info(f"   Test query result: {result}")

                return result is not None and result['test_value'] == 1

        except Exception as e:
            logger.error(f"❌ CockroachDB connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query on CockroachDB

        TODO: Implement query execution
        - Handle parameterized queries safely
        - Convert CockroachDB results to standard format
        - Handle CockroachDB-specific data types
        - Implement proper error handling
        - Log query performance for study metrics

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of dictionaries representing query results
        """
        try:
            if not self.connection:
                logger.error("No database connection available")
                raise RuntimeError("No database connection available")

            # Log query performance for study metrics
            start_time = time.time()
            logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            if params:
                logger.debug(f"Query parameters: {params}")

            with self.connection.cursor() as cursor:
                # Handle parameterized queries safely
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Convert CockroachDB results to standard format
                results = cursor.fetchall()

                # Handle CockroachDB-specific data types
                # Convert RealDictRow objects to regular dictionaries
                formatted_results = []
                for row in results:
                    if row:
                        # Convert to regular dict and handle special data types
                        row_dict = dict(row)
                        # Convert any datetime objects to ISO format strings for JSON serialization
                        for key, value in row_dict.items():
                            if hasattr(value, 'isoformat'):  # datetime objects
                                row_dict[key] = value.isoformat()
                            elif isinstance(value, (bytes, bytearray)):  # binary data
                                row_dict[key] = value.decode('utf-8', errors='ignore')
                        formatted_results.append(row_dict)

                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds

                # Log performance metrics
                logger.info(f"✅ Query executed successfully in {execution_time:.2f}ms, returned {len(formatted_results)} rows")

                return formatted_results

        except Exception as e:
            logger.error(f"❌ CockroachDB query execution failed: {str(e)}")
            logger.error(f"   Query: {query}")
            if params:
                logger.error(f"   Parameters: {params}")
            raise

    def get_provider_name(self) -> str:
        """Return the provider name"""
        return self.provider_name

    def close_connection(self):
        """
        Close database connections

        TODO: Implement connection cleanup
        - Close PostgreSQL client connections
        - Clean up any connection pools
        - Log connection closure for study metrics
        """
        try:
            # Implement connection cleanup
            if self.connection:
                # Close PostgreSQL client connections
                self.connection.close()
                self.connection = None
                # Log cleanup completion
                logger.info("✅ CockroachDB connection closed successfully")
            else:
                logger.debug("No connection to close")
        except Exception as e:
            logger.error(f"❌ Connection cleanup failed: {str(e)}")
            # Ensure connection is set to None even if close fails
            self.connection = None
