"""
Database Connector Factory for TPC-C Application
Creates appropriate database connectors based on configuration
"""

import logging
import os

from .base_connector import BaseDatabaseConnector
from .cockroach_connector import CockroachConnector

logger = logging.getLogger(__name__)


def create_study_connector() -> BaseDatabaseConnector:
    """
    Create a database connector for the study based on environment configuration
    
    Returns:
        BaseDatabaseConnector: Configured database connector
        
    Raises:
        ValueError: If no valid provider is configured
        RuntimeError: If connector initialization fails
    """
    provider = os.getenv("DB_PROVIDER", "").lower()
    
    logger.info(f"Creating database connector for provider: {provider}")
    
    if provider == "cockroach" or provider == "cockroachdb":
        try:
            connector = CockroachConnector()
            logger.info("✅ CockroachDB connector created successfully")
            return connector
        except Exception as e:
            logger.error(f"❌ Failed to create CockroachDB connector: {str(e)}")
            raise RuntimeError(f"Failed to create CockroachDB connector: {str(e)}")
    
    # If no provider specified, try to auto-detect based on available connection strings
    if not provider:
        if os.getenv("COCKROACH_CONNECTION_STRING"):
            logger.info("Auto-detected CockroachDB from connection string")
            try:
                connector = CockroachConnector()
                logger.info("✅ Auto-detected CockroachDB connector created successfully")
                return connector
            except Exception as e:
                logger.error(f"❌ Failed to create auto-detected CockroachDB connector: {str(e)}")
                raise RuntimeError(f"Failed to create CockroachDB connector: {str(e)}")
    
    # No valid provider found
    available_providers = ["cockroach", "cockroachdb"]
    raise ValueError(
        f"No valid database provider configured. "
        f"Set DB_PROVIDER environment variable to one of: {available_providers}. "
        f"Current value: '{provider}'"
    )


def create_connector(provider: str) -> BaseDatabaseConnector:
    """
    Create a database connector for a specific provider
    
    Args:
        provider: Database provider name
        
    Returns:
        BaseDatabaseConnector: Configured database connector
        
    Raises:
        ValueError: If provider is not supported
        RuntimeError: If connector initialization fails
    """
    provider = provider.lower()
    
    if provider in ["cockroach", "cockroachdb"]:
        try:
            return CockroachConnector()
        except Exception as e:
            raise RuntimeError(f"Failed to create CockroachDB connector: {str(e)}")
    
    raise ValueError(f"Unsupported database provider: {provider}")


class DatabaseConnectorFactory:
    """
    Factory class for creating database connectors
    """
    
    @staticmethod
    def create_connector(provider: str) -> BaseDatabaseConnector:
        """
        Create a database connector for a specific provider
        
        Args:
            provider: Database provider name
            
        Returns:
            BaseDatabaseConnector: Configured database connector
        """
        return create_connector(provider)
    
    @staticmethod
    def create_study_connector() -> BaseDatabaseConnector:
        """
        Create a database connector for the study based on environment configuration
        
        Returns:
            BaseDatabaseConnector: Configured database connector
        """
        return create_study_connector()
