"""
Snowflake Configuration and Connection Management
"""

import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import pandas as pd
from typing import Optional, Dict, Any
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class SnowflakeConfig:
    """Snowflake connection configuration"""
    
    def __init__(self):
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        self.database = os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DATALAKE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA')
        self.role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    def get_connection_params(self) -> Dict[str, str]:
        """Get connection parameters as dictionary"""
        return {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
            'role': self.role
        }
    
    def get_sqlalchemy_url(self) -> str:
        """Get SQLAlchemy connection URL"""
        return (
            f"snowflake://{self.user}:{self.password}@{self.account}/"
            f"{self.database}/{self.schema}?warehouse={self.warehouse}&role={self.role}"
        )

class SnowflakeConnection:
    """Snowflake connection manager"""
    
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._connection = None
        self._engine = None
    
    def get_connection(self):
        """Get native Snowflake connection"""
        if not self._connection:
            try:
                self._connection = snowflake.connector.connect(
                    **self.config.get_connection_params()
                )
                logger.info("Successfully connected to Snowflake")
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {e}")
                raise
        return self._connection
    
    def get_engine(self):
        """Get SQLAlchemy engine"""
        if not self._engine:
            try:
                self._engine = create_engine(
                    self.config.get_sqlalchemy_url(),
                    poolclass=NullPool
                )
                logger.info("Successfully created Snowflake engine")
            except Exception as e:
                logger.error(f"Failed to create Snowflake engine: {e}")
                raise
        return self._engine
    
    def execute_sql(self, sql: str) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results as DataFrame"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            # If it's a SELECT query, fetch results
            if sql.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
            else:
                conn.commit()
                logger.info(f"Successfully executed SQL: {sql[:100]}...")
                return None
                
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_sql_file(self, file_path: str):
        """Execute SQL commands from file"""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for stmt in statements:
                if stmt:
                    logger.info(f"Executing: {stmt[:50]}...")
                    self.execute_sql(stmt)
                    
            logger.info(f"Successfully executed SQL file: {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to execute SQL file {file_path}: {e}")
            raise
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'append') -> bool:
        """Load pandas DataFrame to Snowflake table"""
        try:
            conn = self.get_connection()
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name.upper(),
                database=self.config.database,
                schema=self.config.schema,
                auto_create_table=True,
                overwrite=(if_exists == 'replace')
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to {table_name}")
                return True
            else:
                logger.error(f"Failed to load data to {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load DataFrame to {table_name}: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test Snowflake connection"""
        try:
            result = self.execute_sql("SELECT CURRENT_VERSION()")
            if result is not None and not result.empty:
                version = result.iloc[0, 0]
                logger.info(f"Snowflake connection successful. Version: {version}")
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close connections"""
        if self._connection:
            self._connection.close()
            self._connection = None
        if self._engine:
            self._engine.dispose()
            self._engine = None

# Global connection instance
_snowflake_conn = None

def get_snowflake_connection() -> SnowflakeConnection:
    """Get global Snowflake connection instance"""
    global _snowflake_conn
    if not _snowflake_conn:
        config = SnowflakeConfig()
        _snowflake_conn = SnowflakeConnection(config)
    return _snowflake_conn
