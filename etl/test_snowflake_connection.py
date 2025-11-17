#!/usr/bin/env python3
"""
Snowflake Connection Test Script
Tests connection and creates database schema
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.snowflake_config import SnowflakeConnection, SnowflakeConfig
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_connection():
    """Test Snowflake connection"""
    print("🔗 Testing Snowflake Connection...")
    
    try:
        # Create configuration
        config = SnowflakeConfig()
        
        # Check if credentials are set
        if not all([config.account, config.user, config.password]):
            print("❌ Missing Snowflake credentials!")
            print("Please set the following environment variables:")
            print("  - SNOWFLAKE_ACCOUNT")
            print("  - SNOWFLAKE_USER") 
            print("  - SNOWFLAKE_PASSWORD")
            print("\nOr copy .env.example to .env and fill in your credentials")
            return False
        
        # Test connection
        conn = SnowflakeConnection(config)
        success = conn.test_connection()
        
        if success:
            print("✅ Snowflake connection successful!")
            return True
        else:
            print("❌ Snowflake connection failed!")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

def create_database_schema():
    """Create the database schema"""
    print("\n🏗️  Creating Database Schema...")
    
    try:
        config = SnowflakeConfig()
        conn = SnowflakeConnection(config)
        
        # Get schema files directory
        schema_dir = Path(__file__).parent.parent / "data" / "schemas"
        
        # Execute schema files in order
        schema_files = [
            "01_create_database.sql",
            "02_raw_data_tables.sql", 
            "03_processed_data_tables.sql",
            "04_analytics_views.sql",
            "05_metadata_tables.sql"
        ]
        
        for sql_file in schema_files:
            file_path = schema_dir / sql_file
            if file_path.exists():
                print(f"📄 Executing {sql_file}...")
                conn.execute_sql_file(str(file_path))
                print(f"✅ {sql_file} completed")
            else:
                print(f"⚠️  File not found: {sql_file}")
        
        print("✅ Database schema created successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Schema creation failed: {e}")
        return False

def verify_schema():
    """Verify the created schema"""
    print("\n🔍 Verifying Schema...")
    
    try:
        config = SnowflakeConfig()
        conn = SnowflakeConnection(config)
        
        # Check databases
        result = conn.execute_sql("SHOW DATABASES LIKE 'RETAIL_DATALAKE'")
        if result is not None and not result.empty:
            print("✅ Database RETAIL_DATALAKE exists")
        
        # Check schemas
        schemas_query = """
        SELECT SCHEMA_NAME, COMMENT 
        FROM RETAIL_DATALAKE.INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME IN ('RAW_DATA', 'PROCESSED_DATA', 'ANALYTICS', 'METADATA')
        ORDER BY SCHEMA_NAME
        """
        
        schemas = conn.execute_sql(schemas_query)
        if schemas is not None and not schemas.empty:
            print(f"✅ Found {len(schemas)} schemas:")
            for _, row in schemas.iterrows():
                print(f"   • {row['SCHEMA_NAME']}: {row['COMMENT']}")
        
        # Check tables in RAW_DATA schema
        tables_query = """
        SELECT TABLE_NAME, COMMENT 
        FROM RETAIL_DATALAKE.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'RAW_DATA'
        ORDER BY TABLE_NAME
        """
        
        tables = conn.execute_sql(tables_query)
        if tables is not None and not tables.empty:
            print(f"✅ Found {len(tables)} tables in RAW_DATA schema:")
            for _, row in tables.iterrows():
                print(f"   • {row['TABLE_NAME']}")
        
        # Check views in ANALYTICS schema
        views_query = """
        SELECT TABLE_NAME as VIEW_NAME, COMMENT 
        FROM RETAIL_DATALAKE.INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_SCHEMA = 'ANALYTICS'
        ORDER BY TABLE_NAME
        """
        
        views = conn.execute_sql(views_query)
        if views is not None and not views.empty:
            print(f"✅ Found {len(views)} views in ANALYTICS schema:")
            for _, row in views.iterrows():
                print(f"   • {row['VIEW_NAME']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema verification failed: {e}")
        return False

def main():
    """Main execution function"""
    print("=" * 60)
    print("🏔️  SNOWFLAKE DATA LAKE SETUP")
    print("=" * 60)
    
    # Step 1: Test connection
    if not test_connection():
        print("\n❌ Setup failed at connection test")
        return False
    
    # Step 2: Create schema
    if not create_database_schema():
        print("\n❌ Setup failed at schema creation")
        return False
    
    # Step 3: Verify schema
    if not verify_schema():
        print("\n❌ Setup failed at schema verification")
        return False
    
    print("\n" + "=" * 60)
    print("🎉 SNOWFLAKE SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print("\n📋 Next Steps:")
    print("1. Run ETL pipeline to load your retail data")
    print("2. Test analytics views with sample queries")
    print("3. Set up API endpoints for data access")
    print("4. Build dashboard for visualization")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
