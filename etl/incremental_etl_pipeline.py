#!/usr/bin/env python3
"""
Incremental Retail ETL Pipeline
Loads retail data in batches with deduplication and incremental updates
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
from datetime import datetime
import logging
import argparse
from pathlib import Path
from config.snowflake_config import get_snowflake_connection
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IncrementalETLPipeline:
    def __init__(self):
        self.conn = get_snowflake_connection()
        self.batch_size = 1000
        
    def get_last_processed_row(self) -> int:
        """Get the last row number that was processed"""
        try:
            result = self.conn.execute_sql(
                "SELECT MAX(ROW_NUMBER_IN_FILE) FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING"
            )
            if result is not None and not result.empty:
                last_row = result.iloc[0, 0]
                return int(last_row) if last_row is not None else 0
            return 0
        except Exception as e:
            logger.warning(f"Could not get last processed row: {e}")
            return 0
    
    def log_batch_start(self, batch_number: int, start_row: int, end_row: int):
        """Log batch processing start"""
        try:
            sql = f"""
            INSERT INTO RETAIL_DATALAKE.METADATA.PIPELINE_EXECUTION_LOG 
            (PIPELINE_NAME, PIPELINE_VERSION, EXECUTION_TYPE, STATUS, EXECUTED_BY, 
             ROWS_PROCESSED, EXECUTION_CONTEXT)
            VALUES ('INCREMENTAL_ETL', '1.0', 'BATCH_{batch_number}', 'RUNNING', 'ETL_PIPELINE',
                    {end_row - start_row + 1}, 
                    PARSE_JSON('{{"batch_number": {batch_number}, "start_row": {start_row}, "end_row": {end_row}}}'))
            """
            self.conn.execute_sql(sql)
            logger.info(f"✅ Logged batch {batch_number} start")
        except Exception as e:
            logger.warning(f"Could not log batch start: {e}")
    
    def log_batch_end(self, batch_number: int, status: str, rows_loaded: int, error_msg: str = None):
        """Log batch processing end"""
        try:
            error_clause = f", ERROR_MESSAGE = '{error_msg}'" if error_msg else ""
            
            sql = f"""
            UPDATE RETAIL_DATALAKE.METADATA.PIPELINE_EXECUTION_LOG 
            SET END_TIME = CURRENT_TIMESTAMP(),
                STATUS = '{status}',
                ROWS_INSERTED = {rows_loaded}
                {error_clause}
            WHERE PIPELINE_NAME = 'INCREMENTAL_ETL' 
            AND STATUS = 'RUNNING'
            AND EXECUTION_TYPE = 'BATCH_{batch_number}'
            AND START_TIME = (
                SELECT MAX(START_TIME) 
                FROM RETAIL_DATALAKE.METADATA.PIPELINE_EXECUTION_LOG 
                WHERE PIPELINE_NAME = 'INCREMENTAL_ETL' AND EXECUTION_TYPE = 'BATCH_{batch_number}'
            )
            """
            self.conn.execute_sql(sql)
            logger.info(f"✅ Logged batch {batch_number} completion")
        except Exception as e:
            logger.warning(f"Could not log batch end: {e}")
    
    def load_batch_to_staging(self, batch_number: int, start_row: int, end_row: int) -> int:
        """Load a specific batch of data to staging table"""
        logger.info(f"Loading batch {batch_number}: rows {start_row}-{end_row}")
        
        try:
            # Read CSV file
            csv_file = "/Users/tuandang/personal/research/AI/datalake-demo/data/processed/online_retail.csv"
            
            # Read only the specific batch
            df = pd.read_csv(csv_file, skiprows=range(1, start_row), nrows=end_row - start_row + 1)
            logger.info(f"Read {len(df):,} rows from CSV for batch {batch_number}")
            
            if df.empty:
                logger.warning(f"No data found for batch {batch_number}")
                return 0
            
            # Clean column names
            df.columns = [col.upper().replace(' ', '_') for col in df.columns]
            
            # Rename columns to match schema
            column_mapping = {
                'INVOICENO': 'INVOICE_NO',
                'STOCKCODE': 'STOCK_CODE', 
                'INVOICEDATE': 'INVOICE_DATE',
                'UNITPRICE': 'UNIT_PRICE',
                'CUSTOMERID': 'CUSTOMER_ID'
            }
            df = df.rename(columns=column_mapping)
            
            # Data cleaning
            df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
            df['UNIT_PRICE'] = pd.to_numeric(df['UNIT_PRICE'], errors='coerce')
            df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce')
            df['CUSTOMER_ID'] = pd.to_numeric(df['CUSTOMER_ID'], errors='coerce')
            
            # Remove invalid rows
            initial_count = len(df)
            df = df.dropna(subset=['INVOICE_NO', 'STOCK_CODE', 'INVOICE_DATE', 'QUANTITY', 'UNIT_PRICE'])
            df = df[df['QUANTITY'] != 0]
            final_count = len(df)
            
            logger.info(f"Cleaned batch {batch_number}: {final_count:,} valid rows, {initial_count - final_count:,} rejected")
            
            # Add metadata columns
            df['LOAD_TIMESTAMP'] = datetime.now()
            df['FILE_NAME'] = f'online_retail_batch_{batch_number}.csv'
            df['ROW_NUMBER_IN_FILE'] = range(start_row, start_row + len(df))
            df['DATA_SOURCE'] = f'CSV_BATCH_{batch_number}'
            df['HAS_MISSING_CUSTOMER_ID'] = df['CUSTOMER_ID'].isna()
            df['IS_RETURN'] = df['QUANTITY'] < 0
            df['TOTAL_AMOUNT'] = df['QUANTITY'] * df['UNIT_PRICE']
            
            # Check for duplicates against existing data
            existing_transactions = self.conn.execute_sql("""
                SELECT DISTINCT CONCAT(INVOICE_NO, '_', STOCK_CODE, '_', ROW_NUMBER_IN_FILE) as TRANSACTION_KEY
                FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING
            """)
            
            if existing_transactions is not None and not existing_transactions.empty:
                existing_keys = set(existing_transactions['TRANSACTION_KEY'].tolist())
                df['TRANSACTION_KEY'] = df['INVOICE_NO'] + '_' + df['STOCK_CODE'] + '_' + df['ROW_NUMBER_IN_FILE'].astype(str)
                
                # Filter out duplicates
                before_dedup = len(df)
                df = df[~df['TRANSACTION_KEY'].isin(existing_keys)]
                after_dedup = len(df)
                
                if before_dedup > after_dedup:
                    logger.info(f"Removed {before_dedup - after_dedup} duplicate transactions")
                
                # Drop the temporary key column
                df = df.drop('TRANSACTION_KEY', axis=1)
            
            if df.empty:
                logger.warning(f"No new records to load for batch {batch_number} after deduplication")
                return 0
            
            # Load to staging using individual INSERTs (optimized)
            loaded_count = 0
            for idx, row in df.iterrows():
                try:
                    # Prepare values with proper escaping
                    invoice_no = str(row['INVOICE_NO']).replace("'", "''")
                    stock_code = str(row['STOCK_CODE']).replace("'", "''")
                    description = str(row['DESCRIPTION']).replace("'", "''") if pd.notna(row['DESCRIPTION']) else 'NULL'
                    quantity = row['QUANTITY']
                    invoice_date = row['INVOICE_DATE'].strftime('%Y-%m-%d %H:%M:%S')
                    unit_price = row['UNIT_PRICE']
                    customer_id = int(row['CUSTOMER_ID']) if pd.notna(row['CUSTOMER_ID']) else 'NULL'
                    country = str(row['COUNTRY']).replace("'", "''")
                    load_timestamp = row['LOAD_TIMESTAMP'].strftime('%Y-%m-%d %H:%M:%S')
                    file_name = row['FILE_NAME']
                    row_number = row['ROW_NUMBER_IN_FILE']
                    data_source = row['DATA_SOURCE']
                    has_missing_customer = 'TRUE' if row['HAS_MISSING_CUSTOMER_ID'] else 'FALSE'
                    is_return = 'TRUE' if row['IS_RETURN'] else 'FALSE'
                    total_amount = row['TOTAL_AMOUNT']
                    
                    desc_value = f"'{description}'" if description != 'NULL' else 'NULL'
                    
                    insert_sql = f"""
                    INSERT INTO RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING 
                    (INVOICE_NO, STOCK_CODE, DESCRIPTION, QUANTITY, INVOICE_DATE, UNIT_PRICE, 
                     CUSTOMER_ID, COUNTRY, LOAD_TIMESTAMP, FILE_NAME, ROW_NUMBER_IN_FILE, 
                     DATA_SOURCE, HAS_MISSING_CUSTOMER_ID, IS_RETURN, TOTAL_AMOUNT)
                    VALUES ('{invoice_no}', '{stock_code}', {desc_value}, {quantity}, 
                            '{invoice_date}', {unit_price}, {customer_id}, '{country}',
                            '{load_timestamp}', '{file_name}', {row_number}, '{data_source}',
                            {has_missing_customer}, {is_return}, {total_amount})
                    """
                    
                    self.conn.execute_sql(insert_sql)
                    loaded_count += 1
                    
                    if loaded_count % 100 == 0:
                        logger.info(f"Loaded {loaded_count}/{len(df)} rows for batch {batch_number}")
                        
                except Exception as e:
                    logger.error(f"Failed to insert row {idx}: {e}")
                    continue
            
            logger.info(f"✅ Batch {batch_number}: {loaded_count} records loaded to staging")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading batch {batch_number}: {e}")
            return 0
    
    def update_dimensions_incremental(self, batch_number: int):
        """Update dimension tables with new data"""
        logger.info(f"Updating dimensions for batch {batch_number}...")
        
        try:
            # Update Products dimension (UPSERT logic)
            products_sql = """
            MERGE INTO RETAIL_DATALAKE.PROCESSED_DATA.PRODUCTS AS target
            USING (
                SELECT 
                    STOCK_CODE,
                    MAX(DESCRIPTION) as DESCRIPTION,
                    SUM(CASE WHEN QUANTITY > 0 THEN QUANTITY ELSE 0 END) as NEW_QUANTITY_SOLD,
                    SUM(CASE WHEN QUANTITY > 0 THEN TOTAL_AMOUNT ELSE 0 END) as NEW_REVENUE,
                    AVG(CASE WHEN QUANTITY > 0 THEN UNIT_PRICE ELSE NULL END) as AVG_UNIT_PRICE,
                    MIN(CASE WHEN QUANTITY > 0 THEN UNIT_PRICE ELSE NULL END) as MIN_UNIT_PRICE,
                    MAX(CASE WHEN QUANTITY > 0 THEN UNIT_PRICE ELSE NULL END) as MAX_UNIT_PRICE,
                    MIN(CASE WHEN QUANTITY > 0 THEN INVOICE_DATE ELSE NULL END) as FIRST_SALE_DATE,
                    MAX(CASE WHEN QUANTITY > 0 THEN INVOICE_DATE ELSE NULL END) as LAST_SALE_DATE,
                    COUNT(DISTINCT CASE WHEN QUANTITY > 0 THEN CUSTOMER_ID ELSE NULL END) as NEW_UNIQUE_CUSTOMERS
                FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING
                WHERE DATA_SOURCE LIKE '%BATCH_%'
                AND STOCK_CODE IS NOT NULL
                GROUP BY STOCK_CODE
            ) AS source
            ON target.STOCK_CODE = source.STOCK_CODE
            WHEN MATCHED THEN UPDATE SET
                DESCRIPTION = COALESCE(source.DESCRIPTION, target.DESCRIPTION),
                TOTAL_QUANTITY_SOLD = target.TOTAL_QUANTITY_SOLD + source.NEW_QUANTITY_SOLD,
                TOTAL_REVENUE = target.TOTAL_REVENUE + source.NEW_REVENUE,
                AVERAGE_UNIT_PRICE = (target.AVERAGE_UNIT_PRICE + source.AVG_UNIT_PRICE) / 2,
                MIN_UNIT_PRICE = LEAST(target.MIN_UNIT_PRICE, source.MIN_UNIT_PRICE),
                MAX_UNIT_PRICE = GREATEST(target.MAX_UNIT_PRICE, source.MAX_UNIT_PRICE),
                FIRST_SALE_DATE = LEAST(target.FIRST_SALE_DATE, source.FIRST_SALE_DATE),
                LAST_SALE_DATE = GREATEST(target.LAST_SALE_DATE, source.LAST_SALE_DATE),
                UNIQUE_CUSTOMERS = target.UNIQUE_CUSTOMERS + source.NEW_UNIQUE_CUSTOMERS,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT 
                (STOCK_CODE, DESCRIPTION, TOTAL_QUANTITY_SOLD, TOTAL_REVENUE, 
                 AVERAGE_UNIT_PRICE, MIN_UNIT_PRICE, MAX_UNIT_PRICE, 
                 FIRST_SALE_DATE, LAST_SALE_DATE, UNIQUE_CUSTOMERS)
            VALUES 
                (source.STOCK_CODE, source.DESCRIPTION, source.NEW_QUANTITY_SOLD, source.NEW_REVENUE,
                 source.AVG_UNIT_PRICE, source.MIN_UNIT_PRICE, source.MAX_UNIT_PRICE,
                 source.FIRST_SALE_DATE, source.LAST_SALE_DATE, source.NEW_UNIQUE_CUSTOMERS)
            """
            self.conn.execute_sql(products_sql)
            logger.info("✅ Products dimension updated")
            
            # Update Customers dimension (similar UPSERT logic)
            customers_sql = """
            MERGE INTO RETAIL_DATALAKE.PROCESSED_DATA.CUSTOMERS AS target
            USING (
                SELECT 
                    CUSTOMER_ID,
                    MAX(COUNTRY) as COUNTRY,
                    MIN(INVOICE_DATE) as FIRST_PURCHASE_DATE,
                    MAX(INVOICE_DATE) as LAST_PURCHASE_DATE,
                    COUNT(DISTINCT INVOICE_NO) as NEW_ORDERS,
                    SUM(CASE WHEN QUANTITY > 0 THEN QUANTITY ELSE 0 END) as NEW_ITEMS_PURCHASED,
                    SUM(CASE WHEN QUANTITY > 0 THEN TOTAL_AMOUNT ELSE 0 END) as NEW_AMOUNT_SPENT,
                    AVG(CASE WHEN QUANTITY > 0 THEN TOTAL_AMOUNT ELSE NULL END) as AVG_ORDER_VALUE
                FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING
                WHERE DATA_SOURCE LIKE '%BATCH_%'
                AND CUSTOMER_ID IS NOT NULL AND QUANTITY > 0
                GROUP BY CUSTOMER_ID
            ) AS source
            ON target.CUSTOMER_ID = source.CUSTOMER_ID
            WHEN MATCHED THEN UPDATE SET
                COUNTRY = COALESCE(source.COUNTRY, target.COUNTRY),
                FIRST_PURCHASE_DATE = LEAST(target.FIRST_PURCHASE_DATE, source.FIRST_PURCHASE_DATE),
                LAST_PURCHASE_DATE = GREATEST(target.LAST_PURCHASE_DATE, source.LAST_PURCHASE_DATE),
                TOTAL_ORDERS = target.TOTAL_ORDERS + source.NEW_ORDERS,
                TOTAL_ITEMS_PURCHASED = target.TOTAL_ITEMS_PURCHASED + source.NEW_ITEMS_PURCHASED,
                TOTAL_AMOUNT_SPENT = target.TOTAL_AMOUNT_SPENT + source.NEW_AMOUNT_SPENT,
                AVERAGE_ORDER_VALUE = (target.AVERAGE_ORDER_VALUE + source.AVG_ORDER_VALUE) / 2,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT 
                (CUSTOMER_ID, COUNTRY, FIRST_PURCHASE_DATE, LAST_PURCHASE_DATE,
                 TOTAL_ORDERS, TOTAL_ITEMS_PURCHASED, TOTAL_AMOUNT_SPENT, AVERAGE_ORDER_VALUE)
            VALUES 
                (source.CUSTOMER_ID, source.COUNTRY, source.FIRST_PURCHASE_DATE, source.LAST_PURCHASE_DATE,
                 source.NEW_ORDERS, source.NEW_ITEMS_PURCHASED, source.NEW_AMOUNT_SPENT, source.AVG_ORDER_VALUE)
            """
            self.conn.execute_sql(customers_sql)
            
            # Update customer segments
            segment_sql = """
            UPDATE RETAIL_DATALAKE.PROCESSED_DATA.CUSTOMERS 
            SET CUSTOMER_SEGMENT = CASE 
                WHEN TOTAL_AMOUNT_SPENT >= 10000 THEN 'VIP'
                WHEN TOTAL_AMOUNT_SPENT >= 5000 THEN 'HIGH_VALUE'
                WHEN TOTAL_AMOUNT_SPENT >= 1000 THEN 'MEDIUM_VALUE'
                WHEN TOTAL_AMOUNT_SPENT > 0 THEN 'LOW_VALUE'
                ELSE 'NEW'
            END,
            DAYS_SINCE_LAST_PURCHASE = DATEDIFF('day', LAST_PURCHASE_DATE, CURRENT_DATE())
            WHERE UPDATED_AT >= CURRENT_DATE()
            """
            self.conn.execute_sql(segment_sql)
            logger.info("✅ Customers dimension updated")
            
            # Update Countries dimension
            countries_sql = """
            MERGE INTO RETAIL_DATALAKE.PROCESSED_DATA.COUNTRIES AS target
            USING (
                SELECT 
                    COUNTRY,
                    COUNT(DISTINCT CUSTOMER_ID) as NEW_CUSTOMERS,
                    COUNT(DISTINCT INVOICE_NO) as NEW_ORDERS,
                    SUM(CASE WHEN QUANTITY > 0 THEN TOTAL_AMOUNT ELSE 0 END) as NEW_REVENUE,
                    MIN(INVOICE_DATE) as FIRST_ORDER_DATE,
                    MAX(INVOICE_DATE) as LAST_ORDER_DATE
                FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING
                WHERE DATA_SOURCE LIKE '%BATCH_%'
                AND COUNTRY IS NOT NULL
                GROUP BY COUNTRY
            ) AS source
            ON target.COUNTRY = source.COUNTRY
            WHEN MATCHED THEN UPDATE SET
                TOTAL_CUSTOMERS = target.TOTAL_CUSTOMERS + source.NEW_CUSTOMERS,
                TOTAL_ORDERS = target.TOTAL_ORDERS + source.NEW_ORDERS,
                TOTAL_REVENUE = target.TOTAL_REVENUE + source.NEW_REVENUE,
                FIRST_ORDER_DATE = LEAST(target.FIRST_ORDER_DATE, source.FIRST_ORDER_DATE),
                LAST_ORDER_DATE = GREATEST(target.LAST_ORDER_DATE, source.LAST_ORDER_DATE),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT 
                (COUNTRY, TOTAL_CUSTOMERS, TOTAL_ORDERS, TOTAL_REVENUE,
                 FIRST_ORDER_DATE, LAST_ORDER_DATE)
            VALUES 
                (source.COUNTRY, source.NEW_CUSTOMERS, source.NEW_ORDERS, source.NEW_REVENUE,
                 source.FIRST_ORDER_DATE, source.LAST_ORDER_DATE)
            """
            self.conn.execute_sql(countries_sql)
            logger.info("✅ Countries dimension updated")
            
        except Exception as e:
            logger.error(f"Error updating dimensions: {e}")
            raise
    
    def append_to_transactions_fact(self, batch_number: int):
        """Append new transactions to fact table"""
        logger.info(f"Appending batch {batch_number} to transactions fact table...")
        
        try:
            transactions_sql = f"""
            INSERT INTO RETAIL_DATALAKE.PROCESSED_DATA.TRANSACTIONS 
            (TRANSACTION_ID, INVOICE_NO, STOCK_CODE, CUSTOMER_ID, DESCRIPTION,
             QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, INVOICE_DATE, INVOICE_YEAR,
             INVOICE_MONTH, INVOICE_DAY_OF_WEEK, COUNTRY, TRANSACTION_TYPE,
             IS_GUEST_PURCHASE, SOURCE_FILE)
            SELECT 
                CONCAT(INVOICE_NO, '_', STOCK_CODE, '_', ROW_NUMBER_IN_FILE) as TRANSACTION_ID,
                INVOICE_NO,
                STOCK_CODE,
                CUSTOMER_ID,
                DESCRIPTION,
                QUANTITY,
                UNIT_PRICE,
                TOTAL_AMOUNT,
                INVOICE_DATE,
                YEAR(INVOICE_DATE) as INVOICE_YEAR,
                MONTH(INVOICE_DATE) as INVOICE_MONTH,
                DAYOFWEEK(INVOICE_DATE) as INVOICE_DAY_OF_WEEK,
                COUNTRY,
                CASE 
                    WHEN QUANTITY > 0 THEN 'SALE'
                    WHEN QUANTITY < 0 THEN 'RETURN'
                    ELSE 'UNKNOWN'
                END as TRANSACTION_TYPE,
                CASE WHEN CUSTOMER_ID IS NULL THEN TRUE ELSE FALSE END as IS_GUEST_PURCHASE,
                FILE_NAME as SOURCE_FILE
            FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING
            WHERE DATA_SOURCE = 'CSV_BATCH_{batch_number}'
            AND INVOICE_NO IS NOT NULL 
            AND STOCK_CODE IS NOT NULL 
            AND QUANTITY IS NOT NULL 
            AND UNIT_PRICE IS NOT NULL
            AND QUANTITY != 0
            """
            
            self.conn.execute_sql(transactions_sql)
            logger.info("✅ Transactions fact table updated")
            
        except Exception as e:
            logger.error(f"Error appending to transactions: {e}")
            raise
    
    def get_data_summary(self):
        """Get current data summary"""
        try:
            staging_count = self.conn.execute_sql("SELECT COUNT(*) FROM RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING")
            transactions_count = self.conn.execute_sql("SELECT COUNT(*) FROM RETAIL_DATALAKE.PROCESSED_DATA.TRANSACTIONS")
            products_count = self.conn.execute_sql("SELECT COUNT(*) FROM RETAIL_DATALAKE.PROCESSED_DATA.PRODUCTS")
            customers_count = self.conn.execute_sql("SELECT COUNT(*) FROM RETAIL_DATALAKE.PROCESSED_DATA.CUSTOMERS")
            countries_count = self.conn.execute_sql("SELECT COUNT(*) FROM RETAIL_DATALAKE.PROCESSED_DATA.COUNTRIES")
            
            return {
                'staging': staging_count.iloc[0, 0] if staging_count is not None else 0,
                'transactions': transactions_count.iloc[0, 0] if transactions_count is not None else 0,
                'products': products_count.iloc[0, 0] if products_count is not None else 0,
                'customers': customers_count.iloc[0, 0] if customers_count is not None else 0,
                'countries': countries_count.iloc[0, 0] if countries_count is not None else 0
            }
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {}
    
    def run_incremental_batch(self, batch_number: int = None):
        """Run incremental batch loading"""
        try:
            # Determine batch number and row range
            if batch_number is None:
                last_row = self.get_last_processed_row()
                batch_number = (last_row // self.batch_size) + 1
            
            start_row = ((batch_number - 1) * self.batch_size) + 1
            end_row = batch_number * self.batch_size
            
            logger.info(f"🚀 Starting incremental batch {batch_number}")
            logger.info(f"📊 Processing rows {start_row:,} to {end_row:,}")
            
            # Get before summary
            before_summary = self.get_data_summary()
            
            # Log batch start
            self.log_batch_start(batch_number, start_row, end_row)
            
            # Step 1: Load batch to staging
            loaded_count = self.load_batch_to_staging(batch_number, start_row, end_row)
            
            if loaded_count == 0:
                logger.warning(f"No data loaded for batch {batch_number}")
                self.log_batch_end(batch_number, 'COMPLETED', 0, "No new data to load")
                return False
            
            # Step 2: Update dimensions
            self.update_dimensions_incremental(batch_number)
            
            # Step 3: Append to fact table
            self.append_to_transactions_fact(batch_number)
            
            # Step 4: Log success
            self.log_batch_end(batch_number, 'COMPLETED', loaded_count)
            
            # Get after summary
            after_summary = self.get_data_summary()
            
            # Display results
            print("\n" + "=" * 60)
            print(f"🎉 BATCH {batch_number} COMPLETED!")
            print("=" * 60)
            print(f"📊 Records Added: {loaded_count:,}")
            print(f"📈 Total Staging: {before_summary.get('staging', 0):,} → {after_summary.get('staging', 0):,}")
            print(f"💰 Total Transactions: {before_summary.get('transactions', 0):,} → {after_summary.get('transactions', 0):,}")
            print(f"📦 Total Products: {before_summary.get('products', 0):,} → {after_summary.get('products', 0):,}")
            print(f"👥 Total Customers: {before_summary.get('customers', 0):,} → {after_summary.get('customers', 0):,}")
            print(f"🌍 Total Countries: {before_summary.get('countries', 0):,} → {after_summary.get('countries', 0):,}")
            print(f"\n🔍 Ready for next batch or analytics!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_number} failed: {e}")
            self.log_batch_end(batch_number, 'FAILED', 0, str(e))
            return False

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Incremental ETL Pipeline')
    parser.add_argument('--batch', type=int, help='Specific batch number to load')
    parser.add_argument('--next', action='store_true', help='Load next available batch')
    parser.add_argument('--status', action='store_true', help='Show current data status')
    
    args = parser.parse_args()
    
    pipeline = IncrementalETLPipeline()
    
    if args.status:
        summary = pipeline.get_data_summary()
        last_row = pipeline.get_last_processed_row()
        next_batch = (last_row // 1000) + 1
        
        print("\n" + "=" * 50)
        print("📊 CURRENT DATA LAKE STATUS")
        print("=" * 50)
        print(f"📈 Staging Records: {summary.get('staging', 0):,}")
        print(f"💰 Transactions: {summary.get('transactions', 0):,}")
        print(f"📦 Products: {summary.get('products', 0):,}")
        print(f"👥 Customers: {summary.get('customers', 0):,}")
        print(f"🌍 Countries: {summary.get('countries', 0):,}")
        print(f"🔢 Last Processed Row: {last_row:,}")
        print(f"➡️  Next Batch: {next_batch}")
        return True
    
    if args.next:
        batch_number = None
    elif args.batch:
        batch_number = args.batch
    else:
        # Interactive mode
        summary = pipeline.get_data_summary()
        last_row = pipeline.get_last_processed_row()
        suggested_batch = (last_row // 1000) + 1
        
        print(f"\n📊 Current records: {summary.get('staging', 0):,}")
        print(f"🔢 Last processed row: {last_row:,}")
        print(f"➡️  Suggested next batch: {suggested_batch}")
        
        batch_input = input(f"\nEnter batch number to load (or press Enter for batch {suggested_batch}): ")
        batch_number = int(batch_input) if batch_input.strip() else suggested_batch
    
    # Run the batch
    success = pipeline.run_incremental_batch(batch_number)
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
