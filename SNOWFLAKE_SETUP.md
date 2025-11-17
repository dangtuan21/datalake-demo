# Snowflake Setup Guide

## Prerequisites

1. **Snowflake Account**: Sign up at [snowflake.com](https://signup.snowflake.com/)
2. **Python Dependencies**: Install required packages

```bash
pip install snowflake-connector-python python-dotenv
```

## Step 1: Get Snowflake Credentials

After creating your Snowflake account, you'll need:

- **Account Identifier**: Found in your Snowflake URL (e.g., `abc12345.us-east-1`)
- **Username**: Your Snowflake username
- **Password**: Your Snowflake password
- **Warehouse**: Default is `COMPUTE_WH`

## Step 2: Configure Environment Variables

1. Copy the example environment file:
```bash
cp config/.env.example config/.env
```

2. Edit `config/.env` with your credentials:
```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DATALAKE
SNOWFLAKE_SCHEMA=RAW_DATA
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

## Step 3: Test Connection and Create Schema

Run the setup script:
```bash
python etl/test_snowflake_connection.py
```

This script will:
1. ✅ Test your Snowflake connection
2. 🏗️ Create the database and schemas
3. 📊 Create all tables and views
4. 🔍 Verify the setup

## Database Schema Overview

```
RETAIL_DATALAKE/
├── RAW_DATA/
│   ├── ONLINE_RETAIL_STAGING     # Raw data staging
│   ├── DATA_LOAD_ERRORS          # Error logging
│   └── FILE_PROCESSING_LOG       # Processing history
├── PROCESSED_DATA/
│   ├── TRANSACTIONS              # Clean transaction data
│   ├── PRODUCTS                  # Product dimension
│   ├── CUSTOMERS                 # Customer dimension
│   └── COUNTRIES                 # Country reference
├── ANALYTICS/
│   ├── SALES_BY_COUNTRY         # Country sales view
│   ├── MONTHLY_REVENUE_TREND    # Revenue trends
│   ├── TOP_PRODUCTS             # Product rankings
│   ├── CUSTOMER_ANALYSIS        # Customer insights
│   └── DAILY_SALES_SUMMARY      # Daily metrics
└── METADATA/
    ├── DATA_CATALOG             # Data catalog
    ├── DATA_LINEAGE             # Lineage tracking
    ├── PIPELINE_EXECUTION_LOG   # ETL logs
    └── DATA_QUALITY_METRICS     # Quality metrics
```

## Step 4: Verify Setup

After running the setup script, you should see:

```
🎉 SNOWFLAKE SETUP COMPLETED SUCCESSFULLY!
✅ Database RETAIL_DATALAKE exists
✅ Found 4 schemas: RAW_DATA, PROCESSED_DATA, ANALYTICS, METADATA
✅ Found 3 tables in RAW_DATA schema
✅ Found 6 views in ANALYTICS schema
```

## Troubleshooting

### Connection Issues
- Verify account identifier format (should include region)
- Check username/password
- Ensure your IP is not blocked by Snowflake security policies

### Permission Issues
- Make sure your user has ACCOUNTADMIN role or sufficient privileges
- Contact your Snowflake administrator if using enterprise account

### Network Issues
- Check firewall settings
- Verify internet connectivity
- Try connecting from Snowflake web UI first

## Next Steps

Once setup is complete:
1. **Load Data**: Run ETL pipeline to load your retail dataset
2. **Test Queries**: Execute sample analytics queries
3. **API Setup**: Configure REST API endpoints
4. **Dashboard**: Build visualization dashboard

## Sample Queries to Test

```sql
-- Test basic connection
SELECT CURRENT_VERSION();

-- Check data catalog
SELECT * FROM RETAIL_DATALAKE.METADATA.DATA_CATALOG;

-- View table structures
DESCRIBE TABLE RETAIL_DATALAKE.RAW_DATA.ONLINE_RETAIL_STAGING;

-- Test analytics view (will be empty until data is loaded)
SELECT * FROM RETAIL_DATALAKE.ANALYTICS.SALES_BY_COUNTRY LIMIT 5;
```
