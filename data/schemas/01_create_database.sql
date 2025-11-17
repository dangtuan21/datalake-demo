-- =====================================================
-- RETAIL DATA LAKE - DATABASE SETUP
-- =====================================================

-- Create the main database
CREATE DATABASE IF NOT EXISTS RETAIL_DATALAKE
COMMENT = 'Data Lake for Retail Analytics - Online Retail Dataset';

-- Use the database
USE DATABASE RETAIL_DATALAKE;

-- =====================================================
-- SCHEMA CREATION
-- =====================================================

-- Raw Data Schema - for staging and initial data loads
CREATE SCHEMA IF NOT EXISTS RAW_DATA
COMMENT = 'Raw data staging area - unprocessed data from source systems';

-- Processed Data Schema - for cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS PROCESSED_DATA
COMMENT = 'Cleaned and transformed data ready for analytics';

-- Analytics Schema - for aggregated views and reports
CREATE SCHEMA IF NOT EXISTS ANALYTICS
COMMENT = 'Analytics views and aggregated data for reporting';

-- Metadata Schema - for data lineage and catalog information
CREATE SCHEMA IF NOT EXISTS METADATA
COMMENT = 'Data catalog, lineage, and pipeline metadata';

-- =====================================================
-- WAREHOUSE SETUP
-- =====================================================

-- Create compute warehouse if it doesn't exist
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for retail data processing';

-- Set default warehouse
USE WAREHOUSE RETAIL_WH;
