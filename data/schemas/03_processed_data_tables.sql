-- =====================================================
-- PROCESSED DATA SCHEMA TABLES
-- =====================================================

USE SCHEMA RETAIL_DATALAKE.PROCESSED_DATA;

-- =====================================================
-- CLEAN TRANSACTIONS TABLE
-- =====================================================

CREATE OR REPLACE TABLE TRANSACTIONS (
    -- Primary key
    TRANSACTION_ID VARCHAR(100) PRIMARY KEY,
    
    -- Business keys
    INVOICE_NO VARCHAR(50) NOT NULL,
    STOCK_CODE VARCHAR(50) NOT NULL,
    CUSTOMER_ID INTEGER,
    
    -- Transaction details
    DESCRIPTION VARCHAR(500),
    QUANTITY INTEGER NOT NULL,
    UNIT_PRICE DECIMAL(10,2) NOT NULL,
    TOTAL_AMOUNT DECIMAL(12,2) NOT NULL,
    
    -- Date/time information
    INVOICE_DATE TIMESTAMP_NTZ NOT NULL,
    INVOICE_YEAR INTEGER,
    INVOICE_MONTH INTEGER,
    INVOICE_DAY_OF_WEEK INTEGER,
    
    -- Geographic information
    COUNTRY VARCHAR(100) NOT NULL,
    
    -- Business categorization
    TRANSACTION_TYPE VARCHAR(20),
    
    -- Data quality indicators
    IS_GUEST_PURCHASE BOOLEAN,
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR(255)
)
COMMENT = 'Clean transaction data with business rules applied';

-- =====================================================
-- PRODUCTS DIMENSION TABLE
-- =====================================================

CREATE OR REPLACE TABLE PRODUCTS (
    STOCK_CODE VARCHAR(50) PRIMARY KEY,
    DESCRIPTION VARCHAR(500),
    
    -- Aggregated metrics
    TOTAL_QUANTITY_SOLD INTEGER DEFAULT 0,
    TOTAL_REVENUE DECIMAL(15,2) DEFAULT 0,
    AVERAGE_UNIT_PRICE DECIMAL(10,2),
    MIN_UNIT_PRICE DECIMAL(10,2),
    MAX_UNIT_PRICE DECIMAL(10,2),
    
    -- Date tracking
    FIRST_SALE_DATE TIMESTAMP_NTZ,
    LAST_SALE_DATE TIMESTAMP_NTZ,
    
    -- Customer metrics
    UNIQUE_CUSTOMERS INTEGER DEFAULT 0,
    
    -- Product categorization (can be enhanced later)
    PRODUCT_CATEGORY VARCHAR(100),
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Product dimension with aggregated sales metrics';

-- =====================================================
-- CUSTOMERS DIMENSION TABLE
-- =====================================================

CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID INTEGER PRIMARY KEY,
    COUNTRY VARCHAR(100) NOT NULL,
    
    -- Purchase behavior metrics
    FIRST_PURCHASE_DATE TIMESTAMP_NTZ,
    LAST_PURCHASE_DATE TIMESTAMP_NTZ,
    TOTAL_ORDERS INTEGER DEFAULT 0,
    TOTAL_ITEMS_PURCHASED INTEGER DEFAULT 0,
    TOTAL_AMOUNT_SPENT DECIMAL(15,2) DEFAULT 0,
    AVERAGE_ORDER_VALUE DECIMAL(10,2),
    
    -- Customer segmentation
    CUSTOMER_SEGMENT VARCHAR(50),
    
    -- Behavioral indicators
    DAYS_SINCE_LAST_PURCHASE INTEGER,
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Customer dimension with behavioral metrics and segmentation';

-- =====================================================
-- COUNTRIES REFERENCE TABLE
-- =====================================================

CREATE OR REPLACE TABLE COUNTRIES (
    COUNTRY VARCHAR(100) PRIMARY KEY,
    COUNTRY_CODE VARCHAR(3),
    REGION VARCHAR(100),
    
    -- Sales metrics
    TOTAL_CUSTOMERS INTEGER DEFAULT 0,
    TOTAL_ORDERS INTEGER DEFAULT 0,
    TOTAL_REVENUE DECIMAL(15,2) DEFAULT 0,
    
    -- Date tracking
    FIRST_ORDER_DATE TIMESTAMP_NTZ,
    LAST_ORDER_DATE TIMESTAMP_NTZ,
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Country reference data with aggregated sales metrics';

-- =====================================================
-- CLUSTERING AND INDEXING
-- =====================================================

-- Cluster transactions by date and country for better performance
ALTER TABLE TRANSACTIONS CLUSTER BY (INVOICE_DATE, COUNTRY);
