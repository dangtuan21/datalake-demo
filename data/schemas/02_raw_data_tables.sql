-- =====================================================
-- RAW DATA SCHEMA TABLES
-- =====================================================

USE SCHEMA RETAIL_DATALAKE.RAW_DATA;

-- =====================================================
-- STAGING TABLE FOR ONLINE RETAIL DATA
-- =====================================================

CREATE OR REPLACE TABLE ONLINE_RETAIL_STAGING (
    -- Original data fields
    INVOICE_NO VARCHAR(50) NOT NULL,
    STOCK_CODE VARCHAR(50) NOT NULL,
    DESCRIPTION VARCHAR(500),
    QUANTITY INTEGER NOT NULL,
    INVOICE_DATE TIMESTAMP_NTZ NOT NULL,
    UNIT_PRICE DECIMAL(10,2) NOT NULL,
    CUSTOMER_ID INTEGER,
    COUNTRY VARCHAR(100) NOT NULL,
    
    -- Metadata fields
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME VARCHAR(255),
    ROW_NUMBER_IN_FILE INTEGER,
    DATA_SOURCE VARCHAR(100) DEFAULT 'EXCEL_UPLOAD',
    
    -- Data quality flags
    HAS_MISSING_CUSTOMER_ID BOOLEAN,
    IS_RETURN BOOLEAN,
    TOTAL_AMOUNT DECIMAL(12,2)
)
COMMENT = 'Staging table for raw online retail transaction data';

-- =====================================================
-- ERROR LOG TABLE
-- =====================================================

CREATE OR REPLACE TABLE DATA_LOAD_ERRORS (
    ERROR_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME VARCHAR(255),
    ROW_NUMBER INTEGER,
    ERROR_TYPE VARCHAR(100),
    ERROR_MESSAGE VARCHAR(1000),
    RAW_DATA_JSON VARIANT,
    RESOLVED BOOLEAN DEFAULT FALSE
)
COMMENT = 'Log of data loading errors and validation failures';

-- =====================================================
-- FILE PROCESSING LOG
-- =====================================================

CREATE OR REPLACE TABLE FILE_PROCESSING_LOG (
    LOG_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    FILE_NAME VARCHAR(255) NOT NULL,
    FILE_SIZE_BYTES INTEGER,
    PROCESSING_START_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_END_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR(50), -- STARTED, COMPLETED, FAILED
    TOTAL_ROWS_PROCESSED INTEGER,
    ROWS_LOADED INTEGER,
    ROWS_REJECTED INTEGER,
    ERROR_MESSAGE VARCHAR(1000),
    PROCESSING_DURATION_SECONDS INTEGER
)
COMMENT = 'Log of file processing activities and statistics';

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Create clustering key on staging table for better performance
ALTER TABLE ONLINE_RETAIL_STAGING CLUSTER BY (INVOICE_DATE, COUNTRY);
