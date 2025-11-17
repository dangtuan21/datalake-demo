-- =====================================================
-- METADATA SCHEMA TABLES
-- =====================================================

USE SCHEMA RETAIL_DATALAKE.METADATA;

-- =====================================================
-- DATA CATALOG TABLE
-- =====================================================

CREATE OR REPLACE TABLE DATA_CATALOG (
    CATALOG_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- Object identification
    DATABASE_NAME VARCHAR(100) NOT NULL,
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    OBJECT_NAME VARCHAR(100) NOT NULL,
    OBJECT_TYPE VARCHAR(50) NOT NULL, -- TABLE, VIEW, PROCEDURE, etc.
    
    -- Metadata
    DESCRIPTION TEXT,
    BUSINESS_PURPOSE TEXT,
    DATA_OWNER VARCHAR(100),
    TECHNICAL_OWNER VARCHAR(100),
    
    -- Data characteristics
    ROW_COUNT INTEGER,
    COLUMN_COUNT INTEGER,
    DATA_SIZE_BYTES INTEGER,
    
    -- Data quality
    QUALITY_SCORE DECIMAL(3,2), -- 0.00 to 1.00
    LAST_QUALITY_CHECK TIMESTAMP_NTZ,
    
    -- Lineage information
    SOURCE_SYSTEMS VARCHAR(500),
    DEPENDENT_OBJECTS VARCHAR(1000),
    
    -- Usage statistics
    QUERY_COUNT_LAST_30_DAYS INTEGER DEFAULT 0,
    LAST_ACCESSED TIMESTAMP_NTZ,
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    UNIQUE (DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME)
)
COMMENT = 'Catalog of all data objects with metadata and usage statistics';

-- =====================================================
-- DATA LINEAGE TABLE
-- =====================================================

CREATE OR REPLACE TABLE DATA_LINEAGE (
    LINEAGE_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- Source object
    SOURCE_DATABASE VARCHAR(100),
    SOURCE_SCHEMA VARCHAR(100),
    SOURCE_OBJECT VARCHAR(100),
    
    -- Target object
    TARGET_DATABASE VARCHAR(100),
    TARGET_SCHEMA VARCHAR(100),
    TARGET_OBJECT VARCHAR(100),
    
    -- Transformation details
    TRANSFORMATION_TYPE VARCHAR(100), -- ETL, VIEW, PROCEDURE, etc.
    TRANSFORMATION_LOGIC TEXT,
    TRANSFORMATION_FREQUENCY VARCHAR(50), -- REAL_TIME, DAILY, WEEKLY, etc.
    
    -- Dependency information
    DEPENDENCY_TYPE VARCHAR(50), -- DIRECT, INDIRECT
    DEPENDENCY_LEVEL INTEGER, -- 1 = direct, 2+ = indirect levels
    
    -- Audit fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT, 
            TARGET_DATABASE, TARGET_SCHEMA, TARGET_OBJECT)
)
COMMENT = 'Data lineage tracking source-to-target relationships';

-- =====================================================
-- PIPELINE EXECUTION LOG
-- =====================================================

CREATE OR REPLACE TABLE PIPELINE_EXECUTION_LOG (
    EXECUTION_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- Pipeline identification
    PIPELINE_NAME VARCHAR(200) NOT NULL,
    PIPELINE_VERSION VARCHAR(50),
    EXECUTION_TYPE VARCHAR(50), -- SCHEDULED, MANUAL, TRIGGERED
    
    -- Execution details
    START_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    END_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR(50), -- RUNNING, COMPLETED, FAILED, CANCELLED
    
    -- Performance metrics
    ROWS_PROCESSED INTEGER,
    ROWS_INSERTED INTEGER,
    ROWS_UPDATED INTEGER,
    ROWS_REJECTED INTEGER,
    EXECUTION_DURATION_SECONDS INTEGER,
    
    -- Error handling
    ERROR_MESSAGE TEXT,
    ERROR_DETAILS VARIANT,
    
    -- Resource usage
    WAREHOUSE_USED VARCHAR(100),
    CREDITS_CONSUMED DECIMAL(10,4),
    
    -- Audit fields
    EXECUTED_BY VARCHAR(100),
    EXECUTION_CONTEXT VARIANT -- Additional context as JSON
)
COMMENT = 'Log of all pipeline executions with performance metrics';

-- =====================================================
-- DATA QUALITY METRICS
-- =====================================================

CREATE OR REPLACE TABLE DATA_QUALITY_METRICS (
    METRIC_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- Object identification
    DATABASE_NAME VARCHAR(100) NOT NULL,
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    TABLE_NAME VARCHAR(100) NOT NULL,
    COLUMN_NAME VARCHAR(100),
    
    -- Quality check details
    CHECK_TYPE VARCHAR(100) NOT NULL, -- NULL_CHECK, DUPLICATE_CHECK, RANGE_CHECK, etc.
    CHECK_NAME VARCHAR(200) NOT NULL,
    CHECK_DESCRIPTION TEXT,
    
    -- Results
    CHECK_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TOTAL_RECORDS INTEGER,
    FAILED_RECORDS INTEGER,
    PASS_RATE DECIMAL(5,2),
    
    -- Thresholds
    WARNING_THRESHOLD DECIMAL(5,2),
    ERROR_THRESHOLD DECIMAL(5,2),
    
    -- Status
    STATUS VARCHAR(50),
    
    -- Additional details
    CHECK_DETAILS VARIANT,
    REMEDIATION_NOTES TEXT
)
COMMENT = 'Data quality check results and metrics';

-- =====================================================
-- USER ACTIVITY LOG
-- =====================================================

CREATE OR REPLACE TABLE USER_ACTIVITY_LOG (
    ACTIVITY_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- User information
    USER_NAME VARCHAR(100) NOT NULL,
    SESSION_ID VARCHAR(200),
    
    -- Activity details
    ACTIVITY_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVITY_TYPE VARCHAR(100), -- QUERY, UPLOAD, DOWNLOAD, VIEW, etc.
    OBJECT_ACCESSED VARCHAR(500),
    
    -- Query information (if applicable)
    QUERY_TEXT TEXT,
    QUERY_DURATION_MS INTEGER,
    ROWS_RETURNED INTEGER,
    
    -- Access details
    ACCESS_METHOD VARCHAR(100), -- WEB_UI, API, SQL_CLIENT, etc.
    IP_ADDRESS VARCHAR(45),
    USER_AGENT TEXT,
    
    -- Results
    SUCCESS BOOLEAN DEFAULT TRUE,
    ERROR_MESSAGE TEXT
)
COMMENT = 'Log of user activities for audit and usage analysis';

-- =====================================================
-- INITIAL CATALOG ENTRIES
-- =====================================================

-- Insert catalog entries for our main tables
INSERT INTO DATA_CATALOG (
    DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
    DESCRIPTION, BUSINESS_PURPOSE, DATA_OWNER
) VALUES
('RETAIL_DATALAKE', 'RAW_DATA', 'ONLINE_RETAIL_STAGING', 'TABLE',
 'Staging table for raw online retail transaction data',
 'Initial landing area for uploaded retail transaction files',
 'Data Engineering Team'),

('RETAIL_DATALAKE', 'PROCESSED_DATA', 'TRANSACTIONS', 'TABLE',
 'Clean transaction data with business rules applied',
 'Primary fact table for retail transaction analysis',
 'Data Engineering Team'),

('RETAIL_DATALAKE', 'PROCESSED_DATA', 'PRODUCTS', 'TABLE',
 'Product dimension with aggregated sales metrics',
 'Product master data and performance metrics',
 'Product Management Team'),

('RETAIL_DATALAKE', 'PROCESSED_DATA', 'CUSTOMERS', 'TABLE',
 'Customer dimension with behavioral metrics and segmentation',
 'Customer master data and behavioral analysis',
 'Customer Analytics Team'),

('RETAIL_DATALAKE', 'ANALYTICS', 'SALES_BY_COUNTRY', 'VIEW',
 'Sales performance aggregated by country',
 'Geographic sales analysis and reporting',
 'Business Intelligence Team');
