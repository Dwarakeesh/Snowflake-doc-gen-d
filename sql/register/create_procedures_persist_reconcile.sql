
CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.PERSIST_BILLING_RUNS(
  older_than_minutes NUMBER DEFAULT 60,
  preview_only BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@~/persist_billing_proc.py')
HANDLER = 'persist_billing_runs';

CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.RECONCILE_BILLING_VS_USAGE(
  account_id STRING DEFAULT NULL,
  window_start STRING DEFAULT NULL,
  window_end STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@~/reconcile_proc.py')
HANDLER = 'reconcile_billing_vs_usage';

-- Grant execute to the automation role used in CI / scheduled tasks (replace YOUR_RUN_ROLE)
GRANT EXECUTE ON PROCEDURE AI_FEATURE_HUB.PERSIST_BILLING_RUNS TO ROLE YOUR_RUN_ROLE;
GRANT EXECUTE ON PROCEDURE AI_FEATURE_HUB.RECONCILE_BILLING_VS_USAGE TO ROLE YOUR_RUN_ROLE;
```

2) sql/ddl/rate_rule_staging_and_approval.sql — staging DDL + approval tables and stored-proc skeletons (Snowflake-side approval workflow)  
Design: business users (Streamlit) submit changes to RATE_RULES_STAGING; approver calls AI_FEATURE_HUB.APPROVE_RATE_RULE(proc) which validates and moves the row into RATE_RULES (or updates an existing rule) and sets ACTIVE=true; reject simply archives the staging row. This Snowflake-stored workflow keeps the approval authority and audit trail inside Snowflake so the billing engine only reads ACTIVE rules at runtime. This staging+approval pattern is recommended for production admin UIs in your design notes. @546 @36

File: sql/ddl/rate_rule_staging_and_approval.sql
```sql
-- rate_rule_staging_and_approval.sql
USE DATABASE AI_PLATFORM;
USE SCHEMA AI_FEATURE_HUB;

-- Staging table where business UI writes proposed rule changes (unapproved)
CREATE OR REPLACE TABLE AI_FEATURE_HUB.RATE_RULES_STAGING (
  STAGING_ID STRING PRIMARY KEY,
  FEATURE_KEY STRING NOT NULL,
  RULE_TYPE STRING NOT NULL, -- 'TIER','CAP','MINIMUM','DISCOUNT','TAX','FLAT'
  CONFIG VARIANT NOT NULL, -- JSON of the rule config
  PRIORITY NUMBER DEFAULT 100,
  SUBMITTED_BY STRING,
  SUBMITTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  APPROVED BOOLEAN DEFAULT FALSE,
  APPROVED_BY STRING,
  APPROVED_AT TIMESTAMP_LTZ,
  REJECTED BOOLEAN DEFAULT FALSE,
  REJECT_REASON STRING,
  ARCHIVED BOOLEAN DEFAULT FALSE
);

-- Approval audit table
CREATE OR REPLACE TABLE AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (
  AUDIT_ID STRING PRIMARY KEY,
  STAGING_ID STRING,
  ACTION STRING, -- 'SUBMIT','APPROVE','REJECT','ARCHIVE'
  ACTOR STRING,
  ACTION_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  DETAILS VARIANT
);
