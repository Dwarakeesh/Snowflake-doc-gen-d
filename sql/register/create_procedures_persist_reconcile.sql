-- create_procedures_persist_reconcile.sql
-- PUT the Python files to @~/ first:
-- PUT file://sql/billing/persist_billing_proc.py @~/ AUTO_COMPRESS=FALSE;
-- PUT file://sql/reconcile/reconcile_proc.py @~/ AUTO_COMPRESS=FALSE;

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
