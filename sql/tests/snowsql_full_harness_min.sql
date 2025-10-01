-- Full harness: create sample usage, run preview, persist, reconcile, and validate

-- Insert sample tenant feature usage
INSERT INTO AI_FEATURE_HUB.TENANT_FEATURE_USAGE (
    USAGE_ID,
    ACCOUNT_ID,
    FEATURE_KEY,
    UNITS,
    UNIT_PRICE,
    USAGE_TIMESTAMP
)
SELECT 
    OBJECT_CONSTRUCT('id', SEQ4()),
    'acct-001',
    'nlp_search_v1',
    10,
    0.02,
    CURRENT_TIMESTAMP();

-- Run billing enhanced (preview mode)
CALL AI_FEATURE_HUB.RUN_BILLING_RUN_ENHANCED(
    DATEADD('day', -30, CURRENT_TIMESTAMP())::STRING,
    CURRENT_TIMESTAMP()::STRING,
    'acct-001',
    TRUE
);

-- Persist billing runs (preview only)
CALL AI_FEATURE_HUB.PERSIST_BILLING_RUNS(0, TRUE);

-- Reconcile billing vs usage for the account
CALL AI_FEATURE_HUB.RECONCILE_BILLING_VS_USAGE(
    'acct-001',
    NULL,
    NULL
);

-- Validate reconciliation results
SELECT COUNT(*) 
FROM AI_FEATURE_HUB.RECONCILIATION_AUDIT;
