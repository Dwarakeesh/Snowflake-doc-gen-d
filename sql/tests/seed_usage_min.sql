-- Insert sample tenant feature usage for account acct-001
INSERT INTO AI_FEATURE_HUB.TENANT_FEATURE_USAGE (
    USAGE_ID,
    ACCOUNT_ID,
    FEATURE_KEY,
    UNITS,
    UNIT_PRICE,
    USAGE_TIMESTAMP
)
VALUES (
    uuid_string(),
    'acct-001',
    'nlp_search_v1',
    100,
    0.00002,
    CURRENT_TIMESTAMP() - 3600
);

-- Insert sample tenant feature usage for account acct-002
INSERT INTO AI_FEATURE_HUB.TENANT_FEATURE_USAGE (
    USAGE_ID,
    ACCOUNT_ID,
    FEATURE_KEY,
    UNITS,
    UNIT_PRICE,
    USAGE_TIMESTAMP
)
VALUES (
    uuid_string(),
    'acct-002',
    'nlp_search_v1',
    250,
    0.00002,
    CURRENT_TIMESTAMP() - 7200
);
