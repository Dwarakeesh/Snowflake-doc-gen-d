-- Insert default tiered pricing template
INSERT INTO AI_FEATURE_HUB.RATE_RULE_TEMPLATES (
    TEMPLATE_ID,
    NAME,
    DESCRIPTION,
    CONFIG_SAMPLE,
    CREATED_AT
)
VALUES (
    'tmpl-default-tier',
    'DefaultTier',
    'Default tiered pricing sample',
    PARSE_JSON('{
        "tiers": [
            {"upto": 100, "unit_price": 0.02},
            {"upto": 1000, "unit_price": 0.012},
            {"upto": null, "unit_price": 0.008}
        ]
    }'),
    CURRENT_TIMESTAMP()
);

-- Insert sample rate rule staging
INSERT INTO AI_FEATURE_HUB.RATE_RULES_STAGING (
    STAGING_ID,
    FEATURE_KEY,
    RULE_TYPE,
    CONFIG,
    PRIORITY,
    SUBMITTED_BY,
    SUBMITTED_AT
)
VALUES (
    'stg-sample-1',
    'nlp_search_v1',
    'TIER',
    PARSE_JSON('{
        "tiers": [
            {"upto": 100, "unit_price": 0.02},
            {"upto": 1000, "unit_price": 0.012},
            {"upto": null, "unit_price": 0.008}
        ]
    }'),
    10,
    'seed@system',
    CURRENT_TIMESTAMP()
);
