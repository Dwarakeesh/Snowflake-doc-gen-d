-- Procedure to generate alerts for reconciliation mismatches
CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.RUN_RECONCILIATION_ALERTS()
    RETURNS STRING
    LANGUAGE SQL
AS
$$
    INSERT INTO AI_FEATURE_HUB.ALERTS (
        ALERT_ID,
        NAME,
        DETAILS,
        CREATED_AT
    )
    SELECT
        uuid_string(),
        'RECONCILE_MISMATCH',
        OBJECT_CONSTRUCT(
            'account_id', account_id,
            'feature_key', feature_key,
            'diff', mismatch
        ),
        CURRENT_TIMESTAMP()
    FROM
        AI_FEATURE_HUB.RECONCILIATION_AUDIT
    WHERE
        ABS(mismatch) > 0.01;
$$;
