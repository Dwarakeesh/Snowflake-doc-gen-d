-- Helper procedure to write model telemetry; call from app/service
CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.INSERT_MODEL_TELEMETRY(
        account_id STRING,
        model_id STRING,
        latency_ms NUMBER,
        tokens_in NUMBER,
        tokens_out NUMBER,
        cost_estimate NUMBER,
        metadata VARIANT
    )
    RETURNS STRING
    LANGUAGE SQL
AS
$$
    INSERT INTO AI_FEATURE_HUB.MODEL_TELEMETRY (
        TELEMETRY_ID,
        ACCOUNT_ID,
        MODEL_ID,
        LATENCY_MS,
        TOKENS_IN,
        TOKENS_OUT,
        COST_ESTIMATE,
        METADATA,
        CREATED_AT
    )
    VALUES (
        uuid_string(),
        account_id,
        model_id,
        latency_ms,
        tokens_in,
        tokens_out,
        cost_estimate,
        PARSE_JSON(metadata),
        CURRENT_TIMESTAMP()
    );
$$;
