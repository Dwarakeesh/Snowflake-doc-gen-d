-- Procedure that registers an External Function or triggers an external FAISS loader
CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.TRIGGER_FAISS_INDEX_LOAD(
        s3_uri STRING
    )
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    RUNTIME_VERSION = '3.1'
AS
$$
    var s3 = s3_uri;

    return {
        status: 'invoke_external_loader',
        s3: s3
    };
$$;
