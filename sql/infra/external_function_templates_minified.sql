-- Template to register External Function referencing ANN service

-- Create or replace API integration using external OAuth
CREATE OR REPLACE API_INTEGRATION AI_FEATURE_HUB.ANN_API_INTEGRATION
    TYPE = EXTERNAL_OAUTH
    ENABLED = TRUE;

-- Create or replace external function pointing to ANN service
CREATE OR REPLACE EXTERNAL FUNCTION AI_FEATURE_HUB.ANN_SEARCH(
        endpoint_variant VARIANT
    )
    RETURNS VARIANT
    API_INTEGRATION = AI_FEATURE_HUB.ANN_API_INTEGRATION
    HEADERS = ('Content-Type' = 'application/json')
    AS 'https://REPLACE-ANN-API-GW/ann_search';
