-- ANN container authentication template: replace with actual gateway and role details

-- Create API integration using external OAuth
CREATE OR REPLACE API_INTEGRATION AI_FEATURE_HUB.ANN_EXTERNAL
    TYPE = EXTERNAL_OAUTH
    ENABLED = TRUE
    API_AWS_ROLE_ARN = 'arn:aws:iam::REPLACE:role/REPLACE'
    API_PROVIDER = 'aws_apigateway';

-- Create external function pointing to ANN similarity service
CREATE OR REPLACE EXTERNAL FUNCTION AI_FEATURE_HUB.ANN_SIMILARITY(
        q VARIANT
    )
    RETURNS VARIANT
    API_INTEGRATION = AI_FEATURE_HUB.ANN_EXTERNAL
    HEADERS = ('Content-Type' = 'application/json')
    AS 'https://REPLACE-ANN-GW/similarity';
