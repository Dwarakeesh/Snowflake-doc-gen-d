#!/bin/bash
set -e
snowsql -q "insert into AI_FEATURE_HUB.RATE_CARD(RATE_ID,FEATURE_KEY,BASE_UNIT_PRICE,CURRENCY,VALID_FROM) values('rate-ci-1','embed_search_v1',0.00001,'USD',current_date())"