#!/bin/bash
set -e
snowsql -q "select count(*) from AI_FEATURE_HUB.RATE_CARD" && echo "ratecard ok"