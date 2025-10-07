#!/bin/bash
set -e
snowsql -q "select count(*) from AI_FEATURE_HUB.DOCUMENT_EMBEDDINGS" && echo "embeddings ok"