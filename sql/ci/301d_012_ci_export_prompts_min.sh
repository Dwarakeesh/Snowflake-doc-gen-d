#!/bin/bash
set -e
snowsql -q "call AI_FEATURE_HUB.EXPORT_PROMPTS('/tmp')" && echo "prompts exported"