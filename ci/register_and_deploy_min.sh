#!/usr/bin/env bash
set-e
export SNOW_ACCOUNT=${SNOW_ACCOUNT}
export SNOW_USER=${SNOW_USER}
export SNOW_PW=${SNOW_PW}
export SNOW_ROLE=${SNOW_ROLE}
export SNOW_WAREHOUSE=${SNOW_WAREHOUSE}
export SNOW_DB=${SNOW_DB:-AI_PLATFORM}
export SNOW_SCHEMA=${SNOW_SCHEMA:-AI_FEATURE_HUB}
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -q "PUT file://sql/billing/*.py @~/ AUTO_COMPRESS=FALSE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/register/register_all_procs_min.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/ddl/rate_rule_staging_and_approval.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/ddl/seed_rate_rule_templates_min.sql