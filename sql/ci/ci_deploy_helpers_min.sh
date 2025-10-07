#!/bin/bash
set -e
# usage: ./ci_deploy_helpers_min.sh
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -q "PUT file://sql/billing/*.py @~/ AUTO_COMPRESS=FALSE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/register/register_more_min.sql
# seed
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/ddl/seed_rate_rule_templates_min.sql