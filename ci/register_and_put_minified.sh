#!/usr/bin/env bash
set -e
# PUT python files then register procedures
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -q "PUT file://sql/billing/run_billing_enhanced.py @~/ AUTO_COMPRESS=FALSE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -q "PUT file://sql/billing/pricing_engine.py @~/ AUTO_COMPRESS=FALSE;"
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/register/create_procedures_persist_reconcile.sql
snowsql -a $SNOW_ACCOUNT -u $SNOW_USER -r $SNOW_ROLE -w $SNOW_WAREHOUSE -d $SNOW_DB -s $SNOW_SCHEMA -f sql/register/register_submit_approve.sql
