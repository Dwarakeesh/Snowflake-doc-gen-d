# Step 1: Upload Python files to the Snowflake stage
snowsql -q "PUT file://sql/billing/*.py @~/ AUTO_COMPRESS=FALSE;"

# Step 2: Register stored procedures
snowsql -f sql/register/register_more_min.sql

# Step 3: Seed initial rate rule templates
snowsql -f sql/ddl/seed_rate_rule_templates_min.sql

# Step 4: Enable tasks by resuming the usage ingestion task
snowsql -q "ALTER TASK AI_FEATURE_HUB.TASK_INGEST_USAGE RESUME;"
