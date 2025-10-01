import os
import json
import sys
import requests
import base64
from snowflake import connector

def run(sql, fetch=False):
    """Execute SQL query or stored procedure in Snowflake."""
    c = conn.cursor()
    c.execute(sql)
    result = c.fetchall() if fetch else None
    c.close()
    return result


# Establish Snowflake connection using environment variables
conn = connector.connect(
    user=os.getenv("SNOW_USER"),
    account=os.getenv("SNOW_ACCOUNT"),
    password=os.getenv("SNOW_PW"),
    warehouse=os.getenv("SNOW_WAREHOUSE"),
    database=os.getenv("SNOW_DB", "AI_PLATFORM"),
    schema=os.getenv("SNOW_SCHEMA", "AI_FEATURE_HUB")
)

# Run billing enhanced procedure
print(run(
    "CALL AI_FEATURE_HUB.RUN_BILLING_RUN_ENHANCED("
    "'2025-08-01T00:00:00Z', "
    "'2025-08-31T23:59:59Z', "
    "'acct-001', "
    "TRUE);",
    True
))

# Persist billing runs
print(run("CALL AI_FEATURE_HUB.PERSIST_BILLING_RUNS(0, TRUE);", True))

# Reconcile billing vs usage
print(run(
    "CALL AI_FEATURE_HUB.RECONCILE_BILLING_VS_USAGE("
    "'acct-001', "
    "'2025-08-01T00:00:00Z', "
    "'2025-08-31T23:59:59Z');",
    True
))

# Close connection
conn.close()
