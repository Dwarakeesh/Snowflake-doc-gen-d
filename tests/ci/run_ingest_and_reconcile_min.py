importos,json,time
from snowflake import connector
defconn():return connector.connect(user=os.getenv('SNOW_USER'),password=os.getenv('SNOW_PW'),account=os.getenv('SNOW_ACCOUNT'),warehouse=os.getenv('SNOW_WAREHOUSE'),database=os.getenv('SNOW_DB','AI_PLATFORM'),schema=os.getenv('SNOW_SCHEMA','AI_FEATURE_HUB'))
c=conn()
c.cursor().execute("CALL AI_FEATURE_HUB.INGEST_USAGE_TASK(100);")
time.sleep(2)
c.cursor().execute("CALL AI_FEATURE_HUB.RUN_BILLING_RUN_ENHANCED(dateadd('day',-1,current_timestamp())::STRING,current_timestamp()::STRING,NULL,TRUE);")
c.cursor().execute("CALL AI_FEATURE_HUB.PERSIST_BILLING_RUNS(0,TRUE);")
r=c.cursor().execute("CALL AI_FEATURE_HUB.RECONCILE_BILLING_VS_USAGE(NULL,NULL,NULL);").fetchall()
print('RECONCILE_RESULT',r)
c.close()