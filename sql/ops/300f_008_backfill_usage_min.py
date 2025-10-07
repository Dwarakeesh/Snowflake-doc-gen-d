from snowflake.snowpark import Session
defbackfill_usage(session:Session,source_table,limit=1000):rows=session.sql(f"select * from {source_table} limit {limit}").collect();c=0
for r in rows:
 session.call('AI_FEATURE_HUB.UPSERT_USAGE') 
 c+=1
return{'backfilled':c}