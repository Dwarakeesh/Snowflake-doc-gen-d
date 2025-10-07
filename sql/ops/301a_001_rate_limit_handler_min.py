fromsnowflake.snowpark import Session
importjson,os,time
defhandle_rate_limits(session:Session,tenant_id,limit=1000):
 rows=session.sql("selectcount(*)fromAI_FEATURE_HUB.TENANT_FEATURE_USAGE where ACCOUNT_ID=%s",(tenant_id,)).collect()
 cur=rows[0][0] if rows else 0
 if cur>limit:
  session.sql("insert into AI_FEATURE_HUB.ALERTS(ALERT_ID,ALERT_KEY,ACCOUNT_ID,DETAILS) values(LEFT(GENERATOR_RANDOM_STRING(),36),'QUOTA_EXCEEDED',%s,parse_json(%s))",(tenant_id,json.dumps({'current':cur,'limit':limit}))).collect()
 return{'current':cur,'limit':limit}