from snowflake.snowpark import Session,functions as F
importjson
defreconcile_billing_vs_usage(session:Session,account_id=None,window_start=None,window_end=None):
 cond=[]
 if account_id:cond.append(f"account_id='{account_id}'")
 if window_start and window_end:cond.append(f"usage_timestamp between to_timestamp_ltz('{window_start}') and to_timestamp_ltz('{window_end}')")
 where=' and '.join(cond) or '1=1'
 usage=session.sql(f"select account_id,feature_key,sum(units) units from AI_FEATURE_HUB.TENANT_FEATURE_USAGE where {where} group by account_id,feature_key").collect()
 billing=session.sql(f"select account_id,feature_key,sum(line_total) billed from AI_FEATURE_HUB.BILLING_LINE_ITEM where {where} group by account_id,feature_key").collect()
 u_map={(r['ACCOUNT_ID'],r['FEATURE_KEY']):r['UNITS'] for r in usage}
 b_map={(r['ACCOUNT_ID'],r['FEATURE_KEY']):r['BILLED'] for r in billing}
 issues=[]
 keys=set(list(u_map.keys())+list(b_map.keys()))
 for k in keys:
  u=u_map.get(k,0)
  b=b_map.get(k,0)
  if abs(u-b)>0.01:
   issues.append({'account_id':k[0],'feature_key':k[1],'usage':u,'billed':b,'diff':u-b})
 if issues:
  for i in issues:
   session.sql("insert into AI_FEATURE_HUB.RECONCILIATION_AUDIT(AUDIT_ID,ACCOUNT_ID,FEATURE_KEY,USAGE_SUM,BILL_SUM,MISMATCH,RUN_AT) values('{0}','{1}','{2}',{3},{4},{5},CURRENT_TIMESTAMP())".format(uuid.uuid4().hex,i['account_id'],i['feature_key'],i['usage'],i['billed'],i['diff'])).collect()
 return {'issues':len(issues)}