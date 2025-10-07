from snowflake.snowpark import Session,functions as F
importjson,uuid
defpersist_billing_runs(session:Session,older_than_minutes:int=60,preview_only:bool=True):
 rows=session.sql("select * from AI_FEATURE_HUB.BILLING_PREVIEW where PREVIEW_TS<=dateadd('minute',-{} ,current_timestamp())".format(older_than_minutes)).collect()
 persisted=[]
 for r in rows:
  data=json.loads(r['PREVIEW_JSON'])
  if not preview_only:
   session.sql("insert into AI_FEATURE_HUB.SUBSCRIPTION_INVOICES(INVOICE_ID,ACCOUNT_ID,RUN_TS,SUBTOTAL,MARKUP,TAX,TOTAL,INVOICE_HASH) values(%s,%s,CURRENT_TIMESTAMP(),%s,%s,%s,%s,%s)".format()).collect()
  persisted.append(r['PREVIEW_ID'])
 return {'persisted':len(persisted),'preview_only':preview_only}