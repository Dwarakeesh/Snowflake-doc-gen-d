fromsnowflake.snowpark import Session
importhashlib,json
defcompute_invoice_hash(session:Session,invoice_id):
 r=session.sql("select i.invoice_id,i.account_id,i.total,ARRAY_AGG(OBJECT_CONSTRUCT('line',l.line_id,'total',l.line_total)) items from AI_FEATURE_HUB.SUBSCRIPTION_INVOICES i join AI_FEATURE_HUB.INVOICE_ITEMS l on l.invoice_id=i.invoice_id where i.invoice_id=%s group by i.invoice_id,i.account_id,i.total",(invoice_id,)).collect()
 if not r:return{'error':'notfound'}
 obj=r[0].as_dict()
 s=json.dumps(obj,sort_keys=True)
 h=hashlib.sha256(s.encode()).hexdigest()
 session.sql("update AI_FEATURE_HUB.SUBSCRIPTION_INVOICES set INVOICE_HASH=%s where INVOICE_ID=%s",(h,invoice_id)).collect()
 return{'invoice_id':invoice_id,'hash':h}