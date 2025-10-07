
from snowflake.snowpark import Session,functions as F
importjson,hashlib,uuid,datetime
def run_billing_enhanced(session:Session,window_start:str,window_end:str,account_id:str=None,dry_run:bool=True):
 q=f"select u.account_id,u.feature_key,sum(u.units) as units,max(u.unit_price) as unit_price from AI_FEATURE_HUB.TENANT_FEATURE_USAGE u where u.usage_timestamp between to_timestamp_ltz('{window_start}') and to_timestamp_ltz('{window_end}')" + (f" and u.account_id='{account_id}'" if account_id else "") + " group by u.account_id,u.feature_key"
 df=session.sql(q).collect()
 invoices=[]
 for r in df:
  acct=r['ACCOUNT_ID'];fk=r['FEATURE_KEY'];units=float(r['UNITS'] or 0);unit_price=float(r['UNIT_PRICE'] or 0)
  line_total=round(units*unit_price,6)
  invoices.append({'account_id':acct,'feature_key':fk,'units':units,'unit_price':unit_price,'line_total':line_total})
 subtotal=round(sum([i['line_total'] for i in invoices]),6)
 # baseline markup as placeholder; pricing engine should be used in prod
 markup=round(subtotal*0.085,6)
 tax=round((subtotal+markup)*0.0,6)
 total=round(subtotal+markup+tax,6)
 invoice_hash=hashlib.sha256((str(uuid.uuid4())+str(total)).encode()).hexdigest()
 out={'lines':invoices,'subtotal':subtotal,'markup':markup,'tax':tax,'total':total,'invoice_hash':invoice_hash,'dry_run':dry_run}
 if not dry_run:
  # insert invoice header, then line items, idempotent by invoice_hash
  session.sql(f"merge into AI_FEATURE_HUB.SUBSCRIPTION_INVOICES t using (select '{invoice_hash}' as invoice_id) s on t.invoice_id=s.invoice_id when not matched then insert (invoice_id,account_id,period_start,period_end,subtotal,markup,tax,total,created_at) values ('{invoice_hash}','{account_id or 'MULTI'}',to_timestamp_ltz('{window_start}'),to_timestamp_ltz('{window_end}'),{subtotal},{markup},{tax},{total},current_timestamp());").collect()
  for ln in invoices:
   # line insertion idempotent by invoice_id+feature_key
   session.sql(f"merge into AI_FEATURE_HUB.BILLING_LINE_ITEM t using (select '{invoice_hash}' as invoice_id,'{ln['feature_key']}' as feature_key) s on t.billing_run_id=s.invoice_id and t.feature_key=s.feature_key when not matched then insert (billing_run_id,account_id,feature_key,usage_qty,base_cost,line_total,created_at) values ('{invoice_hash}','{ln['account_id']}','{ln['feature_key']}',{ln['units']},{ln['unit_price']},{ln['line_total']},current_timestamp());").collect()
  # write preview/persist audit
  session.sql(f"insert into AI_FEATURE_HUB.BILLING_RUN_AUDIT (RUN_ID,INVOICE_HASH,ACCOUNT_ID,PREVIEW_JSON,CREATED_AT) values ('run-{uuid.uuid4().hex[:12]}','{invoice_hash}','{account_id or 'MULTI'}',parse_json('{json.dumps(out)}'),current_timestamp());").collect()
 return out
