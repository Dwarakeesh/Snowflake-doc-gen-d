from snowflake.snowpark import Session
importjson,uuid

def run_billing(session:Session,run_start:str,run_end:str,preview:bool=True):
    q="select account_id,feature_key,sum(units) units,sum(units*unit_price) cost from AI_FEATURE_HUB.TENANT_FEATURE_USAGE where usage_timestamp between to_timestamp_ltz(%s) and to_timestamp_ltz(%s) group by account_id,feature_key"
    rows=session.sql(q,(run_start,run_end)).collect()
    lines=[]
    for r in rows:
        acct=r[0];feat=r[1];units=r[2];cost=r[3]
        markup=0.10
        line_total=round(cost*(1+markup),6)
        lines.append({'account_id':acct,'feature_key':feat,'units':units,'line_total':line_total})
    if preview:
        return{'preview':True,'lines':lines}
    invoices={}
    for l in lines:
        acct=l['account_id'];invoices.setdefault(acct,[]).append(l)
    results=[]
    for acct,ls in invoices.items():
        inv_id=str(uuid.uuid4())
        subtotal=sum([round(x['line_total']/(1.0+0.10),6) for x in ls])
        total=sum([x['line_total'] for x in ls])
        session.sql("insert into AI_FEATURE_HUB.SUBSCRIPTION_INVOICES(invoice_id,account_id,subtotal,markup,tax,total,currency) values(%s,%s,%s,%s,%s,%s,%s)",(inv_id,acct,subtotal,0.10,0,total,'USD')).collect()
        for x in ls:
            session.sql("insert into AI_FEATURE_HUB.BILLING_LINE_ITEM(line_id,billing_run_id,invoice_id,account_id,feature_key,units,unit_price,line_total) values(%s,%s,%s,%s,%s,%s,%s,%s)",(str(uuid.uuid4()),inv_id,inv_id,acct,x['feature_key'],x['units'],round((x['line_total']/x['units']) if x['units']>0 else 0,9),x['line_total'])).collect()
        results.append({'invoice_id':inv_id,'account':acct,'lines':len(ls)})
    return{'status':'ok','invoices':results}
