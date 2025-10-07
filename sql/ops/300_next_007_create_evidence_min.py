from snowflake.snowpark import Session
importjson,hashlib,uuid

def create_evidence(session:Session,invoice_id:str):
    rows=session.sql("select i.invoice_id,i.account_id,i.subtotal,i.markup,i.tax,i.total,array_agg(object_construct('feature',l.feature_key,'line',l.line_total)) lines from AI_FEATURE_HUB.SUBSCRIPTION_INVOICES i left join AI_FEATURE_HUB.BILLING_LINE_ITEM l on i.invoice_id=l.invoice_id where i.invoice_id=%s group by i.invoice_id,i.account_id,i.subtotal,i.markup,i.tax,i.total",(invoice_id,)).collect()
    if not rows:
        return{'error':'not_found'}
    r=rows[0].as_dict()
    bundle={'invoice':r,'audit':[]}
    audits=session.sql("select * from AI_FEATURE_HUB.BILLING_AUDIT where invoice_id=%s order by created_at",(invoice_id,)).collect()
    bundle['audit']=[a.as_dict() for a in audits]
    bundle_json=json.dumps(bundle)
    bundle_hash=hashlib.sha256(bundle_json.encode()).hexdigest()
    bid=f"bundle-{uuid.uuid4().hex[:12]}"
    session.sql("insert into AI_FEATURE_HUB.EVIDENCE_BUNDLES(bundle_id,invoice_hash,bundle_json,bundle_hash) values(%s,%s,parse_json(%s),%s)",(bid,invoice_id,bundle_json,bundle_hash)).collect()
    return{'bundle_id':bid,'bundle_hash':bundle_hash}
