from snowflake.snowpark import Session
import json,uuid
def write_audit(session:Session,invoice_id:str,event_type:str,details:dict): aid=str(uuid.uuid4()); session.sql("insert into AI_FEATURE_HUB.BILLING_AUDIT(audit_id,invoice_id,event_type,details) values(%s,%s,%s,parse_json(%s))",(aid,invoice_id,event_type,json.dumps(details))).collect(); return{'audit_id':aid}