from snowflake.snowpark import Session
importjson,uuid
defemit_invoice_event(session:Session,invoice_id,event_type,details): eid=str(uuid.uuid4()); session.sql("insert into AI_FEATURE_HUB.INVOICE_EVENTS(event_id,invoice_id,event_type,details) values(%s,%s,%s,parse_json(%s))",(eid,invoice_id,event_type,json.dumps(details))).collect(); return{'event_id':eid}