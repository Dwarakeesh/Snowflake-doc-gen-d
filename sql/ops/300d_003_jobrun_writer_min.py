from snowflake.snowpark import Session
importjson,uuid
defwrite_jobrun(session:Session,name,status,details=None): jid=str(uuid.uuid4()); session.sql("insert into AI_FEATURE_HUB.JOBRUNS(jobrun_id,job_name,status,started_at,details) values(%s,%s,%s,current_timestamp(),parse_json(%s))",(jid,name,status,json.dumps(details or {}))).collect(); return{'jobrun_id':jid}