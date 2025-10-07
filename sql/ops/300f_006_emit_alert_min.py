from snowflake.snowpark import Session
importjson,uuid
defemit_alert(session:Session,name,details,level='WARN'):aid=str(uuid.uuid4());session.sql("insert into AI_FEATURE_HUB.ALERTS(alert_id,name,details,level) values(%s,%s,parse_json(%s),%s)",(aid,name,json.dumps(details),level)).collect();return{'alert_id':aid};