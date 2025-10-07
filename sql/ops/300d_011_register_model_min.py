from snowflake.snowpark import Session
importjson,uuid
defregister_model(session:Session,key,version,meta=None): mid=str(uuid.uuid4()); session.sql("insert into AI_FEATURE_HUB.MODEL_REGISTRY(model_id,model_key,version,deployed_at,is_active,meta) values(%s,%s,%s,current_timestamp(),true,parse_json(%s))",(mid,key,version,json.dumps(meta or {}))).collect(); return{'model_id':mid}