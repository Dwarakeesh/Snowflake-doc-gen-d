from snowflake.snowpark import Session
importjson,uuid
defingest_raw(session:Session,payload): rid=str(uuid.uuid4()); session.sql("insert into AI_FEATURE_HUB.USAGE_RAW(raw_id,account_id,raw_payload) values(%s,%s,parse_json(%s))",(rid,payload.get('account_id','anon'),json.dumps(payload))).collect(); return{'raw_id':rid}