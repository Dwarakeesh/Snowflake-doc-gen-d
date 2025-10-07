from snowflake.snowpark import Session
importjson,uuid

def log_model(session:Session,req):
    pid=str(uuid.uuid4())
    session.sql("insert into AI_FEATURE_HUB.INFERENCE_PROVENANCE(prov_id,request_id,model_id,input,output,tokens,confidence) values(%s,%s,%s,parse_json(%s),parse_json(%s),%s,%s)",(pid,req.get('request_id'),req.get('model_id'),json.dumps(req.get('input')),json.dumps(req.get('output')),req.get('tokens',0),req.get('confidence',None))).collect()
    return{'prov_id':pid}
