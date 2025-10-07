fromsnowflake.snowpark import Session
importhmac,hashlib,json
defverify_and_enqueue(session:Session,payload,signature,secret):
 computed=hmac.new(secret.encode(),json.dumps(payload).encode(),hashlib.sha256).hexdigest()
 if computed!=signature:
  raise Exception('invalidsignature')
 session.sql("insert into AI_FEATURE_HUB.WEBHOOK_QUEUE(webhook_id,payload,status) values(LEFT(GENERATOR_RANDOM_STRING(),36),parse_json(%s),'PENDING')",(json.dumps(payload),)).collect()
 return{'enqueued':True}