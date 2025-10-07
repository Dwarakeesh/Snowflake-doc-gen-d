from snowflake.snowpark import Session
importjson,uuid
def ingest_usage_task(session:Session,limit:int=1000):
 rows=session.sql("select t.$1 from @AI_FEATURE_HUB.USAGE_STAGE (FILE_FORMAT=> 'AI_FEATURE_HUB.JSON_COMPRESSED') t limit {}".format(limit)).collect()
 inserted=0
 for r in rows:
  j=json.loads(r[0])
  uid=j.get('id') or str(uuid.uuid4())
  session.sql("merge into AI_FEATURE_HUB.TENANT_FEATURE_USAGE tgt using (select '{0}' as usage_id) src on tgt.usage_id=src.usage_id when not matched then insert (usage_id,account_id,feature_key,units,unit_price,usage_timestamp,metadata) values ('{0}','{1}','{2}',{3},{4},to_timestamp_ltz('{5}'),parse_json('{6}'))".format(uid,j.get('account'),j.get('feature'),j.get('units',0),j.get('unit_price',0),j.get('ts'),json.dumps(j.get('meta',{})))).collect();inserted+=1
 return{'inserted':inserted}