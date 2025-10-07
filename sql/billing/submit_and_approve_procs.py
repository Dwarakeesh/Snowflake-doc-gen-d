from snowflake.snowpark import Session
importjson,uuid
def submit_rate_rule(session:Session,staging_json:str):
 p=json.loads(staging_json)
 stg_id=f"stg-{uuid.uuid4().hex[:12]}"
 session.sql("insert into AI_FEATURE_HUB.RATE_RULES_STAGING (STAGING_ID,FEATURE_KEY,RULE_TYPE,CONFIG,PRIORITY,SUBMITTED_BY,SUBMITTED_AT) values (%s,%s,%s,PARSE_JSON(%s),%s,%s,current_timestamp())",(stg_id,p.get('feature_key'),p.get('rule_type'),json.dumps(p.get('config')),p.get('priority',100),p.get('submitted_by','streamlit'))).collect()
 session.sql("insert into AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (AUDIT_ID,STAGING_ID,ACTION,ACTOR,ACTION_TS,DETAILS) values (%s,%s,'SUBMIT',%s,current_timestamp(),PARSE_JSON(%s))",(f"audit-{uuid.uuid4().hex[:12]}",stg_id,p.get('submitted_by','streamlit'),json.dumps(p))).collect()
 return {'status':'submitted','staging_id':stg_id}
def approve_rate_rule(session:Session,staging_id:str,approver:str):
 rows=session.sql("select feature_key,rule_type,config,priority from AI_FEATURE_HUB.RATE_RULES_STAGING where staging_id=%s and approved=false and rejected=false",(staging_id,)).collect()
 if not rows: return {'status':'not_found_or_processed','staging_id':staging_id}
 r=rows[0].as_dict()
 rule_id=f"rule-{r['FEATURE_KEY']}-{r['RULE_TYPE']}-{staging_id}"
 session.sql("merge into AI_FEATURE_HUB.RATE_RULES t using (select %s as rule_id, %s as feature_key, %s as rule_type, PARSE_JSON(%s) as config, %s as priority) s on t.rule_id=s.rule_id when matched then update set feature_key=s.feature_key,rule_type=s.rule_type,config=s.config,priority=s.priority,active=true,effective_from=current_timestamp() when not matched then insert (rule_id,feature_key,rule_type,config,priority,effective_from,active,created_at) values (s.rule_id,s.feature_key,s.rule_type,s.config,s.priority,current_timestamp(),true,current_timestamp())",(rule_id,r['FEATURE_KEY'],r['RULE_TYPE'],json.dumps(r['CONFIG']),r['PRIORITY'])).collect()
 session.sql("update AI_FEATURE_HUB.RATE_RULES_STAGING set approved=true,approved_by=%s,approved_at=current_timestamp() where staging_id=%s",(approver,staging_id)).collect()
 session.sql("insert into AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (AUDIT_ID,STAGING_ID,ACTION,ACTOR,ACTION_TS) values (%s,%s,'APPROVE',%s,current_timestamp())",(f"audit-{uuid.uuid4().hex[:12]}",staging_id,approver)).collect()
 return {'status':'approved','staging_id':staging_id,'rule_id':rule_id}
def reject_rate_rule(session:Session,staging_id:str,approver:str,reason:str):
 session.sql("update AI_FEATURE_HUB.RATE_RULES_STAGING set rejected=true,reject_reason=%s,approved_by=%s,approved_at=current_timestamp() where staging_id=%s",(reason,approver,staging_id)).collect()
 session.sql("insert into AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (AUDIT_ID,STAGING_ID,ACTION,ACTOR,ACTION_TS,DETAILS) values (%s,%s,'REJECT',%s,current_timestamp(),PARSE_JSON(%s))",(f"audit-{uuid.uuid4().hex[:12]}",staging_id,approver,json.dumps({'reason':reason}))).collect()
 return {'status':'rejected','staging_id':staging_id}