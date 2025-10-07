fromsnowflake.snowpark import Session
importjson,os
defexport_prompts(session:Session,outdir='/tmp'):
 rows=session.sql("selectprompt_id,model_id,input_prompt,response,confidence,created_at from AI_FEATURE_HUB.PROMPT_LOG").collect()
 out=[dict(r.as_dict()) for r in rows]
 path=outdir+"/prompts_export.json"
 open(path,'w').write(json.dumps(out,default=str))
 return{'path':path,'count':len(out)}