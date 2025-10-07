importjson,os
defprepare_faiss_export(session,stage_path='/tmp/embeddings_export.json'):
 rows=session.sql("select emb_id,document_id,embedding,model_id from AI_FEATURE_HUB.DOCUMENT_EMBEDDINGS").collect()
 out=[{'id':r[0],'doc':r[1],'vec':r[2]}for r in rows]
 open(stage_path,'w').write(json.dumps(out))
 return{'path':stage_path,'count':len(out)}