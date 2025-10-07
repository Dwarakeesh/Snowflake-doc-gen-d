# FAISS index snapshot loader (container entrypoint)
importos,json,faiss,boto3,io,sys
defload_index_from_s3(s3uri,local_path='/tmp/index.faiss'):
 s3=boto3.client('s3')
 parts=s3uri.replace('s3://','').split('/',1)
 bucket=parts[0];key=parts[1]
 obj=s3.get_object(Bucket=bucket,Key=key)
 with open(local_path,'wb') as f: f.write(obj['Body'].read())
 return local_path
defmain():
 s3uri=os.environ.get('INDEX_S3_URI')
 idxpath=load_index_from_s3(s3uri)
 idx=faiss.read_index(idxpath)
 print(json.dumps({'status':'loaded','index_path':idxpath}))
if__name__=='__main__':
 main()