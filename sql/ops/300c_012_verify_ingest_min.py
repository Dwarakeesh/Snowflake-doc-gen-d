from snowflake import connector,errors,sqlalchemy
importos
defcheck(): conn=connector.connect(user=os.getenv('SNOW_USER'),password=os.getenv('SNOW_PW'),account=os.getenv('SNOW_ACCOUNT'),warehouse=os.getenv('SNOW_WAREHOUSE'),database=os.getenv('SNOW_DB'),schema=os.getenv('SNOW_SCHEMA')) cur=conn.cursor(); cur.execute("select count(*) from AI_FEATURE_HUB.USAGE_RAW");print('rawcount',cur.fetchone()[0]);cur.close();conn.close()
if __name__=='__main__': check()