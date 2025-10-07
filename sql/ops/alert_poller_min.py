importos,json,requests
from snowflake import connector
defpoll():
 c=connector.connect(user=os.getenv('SNOW_USER'),password=os.getenv('SNOW_PW'),account=os.getenv('SNOW_ACCOUNT'),warehouse=os.getenv('SNOW_WAREHOUSE'),database=os.getenv('SNOW_DB'),schema=os.getenv('SNOW_SCHEMA'))
 cur=c.cursor()
 cur.execute("select alert_id,name,details from AI_FEATURE_HUB.ALERTS where acked=false")
 for row in cur.fetchall():
  aid,name,details=row
  requests.post(os.getenv('SLACK_WEBHOOK'),json={'text':f"ALERT:{name} {details}"})
  cur2=c.cursor();cur2.execute("update AI_FEATURE_HUB.ALERTS set acked=true,acked_at=current_timestamp() where alert_id=%s",(aid,))
  cur2.close()
 cur.close();c.close()
if __name__=='__main__': poll()