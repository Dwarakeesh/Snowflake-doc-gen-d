from snowflake.snowpark import Session
importjson,requests,os
defnotify(session:Session,invoice_id,summary): # middleware-style call to external notifier; actual network call performed outside Snowflake in production return {'notified':True,'invoice':invoice_id}