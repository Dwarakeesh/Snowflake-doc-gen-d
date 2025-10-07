from snowflake.snowpark import Session
import uuid, datetime

def emit_alert(session: Session, name, details, level='INFO'):
    aid = str(uuid.uuid4())
    session.sql("""INSERT INTO AI_FEATURE_HUB.ALERTS(ALERT_ID, NAME, DETAILS, LEVEL) 
                  VALUES (%s, %s, %s, %s)""", (aid, name, details, level)).collect()
    return {'alert_id': aid}
