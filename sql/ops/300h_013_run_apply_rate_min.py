from snowflake.snowpark import Session
defrun_apply_rate(session:Session,billing_date): session.call('AI_FEATURE_HUB.RUN_APPLY_RATE',billing_date); return{'started':True}