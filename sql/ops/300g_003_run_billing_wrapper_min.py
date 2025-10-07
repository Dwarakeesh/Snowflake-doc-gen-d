from snowflake.snowpark import Session
importjson
defrun_billing_wrapper(session:Session,period_start,period_end,account_id=None,dry_run=True):
 res=session.sql("select * from AI_FEATURE_HUB.RATE_CARD").collect()
 # placeholder: billing logic implemented in run_billing_run SP
 session.call('AI_FEATURE_HUB.RUN_BILLING_RUN',period_start,period_end,account_id,dry_run)
 return{'status':'started'}