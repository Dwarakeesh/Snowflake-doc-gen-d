from snowflake.snowpark import Session
import uuid
def apply_adjustment(session:Session,invoice_id:str,amount:float,reason:str):
    adjustment_id = str(uuid.uuid4())
    session.sql("INSERT INTO AI_FEATURE_HUB.BILLING_ADJUSTMENTS(ADJUSTMENT_ID, INVOICE_ID, AMOUNT, REASON, CREATED_AT) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())", (adjustment_id, invoice_id, amount, reason)).collect()
    # Recalculate invoice total
    session.sql("UPDATE AI_FEATURE_HUB.SUBSCRIPTION_INVOICES SET TOTAL = TOTAL + %s WHERE INVOICE_ID = %s", (amount, invoice_id)).collect()
    return {"adjustment_id": adjustment_id}
