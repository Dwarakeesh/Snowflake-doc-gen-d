from snowflake.snowpark import Session
import uuid
def process_payment(session:Session,invoice_id:str,amount:float,currency:str,payment_method:str,transaction_id:str):
    payment_id = str(uuid.uuid4())
    # Faking payment processing status
    status = "SUCCESS"
    session.sql("INSERT INTO AI_FEATURE_HUB.INVOICE_PAYMENTS(PAYMENT_ID, INVOICE_ID, AMOUNT, CURRENCY, STATUS, TRANSACTION_ID, PAYMENT_METHOD, PAID_AT) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())", (payment_id, invoice_id, amount, currency, status, transaction_id, payment_method)).collect()
    return {"payment_id": payment_id, "status": status}
