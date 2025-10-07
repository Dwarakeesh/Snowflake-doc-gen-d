# advanced_pricing_admin_staging.py
# Streamlit app variant: submits proposed rules to RATE_RULES_STAGING via stored-proc
import os, uuid, json, streamlit as st, snowflake.connector
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PW = os.getenv("SNOW_PW")
SNOW_WAREHOUSE = os.getenv("SNOW_WAREHOUSE","COMPUTE_WH")
SNOW_DB = os.getenv("SNOW_DB","AI_PLATFORM")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA","AI_FEATURE_HUB")
def get_conn(): return snowflake.connector.connect(account=SNOW_ACCOUNT,user=SNOW_USER,password=SNOW_PW,warehouse=SNOW_WAREHOUSE,database=SNOW_DB,schema=SNOW_SCHEMA)
def call_proc_submit(staging_payload_json): conn = get_conn(); cur = conn.cursor(); cur.execute("CALL AI_FEATURE_HUB.SUBMIT_RATE_RULE_TO_STAGING(%s)", (staging_payload_json,)); res = cur.fetchone(); cur.close(); conn.close(); return res
def call_proc_approve(staging_id, approver): conn = get_conn(); cur = conn.cursor(); cur.execute("CALL AI_FEATURE_HUB.APPROVE_RATE_RULE(%s,%s)", (staging_id, approver)); res = cur.fetchone(); cur.close(); conn.close(); return res
st.title("Pricing Rule Admin — Staging & Approval Mode")
st.info("Use this UI to create rule proposals; a reviewer must approve them before they become active.")
with st.form("proposal"):
    feature_key = st.text_input("Feature key","nlp_search_v1")
    rule_type = st.selectbox("Rule type", ["TIER","CAP","MINIMUM","DISCOUNT","TAX","FLAT"])
    priority = st.number_input("Priority", value=10, min_value=0)
    # simple config editor as JSON for staging (business can paste JSON or use a helper)
    config_text = st.text_area("Rule config (JSON)", value=json.dumps({"tiers":[{"upto":100,"unit_price":0.02},{"upto":1000,"unit_price":0.012},{"upto":None,"unit_price":0.008}]}, indent=2), height=200)
    submitted_by = st.text_input("Submitted by (user id/email)", value="biz_user@example.com")
    submitted = st.form_submit_button("Submit proposal to staging")
    if submitted:
        try:
            config_obj = json.loads(config_text)
            payload = {"feature_key": feature_key, "rule_type": rule_type, "config": config_obj, "priority": priority, "submitted_by": submitted_by}
            res = call_proc_submit(json.dumps(payload))
            st.success(f"Submitted to staging: {res}")
        except Exception as e:
            st.error("Invalid JSON or submission error: " + str(e))
st.markdown("---")
st.subheader("Pending proposals (staging)")
# show staging
conn = get_conn()
df = None
try:
    cur = conn.cursor()
    cur.execute("SELECT STAGING_ID, FEATURE_KEY, RULE_TYPE, CONFIG, PRIORITY, SUBMITTED_BY, SUBMITTED_AT FROM AI_FEATURE_HUB.RATE_RULES_STAGING WHERE APPROVED = FALSE AND REJECTED = FALSE ORDER BY SUBMITTED_AT DESC LIMIT 200")
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []
    import pandas as pd
    df = pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame()
    cur.close()
finally:
    conn.close()
if df is None or df.empty:
    st.info("No pending proposals.")
else:
    st.dataframe(df)
    with st.form("approve_form"):
        staging_id_to_approve = st.text_input("Staging ID to approve")
        approver = st.text_input("Approver (user id/email)")
        approve = st.form_submit_button("Approve selected staging")
        if approve:
            try:
                res = call_proc_approve(staging_id_to_approve, approver)
                st.success(f"Approve result: {res}")
            except Exception as e:
                st.error("Approve error: " + str(e))
