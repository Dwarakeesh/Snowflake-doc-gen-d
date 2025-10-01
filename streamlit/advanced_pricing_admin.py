
import os
import streamlit as st
import pandas as pd
import json
import uuid
import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Config / env
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PW = os.getenv("SNOW_PW")
SNOW_ROLE = os.getenv("SNOW_ROLE", "SYSADMIN")
SNOW_WAREHOUSE = os.getenv("SNOW_WAREHOUSE", "COMPUTE_WH")
SNOW_DB = os.getenv("SNOW_DB", "AI_PLATFORM")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA", "AI_FEATURE_HUB")

def get_conn():
    return snowflake.connector.connect(
        account=SNOW_ACCOUNT,
        user=SNOW_USER,
        password=SNOW_PW,
        role=SNOW_ROLE,
        warehouse=SNOW_WAREHOUSE,
        database=SNOW_DB,
        schema=SNOW_SCHEMA,
        client_session_keep_alive=True
    )

def fetch_df(sql):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if cols:
        return pd.DataFrame(rows, columns=cols)
    return pd.DataFrame()

def upsert_rule(rule_id, feature_key, rule_type, config_json, priority=100, effective_from=None, effective_to=None, active=True):
    conn = get_conn()
    cur = conn.cursor()
    ef = effective_from or datetime.datetime.utcnow().isoformat()
    et = effective_to
    cur.execute("""
        MERGE INTO AI_FEATURE_HUB.RATE_RULES t
        USING (SELECT %s AS RULE_ID) s
        ON t.RULE_ID = s.RULE_ID
        WHEN MATCHED THEN UPDATE SET FEATURE_KEY=%s, RULE_TYPE=%s, CONFIG=%s, PRIORITY=%s, EFFECTIVE_FROM=TO_TIMESTAMP_LTZ(%s), EFFECTIVE_TO=%s, ACTIVE=%s, CREATED_AT=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (RULE_ID, FEATURE_KEY, RULE_TYPE, CONFIG, PRIORITY, EFFECTIVE_FROM, EFFECTIVE_TO, ACTIVE, CREATED_AT)
            VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP_LTZ(%s), %s, %s, CURRENT_TIMESTAMP());
    """, (rule_id, feature_key, rule_type, json.dumps(config_json), priority, ef, et,
          rule_id, feature_key, rule_type, json.dumps(config_json), priority, ef, et, active))
    cur.close()
    conn.close()

def insert_template(template_id, name, description, config_sample):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO AI_FEATURE_HUB.RATE_RULE_TEMPLATES (TEMPLATE_ID, NAME, DESCRIPTION, CONFIG_SAMPLE, CREATED_AT)
        VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP())
    """, (template_id, name, description, json.dumps(config_sample)))
    cur.close()
    conn.close()

st.set_page_config(page_title="Advanced Pricing Admin", layout="wide")
st.title("Advanced Pricing Admin (Rate Rules & Templates)")

# Left column: existing templates and rules
col1, col2 = st.columns([1,2])

with col1:
    st.subheader("Existing Rule Templates")
    df_templates = fetch_df("SELECT TEMPLATE_ID, NAME, DESCRIPTION, CONFIG_SAMPLE, CREATED_AT FROM AI_FEATURE_HUB.RATE_RULE_TEMPLATES ORDER BY CREATED_AT DESC LIMIT 100")
    if not df_templates.empty:
        df_templates_display = df_templates.copy()
        df_templates_display['CONFIG_SAMPLE'] = df_templates_display['CONFIG_SAMPLE'].apply(lambda j: json.dumps(j)[:200] + ("..." if len(json.dumps(j))>200 else ""))
        st.dataframe(df_templates_display)
    else:
        st.info("No templates found. Create a template below.")

    st.markdown("---")
    st.subheader("Active Rate Rules")
    df_rules = fetch_df("SELECT RULE_ID, FEATURE_KEY, RULE_TYPE, CONFIG, PRIORITY, EFFECTIVE_FROM, EFFECTIVE_TO, ACTIVE FROM AI_FEATURE_HUB.RATE_RULES WHERE ACTIVE = TRUE ORDER BY PRIORITY, EFFECTIVE_FROM DESC LIMIT 200")
    if not df_rules.empty:
        df_rules_display = df_rules.copy()
        df_rules_display['CONFIG'] = df_rules_display['CONFIG'].apply(lambda j: json.dumps(j)[:200] + ("..." if len(json.dumps(j))>200 else ""))
        st.dataframe(df_rules_display)
    else:
        st.info("No active rules found.")

with col2:
    st.subheader("Create / Edit Rate Rule")
    with st.form("rule_form"):
        feature_key = st.text_input("Feature Key", value="nlp_search_v1")
        rule_type = st.selectbox("Rule Type", ["TIER", "CAP", "MINIMUM", "DISCOUNT", "TAX", "FLAT"])
        priority = st.number_input("Priority (lower = run earlier)", min_value=0, max_value=1000, value=10)
        eff_from = st.date_input("Effective From", value=datetime.date.today())
        eff_to_raw = st.text_input("Effective To (leave blank for open)", value="")
        eff_to = eff_to_raw if eff_to_raw else None
        active = st.checkbox("Active", value=True)

        # Dynamic config editor per rule type
        config = {}
        if rule_type == "TIER":
            st.markdown("Tiers: define % or unit prices per cumulative 'upto' threshold. Leave 'upto' blank for overflow tier.")
            tiers = []
            st.markdown("Add tier rows (click to append).")
            # Use session_state to accumulate tiers
            if 'tiers' not in st.session_state:
                st.session_state['tiers'] = [{"upto": 100, "unit_price": 0.02}, {"upto": 1000, "unit_price": 0.012}, {"upto": None, "unit_price": 0.008}]
            # editable DataFrame approach
            df_t = pd.DataFrame(st.session_state['tiers'])
            edited = st.experimental_data_editor(df_t, num_rows="dynamic")
            tiers = edited.to_dict(orient='records')
            config['tiers'] = tiers
        elif rule_type == "CAP":
            cap_val = st.number_input("Cap value (currency)", value=500.0, format="%.6f")
            config['cap_value'] = cap_val
        elif rule_type == "MINIMUM":
            min_val = st.number_input("Minimum charge value", value=0.0, format="%.6f")
            config['min_value'] = min_val
        elif rule_type == "DISCOUNT":
            discount_percent = st.number_input("Discount percent (0-100)", min_value=0.0, max_value=100.0, value=0.0)
            discount_flat = st.number_input("Discount flat amount", value=0.0, format="%.6f")
            config['percent'] = discount_percent
            config['flat'] = discount_flat
        elif rule_type == "TAX":
            tax_pct = st.number_input("Tax percent", value=0.0, format="%.6f")
            jurisdiction = st.text_input("Tax jurisdiction code", value="default")
            config['tax_pct'] = tax_pct
            config['jurisdiction'] = jurisdiction
        elif rule_type == "FLAT":
            flat_amount = st.number_input("Flat charge amount", value=0.0, format="%.6f")
            config['amount'] = flat_amount

        st.markdown("Preview rule JSON")
        st.json({"feature_key": feature_key, "rule_type": rule_type, "priority": priority, "config": config, "effective_from": str(eff_from), "effective_to": eff_to, "active": active})

        submit = st.form_submit_button("Save Rule")
        if submit:
            new_rule_id = f"rule-{feature_key}-{rule_type.lower()}-{uuid.uuid4().hex[:8]}"
            upsert_rule(new_rule_id, feature_key, rule_type, config, priority, eff_from.isoformat(), eff_to, active)
            st.success(f"Saved rule {new_rule_id}")
            st.experimental_rerun()

    st.markdown("---")
    st.subheader("Create Rate Rule Template")
    with st.form("template_form"):
        t_name = st.text_input("Template name")
        t_desc = st.text_area("Description")
        st.markdown("Create a sample config for the template (JSON schema). Example for a tier template:")
        sample_cfg = st.text_area("Sample Config JSON", value=json.dumps({"tiers":[{"upto":100,"unit_price":0.02},{"upto":1000,"unit_price":0.012},{"upto":None,"unit_price":0.008}]}, indent=2))
        save_template = st.form_submit_button("Save Template")
        if save_template:
            try:
                parsed = json.loads(sample_cfg)
                tid = f"tmpl-{t_name}-{uuid.uuid4().hex[:6]}"
                insert_template(tid, t_name, t_desc, parsed)
                st.success(f"Saved template {tid}")
                st.experimental_rerun()
            except Exception as e:
                st.error("Invalid JSON: " + str(e))

st.markdown("**Notes:** This UI writes RATE_RULES and RATE_RULE_TEMPLATES directly to Snowflake so the billing stored-proc reads them at runtime. Consider adding RBAC / SSO and an approval workflow before enabling highly impactful rules in production.")

