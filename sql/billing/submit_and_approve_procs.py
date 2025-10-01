from snowflake.snowpark import Session, functions as F
import json
import uuid
import datetime


def submit_rate_rule(session: Session, staging_json: str):
    """
    Submit a rate rule to staging.

    Args:
        session (Session): Snowpark session.
        staging_json (str): JSON string with keys: feature_key, rule_type, config (dict), priority, submitted_by

    Returns:
        dict: Status and generated staging_id.
    """
    payload = json.loads(staging_json)
    staging_id = f"stg-{uuid.uuid4().hex[:12]}"

    # Insert into staging table
    session.sql(f"""
        INSERT INTO AI_FEATURE_HUB.RATE_RULES_STAGING (
            STAGING_ID, FEATURE_KEY, RULE_TYPE, CONFIG, PRIORITY, SUBMITTED_BY, SUBMITTED_AT
        )
        VALUES (
            '{staging_id}',
            '{payload.get('feature_key')}',
            '{payload.get('rule_type')}',
            PARSE_JSON('{json.dumps(payload.get('config'))}'),
            {payload.get('priority', 100)},
            '{payload.get('submitted_by', 'streamlit')}',
            CURRENT_TIMESTAMP()
        );
    """).collect()

    # Insert audit
    session.sql(f"""
        INSERT INTO AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (
            AUDIT_ID, STAGING_ID, ACTION, ACTOR, ACTION_TS, DETAILS
        )
        VALUES (
            'audit-{uuid.uuid4().hex[:12]}',
            '{staging_id}',
            'SUBMIT',
            '{payload.get('submitted_by','streamlit')}',
            CURRENT_TIMESTAMP(),
            PARSE_JSON('{json.dumps(payload)}')
        );
    """).collect()

    return {"status": "submitted", "staging_id": staging_id}


def approve_rate_rule(session: Session, staging_id: str, approver: str):
    """
    Approve a rate rule in staging and merge it into RATE_RULES.

    Args:
        session (Session): Snowpark session.
        staging_id (str): Staging ID to approve.
        approver (str): User approving the rule.

    Returns:
        dict: Status, staging_id, and generated rule_id.
    """
    # Fetch staging row
    rows = session.sql(f"""
        SELECT * 
        FROM AI_FEATURE_HUB.RATE_RULES_STAGING 
        WHERE STAGING_ID = '{staging_id}' 
          AND APPROVED = FALSE 
          AND REJECTED = FALSE
    """).collect()

    if not rows:
        return {"status": "not_found_or_already_processed", "staging_id": staging_id}

    r = rows[0].as_dict()
    rule_id = f"rule-{r['FEATURE_KEY']}-{r['RULE_TYPE']}-{staging_id}"
    config_json = json.dumps(r['CONFIG'])

    # Merge into RATE_RULES table
    session.sql(f"""
        MERGE INTO AI_FEATURE_HUB.RATE_RULES tgt
        USING (
            SELECT 
                '{rule_id}' AS RULE_ID,
                '{r['FEATURE_KEY']}' AS FEATURE_KEY,
                '{r['RULE_TYPE']}' AS RULE_TYPE,
                PARSE_JSON('{config_json}') AS CONFIG,
                {r['PRIORITY']} AS PRIORITY
        ) src
        ON tgt.RULE_ID = src.RULE_ID
        WHEN MATCHED THEN UPDATE 
            SET FEATURE_KEY = src.FEATURE_KEY,
                RULE_TYPE = src.RULE_TYPE,
                CONFIG = src.CONFIG,
                PRIORITY = src.PRIORITY,
                ACTIVE = TRUE,
                EFFECTIVE_FROM = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            RULE_ID, FEATURE_KEY, RULE_TYPE, CONFIG, PRIORITY, EFFECTIVE_FROM, ACTIVE, CREATED_AT
        ) VALUES (
            src.RULE_ID, src.FEATURE_KEY, src.RULE_TYPE, src.CONFIG, src.PRIORITY, CURRENT_TIMESTAMP(), TRUE, CURRENT_TIMESTAMP()
        );
    """).collect()

    # Update staging row
    session.sql(f"""
        UPDATE AI_FEATURE_HUB.RATE_RULES_STAGING
        SET APPROVED = TRUE, APPROVED_BY = '{approver}', APPROVED_AT = CURRENT_TIMESTAMP()
        WHERE STAGING_ID = '{staging_id}';
    """).collect()

    # Insert audit
    session.sql(f"""
        INSERT INTO AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (
            AUDIT_ID, STAGING_ID, ACTION, ACTOR, ACTION_TS, DETAILS
        )
        VALUES (
            'audit-{uuid.uuid4().hex[:12]}',
            '{staging_id}',
            'APPROVE',
            '{approver}',
            CURRENT_TIMESTAMP(),
            PARSE_JSON('{json.dumps(r)}')
        );
    """).collect()

    return {"status": "approved", "staging_id": staging_id, "rule_id": rule_id}


def reject_rate_rule(session: Session, staging_id: str, approver: str, reason: str):
    """
    Reject a rate rule in staging and write audit entry.

    Args:
        session (Session): Snowpark session.
        staging_id (str): Staging ID to reject.
        approver (str): User rejecting the rule.
        reason (str): Reason for rejection.

    Returns:
        dict: Status and staging_id.
    """
    # Mark staging as rejected
    session.sql(f"""
        UPDATE AI_FEATURE_HUB.RATE_RULES_STAGING
        SET REJECTED = TRUE,
            REJECT_REASON = '{reason}',
            APPROVED_BY = '{approver}',
            APPROVED_AT = CURRENT_TIMESTAMP()
        WHERE STAGING_ID = '{staging_id}';
    """).collect()

    # Insert audit
    session.sql(f"""
        INSERT INTO AI_FEATURE_HUB.RATE_RULES_APPROVAL_AUDIT (
            AUDIT_ID, STAGING_ID, ACTION, ACTOR, ACTION_TS, DETAILS
        )
        VALUES (
            'audit-{uuid.uuid4().hex[:12]}',
            '{staging_id}',
            'REJECT',
            '{approver}',
            CURRENT_TIMESTAMP(),
            PARSE_JSON('{{"reason": "{reason}"}}')
        );
    """).collect()

    return {"status": "rejected", "staging_id": staging_id}
