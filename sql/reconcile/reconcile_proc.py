from snowflake.snowpark import Session, functions as F
import json
import uuid


def reconcile_billing_vs_usage(
    session: Session,
    account_id: str = None,
    window_start: str = None,
    window_end: str = None
):
    """
    Reconcile billing line items against tenant feature usage and log mismatches.

    Args:
        session (Session): Snowpark session.
        account_id (str, optional): Specific account to reconcile. Defaults to None.
        window_start (str, optional): Start timestamp for usage filter. Defaults to None.
        window_end (str, optional): End timestamp for usage filter. Defaults to None.

    Returns:
        dict: Number of reconciliation issues found.
    """

    # Build conditional WHERE clause
    cond = []
    if account_id:
        cond.append(f"account_id='{account_id}'")
    if window_start and window_end:
        cond.append(
            f"usage_timestamp BETWEEN TO_TIMESTAMP_LTZ('{window_start}') "
            f"AND TO_TIMESTAMP_LTZ('{window_end}')"
        )
    where = ' AND '.join(cond) if cond else '1=1'

    # Fetch usage and billing aggregates
    usage = session.sql(
        f"""
        SELECT account_id, feature_key, SUM(units) AS units
        FROM AI_FEATURE_HUB.TENANT_FEATURE_USAGE
        WHERE {where}
        GROUP BY account_id, feature_key
        """
    ).collect()

    billing = session.sql(
        f"""
        SELECT account_id, feature_key, SUM(line_total) AS billed
        FROM AI_FEATURE_HUB.BILLING_LINE_ITEM
        WHERE {where}
        GROUP BY account_id, feature_key
        """
    ).collect()

    # Map usage and billing for easy comparison
    u_map = {(r['ACCOUNT_ID'], r['FEATURE_KEY']): r['UNITS'] for r in usage}
    b_map = {(r['ACCOUNT_ID'], r['FEATURE_KEY']): r['BILLED'] for r in billing}

    # Identify discrepancies
    issues = []
    keys = set(list(u_map.keys()) + list(b_map.keys()))
    for k in keys:
        u = u_map.get(k, 0)
        b = b_map.get(k, 0)
        if abs(u - b) > 0.01:
            issues.append({
                'account_id': k[0],
                'feature_key': k[1],
                'usage': u,
                'billed': b,
                'diff': u - b
            })

    # Insert reconciliation issues into audit table
    if issues:
        for i in issues:
            insert_sql = f"""
                INSERT INTO AI_FEATURE_HUB.RECONCILIATION_AUDIT(
                    AUDIT_ID, ACCOUNT_ID, FEATURE_KEY, USAGE_SUM, BILL_SUM, MISMATCH, RUN_AT
                )
                VALUES (
                    '{uuid.uuid4().hex}',
                    '{i['account_id']}',
                    '{i['feature_key']}',
                    {i['usage']},
                    {i['billed']},
                    {i['diff']},
                    CURRENT_TIMESTAMP()
                )
            """
            session.sql(insert_sql).collect()

    return {'issues': len(issues)}
