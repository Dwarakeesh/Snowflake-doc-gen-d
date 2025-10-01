from snowflake.snowpark import Session, functions as F
import json
import uuid


def persist_billing_runs(session: Session, older_than_minutes: int = 60, preview_only: bool = True):
    """
    Persist billing runs from the billing preview table into subscription invoices.

    Args:
        session (Session): Snowpark session.
        older_than_minutes (int): Only consider preview rows older than this many minutes.
        preview_only (bool): If True, do not persist, just preview.

    Returns:
        dict: Summary containing number of persisted rows and whether preview_only mode was used.
    """

    # Select rows from billing preview that are older than the specified threshold
    query = f"""
        SELECT *
        FROM AI_FEATURE_HUB.BILLING_PREVIEW
        WHERE PREVIEW_TS <= DATEADD('minute', -{older_than_minutes}, CURRENT_TIMESTAMP())
    """
    rows = session.sql(query).collect()

    persisted = []

    for r in rows:
        # Parse the preview JSON
        data = json.loads(r['PREVIEW_JSON'])

        # Insert into subscription invoices if not preview only
        if not preview_only:
            insert_sql = f"""
                INSERT INTO AI_FEATURE_HUB.SUBSCRIPTION_INVOICES(
                    INVOICE_ID, ACCOUNT_ID, RUN_TS, SUBTOTAL, MARKUP, TAX, TOTAL, INVOICE_HASH
                ) VALUES (
                    '{data['invoice_id']}',
                    '{data['account_id']}',
                    CURRENT_TIMESTAMP(),
                    {data['subtotal']},
                    {data['markup']},
                    {data['tax']},
                    {data['total']},
                    '{data['invoice_hash']}'
                );
            """
            session.sql(insert_sql).collect()

        # Keep track of persisted or previewed rows
        persisted.append(r['PREVIEW_ID'])

    return {
        'persisted': len(persisted),
        'preview_only': preview_only
    }
