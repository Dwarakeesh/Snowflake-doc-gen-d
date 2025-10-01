from snowflake.snowpark import Session, functions as F
import hashlib
import json
import uuid
import datetime


def run_billing_enhanced(
    session: Session,
    window_start: str,
    window_end: str,
    account_id: str = None,
    dry_run: bool = True
):
    """
    Generate billing summary for a given account or all accounts within a specified time window.
    Inserts billing header, line items, and audit if not dry_run.

    Args:
        session (Session): Snowpark session.
        window_start (str): Start timestamp in ISO format.
        window_end (str): End timestamp in ISO format.
        account_id (str, optional): Specific account ID. None for all accounts.
        dry_run (bool): If True, only compute without persisting.

    Returns:
        dict: Invoice summary including line items, totals, and invoice hash.
    """

    # Construct SQL query to aggregate usage
    account_filter = f"AND u.account_id='{account_id}'" if account_id else ""
    q = f"""
        SELECT 
            u.account_id,
            u.feature_key,
            SUM(u.units) AS units,
            MAX(u.unit_price) AS unit_price
        FROM AI_FEATURE_HUB.TENANT_FEATURE_USAGE u
        WHERE u.usage_timestamp BETWEEN TO_TIMESTAMP_LTZ('{window_start}') AND TO_TIMESTAMP_LTZ('{window_end}')
        {account_filter}
        GROUP BY u.account_id, u.feature_key
    """

    # Execute query
    rows = session.sql(q).collect()  # list of Row objects

    # Generate invoice lines
    invoices = []
    for r in rows:
        acct = r['ACCOUNT_ID']
        fk = r['FEATURE_KEY']
        units = float(r['UNITS'] or 0)
        unit_price = float(r['UNIT_PRICE'] or 0)
        line_total = round(units * unit_price, 6)
        invoices.append({
            'account_id': acct,
            'feature_key': fk,
            'units': units,
            'unit_price': unit_price,
            'line_total': line_total
        })

    # Calculate totals
    subtotal = round(sum([i['line_total'] for i in invoices]), 6)
    markup = round(subtotal * 0.085, 6)
    tax = round((subtotal + markup) * 0.0, 6)
    total = round(subtotal + markup + tax, 6)

    # Generate unique invoice hash
    invoice_hash = hashlib.sha256((str(uuid.uuid4()) + str(total)).encode()).hexdigest()

    # Prepare output
    out = {
        'lines': invoices,
        'subtotal': subtotal,
        'markup': markup,
        'tax': tax,
        'total': total,
        'invoice_hash': invoice_hash,
        'dry_run': dry_run
    }

    if not dry_run:
        # Insert invoice header, idempotent by invoice_hash
        session.sql(f"""
            MERGE INTO AI_FEATURE_HUB.SUBSCRIPTION_INVOICES t
            USING (SELECT '{invoice_hash}' AS invoice_id) s
            ON t.invoice_id = s.invoice_id
            WHEN NOT MATCHED THEN
            INSERT (
                invoice_id,
                account_id,
                period_start,
                period_end,
                subtotal,
                markup,
                tax,
                total,
                created_at
            ) VALUES (
                '{invoice_hash}',
                '{account_id or 'MULTI'}',
                TO_TIMESTAMP_LTZ('{window_start}'),
                TO_TIMESTAMP_LTZ('{window_end}'),
                {subtotal},
                {markup},
                {tax},
                {total},
                CURRENT_TIMESTAMP()
            );
        """).collect()

        # Insert line items, idempotent by invoice_id + feature_key
        for ln in invoices:
            session.sql(f"""
                MERGE INTO AI_FEATURE_HUB.BILLING_LINE_ITEM t
                USING (
                    SELECT '{invoice_hash}' AS invoice_id,
                           '{ln['feature_key']}' AS feature_key
                ) s
                ON t.billing_run_id = s.invoice_id AND t.feature_key = s.feature_key
                WHEN NOT MATCHED THEN
                INSERT (
                    billing_run_id,
                    account_id,
                    feature_key,
                    usage_qty,
                    base_cost,
                    line_total,
                    created_at
                ) VALUES (
                    '{invoice_hash}',
                    '{ln['account_id']}',
                    '{ln['feature_key']}',
                    {ln['units']},
                    {ln['unit_price']},
                    {ln['line_total']},
                    CURRENT_TIMESTAMP()
                );
            """).collect()

        # Write audit/preview for tracking
        session.sql(f"""
            INSERT INTO AI_FEATURE_HUB.BILLING_RUN_AUDIT(
                RUN_ID,
                INVOICE_HASH,
                ACCOUNT_ID,
                PREVIEW_JSON,
                CREATED_AT
            )
            VALUES (
                'run-{uuid.uuid4().hex[:12]}',
                '{invoice_hash}',
                '{account_id or 'MULTI'}',
                PARSE_JSON('{json.dumps(out)}'),
                CURRENT_TIMESTAMP()
            );
        """).collect()

    return out
