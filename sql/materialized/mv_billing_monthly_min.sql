-- Create materialized view for monthly billing summary
CREATE OR REPLACE MATERIALIZED VIEW AI_FEATURE_HUB.MV_BILLING_MONTHLY AS
SELECT
    account_id,
    DATE_TRUNC('month', created_at) AS month,
    SUM(total) AS month_total,
    COUNT(DISTINCT invoice_id) AS invoice_count
FROM
    AI_FEATURE_HUB.SUBSCRIPTION_INVOICES
GROUP BY
    account_id,
    DATE_TRUNC('month', created_at);
