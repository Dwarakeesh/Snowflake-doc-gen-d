CREATE OR REPLACE PROCEDURE AI_FEATURE_HUB.CHARGE_LATE_FEE(account_id STRING, amount NUMBER, currency STRING)
    RETURNS VARIANT
    LANGUAGE SQL
AS
$$
    BEGIN
        CALL AI_FEATURE_HUB.POST_BILLING_EVENT(:account_id, 'late_fee', :amount, :currency, null);
        RETURN 'ok';
    END;
$$;