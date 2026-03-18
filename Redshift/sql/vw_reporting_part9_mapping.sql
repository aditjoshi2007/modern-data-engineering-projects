
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_loans_and_balances
-- Purpose   : Provide contract-level outbound metrics covering loans
--             and key balance types, normalized for downstream analytics
--             and Salesforce consumption.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures all metrics are sourced from the
--    most recent effective and load dates.
-- 2. UNION ALL is used to normalize heterogeneous metrics (loans,
--     balances) into a single structure.
-- 3. Business-line normalization (LOWER) ensures consistent filtering
--    across downstream systems.
-- 4. PEP (plan group) logic prevents double-counting by:
--      a) excluding non-master plans, and
--      b) rolling up balances to the group master where applicable.
-- 5. Salesforce enrichment uses prefix/suffix matching, following
--    enterprise CRM integration patterns.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_loans_and_balances AS
SELECT
    sf.contract_sf_case_id AS case_id,
    src.contract_id,
    src.partner_id,

    -- Normalized amount for the given metric type
    src.amount_value,

    -- Business line normalized for analytics
    LOWER(src.business_line) AS business_line,

    src.load_effective_date,
    src.data_domain,
    src.transaction_type,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.parent_plan_key,
    sf.group_key

FROM (
    -- -----------------------------------------------------------------
    -- Base metrics (non-PEP contracts)
    -- -----------------------------------------------------------------
    SELECT
        m.case_id,
        m.contract_id,
        m.partner_id,
        m.amount_value,
        m.business_line,
        m.load_effective_date,
        m.data_domain,
        m.transaction_type
    FROM (
        -- Loan balance
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.loan_balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'loans' AS data_domain,
            'LOAN_BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        -- balance
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        -- balance
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        -- balance
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt
    ) m

    -- Exclude non-master PEP plans to avoid double counting
    WHERE m.case_id NOT IN (
        SELECT master_case_id
        FROM consumption.plan_group_hierarchy
        WHERE is_group_master = FALSE
    )

    UNION ALL

    -- -----------------------------------------------------------------
    -- Rolled-up PEP (plan group) metrics
    -- -----------------------------------------------------------------
    SELECT
        g.master_case_id AS case_id,
        g.master_contract_id AS contract_id,
        g.master_partner_id AS partner_id,
        SUM(m.amount_value) AS amount_value,
        m.business_line,
        m.load_effective_date,
        m.data_domain,
        m.transaction_type
    FROM (
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.loan_balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'loans' AS data_domain,
            'LOAN_BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt

        UNION ALL
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            p.balance AS amount_value,
            p.business_line,
            p.effective_dt AS load_effective_date,
            'balances' AS data_domain,
            'BALANCE' AS transaction_type
        FROM consumption.plan_summary_metrics p
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_summary_metrics
            WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
            GROUP BY effective_dt
        ) latest
            ON p.effective_dt = latest.effective_dt
           AND p.load_dt = latest.load_dt
    ) m
    JOIN consumption.plan_group_hierarchy g
        ON m.case_id = g.related_case_id
    GROUP BY
        g.master_case_id,
        g.master_contract_id,
        g.master_partner_id,
        m.business_line,
        m.load_effective_date,
        m.data_domain,
        m.transaction_type
) src

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON src.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND src.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
