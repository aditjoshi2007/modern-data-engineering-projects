
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_new_loans_mtd
-- Purpose   : Expose contract-level Month-To-Date (MTD) new loan metrics
--             for outbound consumption and Salesforce reporting.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures MTD loan metrics are sourced from the
--    most recent effective and load dates.
-- 2. Metrics are exposed at the contract level to align with CRM needs.
-- 3. Static domain and transaction-type labels standardize downstream
--    analytics and reporting.
-- 4. Salesforce enrichment uses prefix/suffix contract matching,
--    following enterprise CRM integration patterns.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_new_loans_mtd AS
SELECT
    sf.contract_sf_case_id AS case_id,
    psm.contract_id,
    psm.partner_id,

    -- Month-to-date new loan balance
    psm.mtd_new_loan_amount AS loan_balance_mtd,

    -- Snapshot effective date
    psm.effective_dt AS load_effective_date,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.parent_plan_key,
    sf.group_key,

    -- Standardized outbound descriptors
    'loans' AS data_domain,
    'NEW_LOANS_MTD' AS transaction_type

FROM consumption.plan_summary_metrics psm

-- Join to latest available snapshot
JOIN (
    SELECT
        effective_dt,
        MAX(load_dt) AS load_dt
    FROM consumption.plan_summary_metrics
    WHERE effective_dt = (
        SELECT MAX(effective_dt)
        FROM consumption.plan_summary_metrics
    )
    GROUP BY effective_dt
) latest
    ON psm.effective_dt = latest.effective_dt
   AND psm.load_dt = latest.load_dt

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON psm.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND psm.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
