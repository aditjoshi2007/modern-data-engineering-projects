
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_investment_services
-- Purpose   : Provide contract-level investment services and feature
--             adoption metrics (counts and balances) for outbound
--             consumption and Salesforce reporting.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures all metrics are sourced from the most
--    recent effective and load dates.
-- 2. UNION ALL is used to normalize multiple service features into a
--    single, analytics-friendly structure.
-- 3. Feature enablement flags gate inclusion to avoid false positives.
-- 4. Window functions (DENSE_RANK) enable employer (PEP) grouping.
-- 5. Conditional aggregation derives participating employer counts
--    while respecting parent/child plan relationships.
-- 6. Salesforce enrichment uses prefix/suffix contract matching.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_investment_services AS
SELECT
    base.case_id,
    base.contract_id,
    base.partner_id,

    -- Participant counts
    base.active_count,
    base.termed_count,

    -- Participant balances
    base.active_balance,
    base.termed_balance,

    -- Aggregates
    base.total_count,
    base.total_balance,

    base.pe_flag,
    base.data_domain,
    base.service_feature_category,
    base.load_effective_date,

    -- Salesforce contract attributes
    base.contract_sf_id,
    base.last_updated_by,
    base.balance_asof_date,
    base.service_type,
    base.parent_plan_key,
    base.group_key,

    -- Derived participating employer count
    agg.participating_employer_count

FROM (
    -- -----------------------------------------------------------------
    -- Base investment service metrics per contract
    -- -----------------------------------------------------------------
    SELECT
        sf.contract_sf_case_id AS case_id,
        p.contract_id,
        p.partner_id,
        p.active_count,
        p.termed_count,
        p.active_balance,
        p.termed_balance,
        p.total_count,
        p.total_balance,
        'Y' AS pe_flag,
        'Investment Services and Features' AS data_domain,
        p.service_feature_category,
        p.load_effective_date,

        sf.record_id AS contract_sf_id,
        sf.last_updated_by,
        sf.balance_asof_date,
        sf.service_type,
        sf.parent_plan_key,
        sf.group_key,

        DENSE_RANK() OVER (ORDER BY sf.group_key) AS group_rank

    FROM (
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count AS active_count,
            ps.termed_count AS termed_count,
            ps.active_balance AS active_balance,
            ps.termed_balance AS termed_balance,
            ps.count AS total_count,
            ps.balance AS total_balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt AS load_effective_date
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_advisor_managed_advice = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_managed_advice = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_portfolio_xpress = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_auto_rebalance = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_custom_portfolio = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_pcra = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_financial_engines = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_target_fund = 'Enabled'

        UNION ALL
        -- XYZ
        SELECT
            ps.case_id,
            ps.contract_id,
            ps.partner_id,
            ps.active_count,
            ps.termed_count,
            ps.active_balance,
            ps.termed_balance,
            ps.count,
            ps.balance,
            'XYZ' AS service_feature_category,
            ps.effective_dt
        FROM consumption.plan_summary_metrics ps
        WHERE ps.feature_spl = 'Enabled'
    ) p

    -- Latest snapshot enforcement
    JOIN (
        SELECT effective_dt, MAX(load_dt) AS load_dt
        FROM consumption.plan_summary_metrics
        WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
        GROUP BY effective_dt
    ) latest
        ON p.load_effective_date = latest.effective_dt

    -- Salesforce enrichment
    JOIN sf_raw.sf_contract sf
        ON p.contract_id::TEXT = sf.contract_prefix_sf::TEXT
       AND p.partner_id::TEXT = sf.contract_suffix_sf::TEXT
) base

-- ---------------------------------------------------------------------
-- Aggregate participating employer counts per service feature & group
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        service_feature_category,
        group_key,
        group_rank,
        CASE
            WHEN parent_plan_key IS NOT NULL THEN 0
            ELSE SUM(CASE WHEN pe_flag = 'Y' THEN 1 ELSE 0 END)
        END AS participating_employer_count
    FROM (
        SELECT
            base.service_feature_category,
            base.group_key,
            base.group_rank,
            base.pe_flag,
            base.parent_plan_key
        FROM (
            SELECT
                service_feature_category,
                group_key,
                parent_plan_key,
                pe_flag,
                DENSE_RANK() OVER (ORDER BY group_key) AS group_rank
            FROM consumption.vw_outbound_contract_investment_services
        ) base
    ) agg_src
    GROUP BY service_feature_category, group_key, parent_plan_key, group_rank
) agg
    ON base.group_rank = agg.group_rank
   AND base.service_feature_category = agg.service_feature_category;
