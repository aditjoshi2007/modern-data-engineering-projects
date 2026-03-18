
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_balance_fund
-- Purpose   : Provide contract-level fund balance metrics, broken down
--             by investment fund, enriched with Salesforce context for
--             outbound consumption.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures fund balances are sourced from the
--    most recent effective and load dates.
-- 2. Balances are exposed at the fund level to support asset allocation
--    and investment analytics.
-- 3. Zero balances are excluded unless the fund is active and marked
--    for display, reducing noise while preserving valid options.
-- 4. Fund attributes (style, asset class, category) are sourced from
--    dimensional tables to ensure consistent classification.
-- 5. Salesforce enrichment uses prefix/suffix contract matching.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_balance_fund AS
SELECT
    sf.contract_sf_case_id AS case_id,
    f.contract_id,
    f.partner_id,

    -- Fund identifiers & descriptors
    f.fund_id AS fund_provider_id,
    f.fund_style,
    f.fund_descriptor_code,
    f.fund_legal_name,
    f.asset_class,
    f.asset_category,

    -- Aggregated fund balance
    f.total_fund_balance,

    -- Balance snapshot date
    f.load_effective_date,

    'balances' AS data_domain,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.contract_sf_number,
    sf.contract_suffix_sf,
    sf.contract_prefix_sf

FROM (
    -- -----------------------------------------------------------------
    -- Fund balances per contract (snapshot-aligned)
    -- -----------------------------------------------------------------
    SELECT
        base.case_id,
        base.contract_id,
        base.partner_id,
        base.fund_id,
        base.fund_style,
        base.fund_descriptor_code,
        base.fund_legal_name,
        base.asset_class,
        base.asset_category,
        snap.effective_dt AS load_effective_date,
        COALESCE(bal.total_plan_balance, 0) AS total_fund_balance,
        base.fund_status,
        base.fund_display_description

    FROM (
        -- Fund dimension joined to active contracts
        SELECT
            dp.case_id,
            dp.contract_id,
            dp.partner_id,
            df.fund_id,
            df.fund_descriptor_code,
            df.fund_legal_name,
            df.fund_status,
            df.asset_class,
            df.asset_category,
            df.fund_style,
            df.fund_display_description
        FROM core.dim_fund df
        JOIN core.dim_plan dp
            ON dp.case_id = df.case_id
           AND df.current_flag = 1
           AND dp.current_flag = 1
    ) base

    -- Latest fund balance snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_fund_summary_metrics
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_fund_summary_metrics
        )
        GROUP BY effective_dt
    ) snap
        ON 1 = 1

    LEFT JOIN consumption.plan_fund_summary_metrics bal
        ON base.case_id = bal.case_id
       AND base.fund_id = bal.fund_id
       AND snap.effective_dt = bal.effective_dt
       AND snap.load_dt = bal.load_dt

    -- Exclude noise while preserving valid active/displayed funds
    WHERE COALESCE(bal.total_plan_balance, 0) <> 0
       OR (
            UPPER(base.fund_status) LIKE 'ACTIVE%'
        AND UPPER(base.fund_display_description) = 'DISPLAY'
       )
) f

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON f.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND f.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
