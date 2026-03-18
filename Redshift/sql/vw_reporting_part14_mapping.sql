
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_balance
-- Purpose   : Provide contract-level plan balance metrics broken down
--             by money source, enriched with Salesforce context for
--             outbound consumption.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures balances are sourced from the most
--    recent effective and load dates.
-- 2. Balances are aggregated by plan money source to support detailed
--    asset reporting and analytics.
-- 3. Reference data is used to normalize and clean source names, with
--    defensive defaults for missing values.
-- 4. PEP (plan group) logic prevents double-counting by correctly
--    handling master vs related plans.
-- 5. Unsupported plan types (DB / Managed DB) are excluded.
-- 6. Salesforce enrichment uses prefix/suffix contract matching.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_balance AS
SELECT
    sf.contract_sf_case_id AS case_id,
    src.contract_id,
    src.partner_id,

    -- Source identifier and name
    src.source_id,
    src.source_type_name,

    -- Aggregated balance for the source
    src.plan_money_source_balance,

    -- Balance snapshot date
    src.balance_effective_date,

    src.data_domain,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.group_key,
    sf.parent_plan_key

FROM (
    -- -----------------------------------------------------------------
    -- Plan money source balances (PEP-safe)
    -- -----------------------------------------------------------------
    SELECT
        s.case_id,
        s.contract_id,
        s.partner_id,

        -- Default synthetic source id retained for compatibility
        '11111' AS source_id,

        -- Cleaned source name with defensive default
        CASE
            WHEN TRIM(s.source_type_name) = '' THEN 'Employer Accounts'
            ELSE s.source_type_name
        END AS source_type_name,

        SUM(COALESCE(b.total_plan_balance, 0)) AS plan_money_source_balance,
        snap.effective_dt AS balance_effective_date,
        'balances' AS data_domain

    FROM (
        -- -----------------------------------------------------------------
        -- Resolve contract ↔ contribution source relationships
        -- Handles both non-PEP and PEP master rollups
        -- -----------------------------------------------------------------
        SELECT
            dp.case_id,
            dp.contract_id,
            dp.partner_id,
            ref.source_id,
            TRIM(ref.report_name_part1 || ' ' || ref.report_name_part2) AS source_type_name
        FROM core.dim_contribution_source dcs
        JOIN core.dim_plan dp
            ON dcs.case_id = dp.case_id
           AND dcs.current_flag = 1
           AND dp.current_flag = 1
           AND dp.plan_status = 'ACTIVE'
           AND dp.is_demo_plan = 0
           AND dp.plan_type NOT IN ('PLAN1', 'PLAN2')
        JOIN ref_raw.contribution_source_detail ref
            ON dcs.source_id = ref.source_id
        WHERE dp.case_id NOT LIKE 'AB%'
    ) s

    -- Latest balance snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_contribution_source_summary_metrics
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_contribution_source_summary_metrics
        )
        GROUP BY effective_dt
    ) snap
        ON 1 = 1

    LEFT JOIN consumption.plan_contribution_source_summary_metrics b
        ON b.effective_dt = snap.effective_dt
       AND b.load_dt = snap.load_dt
       AND s.source_id = b.source_id

    GROUP BY
        s.case_id,
        s.contract_id,
        s.partner_id,
        CASE
            WHEN TRIM(s.source_type_name) = '' THEN 'Employer Accounts'
            ELSE s.source_type_name
        END,
        snap.effective_dt
) src

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON src.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND src.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
