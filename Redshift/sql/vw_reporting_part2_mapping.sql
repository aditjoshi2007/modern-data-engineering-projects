
-- =====================================================================
-- View Name : consumption.vw_outbound_fund_mapping
-- Purpose   : Expose active and relevant fund-level information for
--             outbound consumption, aligned with Salesforce contracts.
--
-- Design Notes:
-- 1. Current-record filtering ensures only active plan and fund records
--    are included (is_current = 1, valid_to IS NULL).
-- 2. Effective-date logic selects the most recent snapshot from fund
--    summary metrics to guarantee reporting consistency.
-- 3. COALESCE is used to safely handle missing balance metrics.
-- 4. Business filtering includes only funds that are either:
--      a) Holding non-zero balances, OR
--      b) Actively marked for display.
-- 5. GROUP BY is applied to stabilize joins against raw/current tables.
-- 6. Final join maps internal contracts to Salesforce using
--    prefix/suffix matching (enterprise integration pattern).
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_fund_mapping AS
SELECT
    src.case_id,
    src.contract_id,
    src.partner_id,
    src.fund_key,
    src.fund_legal_name,
    src.load_effective_date,

    -- Salesforce contract attributes
    sf.record_id,
    sf.contract_id_sf,
    sf.contract_suffix_sf,
    sf.contract_prefix_sf,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.group_key,
    sf.parent_plan_key

FROM (
    -- -----------------------------------------------------------------
    -- Source subquery
    -- Combines plan, fund dimension, and latest fund metrics snapshot
    -- -----------------------------------------------------------------
    SELECT
        base.case_id,
        base.contract_id,
        base.partner_id,
        base.fund_key,
        base.fund_legal_name,
        base.load_effective_date
    FROM (
        SELECT
            plan.case_id,
            plan.contract_id,
            plan.partner_id,
            fund.fund_key,
            fund.fund_legal_name,
            fund.fund_status,
            fund.display_flag,
            metrics.effective_dt AS load_effective_date,
            COALESCE(metrics.total_plan_balance, 0) AS total_plan_balance
        FROM (
            -- Active plan and fund dimension join
            SELECT
                plan.case_id,
                plan.contract_id,
                plan.partner_id,
                fund.fund_key,
                fund.fund_legal_name,
                fund.fund_status,
                fund.display_flag
            FROM core.fund_dim fund
            JOIN core.plan_dim plan
                ON fund.case_id = plan.case_id
               AND fund.is_current = 1
               AND fund.valid_to IS NULL
               AND plan.is_current = 1
               AND plan.valid_to IS NULL
            GROUP BY
                plan.case_id,
                plan.contract_id,
                plan.partner_id,
                fund.fund_key,
                fund.fund_legal_name,
                fund.fund_status,
                fund.display_flag
        ) fund

        -- Join to latest available fund summary snapshot
        JOIN (
            SELECT
                effective_dt,
                MAX(load_dt) AS load_dt
            FROM consumption.fund_summary_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.fund_summary_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON 1 = 1

        -- Join to detailed fund metrics for balance information
        LEFT JOIN consumption.fund_summary_metrics metrics
            ON fund.case_id = metrics.case_id
           AND fund.fund_key = metrics.fund_key
           AND latest.effective_dt = metrics.effective_dt
           AND latest.load_dt = metrics.load_dt
    ) base

    -- Business rule filter:
    -- Include funds with balances OR explicitly marked active/displayable
    WHERE base.total_plan_balance <> 0
       OR (
            UPPER(base.fund_status) LIKE 'ACTIVE%'
        AND UPPER(base.display_flag) = 'DISPLAY'
       )
) src

-- ---------------------------------------------------------------------
-- Salesforce contract context
-- Grouped to prevent duplication from raw ingestion tables
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        record_id,
        contract_id_sf,
        contract_suffix_sf,
        contract_prefix_sf,
        last_updated_by,
        balance_asof_date,
        service_type,
        group_key,
        parent_plan_key
    FROM sf_raw.sf_contract
    GROUP BY
        record_id,
        contract_id_sf,
        contract_suffix_sf,
        contract_prefix_sf,
        last_updated_by,
        balance_asof_date,
        service_type,
        group_key,
        parent_plan_key
) sf
    ON src.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND src.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
