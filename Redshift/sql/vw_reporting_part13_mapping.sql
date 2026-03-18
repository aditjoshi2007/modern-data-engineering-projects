
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_contribution_monthly
-- Purpose   : Provide contract-level monthly contribution metrics,
--             aligned to calendar months and enriched with Salesforce
--             contract context for outbound consumption.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures monthly contribution metrics are
--    sourced from the most recent effective and load dates.
-- 2. Month start dates are normalized using DATE_TRUNC to ensure
--    consistent monthly aggregation boundaries.
-- 3. LEFT JOIN logic ensures months with no contribution activity
--    are still represented with zero values.
-- 4. ISO-8601 timestamp formatting is applied for downstream API
--    and CRM compatibility.
-- 5. Salesforce enrichment uses prefix/suffix contract matching,
--    following enterprise CRM integration standards.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_contribution_monthly AS
SELECT
    sf.contract_sf_case_id AS case_id,
    base.contract_id,
    base.partner_id,

    -- Month start timestamp (ISO-8601)
    TO_CHAR(
        base.month_start_dt,
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ) AS start_date,

    -- Monthly contribution amount (zero-filled)
    base.monthly_contribution_amount,

    base.load_effective_date,
    base.data_domain,
    base.transaction_type,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.group_key,
    sf.parent_plan_key

FROM (
    -- -----------------------------------------------------------------
    -- Calendarized monthly contribution base
    -- Ensures a row exists for each contract/month
    -- -----------------------------------------------------------------
    SELECT
        c.case_id,
        c.contract_id,
        c.partner_id,
        m.month_start_dt,
        COALESCE(mc.monthly_contribution_amount, 0) AS monthly_contribution_amount,
        c.load_effective_date,
        'Contributions' AS data_domain,
        'Monthly Contribution' AS transaction_type
    FROM (
        -- Distinct contracts from latest snapshot
        SELECT
            wp.case_id,
            wp.contract_id,
            wp.partner_id,
            wp.effective_dt AS load_effective_date
        FROM consumption.plan_monthly_contribution_metrics wp
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_monthly_contribution_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_monthly_contribution_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON wp.effective_dt = latest.effective_dt
           AND wp.load_dt = latest.load_dt
        GROUP BY wp.case_id, wp.contract_id, wp.partner_id, wp.effective_dt
    ) c

    -- Generate distinct month start dates from snapshot
    JOIN (
        SELECT DISTINCT
            DATE_TRUNC('month', wp.month_end_dt) AS month_start_dt
        FROM consumption.plan_monthly_contribution_metrics wp
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_monthly_contribution_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_monthly_contribution_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON wp.effective_dt = latest.effective_dt
           AND wp.load_dt = latest.load_dt
    ) m
        ON 1 = 1

    -- Actual monthly contribution values
    LEFT JOIN (
        SELECT
            wp.case_id,
            wp.contract_id,
            wp.partner_id,
            DATE_TRUNC('month', wp.month_end_dt) AS month_start_dt,
            wp.mtd_net_participant_contribution AS monthly_contribution_amount,
            wp.effective_dt AS load_effective_date
        FROM consumption.plan_monthly_contribution_metrics wp
        JOIN (
            SELECT effective_dt, MAX(load_dt) AS load_dt
            FROM consumption.plan_monthly_contribution_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_monthly_contribution_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON wp.effective_dt = latest.effective_dt
           AND wp.load_dt = latest.load_dt
    ) mc
        ON c.case_id = mc.case_id
       AND c.contract_id = mc.contract_id
       AND c.partner_id = mc.partner_id
       AND m.month_start_dt = mc.month_start_dt
) base

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON base.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND base.partner_id::TEXT = sf.contract_suffix_sf::TEXT;
