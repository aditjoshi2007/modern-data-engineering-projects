
-- =====================================================================
-- View Name : consumption.vw_outbound_financial_summary
-- Purpose   : Provide an outbound, consumption-ready financial summary
--             at the contract level for downstream systems such as
--             Salesforce and reporting consumers.
--
-- Design Notes:
-- 1. Current-record filtering ensures only active plan records
--    are included (is_current = 1, valid_to IS NULL).
-- 2. Effective-date logic always selects the most recent snapshot
--    from plan summary metrics to maintain reporting consistency.
-- 3. Business exclusions remove unsupported plan types from outbound
--    feeds (e.g., managed DB and defined benefit plans).
-- 4. Constant placeholders are used to stabilize schema contracts
--    when upstream metrics are not yet available.
-- 5. Final join aligns internal contracts with Salesforce contracts
--    using prefix/suffix matching (enterprise integration pattern).
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_financial_summary AS
SELECT
    src.case_id,
    src.contract_id,
    src.partner_id,

    -- Year-to-date contribution metrics
    src.ytd_contribution_amount,
    src.contributor_count,

    -- Load dates for withdrawal and contribution snapshots
    src.withdrawal_snapshot_dt,
    src.contribution_snapshot_dt,

    -- Year-to-date withdrawal metrics
    src.withdrawal_amount_ytd,
    src.withdrawal_count_ytd,

    -- Overall snapshot effective date
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
    -- Pulls active plans and aligns them with the latest metrics snapshot
    -- -----------------------------------------------------------------
    SELECT
        plan.case_id,
        plan.contract_id,
        plan.partner_id,

        -- Placeholder metrics (schema-stabilization pattern)
        2 AS ytd_contribution_amount,
        2 AS contributor_count,

        -- Snapshot dates sourced from latest metrics load
        latest.load_dt AS withdrawal_snapshot_dt,
        latest.load_dt AS contribution_snapshot_dt,

        -- Placeholder withdrawal metrics
        2 AS withdrawal_amount_ytd,
        2 AS withdrawal_count_ytd,

        latest.load_dt AS load_effective_date
    FROM core.plan_dim plan

    -- Join to most recent plan summary snapshot
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
        ON 1 = 1

    -- Business exclusions and current-record filtering
    WHERE plan.plan_type <> 'plan_type'
      AND plan.plan_type <> 'plan_type'
      AND plan.is_current = 1
      AND plan.valid_to IS NULL
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
