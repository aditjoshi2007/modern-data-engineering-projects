
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_withdrawal_type
-- Purpose   : Expose contract-level withdrawal metrics broken down by
--             withdrawal type, enriched with Salesforce context and
--             employer participation indicators.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures all withdrawal metrics are sourced
--    from the most recent effective and load dates.
-- 2. Metrics are grouped by withdrawal type to support detailed
--    outbound analytics and CRM reporting.
-- 3. Window functions (DENSE_RANK) are used to group contracts by
--    employer hierarchy (group key).
-- 4. Conditional logic derives participating employer counts while
--    respecting parent/child contract relationships.
-- 5. Salesforce enrichment uses prefix/suffix contract matching,
--    following enterprise CRM integration standards.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_withdrawal_type AS
SELECT
    base.case_id,
    base.contract_id,
    base.partner_id,
    base.withdrawal_type,

    -- Withdrawal metrics
    base.withdrawal_txn_count,
    base.withdrawal_txn_amount,
    base.unique_withdrawal_count,

    base.load_effective_date,
    base.data_domain,
    base.transaction_type,

    -- Salesforce contract attributes
    base.contract_sf_id,
    base.last_updated_by,
    base.balance_asof_date,
    base.service_type,
    base.parent_plan_key,
    base.group_key,

    -- Derived participating employer count
    CASE
        WHEN base.parent_plan_key IS NOT NULL
         AND base.service_type IS NOT NULL
        THEN 0
        ELSE agg.participating_employer_count
    END AS participating_employer_count

FROM (
    -- -----------------------------------------------------------------
    -- Base withdrawal metrics per contract and withdrawal type
    -- -----------------------------------------------------------------
    SELECT
        sf.contract_sf_case_id AS case_id,
        wd.contract_id,
        wd.partner_id,
        wd.withdrawal_type,
        wd.withdrawal_txn_count,
        wd.withdrawal_txn_amount,
        wd.unique_withdrawal_count,
        wd.load_effective_date,
        'withdrawals' AS data_domain,
        'withdrawal_type' AS transaction_type,

        sf.record_id AS contract_sf_id,
        sf.last_updated_by,
        sf.balance_asof_date,
        sf.service_type,
        sf.parent_plan_key,
        sf.group_key,

        -- Rank contracts within the same employer group
        DENSE_RANK() OVER (ORDER BY sf.group_key) AS group_rank,

        -- Flag indicating participation for this contract/type
        CASE
            WHEN COALESCE(wd.withdrawal_txn_amount, 0) = 0 THEN 0
            ELSE 1
        END AS participates_flag

    FROM (
        -- Withdrawal metrics from latest snapshot
        SELECT
            src.case_id,
            src.contract_id,
            src.partner_id,
            src.withdrawal_type,
            src.total_withdrawal_type_count AS withdrawal_txn_count,
            src.total_withdrawal_type_amount AS withdrawal_txn_amount,
            src.unique_withdrawal_type AS unique_withdrawal_count,
            src.effective_dt AS load_effective_date
        FROM consumption.plan_withdrawal_type_metrics src
        JOIN (
            SELECT
                effective_dt,
                MAX(load_dt) AS load_dt
            FROM consumption.plan_withdrawal_type_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_withdrawal_type_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON src.effective_dt = latest.effective_dt
           AND src.load_dt = latest.load_dt
    ) wd

    -- Salesforce contract enrichment
    JOIN sf_raw.sf_contract sf
        ON wd.contract_id::TEXT = sf.contract_prefix_sf::TEXT
       AND wd.partner_id::TEXT = sf.contract_suffix_sf::TEXT
) base

-- ---------------------------------------------------------------------
-- Aggregate participating employer counts per withdrawal type & group
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        withdrawal_type,
        group_key,
        group_rank,
        SUM(participates_flag) AS participating_employer_count
    FROM (
        SELECT
            sf.contract_sf_case_id AS case_id,
            wd.contract_id,
            wd.partner_id,
            wd.withdrawal_type,
            wd.withdrawal_txn_amount,
            sf.group_key,
            DENSE_RANK() OVER (ORDER BY sf.group_key) AS group_rank,
            CASE
                WHEN COALESCE(wd.withdrawal_txn_amount, 0) = 0 THEN 0
                ELSE 1
            END AS participates_flag
        FROM consumption.plan_withdrawal_type_metrics wd
        JOIN (
            SELECT
                effective_dt,
                MAX(load_dt) AS load_dt
            FROM consumption.plan_withdrawal_type_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_withdrawal_type_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON wd.effective_dt = latest.effective_dt
           AND wd.load_dt = latest.load_dt
        JOIN sf_raw.sf_contract sf
            ON wd.contract_id::TEXT = sf.contract_prefix_sf::TEXT
           AND wd.partner_id::TEXT = sf.contract_suffix_sf::TEXT
    ) grouped
    GROUP BY withdrawal_type, group_key, group_rank
) agg
    ON base.group_rank = agg.group_rank
   AND base.withdrawal_type = agg.withdrawal_type;
