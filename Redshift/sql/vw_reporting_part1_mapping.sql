
-- =====================================================================
-- View Name : consumption.vw_outbound_money_source_mapping
-- Purpose   : Provide a consumption-ready view that maps internal
--             contribution source data to Salesforce contract context.
--
-- Design Notes:
-- 1. Current-record filtering ensures only active dimensional records
--    are used (valid_to IS NULL, is_current = 1).
-- 2. Effective-date logic always selects the most recent snapshot
--    from summary metrics to keep reporting consistent.
-- 3. String normalization (LTRIM/RTRIM) guarantees clean display values
--    across upstream systems.
-- 4. GROUP BY is used to stabilize joins and avoid duplication from
--    raw-current style source tables.
-- 5. Final join aligns warehouse contracts with Salesforce contracts
--    using prefix/suffix matching (enterprise integration pattern).
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_money_source_mapping AS
SELECT
    src.contract_id,                -- Business contract identifier
    src.partner_id,                 -- Partner / affiliate identifier
    src.source_key,                 -- Contribution source surrogate key

    -- Cleaned and user-facing source name
    CASE
        WHEN src.source_display_name = '' THEN ''
        ELSE src.source_display_name
    END AS source_display_name,

    src.effective_dt AS load_effective_date, -- Snapshot effective date

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
    -- Combines plan, contribution source, and latest metrics snapshot
    -- -----------------------------------------------------------------
    SELECT
        plan.case_id,
        plan.contract_id,
        plan.partner_id,
        src_detail.source_key,

        -- Concatenate and trim multi-part source name fields
        RTRIM(
            LTRIM(
                RTRIM(LTRIM(src_detail.source_name_part1)) || ' ' ||
                RTRIM(LTRIM(src_detail.source_name_part2))
            )
        ) AS source_display_name,

        metrics.effective_dt
    FROM core.contribution_source cs

    -- Join to plan dimension (current records only)
    JOIN core.plan_dim plan
        ON cs.case_id = plan.case_id
       AND cs.is_current = 1
       AND cs.valid_to IS NULL
       AND plan.is_current = 1
       AND plan.valid_to IS NULL

    -- Join to raw-current source details
    JOIN rawcurrent.source_detail src_detail
        ON cs.source_key = src_detail.source_key

    -- Pull only the most recent effective-date snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.source_summary_metrics
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.source_summary_metrics
        )
        GROUP BY effective_dt
    ) metrics
        ON 1 = 1

    GROUP BY
        plan.case_id,
        plan.contract_id,
        plan.partner_id,
        src_detail.source_key,
        RTRIM(
            LTRIM(
                RTRIM(LTRIM(src_detail.source_name_part1)) || ' ' ||
                RTRIM(LTRIM(src_detail.source_name_part2))
            )
        ),
        metrics.effective_dt
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
