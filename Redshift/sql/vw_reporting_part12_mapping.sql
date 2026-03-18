
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_contribution_source
-- Purpose   : Provide contract-level contribution metrics broken down
--             by contribution source, enriched with Salesforce context
--             for outbound consumption.
--
-- Design Notes:
-- 1. Snapshot-based logic ensures contribution metrics are sourced from
--    the most recent effective and load dates.
-- 2. Contribution data is exposed at the source level (e.g. payroll,
--    rollover, employer) to support detailed analytics.
-- 3. COALESCE logic resolves source names using a reference lookup when
--    available, falling back to the transactional source name.
-- 4. Standardized domain and transaction labels ensure consistency
--    across outbound contribution datasets.
-- 5. Salesforce enrichment uses prefix/suffix contract matching,
--    following enterprise CRM integration standards.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_contribution_source AS
SELECT
    sf.contract_sf_case_id AS case_id,
    src.contract_id,
    src.partner_id,

    -- Contribution source name (resolved via reference mapping)
    COALESCE(ref.source_type_name, src.source_type_name) AS source_type_name,

    -- Source identifier (retained for traceability)
    src.source_id,

    -- Contribution metric
    src.total_contribution_amount,

    -- Snapshot effective date
    src.contribution_effective_date,

    -- Salesforce contract attributes
    sf.record_id AS contract_sf_id,
    sf.last_updated_by,
    sf.balance_asof_date,
    sf.service_type,
    sf.parent_plan_key,
    sf.group_key,

    -- Standardized outbound descriptors
    src.data_domain,
    src.transaction_type

FROM (
    -- -----------------------------------------------------------------
    -- Base contribution metrics per contract and source
    -- -----------------------------------------------------------------
    SELECT
        wp.case_id,
        wp.contract_id,
        wp.partner_id,
        wp.source_report_name AS source_type_name,
        wp.source_id::BIGINT AS source_id,
        wp.ytd_participant_contribution AS total_contribution_amount,
        wp.effective_dt AS contribution_effective_date,
        'contributions' AS data_domain,
        'YTD' AS transaction_type
    FROM consumption.plan_money_type_contribution_summary wp

    -- Join to latest available snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_money_type_contribution_summary
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_money_type_contribution_summary
        )
        GROUP BY effective_dt
    ) latest
        ON wp.effective_dt = latest.effective_dt
       AND wp.load_dt = latest.load_dt
) src

-- Salesforce contract enrichment
JOIN sf_raw.sf_contract sf
    ON src.contract_id::TEXT = sf.contract_prefix_sf::TEXT
   AND src.partner_id::TEXT = sf.contract_suffix_sf::TEXT

-- ---------------------------------------------------------------------
-- Reference lookup for normalized contribution source names
-- ---------------------------------------------------------------------
LEFT JOIN (
    SELECT
        ref.source_id,
        MAX(TRIM(ref.report_name_part1 || ' ' || ref.report_name_part2)) AS source_type_name
    FROM ref_raw.contribution_source_detail ref
    GROUP BY ref.source_id
) ref
    ON src.source_id = ref.source_id;
