
-- =====================================================================
-- View Name : consumption.vw_outbound_division
-- Purpose   : Provide division-level outbound data aligned with
--             contracts and Salesforce context for downstream consumers.
--
-- Design Notes:
-- 1. Latest-snapshot logic ensures division data is sourced from the
--    most recent effective and load dates.
-- 2. Division numbers are trimmed to avoid formatting inconsistencies
--    across upstream systems.
-- 3. MIN() aggregation on division status stabilizes results when
--    multiple records exist per division in raw summary tables.
-- 4. GROUP BY enforces one row per contract / division / snapshot.
-- 5. Salesforce enrichment uses prefix/suffix matching, a common
--    enterprise integration pattern.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_division AS
SELECT
    src.case_id,
    src.contract_id,
    src.partner_id,
    src.division_id,
    src.division_status,
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
    -- Division snapshot subquery
    -- Pulls latest division-level metrics per contract
    -- -----------------------------------------------------------------
    SELECT
        d.case_id,
        d.contract_id,
        d.partner_id,

        -- Normalized division identifier
        TRIM(d.division_id) AS division_id,

        -- Stabilized division status
        MIN(d.division_status) AS division_status,

        d.effective_dt AS load_effective_date
    FROM consumption.plan_division_metrics d

    -- Join to latest available snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_division_metrics
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_division_metrics
        )
        GROUP BY effective_dt
    ) latest
        ON d.effective_dt = latest.effective_dt
       AND d.load_dt = latest.load_dt

    GROUP BY
        d.case_id,
        d.contract_id,
        d.partner_id,
        TRIM(d.division_id),
        d.effective_dt
) src

-- ---------------------------------------------------------------------
-- Salesforce contract enrichment
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
