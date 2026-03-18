
-- =====================================================================
-- View Name : consumption.vw_outbound_financial_detail_contribution
-- Purpose   : Provide asset-class-level contribution details aligned
--             to contracts and Salesforce financial summaries for
--             outbound consumption.
--
-- Design Notes:
-- 1. Latest-snapshot logic ensures contributions are sourced from the
--    most recent effective and load dates.
-- 2. Aggregation is performed at the asset-class level to support
--    downstream reporting and CRM use cases.
-- 3. HAVING clause removes zero-value contribution records, reducing
--    noise in outbound datasets.
-- 4. Salesforce enrichment aligns contribution data with contract and
--    financial summary context.
-- 5. Prefix/suffix matching is used for contract alignment, following
--    enterprise Salesforce integration patterns.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_financial_detail_contribution AS
SELECT
    sf_contract.contract_sf_case_id AS case_id,
    contrib.contract_id,
    contrib.partner_id,
    contrib.asset_class,

    -- Aggregated contribution metric
    contrib.total_contribution_amount,

    -- Effective date of the contribution snapshot
    contrib.contribution_effective_date,

    -- Salesforce financial summary context
    sf_summary.financial_summary_id,

    -- Salesforce contract metadata
    sf_contract.last_updated_by,
    sf_contract.balance_asof_date,
    sf_contract.service_type,
    sf_contract.parent_plan_key,
    sf_contract.group_key

FROM (
    -- -----------------------------------------------------------------
    -- Contribution aggregation subquery
    -- Aggregates participant contributions by asset class
    -- -----------------------------------------------------------------
    SELECT
        src.case_id,
        src.contract_id,
        src.partner_id,
        src.asset_class,
        SUM(src.ytd_participant_contribution) AS total_contribution_amount,
        src.effective_dt AS contribution_effective_date
    FROM consumption.plan_fund_contribution_summary src

    -- Join to latest available snapshot
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_fund_contribution_summary
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_fund_contribution_summary
        )
        GROUP BY effective_dt
    ) latest
        ON src.effective_dt = latest.effective_dt
       AND src.load_dt = latest.load_dt

    GROUP BY
        src.case_id,
        src.contract_id,
        src.partner_id,
        src.asset_class,
        src.effective_dt

    -- Exclude zero-value contribution groups
    HAVING SUM(src.ytd_participant_contribution) <> 0
) contrib

-- ---------------------------------------------------------------------
-- Salesforce contract enrichment
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        contract_prefix_sf,
        contract_suffix_sf,
        record_id,
        contract_sf_case_id,
        last_updated_by,
        balance_asof_date,
        service_type,
        parent_plan_key,
        group_key
    FROM sf_raw.sf_contract
    GROUP BY
        contract_prefix_sf,
        contract_suffix_sf,
        record_id,
        contract_sf_case_id,
        last_updated_by,
        balance_asof_date,
        service_type,
        parent_plan_key,
        group_key
) sf_contract
    ON contrib.contract_id::TEXT = sf_contract.contract_prefix_sf::TEXT
   AND contrib.partner_id::TEXT = sf_contract.contract_suffix_sf::TEXT

-- ---------------------------------------------------------------------
-- Latest Salesforce financial summary per contract
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        fs.contract_sf_ref,
        fs.financial_summary_id
    FROM sf_raw.sf_financial_summary fs
    JOIN (
        SELECT
            contract_sf_ref,
            MAX(statement_date) AS statement_date
        FROM sf_raw.sf_financial_summary
        WHERE statement_date <= CURRENT_DATE
        GROUP BY contract_sf_ref
    ) latest
        ON fs.contract_sf_ref = latest.contract_sf_ref
       AND fs.statement_date = latest.statement_date
) sf_summary
    ON sf_contract.record_id::TEXT = sf_summary.contract_sf_ref::TEXT;
