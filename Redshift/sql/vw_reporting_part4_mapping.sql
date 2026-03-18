
-- =====================================================================
-- View Name : consumption.vw_outbound_financial_detail
-- Purpose   : Provide fund-level financial detail aligned with contracts
--             and Salesforce financial summaries for outbound consumers.
--
-- Design Notes:
-- 1. Current-record filtering ensures only active plans and funds
--    are included (is_current = 1, valid_to IS NULL).
-- 2. Business rules exclude unsupported plan types and inactive plans.
-- 3. Asset-class-based sequencing enables consistent downstream ordering.
-- 4. Latest snapshot logic guarantees consistent balances and metrics.
-- 5. COALESCE protects against missing balance records.
-- 6. Final filters restrict output to financially relevant or displayable funds.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_financial_detail AS
SELECT
    src.contract_id,
    src.partner_id,

    -- Derived case identifier (contract + partner)
    src.case_id,

    -- Fund identifiers and attributes
    src.fund_key,
    src.fund_style,
    src.fund_descriptor_code,
    src.fund_legal_name,
    src.ticker_symbol,
    src.fund_status,
    src.asset_class,
    src.asset_category,

    -- Financial metrics
    src.total_fund_balance,
    src.load_effective_date,

    -- Custom classification & ordering
    src.custom_asset_class,
    src.sequence_number,

    -- Salesforce enrichment
    src.contract_sf_id,
    src.last_updated_by,
    src.balance_asof_date,
    src.service_type,
    src.parent_plan_key,
    src.group_key,

    -- Salesforce financial summary context
    src.financial_summary_id,
    src.statement_date,
    src.contract_sf_ref

FROM (
    SELECT
        plan.contract_id,
        plan.partner_id,
        plan.contract_id || ' ' || plan.partner_id AS case_id,

        fund.fund_key,
        fund.fund_style,
        fund.fund_descriptor_code,
        fund.fund_legal_name,
        fund.ticker_symbol,
        fund.fund_status,
        fund.fund_display_flag,
        fund.asset_class,
        fund.asset_category,

        -- Latest available balance (defaults to zero if missing)
        COALESCE(balance.total_fund_balance, 0) AS total_fund_balance,

        snapshot.load_dt AS load_effective_date,

        -- Placeholder custom classification
        0 AS custom_asset_class,

        -- Deterministic ordering by asset class
        CASE fund.asset_class
            WHEN 'asset_class_1' THEN 1
            WHEN 'asset_class_2' THEN 2
            WHEN 'asset_class_3' THEN 3
            WHEN 'asset_class_4' THEN 4
            WHEN 'asset_class_5' THEN 5
            WHEN 'asset_class_6' THEN 6
            WHEN 'asset_class_7' THEN 7
            WHEN 'asset_class_8' THEN 8
            ELSE 0
        END AS sequence_number,

        sf_contract.record_id AS contract_sf_id,
        sf_contract.last_updated_by,
        sf_contract.balance_asof_date,
        sf_contract.service_type,
        sf_contract.parent_plan_key,
        sf_contract.group_key,

        sf_summary.financial_summary_id,
        sf_summary.statement_date,
        sf_summary.contract_sf_ref

    FROM (
        -- -------------------------------------------------------------
        -- Active plan and fund dimension join
        -- -------------------------------------------------------------
        SELECT
            p.case_id,
            p.contract_id,
            p.partner_id,
            f.fund_key,
            f.fund_style,
            f.fund_descriptor_code,
            f.fund_legal_name,
            f.ticker_symbol,
            f.fund_status,
            f.fund_display_flag,
            f.asset_class,
            f.asset_category
        FROM core.fund_dim f
        JOIN core.plan_dim p
            ON f.case_id = p.case_id
        WHERE f.is_current = 1
          AND f.valid_to IS NULL
          AND p.is_current = 1
          AND p.valid_to IS NULL
          AND p.plan_status = 'ACTIVE'
          AND p.plan_type NOT IN ('plan_type_1', 'plan_type_2')
        GROUP BY
            p.case_id,
            p.contract_id,
            p.partner_id,
            f.fund_key,
            f.fund_style,
            f.fund_descriptor_code,
            f.fund_legal_name,
            f.ticker_symbol,
            f.fund_status,
            f.fund_display_flag,
            f.asset_class,
            f.asset_category
    ) fund

    -- Salesforce contract enrichment
    JOIN sf_raw.sf_contract sf_contract
        ON fund.contract_id::TEXT = sf_contract.contract_prefix_sf::TEXT
       AND fund.partner_id::TEXT = sf_contract.contract_suffix_sf::TEXT

    -- Latest Salesforce financial summary per contract
    JOIN (
        SELECT
            fs.contract_sf_ref,
            fs.financial_summary_id,
            fs.statement_date
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
        GROUP BY fs.contract_sf_ref, fs.financial_summary_id, fs.statement_date
    ) sf_summary
        ON sf_contract.record_id::TEXT = sf_summary.contract_sf_ref::TEXT

    -- Latest plan-fund snapshot date
    JOIN (
        SELECT
            effective_dt,
            MAX(load_dt) AS load_dt
        FROM consumption.plan_fund_metrics
        WHERE effective_dt = (
            SELECT MAX(effective_dt)
            FROM consumption.plan_fund_metrics
        )
        GROUP BY effective_dt
    ) snapshot
        ON 1 = 1

    -- Fund-level balances aligned to latest snapshot
    LEFT JOIN (
        SELECT
            case_id,
            fund_key,
            total_fund_balance
        FROM consumption.plan_fund_metrics pf
        JOIN (
            SELECT
                effective_dt,
                MAX(load_dt) AS load_dt
            FROM consumption.plan_fund_metrics
            WHERE effective_dt = (
                SELECT MAX(effective_dt)
                FROM consumption.plan_fund_metrics
            )
            GROUP BY effective_dt
        ) latest
            ON pf.effective_dt = latest.effective_dt
           AND pf.load_dt = latest.load_dt
        GROUP BY case_id, fund_key, total_fund_balance
    ) balance
        ON fund.case_id = balance.case_id
       AND fund.fund_key = balance.fund_key
) src

-- ---------------------------------------------------------------------
-- Final business filter:
-- Include funds with balances OR active/displayable funds
-- ---------------------------------------------------------------------
WHERE src.total_fund_balance <> 0
   OR (
        UPPER(src.fund_status) LIKE 'ACTIVE%'
    AND src.fund_display_flag = 'DISPLAY'
   );
