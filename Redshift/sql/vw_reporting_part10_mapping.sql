
-- =====================================================================
-- View Name : consumption.vw_outbound_contract_loan_type
-- Purpose   : Expose contract-level outstanding loan balances broken
--             down by loan type, enriched with Salesforce context and
--             employer participation indicators.
--
-- Design Notes:
-- 1. UNPIVOT is used to normalize multiple loan balance columns into
--    a single loan_type / amount structure.
-- 2. Snapshot-based logic ensures all loan balances are sourced from
--    the most recent effective and load dates.
-- 3. Window functions (DENSE_RANK) enable grouping by employer (PEP)
--    hierarchy for participation metrics.
-- 4. Conditional aggregation derives participating employer counts
--    while respecting parent/child plan relationships.
-- 5. Zero-balance loan types are excluded to reduce outbound noise.
-- 6. Salesforce enrichment uses prefix/suffix contract matching.
-- =====================================================================

CREATE OR REPLACE VIEW consumption.vw_outbound_contract_loan_type AS
SELECT
    base.case_id,
    base.contract_id,
    base.partner_id,

    UPPER(base.loan_type) AS loan_type,

    -- Placeholder for future individual-level metrics
    CAST('' AS VARCHAR) AS unique_individual_count,

    -- Outstanding loan balance for the given loan type
    base.loan_balance_amount,

    base.load_effective_date,
    'loans' AS data_domain,
    'LOAN_TYPE' AS transaction_type,

    -- Salesforce contract attributes
    base.contract_sf_id,
    base.last_updated_by,
    base.balance_asof_date,
    base.service_type,
    base.parent_plan_key,

    -- Normalize missing group keys
    CASE
        WHEN base.group_key = 'DUMMY' THEN ''
        ELSE base.group_key
    END AS group_key,

    agg.participating_employer_count,

    -- Placeholder for PEP contract reference
    CAST('' AS VARCHAR) AS pep_contract_id

FROM (
    -- -----------------------------------------------------------------
    -- Base loan-type balances per contract
    -- -----------------------------------------------------------------
    SELECT
        sf.contract_sf_case_id AS case_id,
        ud.contract_id,
        ud.partner_id,
        ud.loan_type,
        ud.loan_balance_amount,
        ud.load_effective_date,

        sf.record_id AS contract_sf_id,
        sf.last_updated_by,
        sf.balance_asof_date,
        sf.service_type,
        sf.parent_plan_key,
        COALESCE(sf.group_key, 'DUMMY') AS group_key,

        DENSE_RANK() OVER (ORDER BY COALESCE(sf.group_key, 'DUMMY')) AS group_rank

    FROM (
        -- Unpivot loan balances into loan types
        SELECT
            src.case_id,
            src.contract_id,
            src.partner_id,
            src.load_effective_date,
            loan_type,
            loan_balance_amount
        FROM (
            SELECT
                p.case_id,
                p.contract_id,
                p.partner_id,
                p.personal_loan_balance,
                p.residential_loan_balance,
                p.personal_hardship_loan_balance,
                p.residential_hardship_loan_balance,
                p.effective_dt AS load_effective_date
            FROM consumption.plan_summary_metrics p
            JOIN (
                SELECT effective_dt, MAX(load_dt) AS load_dt
                FROM consumption.plan_summary_metrics
                WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
                GROUP BY effective_dt
            ) latest
                ON p.effective_dt = latest.effective_dt
               AND p.load_dt = latest.load_dt
        ) src
        UNPIVOT INCLUDE NULLS (
            loan_balance_amount FOR loan_type IN (
                personal_loan_balance AS 'personal',
                residential_loan_balance AS 'residential',
                personal_hardship_loan_balance AS 'personal_hardship',
                residential_hardship_loan_balance AS 'residential_hardship'
            )
        ) ud

    -- Salesforce contract enrichment
    JOIN sf_raw.sf_contract sf
        ON ud.contract_id = sf.contract_prefix_sf
       AND ud.partner_id = sf.contract_suffix_sf

    -- Exclude zero-balance loan types
    WHERE ud.loan_balance_amount > 0
) base

-- ---------------------------------------------------------------------
-- Aggregate participating employer counts per loan type and group
-- ---------------------------------------------------------------------
JOIN (
    SELECT
        loan_type,
        group_key,
        group_rank,
        SUM(participates_flag) AS participating_employer_count
    FROM (
        SELECT
            COALESCE(sf.group_key, 'DUMMY') AS group_key,
            loan_type,
            DENSE_RANK() OVER (ORDER BY COALESCE(sf.group_key, 'DUMMY')) AS group_rank,
            CASE
                WHEN COALESCE(loan_balance_amount, 0) = 0 THEN 0
                ELSE 1
            END AS participates_flag
        FROM (
            SELECT
                src.contract_id,
                src.partner_id,
                loan_type,
                loan_balance_amount
            FROM (
                SELECT
                    p.contract_id,
                    p.partner_id,
                    p.personal_loan_balance,
                    p.residential_loan_balance,
                    p.personal_hardship_loan_balance,
                    p.residential_hardship_loan_balance
                FROM consumption.plan_summary_metrics p
                JOIN (
                    SELECT effective_dt, MAX(load_dt) AS load_dt
                    FROM consumption.plan_summary_metrics
                    WHERE effective_dt = (SELECT MAX(effective_dt) FROM consumption.plan_summary_metrics)
                    GROUP BY effective_dt
                ) latest
                    ON p.effective_dt = latest.effective_dt
                   AND p.load_dt = latest.load_dt
            ) src
            UNPIVOT INCLUDE NULLS (
                loan_balance_amount FOR loan_type IN (
                    personal_loan_balance AS 'personal',
                    residential_loan_balance AS 'residential',
                    personal_hardship_loan_balance AS 'personal_hardship',
                    residential_hardship_loan_balance AS 'residential_hardship'
                )
            ) u
        JOIN sf_raw.sf_contract sf
            ON u.contract_id = sf.contract_prefix_sf
           AND u.partner_id = sf.contract_suffix_sf
    ) grouped
    GROUP BY loan_type, group_key, group_rank
) agg
    ON base.group_rank = agg.group_rank
   AND base.loan_type = agg.loan_type;
