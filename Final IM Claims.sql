-- Inland Marine (IM) Claims Data Query
-- FINAL COMBINED VERSION: Includes all business characteristics, premium, marketing, credit, crime scores, submission details, and package name.

WITH
    -- CTE 1: Define the evaluation date windows for the analysis.
    dates_table AS (
        SELECT DISTINCT
               EXTRACT(YEAR FROM a.monthlastday) * 100 + EXTRACT(MONTH FROM a.monthlastday) AS eval_date,
               a.monthlastday                                                               AS eval_date_full
        FROM bi_workspace.periods AS a
        WHERE a.date <= last_day(date_add('month', -1, current_date))
          AND a.date >= '2020-07-30' -- Adjusted for IM
        ORDER BY 1
    ),

    -- CTE 2: Extract and clean raw loss and exposure data for each evaluation period.
    loss_exposure_table AS (
        SELECT DISTINCT
               d.eval_date,
               d.eval_date_full,
               a.business_id, a.policy_reference, p.policy_id, a.tpa_name,
               CASE WHEN a.tpa_name = 'Gallagher Bassett' THEN substring(a.claim_number, 1, 13) ELSE a.claim_number END AS claim_number,
               CASE WHEN a.tpa_name = 'Gallagher Bassett' THEN substring(a.claim_id, 1, 13) ELSE a.claim_id END AS claim_id,
               a.exposure_number, a.exposure_id,
               CAST(a.policy_start_date AS DATE) AS eff_date,
               CAST(a.policy_end_date AS DATE) AS exp_date,
               a.date_of_loss AS acc_date,
               MIN(a.date_submitted) OVER (PARTITION BY a.claim_id) AS rpt_date,
               datediff(day, acc_date, rpt_date) AS rpt_lag,
               CASE WHEN datediff(day, acc_date, rpt_date) > 50 THEN 50 ELSE datediff(day, acc_date, rpt_date) END AS rpt_lag_capped,
               EXTRACT(YEAR FROM a.policy_start_date) * 100 + EXTRACT(MONTH FROM a.policy_start_date) AS eff_period,
               EXTRACT(YEAR FROM a.date_of_loss) * 100 + EXTRACT(MONTH FROM a.date_of_loss) AS acc_period,
               date_trunc('month', d.eval_date_full)::date AS eval_date_trunc,
               isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0) AS paid,
               (isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0)) + isnull(a.loss_reserve_total, 0) + isnull(a.expense_ao_reserve_total, 0) + isnull(a.expense_dcc_reserve_total, 0) AS incurred,
               isnull(a.loss_paid_total, 0) + isnull(a.loss_reserve_total, 0) AS incurred_loss_only,
               isnull(a.expense_dcc_paid_total, 0) + isnull(a.expense_dcc_reserve_total, 0) AS incurred_dcc,
               isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_ao_reserve_total, 0) AS incurred_ao,
               isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.recovery_salvage_collected_total, 0) AS incurred_ss,
               CASE WHEN SUM(isnull(a.loss_reserve_total, 0) + isnull(a.expense_dcc_reserve_total, 0) + isnull(a.expense_ao_reserve_total, 0)) OVER (PARTITION BY a.claim_id, d.eval_date_full) > 0 THEN 'Open' ELSE 'closed' END AS claim_status,
               CASE WHEN SUM(isnull(a.loss_reserve_total, 0) + isnull(a.expense_dcc_reserve_total, 0) + isnull(a.expense_ao_reserve_total, 0)) OVER (PARTITION BY a.exposure_id, d.eval_date_full) > 0 THEN 'Open' ELSE 'closed' END AS exposure_status,
               SUM(isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0)) OVER (PARTITION BY a.claim_id, d.eval_date_full) AS claim_lvl_eval_paid,
               CASE
                   WHEN (a.catastrophe_code IS NULL OR a.catastrophe_code = '' OR a.catastrophe_code IN ('1111/BFA', '2212', 'NRP-X-R-0-0', '2222/XOL', 'CD', 'SIU', 'Bad Faith Suit', 'Bad Faith Suit / SIU', 'COVID19', '1111/BFA and SIU', 'HSB')) THEN 'non-CAT'
                   WHEN lower(a.catastrophe_code) LIKE '%subro%' OR lower(a.catastrophe_code) LIKE '%salv%' THEN 'non-CAT'
                   ELSE 'CAT'
               END AS cat_ind,
               a.coverage, a.loss_cause_type_name, a.marketing_cob_group AS cob_group, a.cob_name, a.business_state AS state, a.in_suit_flag, a.attorney_authorized_representative_flag,
               a.location_of_loss
        FROM dwh.all_claims_financial_changes_ds AS a
        JOIN dates_table d ON a.date = d.eval_date_full
        LEFT JOIN nimi_svc_prod.policies AS p ON p.policy_reference = a.policy_reference
        WHERE a.lob = 'IM' AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
    ),

    -- CTE 3: Aggregate loss data to the claim level for each evaluation date.
    loss_table AS (
        SELECT DISTINCT
               a.claim_number, a.claim_id, a.policy_reference, a.policy_id, a.business_id, a.eff_date, a.exp_date,
               a.acc_date, a.eff_period, a.acc_period, a.eval_date, a.claim_status, a.loss_cause_type_name, a.cat_ind,
               SUM(a.paid) AS paid,
               SUM(a.incurred) AS incurred
        FROM loss_exposure_table AS a
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    ),

    -- CTE 4: Get lifetime claim history for each policy to determine prior loss history.
    claims_all AS (
        WITH latest_claims AS (
            SELECT
                   policy_reference,
                   coalesce(loss_paid_total, 0) AS loss_paid,
                   row_number() OVER (PARTITION BY policy_reference, claim_id, exposure_id ORDER BY date DESC) AS rn
            FROM dwh.all_claims_financial_changes_ds
            WHERE lob = 'IM'
        )
        SELECT
            p.business_id,
            lc.policy_reference,
            CASE WHEN SUM(lc.loss_paid) > 0 THEN 'Y' ELSE 'N' END as prior_loss_hist
        FROM latest_claims lc
        JOIN nimi_svc_prod.policies p ON p.policy_reference = lc.policy_reference
        WHERE lc.rn = 1
        GROUP BY 1, 2
    ),

    -- CTE 5: De-duplicate S3 rating calculations to get the latest version for IM.
    s3_im_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY dateid DESC, update_time DESC) AS rn
            FROM s3_operational.rating_svc_prod_calculations
            WHERE lob = 'IM'
        )
        WHERE rn = 1
    ),

    -- CTE 6: De-duplicate IM quotes to get the latest version.
    im_quotes_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY creation_time DESC) AS rn
            FROM external_dwh.im_quotes
        )
        WHERE rn = 1
    ),

    -- CTE 7: De-duplicate policy transactions to get the latest bind transaction.
    policy_transactions_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY tx_effective_date DESC, policy_transaction_id DESC) as rn
            FROM prod.dwh.policy_transactions
            WHERE transaction_type = 'BIND' AND lob = 'IM'
        )
        WHERE rn = 1
    ),

    -- CTE 8: Get the latest personal credit score for each business.
    latest_credit_score AS (
        SELECT business_id, score as credit_score
        FROM (
            SELECT business_id, score, rank() OVER (PARTITION BY business_id ORDER BY creation_time DESC) AS rnk
            FROM riskmgmt_svc_prod.risk_score_result
            WHERE score IS NOT NULL
        )
        WHERE rnk = 1
    ),

    -- CTE 9: Pull and de-duplicate Verisk crime score data from multiple sources.
    verisk_table as (
        select * from (
            select
                *
                , row_number() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
            from (
                select
                   cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', true),'') as integer)         as Arson
                  , cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', true),'') as integer)        as Burglary
                  , cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', true),'') as integer)         as Larceny
                  , cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', true),'') as integer)         as AutoTheft
                  , lower(json_extract_path_text(f.response_data, 'Address','StreetAddress1', true))                                     as street
                  , cast(right('00000' + json_extract_path_text(f.response_data, 'Address','Zip', true), 5) as varchar(5))          as zip_code_5digit
                  , creation_time
            from insurance_data_gateway_svc_prod.third_parties_data as f
            where provider = 'Verisk' and json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', true) is not null

            UNION

            select
                 cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', true), '') as integer)   as Arson
                , cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', true), '') as integer)   as Burglary
                , cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', true), '') as integer)   as Larceny
                , cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', true), '') as integer)   as AutoTheft
                , lower(street) as street
                , cast(right('00000' + zip_code, 5) as varchar(5))                               as zip_code_5digit
                , creation_time
            from riskmgmt_svc_prod.verisk_property_risk_request_response
            where json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', true) is not null
            )) where rnk = 1
    ),

    -- CTE 10: Get the latest business address to ensure a clean join for crime scores.
    latest_address AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY business_id ORDER BY creation_time DESC) as rnk
            FROM nimi_svc_prod.addresses
        )
        WHERE rnk = 1
    ),

    -- CTE 11: Consolidate all static policy-level attributes (pre-deduplication).
    policies_table_pre AS (
        SELECT
            a.policy_id, a.policy_reference, a.business_id,
            CAST(a.start_date AS DATE) AS eff_date,
            CAST(a.end_date AS DATE) AS exp_date,
            EXTRACT(YEAR FROM a.start_date) * 100 + EXTRACT(MONTH FROM a.start_date) AS eff_period,
            a.state, a.county, a.status_name, a.new_renewal, a.cob_name, a.cob_group, a.cob_industry, a.carrier_name, a.channel,
            pt.business_ownership_structure,
            pt.revenue_next_12_months AS revenue_in_12_months,
            pt.num_of_owners,
            EXTRACT(YEAR FROM a.start_date) - pt.year_business_started AS years_in_bus,
            ca.prior_loss_hist,
            CASE
                WHEN regexp_instr(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE), '^[0-9\.]+$') > 0
                THEN cast(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE) AS NUMERIC)
                ELSE NULL
            END AS blanket_equip_occ_lim,
            clm.distribution_channel_attributed as distribution_channel,
            clm.agency_aggregator as agency_aggregator_name,
            clm.agency_type as current_agencytype,
            clm.package as highest_status_package,
            clm.policy_status_name as highest_status_name,
            pol.yearly_premium,
            pol.tria,
            pol.surcharges,
            cs.credit_score,
            vt.Arson, vt.Burglary, vt.Larceny, vt.AutoTheft,
            sub.num_of_employees,
            CASE
                WHEN regexp_instr(json_extract_path_text(sub.questionnaire_answers, 'location.subcontractors_cost', TRUE), '^[0-9\.]+$') > 0
                THEN cast(json_extract_path_text(sub.questionnaire_answers, 'location.subcontractors_cost', TRUE) AS NUMERIC)
                ELSE NULL
            END AS subcont_costs,
            pt.package_name -- Added package name from policy transactions
        FROM reporting.gaap_snapshots_asl AS a
        LEFT JOIN policy_transactions_deduped pt ON a.policy_id = pt.policy_id
        LEFT JOIN im_quotes_deduped iq ON pt.last_quote_id = iq.quote_id
        LEFT JOIN external_dwh.im_submissions sub ON iq.offer_id = sub.first_offer_id -- CORRECTED: Join directly
        LEFT JOIN s3_im_deduped s3 ON iq.job_id = s3.job_id
        LEFT JOIN claims_all ca ON a.policy_reference = ca.policy_reference
        LEFT JOIN dwh.company_level_metrics_ds clm ON a.policy_reference = clm.policy_reference
        LEFT JOIN nimi_svc_prod.policies pol ON a.policy_id = pol.policy_id
        LEFT JOIN latest_credit_score cs ON a.business_id = cs.business_id
        LEFT JOIN latest_address addr ON a.business_id = addr.business_id
        LEFT JOIN verisk_table vt ON LOWER(addr.street_address) = vt.street AND cast(right('00000' + addr.zip_code, 5) as varchar(5)) = vt.zip_code_5digit
        WHERE a.lob = 'IM'
          AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
          AND a.trans IN ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
          AND a.date <= (SELECT max(eval_date_full) FROM dates_table)
    ),

    -- CTE 12: Deduplicate policy attributes.
    policies_table AS (
        SELECT *
        FROM (
            SELECT *, row_number() OVER (PARTITION BY policy_reference ORDER BY eff_date DESC, status_name) AS rn
            FROM policies_table_pre
        )
        WHERE rn = 1
    ),

    -- CTE 13: Attach policy attributes to loss data.
    attach_attributes AS (
        SELECT
            a.*,
            b.blanket_equip_occ_lim, b.years_in_bus, b.prior_loss_hist, b.revenue_in_12_months, b.num_of_owners, b.business_ownership_structure,
            b.distribution_channel, b.agency_aggregator_name, b.current_agencytype, b.highest_status_package, b.highest_status_name,
            b.yearly_premium, b.tria, b.surcharges, b.credit_score,
            b.Arson, b.Burglary, b.Larceny, b.AutoTheft,
            b.new_renewal, b.cob_industry, b.channel, b.carrier_name,
            b.num_of_employees, b.subcont_costs,
            b.package_name -- Added package name to the final attributes
        FROM loss_exposure_table AS a
        LEFT JOIN policies_table AS b ON a.policy_reference = b.policy_reference
    ),

    -- CTE 14: Summarize claims data and prepare for incremental calculations.
    summary_claims AS (
        SELECT DISTINCT
               a.*,
               CASE WHEN a.cat_ind = 'CAT' THEN 0 ELSE a.incurred END AS incurred_non_cat,
               CASE WHEN a.cat_ind = 'CAT' THEN 0 ELSE a.paid END AS paid_non_cat,
               count(a.exposure_id) OVER (PARTITION BY a.exposure_id, a.eval_date) AS exp_cnt,
               CASE WHEN a.paid > 0 THEN count(a.exposure_id) OVER (PARTITION BY a.exposure_id, a.eval_date) ELSE 0 END AS exp_cnt_paid,
               CASE WHEN a.exposure_status = 'closed' THEN count(a.exposure_id) OVER (PARTITION BY a.exposure_id, a.eval_date) ELSE 0 END AS exp_cnt_closed,
               CASE WHEN a.paid = 0 AND a.exposure_status = 'closed' THEN count(a.exposure_id) OVER (PARTITION BY a.exposure_id, a.eval_date) ELSE 0 END AS exp_cnt_closed_0paid,
               CASE WHEN a.cat_ind = 'non-CAT' THEN count(a.exposure_id) OVER (PARTITION BY a.exposure_id, a.eval_date) ELSE 0 END AS exp_cnt_non_cat,
               cast(isnull(1.0 / nullif((count(a.exposure_id) OVER (PARTITION BY a.claim_id, a.eval_date)), 0), 0) AS DECIMAL(15, 6)) AS count_claim,
               cast(isnull(1.0 / nullif((CASE WHEN a.claim_lvl_eval_paid > 0 THEN (count(a.exposure_id) OVER (PARTITION BY a.claim_id, a.eval_date)) ELSE 0 END), 0), 0) AS DECIMAL(15, 6)) AS count_claim_paid,
               cast(isnull(1.0 / nullif((CASE WHEN a.claim_status = 'closed' THEN (count(a.exposure_id) OVER (PARTITION BY a.claim_id, a.eval_date)) ELSE 0 END), 0), 0) AS DECIMAL(15, 6)) AS count_claim_closed,
               cast(isnull(1.0 / nullif((CASE WHEN a.claim_status = 'closed' AND a.claim_lvl_eval_paid = 0 THEN (count(a.exposure_id) OVER (PARTITION BY a.claim_id, a.eval_date)) ELSE 0 END), 0), 0) AS DECIMAL(15, 6)) AS count_claim_closed_0paid,
               cast(isnull(1.0 / nullif((CASE WHEN a.cat_ind = 'non-CAT' THEN (count(a.exposure_id) OVER (PARTITION BY a.claim_id, a.eval_date)) ELSE 0 END), 0), 0) AS DECIMAL(15, 6)) AS count_claim_non_cat
        FROM attach_attributes AS a
        WHERE a.eval_date >= a.eff_period
    )

-- Final SELECT: Calculate incremental values and select all columns for the final table.
SELECT
    f.*,
    CAST(isnull(paid - lag(paid) OVER (PARTITION BY exposure_id ORDER BY eval_date), paid) AS DECIMAL(15, 2)) AS paid_inc,
    CAST(isnull(incurred - lag(incurred) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred) AS DECIMAL(15, 2)) AS incurred_inc,
    CAST(isnull(incurred_non_cat - lag(incurred_non_cat) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred_non_cat) AS DECIMAL(15, 2)) AS incurred_non_cat_inc,
    CAST(isnull(incurred_loss_only - lag(incurred_loss_only) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred_loss_only) AS DECIMAL(15, 2)) AS incurred_loss_only_inc,
    CAST(isnull(incurred_dcc - lag(incurred_dcc) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred_dcc) AS DECIMAL(15, 2)) AS incurred_dcc_inc,
    CAST(isnull(incurred_ao - lag(incurred_ao) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred_ao) AS DECIMAL(15, 2)) AS incurred_ao_inc,
    CAST(isnull(incurred_ss - lag(incurred_ss) OVER (PARTITION BY exposure_id ORDER BY eval_date), incurred_ss) AS DECIMAL(15, 2)) AS incurred_ss_inc,
    isnull(exp_cnt - lag(exp_cnt) OVER (PARTITION BY exposure_id ORDER BY eval_date), exp_cnt) AS exp_cnt_inc,
    isnull(exp_cnt_paid - lag(exp_cnt_paid) OVER (PARTITION BY exposure_id ORDER BY eval_date), exp_cnt_paid) AS exp_cnt_paid_inc,
    isnull(exp_cnt_closed - lag(exp_cnt_closed) OVER (PARTITION BY exposure_id ORDER BY eval_date), exp_cnt_closed) AS exp_cnt_closed_inc,
    isnull(exp_cnt_closed_0paid - lag(exp_cnt_closed_0paid) OVER (PARTITION BY exposure_id ORDER BY eval_date), exp_cnt_closed_0paid) AS exp_cnt_closed_0paid_inc,
    isnull(exp_cnt_non_cat - lag(exp_cnt_non_cat) OVER (PARTITION BY exposure_id ORDER BY eval_date), exp_cnt_non_cat) AS exp_cnt_non_cat_inc,
    CAST(isnull(count_claim - lag(count_claim) OVER (PARTITION BY exposure_id ORDER BY eval_date), count_claim) AS DECIMAL(15, 6)) AS count_claim_inc,
    CAST(isnull(count_claim_paid - lag(count_claim_paid) OVER (PARTITION BY exposure_id ORDER BY eval_date), count_claim_paid) AS DECIMAL(15, 6)) AS count_claim_paid_inc,
    CAST(isnull(count_claim_closed - lag(count_claim_closed) OVER (PARTITION BY exposure_id ORDER BY eval_date), count_claim_closed) AS DECIMAL(15, 6)) AS count_claim_closed_inc,
    CAST(isnull(count_claim_closed_0paid - lag(count_claim_closed_0paid) OVER (PARTITION BY exposure_id ORDER BY eval_date), count_claim_closed_0paid) AS DECIMAL(15, 6)) AS count_claim_closed_0paid_inc,
    CAST(isnull(count_claim_non_cat - lag(count_claim_non_cat) OVER (PARTITION BY exposure_id ORDER BY eval_date), count_claim_non_cat) AS DECIMAL(15, 6)) AS count_claim_non_cat_inc
FROM summary_claims f
LIMIT 20;