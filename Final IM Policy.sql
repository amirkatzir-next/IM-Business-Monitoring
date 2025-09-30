-- Use: for policy monitoring, source to business monitoring dashboard
-- Version 3.3: Final version. Removed fields causing errors and sourced new fields from core tables as requested.

WITH
    -- CTE 1: Define the evaluation date windows for the analysis.
    dates_table AS (
        SELECT DISTINCT
               EXTRACT(YEAR FROM a.monthlastday) * 100 + EXTRACT(MONTH FROM a.monthlastday) AS eval_date,
               a.monthlastday AS eval_date_full
        FROM bi_workspace.periods AS a
        WHERE a.date <= last_day(date_add('month', -1, current_date))
          AND a.date >= '2020-07-30'
    ),

    -- CTEs 2-4: Loss Data Aggregation
    loss_exposure_table AS (
        SELECT DISTINCT
               CASE WHEN a.tpa_name = 'Gallagher Bassett' THEN substring(a.claim_number, 1, 13) ELSE a.claim_number END AS claim_number,
               CASE WHEN a.tpa_name = 'Gallagher Bassett' THEN substring(a.claim_id, 1, 13) ELSE a.claim_id END AS claim_id,
               a.exposure_number, a.exposure_id, a.policy_reference, c.policy_id,
               CAST(a.policy_start_date AS DATE) AS eff_date, CAST(a.policy_end_date AS DATE) AS exp_date, a.date_of_loss AS acc_date, a.date_submitted AS rpt_date,
               (rpt_date - acc_date) AS rpt_lag, CASE WHEN (rpt_date - acc_date) > 50 THEN 50 ELSE (rpt_date - acc_date) END AS rpt_lag_capped,
               EXTRACT(YEAR FROM a.policy_start_date) * 100 + EXTRACT(MONTH FROM a.policy_start_date) AS eff_period, EXTRACT(YEAR FROM a.policy_end_date) * 100 + EXTRACT(MONTH FROM a.policy_end_date) AS exp_period,
               EXTRACT(YEAR FROM a.date_of_loss) * 100 + EXTRACT(MONTH FROM a.date_of_loss) AS acc_period, EXTRACT(YEAR FROM a.date) * 100 + EXTRACT(MONTH FROM a.date) AS eval_date,
               CASE WHEN (SUM(isnull(a.loss_reserve_total,0) + isnull(a.expense_dcc_reserve_total,0) + isnull(a.expense_ao_reserve_total,0) + isnull(a.recovery_salvage_reserve_total,0) + isnull(a.recovery_subrogation_reserve_total,0)) OVER (PARTITION BY a.claim_id, (EXTRACT(YEAR FROM a.date) * 100 + EXTRACT(MONTH FROM a.date)))) > 0 THEN 'Open' ELSE 'closed' END AS claim_status,
               CASE WHEN (SUM(isnull(a.loss_reserve_total,0) + isnull(a.expense_dcc_reserve_total,0) + isnull(a.expense_ao_reserve_total,0) + isnull(a.recovery_salvage_reserve_total,0) + isnull(a.recovery_subrogation_reserve_total,0)) OVER (PARTITION BY a.exposure_id, (EXTRACT(YEAR FROM a.date) * 100 + EXTRACT(MONTH FROM a.date)))) > 0 THEN 'Open' ELSE 'closed' END AS exposure_status,
               CASE WHEN a.coverage IN ('BLANKET_EQUIPMENT', 'BLANKET_MISC') THEN 'BLANKET_EQUIPMENT' ELSE a.coverage END AS coverage,
               a.loss_cause_type_name, a.marketing_cob_group AS cob_group, a.cob_name, a.location_of_loss, a.business_state AS state,
               CASE WHEN (a.catastrophe_code IS NULL OR a.catastrophe_code = '' OR a.catastrophe_code IN ('1111/BFA','2212','NRP-X-R-0-0','2222/XOL','CD','SIU','Bad Faith Suit')) THEN 'non-CAT' ELSE 'CAT' END AS cat_ind,
               isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0) AS paid,
               (isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0)) + isnull(a.loss_reserve_total, 0) + isnull(a.expense_ao_reserve_total, 0) + isnull(a.expense_dcc_reserve_total, 0) AS incurred,
               isnull(a.loss_paid_total, 0) + isnull(a.loss_reserve_total, 0) AS incurred_loss_only, isnull(a.expense_dcc_paid_total, 0) + isnull(a.expense_dcc_reserve_total, 0) AS incurred_dcc,
               isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_ao_reserve_total, 0) AS incurred_ao, isnull(a.recovery_subrogation_collected_total, 0) + isnull(a.recovery_salvage_collected_total, 0) AS incurred_ss,
               CASE WHEN a.loss_cause_type_name IN ('FIRE','FIRE_FOLLOWING_ANOTHER_PERIL','SMOKE','LIGHTNING','EXPLOSION') THEN 1 ELSE 0 END AS peril_fire,
               CASE WHEN a.loss_cause_type_name IN ('THEFT_OTHER','THEFT_EMPLOYEE_DISHONESTY','VANDALISM','CIVIL_COMMOTION') THEN 1 ELSE 0 END AS peril_theft,
               CASE WHEN a.loss_cause_type_name LIKE 'EQUIPMENT_BREAKDOWN%' THEN 1 ELSE 0 END AS peril_eqbd, CASE WHEN a.coverage = 'BLANKET_EQUIP' THEN 1 ELSE 0 END AS cov_blanket,
               CASE WHEN a.coverage IN ('TRAILERS_AND_CONTENTS', 'FUEL_ACCESSORIES_AND_SPARE_PARTS', 'RENTAL_REIMBURSEMENT', 'DEBRIS_REMOVAL', 'REWARDS') OR (a.business_state <> 'CA' AND a.coverage = 'EMPLOYEE_TOOlS_AND_CLOTHING') THEN 1 ELSE 0 END AS cov_addl,
               CASE WHEN a.coverage IN ('EQUIPMENT_BORROWED_FROM_OTHERS', 'BLANKET_MISC') OR (a.business_state = 'CA' AND a.coverage = 'EMPLOYEE_TOOLS_AND_CLOTHING') THEN 1 ELSE 0 END AS cov_opt
        FROM dwh.all_claims_financial_changes_ds AS a
        LEFT JOIN nimi_svc_prod.policies AS c ON c.policy_reference = a.policy_reference
        WHERE a.lob = 'IM' AND a.date IN (SELECT DISTINCT eval_date_full FROM dates_table) AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
    ),
    loss_table AS (SELECT DISTINCT a.claim_number, a.claim_id, a.policy_reference, a.policy_id, a.eff_date, a.exp_date, a.acc_date, a.eff_period, a.exp_period, a.acc_period, a.eval_date, a.claim_status, a.cat_ind, sum(a.paid) AS paid_cum, sum(a.incurred) AS incurred_cum, CASE WHEN a.cat_ind = 'CAT' THEN 0 ELSE sum(a.incurred) END AS incurred_cum_non_cat, sum(incurred_loss_only) AS incurred_cum_loss_only, sum(incurred_dcc) AS incurred_cum_dcc, sum(incurred_ao) AS incurred_cum_ao, sum(incurred_ss) AS incurred_cum_ss, sum(CASE WHEN peril_fire = 1 THEN incurred ELSE 0 END) AS incurred_peril_fire, sum(CASE WHEN peril_theft = 1 THEN incurred ELSE 0 END) AS incurred_peril_theft, sum(CASE WHEN peril_eqbd = 1 THEN incurred ELSE 0 END) AS incurred_peril_eqbd, sum(CASE WHEN cov_blanket = 1 THEN incurred ELSE 0 END) AS incurred_cov_blanket, sum(CASE WHEN cov_addl = 1 THEN incurred ELSE 0 END) AS incurred_cov_addl, sum(CASE WHEN cov_opt = 1 THEN incurred ELSE 0 END) AS incurred_cov_opt, count(DISTINCT a.claim_number) AS count_claim, count(DISTINCT CASE WHEN paid > 0 THEN a.claim_number ELSE NULL END) AS count_claim_paid, count(DISTINCT CASE WHEN claim_status = 'closed' THEN a.claim_number ELSE NULL END) AS count_claim_closed, count(DISTINCT CASE WHEN paid = 0 AND claim_status = 'closed' THEN a.claim_number ELSE NULL END) AS count_claim_closed_0paid, count(a.exposure_number) AS exp_cnt, sum(CASE WHEN paid > 0 THEN 1 ELSE 0 END) AS exp_cnt_paid, sum(CASE WHEN a.exposure_status = 'closed' THEN 1 ELSE 0 END) AS exp_cnt_closed, sum(CASE WHEN a.exposure_status = 'closed' AND a.paid = 0 THEN 1 ELSE 0 END) AS exp_cnt_closed_0paid, count(DISTINCT CASE WHEN cat_ind = 'non-CAT' THEN a.claim_number ELSE NULL END) AS count_claim_non_cat, count(DISTINCT CASE WHEN cat_ind = 'non-CAT' AND paid > 0 THEN a.claim_number ELSE NULL END) AS count_claim_paid_non_cat, count(DISTINCT CASE WHEN cat_ind = 'non-CAT' AND claim_status = 'closed' THEN a.claim_number ELSE NULL END) AS count_claim_closed_non_cat, count(DISTINCT CASE WHEN cat_ind = 'non-CAT' AND paid = 0 AND claim_status = 'closed' THEN a.claim_number ELSE NULL END) AS count_claim_closed_0paid_non_cat, sum(CASE WHEN cat_ind = 'non-CAT' THEN 1 ELSE 0 END) AS exp_cnt_non_cat, sum(CASE WHEN cat_ind = 'non-CAT' AND paid > 0 THEN 1 ELSE 0 END) AS exp_cnt_paid_non_cat, sum(CASE WHEN cat_ind = 'non-CAT' AND a.exposure_status = 'closed' THEN 1 ELSE 0 END) AS exp_cnt_closed_non_cat, sum(CASE WHEN cat_ind = 'non-CAT' AND a.exposure_status = 'closed' AND a.paid = 0 THEN 1 ELSE 0 END) AS exp_cnt_closed_0paid_non_cat, count(DISTINCT CASE WHEN peril_fire = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_peril_fire, count(DISTINCT CASE WHEN peril_theft = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_peril_theft, count(DISTINCT CASE WHEN peril_eqbd = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_peril_eqbd, count(DISTINCT CASE WHEN peril_fire = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_peril_fire, count(DISTINCT CASE WHEN peril_theft = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_peril_theft, count(DISTINCT CASE WHEN peril_eqbd = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_peril_eqbd, count(DISTINCT CASE WHEN cov_blanket = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_cov_blanket, count(DISTINCT CASE WHEN cov_addl = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_cov_addl, count(DISTINCT CASE WHEN cov_opt = 1 THEN a.claim_id ELSE NULL END) AS claim_cnt_cov_opt, count(DISTINCT CASE WHEN cov_blanket = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_cov_blanket, count(DISTINCT CASE WHEN cov_addl = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_cov_addl, count(DISTINCT CASE WHEN cov_opt = 1 THEN a.exposure_id ELSE NULL END) AS exp_cnt_cov_opt FROM loss_exposure_table AS a GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
    loss_table_agg AS (SELECT policy_reference, eval_date, SUM(paid_cum) AS paid_cum, SUM(incurred_cum) AS incurred_cum, SUM(incurred_cum_non_cat) AS incurred_cum_non_cat, SUM(incurred_cum_loss_only) AS incurred_cum_loss_only, SUM(incurred_cum_dcc) AS incurred_cum_dcc, SUM(incurred_cum_ao) AS incurred_cum_ao, SUM(incurred_cum_ss) AS incurred_cum_ss, SUM(incurred_peril_fire) AS incurred_peril_fire, SUM(incurred_peril_theft) AS incurred_peril_theft, SUM(incurred_peril_eqbd) AS incurred_peril_eqbd, SUM(incurred_cov_blanket) AS incurred_cov_blanket, SUM(incurred_cov_addl) AS incurred_cov_addl, SUM(incurred_cov_opt) AS incurred_cov_opt, SUM(count_claim) AS count_claim, SUM(count_claim_paid) AS count_claim_paid, SUM(count_claim_closed) AS count_claim_closed, SUM(count_claim_closed_0paid) AS count_claim_closed_0paid, SUM(count_claim_non_cat) AS count_claim_non_cat, SUM(exp_cnt) AS exp_cnt, SUM(exp_cnt_paid) AS exp_cnt_paid, SUM(exp_cnt_closed) AS exp_cnt_closed, SUM(exp_cnt_closed_0paid) AS exp_cnt_closed_0paid, SUM(exp_cnt_non_cat) AS exp_cnt_non_cat, SUM(claim_cnt_peril_fire) AS claim_cnt_peril_fire, SUM(claim_cnt_peril_theft) AS claim_cnt_peril_theft, SUM(claim_cnt_peril_eqbd) AS claim_cnt_peril_eqbd, SUM(claim_cnt_cov_blanket) AS claim_cnt_cov_blanket, SUM(claim_cnt_cov_addl) AS claim_cnt_cov_addl, SUM(claim_cnt_cov_opt) AS claim_cnt_cov_opt, SUM(exp_cnt_peril_fire) AS exp_cnt_peril_fire, SUM(exp_cnt_peril_theft) AS exp_cnt_peril_theft, SUM(exp_cnt_peril_eqbd) AS exp_cnt_peril_eqbd, SUM(exp_cnt_cov_blanket) AS exp_cnt_cov_blanket, SUM(exp_cnt_cov_addl) AS exp_cnt_cov_addl, SUM(exp_cnt_cov_opt) AS exp_cnt_cov_opt, SUM(count_claim_paid_non_cat) AS count_claim_paid_non_cat, SUM(count_claim_closed_non_cat) AS count_claim_closed_non_cat, SUM(count_claim_closed_0paid_non_cat) AS count_claim_closed_0paid_non_cat, SUM(exp_cnt_paid_non_cat) AS exp_cnt_paid_non_cat, SUM(exp_cnt_closed_non_cat) AS exp_cnt_closed_non_cat, SUM(exp_cnt_closed_0paid_non_cat) AS exp_cnt_closed_0paid_non_cat FROM loss_table GROUP BY 1, 2),

    -- CTEs 5 & 6: Premium Calculation
    premium_table_frame AS (SELECT DISTINCT a.policy_reference, b.eval_date, b.eval_date_full FROM reporting.gaap_snapshots_asl AS a CROSS JOIN dates_table AS b WHERE a.date <= (SELECT max(eval_date_full) FROM dates_table) AND a.lob = 'IM' AND a.carrier_name in ('next-insurance', 'next-carrier', 'national-specialty') AND a.trans in ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')),
    premium_table AS (SELECT a.policy_reference, a.eval_date, a.eval_date_full, sum(CASE WHEN (EXTRACT(YEAR FROM b.date) * 100 + EXTRACT(MONTH FROM b.date) = a.eval_date) AND trans IN ('monthly earned premium', 'monthly earned premium endorsement') THEN b.dollar_amount ELSE 0 END) AS ep, sum(ep) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ep_cum, sum(CASE WHEN (EXTRACT(YEAR FROM b.date) * 100 + EXTRACT(MONTH FROM b.date) = a.eval_date) AND trans IN ('New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal') THEN b.dollar_amount ELSE 0 END) AS wp, sum(wp) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wp_cum FROM premium_table_frame AS a LEFT JOIN reporting.gaap_snapshots_asl AS b ON b.policy_reference = a.policy_reference WHERE b.date <= (SELECT max(eval_date_full) FROM dates_table) AND b.lob = 'IM' AND b.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty') AND b.trans IN ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal') GROUP BY 1, 2, 3),

    -- CTEs 7-13: Attribute Helper Tables
    claims_all AS (WITH latest_claims AS (SELECT policy_reference, coalesce(loss_paid_total, 0) AS loss_paid, row_number() OVER (PARTITION BY policy_reference, claim_id, exposure_id ORDER BY date DESC) AS rn FROM dwh.all_claims_financial_changes_ds WHERE lob = 'IM') SELECT p.business_id, lc.policy_reference, CASE WHEN SUM(lc.loss_paid) > 0 THEN 'Y' ELSE 'N' END AS prior_loss_hist FROM latest_claims lc JOIN nimi_svc_prod.policies p ON p.policy_reference = lc.policy_reference WHERE lc.rn = 1 GROUP BY 1, 2),
    s3_im_deduped AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY dateid DESC, update_time DESC) AS rn FROM s3_operational.rating_svc_prod_calculations WHERE lob = 'IM') WHERE rn = 1),
    im_quotes_deduped AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY creation_time DESC) AS rn FROM external_dwh.im_quotes) WHERE rn = 1),
    policy_transactions_deduped AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY tx_effective_date DESC, policy_transaction_id DESC) AS rn FROM prod.dwh.policy_transactions WHERE transaction_type = 'BIND' AND lob = 'IM') WHERE rn = 1),
    latest_credit_score AS (SELECT business_id, score AS credit_score FROM (SELECT business_id, score, rank() OVER (PARTITION BY business_id ORDER BY creation_time DESC) AS rnk FROM riskmgmt_svc_prod.risk_score_result WHERE score IS NOT NULL) WHERE rnk = 1),
    verisk_table AS (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY street, zip_code_5digit ORDER BY creation_time DESC) AS rnk FROM (SELECT cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE),'') AS INTEGER) AS Arson, cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', TRUE),'') AS INTEGER) AS Burglary, cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', TRUE),'') AS INTEGER) AS Larceny, cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', TRUE),'') AS INTEGER) AS AutoTheft, lower(json_extract_path_text(f.response_data, 'Address','StreetAddress1', TRUE)) AS street, cast(right('00000' + json_extract_path_text(f.response_data, 'Address','Zip', TRUE), 5) AS VARCHAR(5)) AS zip_code_5digit, creation_time FROM insurance_data_gateway_svc_prod.third_parties_data AS f WHERE provider = 'Verisk' AND json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE) IS NOT NULL UNION SELECT cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Arson, cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Burglary, cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Larceny, cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS AutoTheft, lower(street) AS street, cast(right('00000' + zip_code, 5) AS VARCHAR(5)) AS zip_code_5digit, creation_time FROM riskmgmt_svc_prod.verisk_property_risk_request_response WHERE json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE) IS NOT NULL)) WHERE rnk = 1),
    latest_address AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY business_id ORDER BY creation_time DESC) AS rnk FROM nimi_svc_prod.addresses) WHERE rnk = 1),

    -- CTE 14: Consolidate policy attributes
    policies_table_pre AS (
        SELECT DISTINCT
            a.policy_id, a.policy_reference, a.business_id,
            CAST(a.start_date AS DATE) AS eff_date, CAST(a.end_date AS DATE) AS exp_date,
            EXTRACT(YEAR FROM a.start_date) * 100 + EXTRACT(MONTH FROM a.start_date) AS eff_period,
            a.state, a.county, a.status_name, a.new_renewal, a.cob_name, a.cob_group,
            isnull(nullif(a.cob_industry, ''), 'Construction') AS cob_industry, a.carrier_name, a.channel,
            pt.business_ownership_structure, pt.revenue_next_12_months AS revenue_in_12_months, pt.num_of_owners,
            EXTRACT(YEAR FROM a.start_date) - pt.year_business_started AS years_in_bus, ca.prior_loss_hist,
            CASE WHEN regexp_instr(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE), '^[0-9\.]+$') > 0
                 THEN cast(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE) AS NUMERIC)
                 ELSE NULL END AS blanket_equip_occ_lim,
            clm.distribution_channel_attributed AS distribution_channel, clm.agency_aggregator AS agency_aggregator_name, clm.agency_type AS current_agencytype,
            clm.package AS highest_status_package, clm.policy_status_name AS highest_status_name,
            pol.yearly_premium, pol.tria, pol.surcharges, cs.credit_score, vt.Arson, vt.Burglary, vt.Larceny, vt.AutoTheft, sub.num_of_employees,
            CASE WHEN regexp_instr(json_extract_path_text(sub.questionnaire_answers, 'location.subcontractors_cost', TRUE), '^[0-9\.]+$') > 0
                 THEN cast(json_extract_path_text(sub.questionnaire_answers, 'location.subcontractors_cost', TRUE) AS NUMERIC)
                 ELSE NULL END AS subcont_costs,

            -- START: Added fields
            pt.package_name,
            a.affiliate_name,
            date_diff('day',pol.bind_date::date,a.start_date::date) as days_between_bind_to_policy_start
            -- END: Added fields
        FROM reporting.gaap_snapshots_asl AS a
        LEFT JOIN policy_transactions_deduped pt ON a.policy_id = pt.policy_id
        LEFT JOIN im_quotes_deduped iq ON pt.last_quote_id = iq.quote_id
        LEFT JOIN external_dwh.im_submissions sub ON iq.offer_id = sub.first_offer_id
        LEFT JOIN s3_im_deduped s3 ON iq.job_id = s3.job_id
        LEFT JOIN claims_all ca ON a.policy_reference = ca.policy_reference
        LEFT JOIN dwh.company_level_metrics_ds clm ON a.policy_reference = clm.policy_reference
        LEFT JOIN nimi_svc_prod.policies pol ON a.policy_id = pol.policy_id
        LEFT JOIN latest_credit_score cs ON a.business_id = cs.business_id
        LEFT JOIN latest_address addr ON a.business_id = addr.business_id
        LEFT JOIN verisk_table vt ON LOWER(addr.street_address) = vt.street AND cast(right('00000' + addr.zip_code, 5) AS VARCHAR(5)) = vt.zip_code_5digit
        WHERE a.lob = 'IM' AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
          AND a.date <= (SELECT max(eval_date_full) FROM dates_table)
          AND a.trans IN ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    ),

    -- CTE 15: De-duplicate policy attributes
    policies_table AS (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_reference ORDER BY eff_date DESC, status_name) AS rn
            FROM policies_table_pre
        ) WHERE rn = 1
    ),

    -- *** START MODIFICATION ***
    -- CTE 16: NEW HELPER - Get policy dates directly from loss data as a fallback
    policy_dates_from_loss AS (
        SELECT
            policy_reference,
            MIN(eff_date) AS eff_date,
            MIN(exp_date) AS exp_date
        FROM loss_exposure_table
        GROUP BY 1
    ),

    -- CTE 17: Create a unified frame of all policy-month combinations with any activity
    policy_monthly_frame AS (
        SELECT policy_reference, eval_date FROM premium_table
        UNION
        SELECT policy_reference, eval_date FROM loss_table_agg
    ),

    -- CTE 18: Combine attributes and premium data onto the unified frame, with fallback logic
    policy_table_combined AS (
        SELECT
            frame.policy_reference,
            frame.eval_date,
            COALESCE(prem.eval_date_full, d.eval_date_full) AS eval_date_full,
            -- Attributes from policies_table, with fallback for key dates
            attr.policy_id,
            attr.business_id,
            -- Use attribute effective date, but fall back to the date from the loss record if needed
            COALESCE(attr.eff_date, loss_dates.eff_date) as eff_date,
            COALESCE(attr.exp_date, loss_dates.exp_date) as exp_date,
            attr.eff_period, attr.state,
            attr.county, attr.status_name, attr.new_renewal, attr.cob_name, attr.cob_group, attr.cob_industry,
            attr.carrier_name, attr.channel, attr.business_ownership_structure, attr.revenue_in_12_months,
            attr.num_of_owners, attr.years_in_bus, attr.prior_loss_hist, attr.blanket_equip_occ_lim,
            attr.distribution_channel, attr.agency_aggregator_name, attr.current_agencytype,
            attr.highest_status_package, attr.highest_status_name, attr.yearly_premium, attr.tria,
            attr.surcharges, attr.credit_score, attr.Arson, attr.Burglary, attr.Larceny, attr.AutoTheft,
            attr.num_of_employees, attr.subcont_costs,

            -- START: Added fields
            attr.package_name,
            attr.affiliate_name,
            attr.days_between_bind_to_policy_start,
            -- END: Added fields

            -- Premium data
            prem.ep_cum,
            prem.wp_cum
        FROM
            policy_monthly_frame AS frame
        LEFT JOIN policies_table AS attr ON frame.policy_reference = attr.policy_reference
        LEFT JOIN premium_table AS prem ON frame.policy_reference = prem.policy_reference AND frame.eval_date = prem.eval_date
        LEFT JOIN dates_table AS d ON frame.eval_date = d.eval_date
        LEFT JOIN policy_dates_from_loss AS loss_dates ON frame.policy_reference = loss_dates.policy_reference
    )
    -- *** END MODIFICATION ***

-- Final SELECT: Calculate earned exposure and join final loss data
SELECT a.*,
       1 AS policy_count,
       CASE WHEN datediff(day, a.eff_date, a.eval_date_full) > datediff(day, a.eff_date, a.exp_date)
            THEN cast(datediff(day, a.eff_date, a.exp_date) / cast(365 AS DECIMAL(10, 6)) AS DECIMAL(10,6))
            ELSE cast(datediff(day, a.eff_date, a.eval_date_full) / cast(365 AS DECIMAL(10, 6)) AS DECIMAL(10,6)) END AS ee_cum_py,
       cast(datediff(day, a.eff_date, a.exp_date) / cast(365 AS DECIMAL(10, 6)) AS DECIMAL(10,6)) AS we_cum_py,
       isnull(policy_count - lag(policy_count) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), policy_count) AS policy_count_inc,
       cast(isnull(a.ep_cum - lag(a.ep_cum) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), a.ep_cum) AS DECIMAL(10,6)) AS ep_inc,
       cast(isnull(a.wp_cum - lag(a.wp_cum) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), a.wp_cum) AS DECIMAL(10,6)) AS wp_inc,
       cast(isnull(ee_cum_py - lag(ee_cum_py) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), ee_cum_py) AS DECIMAL(10, 6)) AS ee_cum_py_inc,
       cast(c.paid_cum AS DECIMAL(15,2)) AS paid_cum, cast(c.incurred_cum AS DECIMAL(15,2)) AS incurred_cum, cast(c.incurred_cum_non_cat AS DECIMAL(15,2)) AS incurred_cum_non_cat,
       cast(c.incurred_cum_loss_only AS DECIMAL(15,2)) AS incurred_cum_loss_only, cast(c.incurred_cum_dcc AS DECIMAL(15,2)) AS incurred_cum_dcc, cast(c.incurred_cum_ao AS DECIMAL(15,2)) AS incurred_cum_ao, cast(c.incurred_cum_ss AS DECIMAL(15,2)) AS incurred_cum_ss,
       cast(c.incurred_peril_fire AS DECIMAL(15,2)) AS incurred_peril_fire, cast(c.incurred_peril_theft AS DECIMAL(15,2)) AS incurred_peril_theft, cast(c.incurred_peril_eqbd AS DECIMAL(15,2)) AS incurred_peril_eqbd,
       cast(c.incurred_cov_blanket AS DECIMAL(15,2)) AS incurred_cov_blanket, cast(c.incurred_cov_addl AS DECIMAL(15,2)) AS incurred_cov_addl, cast(c.incurred_cov_opt AS DECIMAL(15,2)) AS incurred_cov_opt,
       cast(c.count_claim AS DECIMAL(15,2)) AS count_claim, cast(c.count_claim_paid AS DECIMAL(15,2)) AS count_claim_paid, cast(c.count_claim_closed AS DECIMAL(15,2)) AS count_claim_closed, cast(c.count_claim_closed_0paid AS DECIMAL(15,2)) AS count_claim_closed_0paid,
       cast(c.count_claim_non_cat AS DECIMAL(15,2)) AS count_claim_non_cat, cast(c.exp_cnt AS DECIMAL(15,2)) AS exp_cnt, cast(c.exp_cnt_paid AS DECIMAL(15,2)) AS exp_cnt_paid, cast(c.exp_cnt_closed AS DECIMAL(15,2)) AS exp_cnt_closed, cast(c.exp_cnt_closed_0paid AS DECIMAL(15,2)) AS exp_cnt_closed_0paid,
       cast(c.exp_cnt_non_cat AS DECIMAL(15,2)) AS exp_cnt_non_cat, cast(c.claim_cnt_peril_fire AS DECIMAL(15,2)) AS claim_cnt_peril_fire, cast(c.claim_cnt_peril_theft AS DECIMAL(15,2)) AS claim_cnt_peril_theft, cast(c.claim_cnt_peril_eqbd AS DECIMAL(15,2)) AS claim_cnt_peril_eqbd,
       cast(c.claim_cnt_cov_blanket AS DECIMAL(15,2)) AS claim_cnt_cov_blanket, cast(c.claim_cnt_cov_addl AS DECIMAL(15,2)) AS claim_cnt_cov_addl, cast(c.claim_cnt_cov_opt AS DECIMAL(15,2)) AS claim_cnt_cov_opt, cast(c.exp_cnt_peril_fire AS DECIMAL(15,2)) AS exp_cnt_peril_fire,
       cast(c.exp_cnt_peril_theft AS DECIMAL(15,2)) AS exp_cnt_peril_theft, cast(c.exp_cnt_peril_eqbd AS DECIMAL(15,2)) AS exp_cnt_peril_eqbd, cast(c.exp_cnt_cov_blanket AS DECIMAL(15,2)) AS exp_cnt_cov_blanket, cast(c.exp_cnt_cov_addl AS DECIMAL(15,2)) AS exp_cnt_cov_addl, cast(c.exp_cnt_cov_opt AS DECIMAL(15,2)) AS exp_cnt_cov_opt,
       cast(c.count_claim_paid_non_cat AS DECIMAL(15,2)) AS count_claim_paid_non_cat, cast(c.count_claim_closed_non_cat AS DECIMAL(15,2)) AS count_claim_closed_non_cat, cast(c.count_claim_closed_0paid_non_cat AS DECIMAL(15,2)) AS count_claim_closed_0paid_non_cat,
       cast(c.exp_cnt_paid_non_cat AS DECIMAL(15,2)) AS exp_cnt_paid_non_cat, cast(c.exp_cnt_closed_non_cat AS DECIMAL(15,2)) AS exp_cnt_closed_non_cat, cast(c.exp_cnt_closed_0paid_non_cat AS DECIMAL(15,2)) AS exp_cnt_closed_0paid_non_cat,
       cast(isnull(c.paid_cum - lag(c.paid_cum) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.paid_cum) AS DECIMAL(15,2)) AS paid_cum_inc,
       cast(isnull(c.incurred_cum - lag(c.incurred_cum) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum) AS DECIMAL(15,2)) AS incurred_cum_inc,
       cast(isnull(c.incurred_cum_non_cat - lag(c.incurred_cum_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum_non_cat) AS DECIMAL(15,2)) AS incurred_cum_non_cat_inc,
       cast(isnull(c.incurred_cum_loss_only - lag(c.incurred_cum_loss_only) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum_loss_only) AS DECIMAL(15,2)) AS incurred_cum_loss_only_inc,
       cast(isnull(c.incurred_cum_dcc - lag(c.incurred_cum_dcc) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum_dcc) AS DECIMAL(15,2)) AS incurred_cum_dcc_inc,
       cast(isnull(c.incurred_cum_ao - lag(c.incurred_cum_ao) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum_ao) AS DECIMAL(15,2)) AS incurred_cum_ao_inc,
       cast(isnull(c.incurred_cum_ss - lag(c.incurred_cum_ss) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cum_ss) AS DECIMAL(15,2)) AS incurred_cum_ss_inc,
       cast(isnull(c.incurred_peril_fire - lag(c.incurred_peril_fire) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_peril_fire) AS DECIMAL(15,2)) AS incurred_peril_fire_inc,
       cast(isnull(c.incurred_peril_theft - lag(c.incurred_peril_theft) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_peril_theft) AS DECIMAL(15,2)) AS incurred_peril_theft_inc,
       cast(isnull(c.incurred_peril_eqbd - lag(c.incurred_peril_eqbd) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_peril_eqbd) AS DECIMAL(15,2)) AS incurred_peril_eqbd_inc,
       cast(isnull(c.incurred_cov_blanket - lag(c.incurred_cov_blanket) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cov_blanket) AS DECIMAL(15,2)) AS incurred_cov_blanket_inc,
       cast(isnull(c.incurred_cov_addl - lag(c.incurred_cov_addl) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cov_addl) AS DECIMAL(15,2)) AS incurred_cov_addl_inc,
       cast(isnull(c.incurred_cov_opt - lag(c.incurred_cov_opt) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.incurred_cov_opt) AS DECIMAL(15,2)) AS incurred_cov_opt_inc,
       cast(isnull(c.count_claim - lag(c.count_claim) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim) AS DECIMAL(15,2)) AS count_claim_inc,
       cast(isnull(c.count_claim_paid - lag(c.count_claim_paid) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_paid) AS DECIMAL(15,2)) AS count_claim_paid_inc,
       cast(isnull(c.count_claim_closed - lag(c.count_claim_closed) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_closed) AS DECIMAL(15,2)) AS count_claim_closed_inc,
       cast(isnull(c.count_claim_closed_0paid - lag(c.count_claim_closed_0paid) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_closed_0paid) AS DECIMAL(15,2)) AS count_claim_closed_0paid_inc,
       cast(isnull(c.count_claim_non_cat - lag(c.count_claim_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_non_cat) AS DECIMAL(15,2)) AS count_claim_non_cat_inc,
       cast(isnull(c.exp_cnt - lag(c.exp_cnt) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt) AS DECIMAL(15,2)) AS exp_cnt_inc,
       cast(isnull(c.exp_cnt_paid - lag(c.exp_cnt_paid) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_paid) AS DECIMAL(15,2)) AS exp_cnt_paid_inc,
       cast(isnull(c.exp_cnt_closed - lag(c.exp_cnt_closed) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_closed) AS DECIMAL(15,2)) AS exp_cnt_closed_inc,
       cast(isnull(c.exp_cnt_closed_0paid - lag(c.exp_cnt_closed_0paid) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_closed_0paid) AS DECIMAL(15,2)) AS exp_cnt_closed_0paid_inc,
       cast(isnull(c.exp_cnt_non_cat - lag(c.exp_cnt_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_non_cat) AS DECIMAL(15,2)) AS exp_cnt_non_cat_inc,
       cast(isnull(c.claim_cnt_peril_fire - lag(c.claim_cnt_peril_fire) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_peril_fire) AS DECIMAL(15,2)) AS claim_cnt_peril_fire_inc,
       cast(isnull(c.claim_cnt_peril_theft - lag(c.claim_cnt_peril_theft) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_peril_theft) AS DECIMAL(15,2)) AS claim_cnt_peril_theft_inc,
       cast(isnull(c.claim_cnt_peril_eqbd - lag(c.claim_cnt_peril_eqbd) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_peril_eqbd) AS DECIMAL(15,2)) AS claim_cnt_peril_eqbd_inc,
       cast(isnull(c.claim_cnt_cov_blanket - lag(c.claim_cnt_cov_blanket) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_cov_blanket) AS DECIMAL(15,2)) AS claim_cnt_cov_blanket_inc,
       cast(isnull(c.claim_cnt_cov_addl - lag(c.claim_cnt_cov_addl) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_cov_addl) AS DECIMAL(15,2)) AS claim_cnt_cov_addl_inc,
       cast(isnull(c.claim_cnt_cov_opt - lag(c.claim_cnt_cov_opt) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.claim_cnt_cov_opt) AS DECIMAL(15,2)) AS claim_cnt_cov_opt_inc,
       cast(isnull(c.exp_cnt_peril_fire - lag(c.exp_cnt_peril_fire) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_peril_fire) AS DECIMAL(15,2)) AS exp_cnt_peril_fire_inc,
       cast(isnull(c.exp_cnt_peril_theft - lag(c.exp_cnt_peril_theft) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_peril_theft) AS DECIMAL(15,2)) AS exp_cnt_peril_theft_inc,
       cast(isnull(c.exp_cnt_peril_eqbd - lag(c.exp_cnt_peril_eqbd) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_peril_eqbd) AS DECIMAL(15,2)) AS exp_cnt_peril_eqbd_inc,
       cast(isnull(c.exp_cnt_cov_blanket - lag(c.exp_cnt_cov_blanket) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_cov_blanket) AS DECIMAL(15,2)) AS exp_cnt_cov_blanket_inc,
       cast(isnull(c.exp_cnt_cov_addl - lag(c.exp_cnt_cov_addl) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_cov_addl) AS DECIMAL(15,2)) AS exp_cnt_cov_addl_inc,
       cast(isnull(c.exp_cnt_cov_opt - lag(c.exp_cnt_cov_opt) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_cov_opt) AS DECIMAL(15,2)) AS exp_cnt_cov_opt_inc,
       cast(isnull(c.count_claim_paid_non_cat - lag(c.count_claim_paid_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_paid_non_cat) AS DECIMAL(15,2)) AS count_claim_paid_non_cat_inc,
       cast(isnull(c.count_claim_closed_non_cat - lag(c.count_claim_closed_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_closed_non_cat) AS DECIMAL(15,2)) AS count_claim_closed_non_cat_inc,
       cast(isnull(c.count_claim_closed_0paid_non_cat - lag(c.count_claim_closed_0paid_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.count_claim_closed_0paid_non_cat) AS DECIMAL(15,2)) AS count_claim_closed_0paid_non_cat_inc,
       cast(isnull(c.exp_cnt_paid_non_cat - lag(c.exp_cnt_paid_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_paid_non_cat) AS DECIMAL(15,2)) AS exp_cnt_paid_non_cat_inc,
       cast(isnull(c.exp_cnt_closed_non_cat - lag(c.exp_cnt_closed_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_closed_non_cat) AS DECIMAL(15,2)) AS exp_cnt_closed_non_cat_inc,
       cast(isnull(c.exp_cnt_closed_0paid_non_cat - lag(c.exp_cnt_closed_0paid_non_cat) OVER (PARTITION BY a.policy_reference ORDER BY a.eval_date), c.exp_cnt_closed_0paid_non_cat) AS DECIMAL(15,2)) AS exp_cnt_closed_0paid_non_cat_inc
FROM policy_table_combined AS a
LEFT JOIN loss_table_agg AS c ON a.policy_reference = c.policy_reference AND a.eval_date = c.eval_date
ORDER BY a.policy_reference, a.eval_date;