WITH dates_table AS (
    SELECT DISTINCT extract(YEAR FROM a.monthlastday)*100 + extract(MONTH FROM a.monthlastday) AS eval_date
    , a.monthlastday AS eval_date_full
    FROM bi_workspace.periods AS a
    -- FIXED: Changed date_add to DATEADD for Redshift compatibility
    WHERE a.date <= last_day(DATEADD('month', -1, current_date))
    AND a.date >= '2020-07-30'
    ORDER BY 1
)

, customer_inception AS (
    SELECT DISTINCT a.business_id
    , cast(min(a.start_date) AS DATE) AS customer_inception
    FROM reporting.gaap_snapshots_asl AS a
    WHERE a.lob = 'IM'
    AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
    AND a.trans IN ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    AND a.date <= (SELECT max(eval_date_full) FROM dates_table)
    AND a.end_date > a.start_date
    GROUP BY a.business_id
    ORDER BY a.business_id
)

-- Helper CTEs for deduping records, based on the provided reference queries
, s3_im_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY dateid DESC, update_time DESC) AS rn
        FROM s3_operational.rating_svc_prod_calculations
        WHERE lob = 'IM'
    )
    WHERE rn = 1
)

, im_quotes_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY creation_time DESC) AS rn
        -- FIXED: Pointed to temp_im_quotes instead of external_dwh.im_quotes
        FROM temp_im_quotes
    )
    WHERE rn = 1
)

, policy_transactions_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY tx_effective_date DESC, policy_transaction_id DESC) as rn
        FROM prod.dwh.policy_transactions
        WHERE transaction_type = 'BIND' AND lob = 'IM'
    )
    WHERE rn = 1
)

, pricing_breakdown AS (
    SELECT DISTINCT a.business_id
    , a.policy_id
    , a.policy_reference
    , cast(a.start_date AS DATE) AS eff_date
    , cast(a.end_date AS DATE) AS exp_date
    -- FIXED: Capitalized DATEDIFF
    , cast(DATEDIFF(day, a.start_date, a.end_date) AS INTEGER) AS policy_duration
    , e.customer_inception
    , a.new_renewal
    , a.carrier_name
    , a.state
    , a.cob_name
    , a.cob_group
    , isnull(nullif(a.cob_industry, ''), 'Construction') AS cob_industry
    , a.channel
    , clm.policy_status_name AS highest_status_name
    -- FIXED: Capitalized DATEDIFF
    , DATEDIFF(day, e.customer_inception, a.start_date) / 365 AS policy_term
    -- pricing metrics attributes
    , CASE WHEN regexp_instr(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE), '^[0-9\.]+$') > 0
           THEN cast(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', TRUE) AS NUMERIC)
           ELSE NULL END AS blanket_equip_occ_lim
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketFactor', TRUE),'') AS decimal(4,2)), 1.5) AS blanket_factor
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'deductibleFactor', TRUE),'') AS decimal(4,2)), 1) AS ded_factor
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lossCost', TRUE),'') AS decimal(4,2)), 2.67) AS base_premium_loss_cost
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lcm', TRUE), '') AS decimal(4,2)), 2) AS base_premium_lcm
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lossCostOver10K', TRUE), '') AS decimal(4,2)), 1.84) AS loss_cost_10K
    -- pricing metrics calculations
    , CASE WHEN blanket_equip_occ_lim < 10000 THEN (blanket_equip_occ_lim / 100) * base_premium_loss_cost
    ELSE 100 * base_premium_loss_cost
    END AS pure_premium_base
    , CASE WHEN blanket_equip_occ_lim > 10000 THEN ((blanket_equip_occ_lim - 10000) / 100 ) * loss_cost_10K
    ELSE 0 END AS pure_premium_ilf
    , pure_premium_base + pure_premium_ilf AS pure_premium
    , pure_premium * ded_factor * base_premium_lcm AS manual_premium
    , manual_premium * blanket_factor AS modified_premium
    , pol.yearly_premium AS sold_premium
    FROM reporting.gaap_snapshots_asl AS a
    INNER JOIN nimi_svc_prod.policies AS pol ON pol.policy_id = a.policy_id
    LEFT JOIN customer_inception AS e ON e.business_id = a.business_id
    -- New join path to get S3 and related data
    LEFT JOIN policy_transactions_deduped pt ON a.policy_id = pt.policy_id
    LEFT JOIN im_quotes_deduped iq ON pt.last_quote_id = iq.quote_id
    LEFT JOIN s3_im_deduped s3 ON iq.job_id = s3.job_id
    LEFT JOIN dwh.company_level_metrics_ds clm ON a.policy_reference = clm.policy_reference
    WHERE a.lob='IM'
    AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
    AND a.trans IN ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    AND a.date <= (SELECT max(eval_date_full) FROM dates_table)
    AND a.end_date > a.start_date
    ORDER BY a.business_id, a.start_date
)

, term_rank AS (
    SELECT DISTINCT a.*
    , rank() OVER (PARTITION BY business_id, policy_term ORDER BY eff_date ASC) AS order_asc
    , rank () OVER (PARTITION BY business_id, policy_term ORDER BY eff_date DESC) AS order_desc
    FROM pricing_breakdown AS a
    ORDER BY a.business_id, a.eff_date
)

, combined_data AS (
    SELECT DISTINCT a.business_id
    , a.policy_id
    , a.policy_reference
    , a.eff_date
    , a.exp_date
    , a.customer_inception
    -- marketing fields
    , a.new_renewal
    , a.carrier_name
    , a.state
    , a.cob_name
    , a.cob_group
    , a.cob_industry
    , a.channel
    -- policy characteristics, current term
    , a.highest_status_name
    , a.policy_term
    --pricing metrics , current term
    , a.blanket_equip_occ_lim AS exposure
    , a.pure_premium AS pure_premium_total
    , a.manual_premium
    , a.modified_premium
    , a.sold_premium
    -- policy characteristics, prior term
    , b.highest_status_name AS highest_status_name_prior
    , b.policy_term AS policy_term_prior
    -- pricing metrics, prior term
    , b.blanket_equip_occ_lim AS exposure_prior
    , b.pure_premium AS pure_premium_total_prior
    , b.manual_premium AS manual_premium_prior
    , b.modified_premium AS modified_premium_prior
    , b.sold_premium AS sold_premium_prior
    FROM term_rank AS a
    LEFT JOIN (SELECT DISTINCT * FROM term_rank WHERE order_desc = 1) AS b ON b.business_id = a.business_id AND b.policy_term = a.policy_term - 1
)

SELECT * FROM combined_data;