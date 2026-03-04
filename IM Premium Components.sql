-- Use for: calculate premium and loss data broken down to coverage level
-- To update for different LOB, update and note the following:
--     Columns for premium / losses by coverage are line specific. Will need to update to be line specific
--     IM does not use credit in rating and the field is not collected. May need to update tables that remove duplicates to include.
--     This includes adding zip code fields to the policies_table_pre in order to have all attributes needed to remove duplicate policies
--     Claim counts will be inaccurate. One claim id can have multiple coverages causing duplicate claim amounts. 1 claim with 3 types of applicable coverage will sum to 3

with dates_table as (select distinct
        extract(year from a.monthlastday) * 100 + extract(month from a.monthlastday) as eval_date
        , a.monthlastday as eval_date_full
        from bi_workspace.periods as a
        -- FIXED: Changed date_add to DATEADD for Redshift compatibility
        where a.date <= last_day(DATEADD('month', -1, current_date))
          and a.date >= '2020-07-30'
        order by 1)

-- begin pull loss data. In dwh.all_claims_financial_changes_ds, all historical claim amounts are carried forward to the latest calendar date.
-- At each eval date, the amounts are cumulative. The difference between eval dates would be incremental for that period.
,loss_exposure_table as (select distinct
        case when a.tpa_name = 'Gallagher Bassett' then substring(a.claim_number, 1, 13)
            else a.claim_number end as claim_number
        , case when a.tpa_name = 'Gallagher Bassett' then substring(a.claim_id, 1, 13)
            else a.claim_id end as claim_id
        , a.exposure_number
        , a.exposure_id
        , a.policy_reference
        , c.policy_id
    -- date fields
        , cast(a.policy_start_date as date) as eff_date
        , cast(a.policy_end_date as date) as exp_date
        , a.date_of_loss as acc_date
        , a.date_submitted as rpt_date
        , extract(year from a.policy_start_date) * 100 + extract(month from a.policy_start_date) as eff_period
        , extract(year from a.policy_end_date) * 100 + extract(month from a.policy_end_date) as exp_period
        , extract(year from a.date_of_loss) * 100 + extract(month from a.date_of_loss) as acc_period
        , extract(year from a.date) * 100 + extract(month from a.date) as eval_date
    -- loss details/ characteristics
        , case when (SUM(isnull(a.loss_reserve_total,0) + isnull(a.expense_dcc_reserve_total,0) + isnull(a.expense_ao_reserve_total,0)
            + isnull(a.recovery_salvage_reserve_total,0) + isnull(a.recovery_subrogation_reserve_total,0))
            over (partition by a.claim_id, eval_date)) > 0 then 'Open'
            else 'closed' end as claim_status
        , case when (SUM(isnull(a.loss_reserve_total,0) + isnull(a.expense_dcc_reserve_total,0) + isnull(a.expense_ao_reserve_total,0)
            + isnull(a.recovery_salvage_reserve_total,0) + isnull(a.recovery_subrogation_reserve_total,0))
            over (partition by a.exposure_id, eval_date)) > 0 then 'Open'
            else 'closed' end as exposure_status
        , a.coverage
        , a.loss_cause_type_name
        , a.marketing_cob_group as cob_group
        , a.cob_name
        , a.location_of_loss
        , a.business_state as state
         , case when (a.catastrophe_code is NULL or a.catastrophe_code = '' or
            a.catastrophe_code in ('1111/BFA','2212','NRP-X-R-0-0','2222/XOL','CD','SIU','Bad Faith Suit')) then 'non-CAT'
            else 'CAT'
            end as cat_ind
         , case when a.accident_zip_code = 0 then 'n.a.'
            when a.accident_zip_code = '' then 'n.a.'
            else cast(right('00000' + a.accident_zip_code, 5) as varchar(5)) end as acc_zip_code
    -- calculate aggregate loss amounts
        , isnull(a.loss_paid_total, 0) + isnull(a.recovery_salvage_collected_total, 0) + isnull(a.recovery_subrogation_collected_total, 0)
            + isnull(a.expense_ao_paid_total, 0) + isnull(a.expense_dcc_paid_total, 0) as paid
        , paid + isnull(a.loss_reserve_total, 0) + isnull(a.expense_ao_reserve_total, 0) + isnull(a.expense_dcc_reserve_total, 0) as incurred
    -- incurred loss by coverage, lob specific
        , case when a.coverage in ('BLANKET_EQUIPMENT', 'BLANKET_MISC') then incurred else 0 end as blanket_inc_total
        , case when a.coverage = 'BLANKET_MISC' then incurred else 0 end as misc_tools_inc_total
        , case when a.coverage = 'DEBRIS_REMOVAL' THEN incurred else 0 end as debris_removal_inc_total
        , case when a.coverage = 'EMPLOYEE_TOOLS_AND_CLOTHING' THEN incurred else 0 end as employee_tc_inc_total
        , case when a.coverage = 'EQUIPMENT_BORROWED_FROM_OTHERS' THEN incurred else 0 end as borrowed_equip_inc_total
        , case when a.coverage = 'FIRE_DEPARTMENT_SERVICE_CHARGE' then incurred else 0 end as fire_dept_inc_total
        , case when a.coverage = 'FUEL_ACCESSORIES_AND_SPARE_PARTS' then incurred else 0 end as fuel_access_inc_total
        , case when a.coverage = 'RENTAL_REIMBURSEMENT' then incurred else 0 end as rental_reimb_inc_total
        , case when a.coverage = 'REWARDS' then incurred else 0 end as rewards_inc_total
        , case when a.coverage = 'TRAILERS_AND_CONTENTS' then incurred else 0 end as trailers_inc_total
    -- coverage indicator, lob specific
        , case when a.coverage in ('BLANKET_EQUIPMENT', 'BLANKET_MISC') then 1 else 0 end as blanket_ind
        , case when a.coverage = 'DEBRIS_REMOVAL' THEN 1 else 0 end as debris_removal_ind
        , case when a.coverage = 'EMPLOYEE_TOOLS_AND_CLOTHING' THEN 1 else 0 end as employee_tc_ind
        , case when a.coverage = 'EQUIPMENT_BORROWED_FROM_OTHERS' THEN 1 else 0 end as borrowed_equip_ind
        , case when a.coverage = 'FIRE_DEPARTMENT_SERVICE_CHARGE' then 1 else 0 end as fire_dept_ind
        , case when a.coverage = 'FUEL_ACCESSORIES_AND_SPARE_PARTS' then 1 else 0 end as fuel_access_ind
        , case when a.coverage = 'RENTAL_REIMBURSEMENT' then 1 else 0 end as rental_reimb_ind
        , case when a.coverage = 'REWARDS' then 1 else 0 end as rewards_ind
        , case when a.coverage = 'TRAILERS_AND_CONTENTS' then 1 else 0 end as trailers_ind
    from dwh.all_claims_financial_changes_ds as a
    left join nimi_svc_prod.policies as c on c.policy_reference = a.policy_reference
    where a.lob = 'IM'
        and a.date in (select distinct eval_date_full from dates_table)
        and a.date_of_loss >= '2020-07-30'
        and a.carrier_name in ('next-insurance', 'next-carrier', 'national-specialty')
    order by a.policy_reference, a.claim_number, a.exposure_number, a.date)

-- create capped amounts broken into non-cat and total
    ,loss_table as (select distinct
            a.claim_number
            , a.claim_id
            , a.policy_reference
            , a.policy_id
            , a.eff_date
            , a.exp_date
            , a.acc_date
            , a.eff_period
            , a.exp_period
            , a.acc_period
            , a.eval_date
            , a.claim_status
            , a.coverage
            , a.loss_cause_type_name
            , a.cat_ind
        -- aggregate loss amounts
            , sum(a.paid) as paid_cum
            , sum(a.incurred) as incurred_cum
        -- define non-cat amounts
            , case when a.cat_ind = 'CAT' then 0
            else sum(a.incurred) end
            as incurred_cum_non_cat
        -- calculate incurred loss amounts by coverage
            , sum(a.blanket_inc_total) as blanket_equip_incurred_cum
            , sum(a.debris_removal_inc_total) as debris_removal_incurred_cum
            , sum(a.employee_tc_inc_total) as employee_tc_incurred_cum
            , sum(a.borrowed_equip_inc_total) as borrowed_equip_incurred_cum
            , sum(a.fire_dept_inc_total) as fire_dept_incurred_cum
            , sum(a.fuel_access_inc_total) as fuel_access_incurred_cum
            , sum(a.rental_reimb_inc_total) as rental_reimb_incurred_cum
            , sum(a.rewards_inc_total) as rewards_incurred_cum
            , sum(a.trailers_inc_total) as trailers_incurred_cum
        -- calculate total claim counts
            ,count(distinct a.claim_number) as count_claim
            ,count(distinct case when paid >0 then a.claim_number else NULL end) as count_claim_paid
            ,count(distinct case when claim_status = 'closed' then a.claim_number else NULL end) as count_claim_closed
            ,count(distinct case when paid =0 and claim_status = 'closed' then a.claim_number else NULL end) as count_claim_closed_0paid
        -- calculate total exposure counts
            , count(a.exposure_number) as exp_cnt
            , sum(case when paid >0 then 1 else 0 end) as exp_cnt_paid
            , sum(case when a.exposure_status = 'closed' then 1 else 0 end) as exp_cnt_closed
            , sum(case when a.exposure_status = 'closed' and a.paid = 0 then 1 else 0 end) as exp_cnt_closed_0paid
        -- calculate non-cat claim counts
            ,count(distinct case when cat_ind = 'non-CAT' then a.claim_number else NULL end) as count_claim_non_cat
            ,count(distinct case when cat_ind = 'non-CAT' and paid >0 then a.claim_number else NULL end) as count_claim_paid_non_cat
            ,count(distinct case when cat_ind = 'non-CAT' and claim_status = 'closed' then a.claim_number else NULL end) as count_claim_closed_non_cat
            ,count(distinct case when cat_ind = 'non-CAT' and paid =0 and claim_status = 'closed' then a.claim_number else NULL end) as count_claim_closed_0paid_non_cat
        -- calculate non-cat exposure counts
            , sum(case when cat_ind = 'non-CAT' then 1 else 0 end) as exp_cnt_non_cat
            , sum(case when cat_ind = 'non-CAT' and paid >0 then 1 else 0 end) as exp_cnt_paid_non_cat
            , sum(case when cat_ind = 'non-CAT' and a.exposure_status = 'closed' then 1 else 0 end) as exp_cnt_closed_non_cat
            , sum(case when cat_ind = 'non-CAT' and a.exposure_status = 'closed' and a.paid = 0 then 1 else 0 end) as exp_cnt_closed_0paid_non_cat
        -- calculate claim counts by coverage
            , count(distinct case when blanket_ind = 1 then a.claim_id else null end) as claim_cnt_blanket_equip
            , count(distinct case when debris_removal_ind = 1 then a.claim_id else null end) as claim_cnt_debris_removal
            , count(distinct case when employee_tc_ind = 1 then a.claim_id else null end) as claim_cnt_employee_tc
            , count(distinct case when borrowed_equip_ind = 1 then a.claim_id else null end) as claim_cnt_borrowed_equip
            , count(distinct case when fire_dept_ind = 1 then a.claim_id else null end) as claim_cnt_fire_dept
            , count(distinct case when fuel_access_ind = 1 then a.claim_id else null end) as claim_cnt_fuel_access
            , count(distinct case when rental_reimb_ind = 1 then a.claim_id else null end) as claim_cnt_rental_reimb
            , count(distinct case when rewards_ind = 1 then a.claim_id else null end) as claim_cnt_rewards
             , count(distinct case when trailers_ind = 1 then a.claim_id else null end) as claim_cnt_trailers
        -- calculate exposure counts by coverage
            , count(distinct case when blanket_ind = 1 then a.exposure_id else null end) as exp_cnt_blanket_equip
            , count(distinct case when debris_removal_ind = 1 then a.exposure_id else null end) as exp_cnt_debris_removal
            , count(distinct case when employee_tc_ind = 1 then a.exposure_id else null end) as exp_cnt_employee_tc
            , count(distinct case when borrowed_equip_ind = 1 then a.exposure_id else null end) as exp_cnt_borrowed_equip
            , count(distinct case when fire_dept_ind = 1 then a.exposure_id else null end) as exp_cnt_fire_dept
            , count(distinct case when fuel_access_ind = 1 then a.exposure_id else null end) as exp_cnt_fuel_access
            , count(distinct case when rental_reimb_ind = 1 then a.exposure_id else null end) as exp_cnt_rental_reimb
            , count(distinct case when rewards_ind = 1 then a.exposure_id else null end) as exp_cnt_rewards
             , count(distinct case when trailers_ind = 1 then a.exposure_id else null end) as exp_cnt_trailers
    from loss_exposure_table as a
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    order by a.policy_reference, a.claim_number, a.eval_date)

--aggregate to policy/coverage to be combined with policy data
, loss_table_agg as (select
    policy_reference
    , acc_date
    , acc_period
    , coverage
    , loss_cause_type_name
    , eval_date
-- aggregate loss amounts
    , SUM(paid_cum) as paid_cum
    , SUM(incurred_cum) as incurred_cum
    , SUM(incurred_cum_non_cat) as incurred_cum_non_cat
    -- aggregate coverage loss amounts
    , sum(blanket_equip_incurred_cum) as blanket_equip_incurred_cum
    , sum(debris_removal_incurred_cum) as debris_removal_incurred_cum
    , sum(employee_tc_incurred_cum) as employee_tc_incurred_cum
    , sum(borrowed_equip_incurred_cum) as borrowed_equip_incurred_cum
    , sum(fire_dept_incurred_cum) as fire_dept_incurred_cum
    , sum(fuel_access_incurred_cum) as fuel_access_incurred_cum
    , sum(rental_reimb_incurred_cum) as rental_reimb_incurred_cum
    , sum(rewards_incurred_cum) as rewards_incurred_cum
    , sum(trailers_incurred_cum) as trailers_incurred_cum
-- aggregate claim counts
    , SUM(count_claim) as count_claim
    , SUM(count_claim_paid) as count_claim_paid
    , SUM(count_claim_closed) as count_claim_closed
    , SUM(count_claim_closed_0paid) as count_claim_closed_0paid
    , SUM(count_claim_non_cat) as count_claim_non_cat
    , SUM(count_claim_paid_non_cat) as count_claim_paid_non_cat
    , SUM(count_claim_closed_non_cat) as count_claim_closed_non_cat
    , SUM(count_claim_closed_0paid_non_cat) as count_claim_closed_0paid_non_cat
    -- aggregate coverage claim counts
    , SUM(claim_cnt_blanket_equip) as claim_cnt_blanket_equip
    , SUM(claim_cnt_debris_removal) as claim_cnt_debris_removal
    , SUM(claim_cnt_employee_tc) as claim_cnt_employee_tc
    , SUM(claim_cnt_borrowed_equip) as claim_cnt_borrowed_equip
    , SUM(claim_cnt_fire_dept) as claim_cnt_fire_dept
    , SUM(claim_cnt_fuel_access) as claim_cnt_fuel_access
    , SUM(claim_cnt_rental_reimb) as claim_cnt_rental_reimb
    , SUM(claim_cnt_rewards) as claim_cnt_rewards
    , SUM(claim_cnt_trailers) as claim_cnt_trailers
-- aggregate exposure amounts
    , SUM(exp_cnt) as exp_cnt
    , SUM(exp_cnt_paid) as exp_cnt_paid
    , SUM(exp_cnt_closed) as exp_cnt_closed
    , SUM(exp_cnt_closed_0paid) as exp_cnt_closed_0paid
    , SUM(exp_cnt_non_cat) as exp_cnt_non_cat
    , SUM(exp_cnt_paid_non_cat) as exp_cnt_paid_non_cat
    , SUM(exp_cnt_closed_non_cat) as exp_cnt_closed_non_cat
    , SUM(exp_cnt_closed_0paid_non_cat) as exp_cnt_closed_0paid_non_cat
    -- aggregate coverage exposure amounts
    , SUM(exp_cnt_blanket_equip) as exp_cnt_blanket_equip
    , SUM(exp_cnt_debris_removal) as exp_cnt_debris_removal
    , SUM(exp_cnt_employee_tc) as exp_cnt_employee_tc
    , SUM(exp_cnt_borrowed_equip) as exp_cnt_borrowed_equip
    , SUM(exp_cnt_fire_dept) as exp_cnt_fire_dept
    , SUM(exp_cnt_fuel_access) as exp_cnt_fuel_access
    , SUM(exp_cnt_rental_reimb) as exp_cnt_rental_reimb
    , SUM(exp_cnt_rewards) as exp_cnt_rewards
    , SUM(exp_cnt_trailers) as exp_cnt_trailers
  from loss_table
  group by 1,2,3, 4, 5 ,6
  order by policy_reference,eval_date)

-- cross joined desired eval dates with policy_id
, premium_table_frame as (select distinct
        a.policy_id
        , b.eval_date
        , b.eval_date_full
    from reporting.gaap_snapshots_asl as a
        cross join dates_table as b
    where a.date <= (select max(eval_date_full) from dates_table)
    and a.lob = 'IM'
    and a.carrier_name in ('next-insurance', 'next-carrier', 'national-specialty')
    and a.trans in ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New',
        'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    order by 1, 2, 3)

-- attach premium to premium_table_frame and calculate cumulative premium.
-- unlike losses in dwh.all_claims_financial_changes_ds, premium in reporting.gaap_snapshots_asl are not cumulative; they are just for that calendar date
,premium_table as (select a.policy_id
        , a.eval_date
        , a.eval_date_full
        , sum(case when (extract(year from b.date) * 100 + extract(month from b.date) = a.eval_date)
            and trans in ('monthly earned premium', 'monthly earned premium endorsement')
            then b.dollar_amount
            else 0 end) as ep
        , sum(ep) over (partition by a.policy_id order by a.eval_date rows between unbounded preceding and current row ) as ep_cum
        , sum(case when (extract(year from b.date) * 100 + extract(month from b.date) = a.eval_date)
            and trans in ('New', 'Renewal', 'Cancellation - New', 'Cancellation - Renewal', 'Undo Cancellation - New',
            'Undo Cancellation - Renewal') then b.dollar_amount
            else 0 end) as wp
       , sum(wp) over (partition by a.policy_id order by a.eval_date rows between unbounded preceding and current row ) as wp_cum
    from premium_table_frame as a
    left join reporting.gaap_snapshots_asl as b on b.policy_id = a.policy_id
    where b.date <= (select max(eval_date_full) from dates_table)
        and b.lob = 'IM'
        and b.carrier_name in ('next-insurance', 'next-carrier', 'national-specialty')
        and b.trans in ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New',
            'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    group by 1, 2, 3
    order by 1, 2)

-- De-duplicate S3 rating calculations to get the latest version for IM.
, s3_im_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY dateid DESC, update_time DESC) AS rn
        FROM s3_operational.rating_svc_prod_calculations
        WHERE lob = 'IM'
    )
    WHERE rn = 1
)

-- De-duplicate IM quotes to get the latest version.
, im_quotes_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY creation_time DESC) AS rn
        -- FIXED: Pointed to temp_im_quotes instead of external_dwh.im_quotes
        FROM temp_im_quotes
    )
    WHERE rn = 1
)

-- De-duplicate policy transactions to get the latest bind transaction.
, policy_transactions_deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY tx_effective_date DESC, policy_transaction_id DESC) as rn
        FROM prod.dwh.policy_transactions
        WHERE transaction_type = 'BIND' AND lob = 'IM'
    )
    WHERE rn = 1
)

-- attach policy attributes to the policy id including coverage level premium
,policies_table_pre as (select distinct
    a.policy_id
    , a.policy_reference
    , a.business_id
    -- date fields
    , cast(a.start_date as date) as eff_date
    , cast(a.end_date as date) as exp_date
    , extract(year from a.start_date) * 100 + extract(month from a.start_date) as eff_period
    -- policy details/characteristics
    , a.state
    , a.county
    , a.new_renewal
    , a.cob_name
    , a.cob_group
    , isnull(nullif(a.cob_industry, ''), 'Construction') as cob_industry
    , cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'contractors_equipment_limit', true),'') as numeric) as blanket_equip_occ_lim
    -- marketing fields
    , a.carrier_name
    , a.channel
    , clm.distribution_channel_attributed as distribution_channel
    , clm.agency_aggregator as agency_aggregator_name
    , clm.agency_type as current_agencytype
    , pt.business_ownership_structure
    , clm.package as highest_status_package
    , clm.policy_status_name as highest_status_name
    -- premium amounts
    , b.yearly_premium
    , b.tria
    , b.surcharges
    -- premium components
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'base_premium', true),'') as numeric), 0) as base_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'optional_coverage_premium' , true),'') as numeric), 0) as optional_coverage_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'additional_coverage_premium' , true),'') as numeric), 0) as additional_coverage_premium
    -- premium components for over under base limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketFactor', true),'') as decimal(4,2)), 1.5) as blanket_factor
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'deductibleFactor', true),'') as decimal(4,2)), 1) as ded_factor
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lossCost', true),'') as decimal(4,2)), 2.67) as base_premium_loss_cost
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lcm', true), '') as decimal(4,2)), 2) as base_premium_lcm
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'lossCostOver10K', true), '') as decimal(4,2)), 1.84) as loss_cost_10K
    -- endorsement limits
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'rewards_coverage' , true),'') as numeric), 0) as rewards_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'debris_removal' , true),'') as numeric), 0) as debris_removal_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'false_pretense' , true),'') as numeric), 0) as false_pretense_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fire_department_service', true),'') as numeric), 0) as fire_dpmt_service_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fire_extinguishing_systems', true), '') as numeric),0) as fire_extg_systems_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fuel_accessories_spare_parts', true), '') as numeric), 0) as fuel_acess_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'inventory_and_appraisal', true), '') as numeric), 0) as inventory_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'pollutant_removal', true), '') as numeric), 0) as polluntant_removal_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'trailers_and_contents', true), '') as numeric), 0) as trailers_contents_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'employee_tools_and_clothing_per_occurrence', true), '') as numeric), 0) as employees_tc_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'borrowed_equipment_limit', true), '') as numeric), 0) as borrowed_equip_limit
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'misc_tools_limit', true), '') as numeric), 0) as misc_tools_limit
    -- endorsement premiums
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'rewards_coverage_premium' , true),'') as numeric), 0) as rewards_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'debris_removal_premium' , true),'') as numeric), 0) as debris_removal_premiunm
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'false_pretense_premium' , true),'') as numeric), 0) as false_pretense_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fire_dept_premium' , true),'') as numeric), 0) as fire_dpmt_service_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fire_extinguishing_service_premium' , true),'') as numeric), 0) as fire_extg_systems_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'fuel_accessory_parts_premium' , true),'') as numeric), 0) fuel_acess_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'inventory_appraisal_premium' , true),'') as numeric), 0) as inventory_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'pollutant_removal_premium' , true),'') as numeric), 0) as polluntant_removal_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'trailers_contents_premium' , true),'') as numeric), 0) as trailers_contents_premium
    , coalesce(cast(nullif(json_extract_path_text(s3.calculation_summary, 'lob specific', 'totalResults', 'employee_tools_clothing_premium' , true),'') as numeric), 0) as employees_tc_premium
    , case when s3.state <> 'CA' then optional_coverage_premium
        when s3.state = 'CA' then cast(nullif(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f1 <- borrowedEquipmentCoverage', true), '') as numeric)
        else 0 end as borrowed_equip_premium
    , case when s3.state = 'CA'
        then coalesce(cast(nullif(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f6 <- miscToolsCoverage', true), '') as numeric), 0)
        else 0 end as misc_tools_prem
from reporting.gaap_snapshots_asl as a
    left join nimi_svc_prod.policies as b on b.policy_id = a.policy_id
    left join policy_transactions_deduped pt ON a.policy_id = pt.policy_id
    left join im_quotes_deduped iq ON pt.last_quote_id = iq.quote_id
    left join s3_im_deduped s3 ON iq.job_id = s3.job_id
    left join dwh.company_level_metrics_ds clm ON a.policy_reference = clm.policy_reference
where a.lob = 'IM'
    and a.carrier_name in ('next-insurance', 'next-carrier', 'national-specialty')
    and a.trans in ('monthly earned premium', 'monthly earned premium endorsement', 'New', 'Renewal', 'Cancellation - New',
        'Cancellation - Renewal', 'Undo Cancellation - New', 'Undo Cancellation - Renewal')
    and a.date <= (select max(eval_date_full) from dates_table)
    and a.end_date > a.start_date
order by a.business_id, a.policy_reference)

-- remove duplicates
,remove_duplicates as (select
       policy_id
       , count(policy_id) as count
    from policies_table_pre
    group by 1 )

,policies_table as (select distinct a.*
    from policies_table_pre as a
    left join remove_duplicates as b on b.policy_id = a.policy_id
    where b.count = 1)

-- combined all the tables together
-- filter out same day cancellations AND records where there is no ep due to the cross join data frame earlier
, policy_table_xloss as (select a.*
       , b.eval_date
       , b.eval_date_full
       , b.ep_cum
       , b.wp_cum
    from policies_table as a
        full outer join premium_table as b on b.policy_id = a.policy_id
    where a.exp_date > a.eff_date
    order by a.business_id, a.policy_id, b.eval_date)

--Bring in losses
select a.*
    -- FIXED: Capitalized DATEDIFF
    , case when DATEDIFF(day, a.eff_date, a.eval_date_full) > DATEDIFF(day, a.eff_date, a.exp_date)
        then cast(DATEDIFF(day, a.eff_date, a.exp_date) / cast(365 as decimal(10, 6)) as decimal(10,6))
        else cast(DATEDIFF(day, a.eff_date, a.eval_date_full) / cast(365 as decimal(10, 6)) as decimal(10,6)) end as ee_cum_py
    , cast(DATEDIFF(day, a.eff_date, a.exp_date) / cast(365 as decimal(10, 6)) as decimal(15,2)) as we_cum_py
    , 1 as policy_count
    , isnull(policy_count - lag(policy_count) over (partition by a.policy_id order by a.eval_date), policy_count) as policy_count_inc
    , b.acc_date
    , b.acc_period
    , b.coverage
    , b.loss_cause_type_name
-- loss amounts
    , cast(paid_cum as decimal(15,2)) as paid_cum
    , cast(incurred_cum as decimal(15,2)) as incurred_cum
    , cast(incurred_cum_non_cat as decimal(15,2)) as incurred_cum_non_cat
    -- coverage loss amounts
     , cast(blanket_equip_incurred_cum as decimal(15,2)) as blanket_equip_incurred_cum
     , cast(debris_removal_incurred_cum as decimal(15,2)) as debris_removal_incurred_cum
     , cast(employee_tc_incurred_cum as decimal(15,2)) as employee_tc_incurred_cum
     , cast(borrowed_equip_incurred_cum as decimal(15,2)) as borrowed_equip_incurred_cum
     , cast(fire_dept_incurred_cum as decimal(15,2)) as fire_dept_incurred_cum
     , cast(fuel_access_incurred_cum as decimal(15,2)) as fuel_access_incurred_cum
     , cast(rental_reimb_incurred_cum as decimal(15,2)) as rental_reimb_incurred_cum
     , cast(rewards_incurred_cum as decimal(15,2)) as rewards_incurred_cum
     , cast(trailers_incurred_cum as decimal(15,2)) as trailers_incurred_cum
-- claim counts
    , cast(count_claim as decimal(15,2)) as count_claim
    , cast(count_claim_paid as decimal(15,2)) as count_claim_paid
    , cast(count_claim_closed as decimal(15,2)) as count_claim_closed
    , cast(count_claim_closed_0paid as decimal(15,2)) as count_claim_closed_0paid
    , cast(count_claim_non_cat as decimal(15,2)) as count_claim_non_cat
    ,cast(count_claim_paid_non_cat as decimal(15,2)) as count_claim_paid_non_cat
    ,cast(count_claim_closed_non_cat as decimal(15,2)) as count_claim_closed_non_cat
    ,cast(count_claim_closed_0paid_non_cat as decimal(15,2)) as count_claim_closed_0paid_non_cat
    --coverage claim counts
    ,cast(claim_cnt_blanket_equip as decimal(15,2)) as claim_cnt_blanket_equip
     ,cast(claim_cnt_debris_removal as decimal(15,2)) as claim_cnt_debris_removal
     ,cast(claim_cnt_employee_tc as decimal(15,2)) as claim_cnt_employee_tc
    ,cast(claim_cnt_borrowed_equip as decimal(15,2)) as claim_cnt_borrowed_equip
    ,cast(claim_cnt_fire_dept as decimal(15,2)) as claim_cnt_fire_dept
    ,cast(claim_cnt_fuel_access as decimal(15,2)) as claim_cnt_fuel_access
    ,cast(claim_cnt_rental_reimb as decimal(15,2)) as claim_cnt_rental_reimb
    ,cast(claim_cnt_rewards as decimal(15,2)) as claim_cnt_rewards
     ,cast(claim_cnt_trailers as decimal(15,2)) as claim_cnt_trailers
  -- exposure amounts
    , cast(exp_cnt as decimal(15,2)) as exp_cnt
    , cast(exp_cnt_paid as decimal(15,2)) as exp_cnt_paid
    , cast(exp_cnt_closed as decimal(15,2)) as exp_cnt_closed
    , cast(exp_cnt_closed_0paid as decimal(15,2)) as exp_cnt_closed_0paid
    , cast(exp_cnt_non_cat as decimal(15,2)) as exp_cnt_non_cat
    ,cast(exp_cnt_paid_non_cat as decimal(15,2)) as exp_cnt_paid_non_cat
    ,cast(exp_cnt_closed_non_cat as decimal(15,2)) as exp_cnt_closed_non_cat
    ,cast(exp_cnt_closed_0paid_non_cat as decimal(15,2)) as exp_cnt_closed_0paid_non_cat
    -- coverage exposure amounts
     ,cast(exp_cnt_blanket_equip as decimal(15,2)) as exp_cnt_blanket_equip
     ,cast(exp_cnt_debris_removal as decimal(15,2)) as exp_cnt_debris_removal
     ,cast(exp_cnt_employee_tc as decimal(15,2)) as exp_cnt_employee_tc
     ,cast(exp_cnt_borrowed_equip as decimal(15,2)) as exp_cnt_borrowed_equip
     ,cast(exp_cnt_fire_dept as decimal(15,2)) as exp_cnt_fire_dept
     ,cast(exp_cnt_fuel_access as decimal(15,2)) as exp_cnt_fuel_access
     ,cast(exp_cnt_rental_reimb as decimal(15,2)) as exp_cnt_rental_reimb
     ,cast(exp_cnt_rewards as decimal(15,2)) as exp_cnt_rewards
     ,cast(exp_cnt_trailers as decimal(15,2)) as exp_cnt_trailers
 from policy_table_xloss as a
    left join loss_table_agg as b
        on a.policy_reference = b.policy_reference and a.eval_date = b.eval_date
    where ep_cum > 0
order by a.policy_reference, a.eval_date;