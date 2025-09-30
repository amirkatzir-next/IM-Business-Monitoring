-- Inland Marine (IM) Master Data Query
-- Version 5.5: Added lifetime loss and claim count metrics.

WITH
    -- CTE 1: Define the evaluation date window.
    dates_table AS (
        SELECT DISTINCT
            a.monthlastday AS eval_date_full
        FROM bi_workspace.periods AS a
        WHERE a.date <= '2025-06-30'
          AND a.date >= '2020-07-30'
    ),

    -- CTE 2: De-duplicate S3 rating calculations.
    s3_im_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY dateid DESC, update_time DESC) AS rn
            FROM s3_operational.rating_svc_prod_calculations
            WHERE lob = 'IM'
        )
        WHERE rn = 1
    ),

    -- CTE 3: De-duplicate IM quotes.
    im_quotes_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY creation_time DESC) AS rn
            FROM external_dwh.im_quotes
        )
        WHERE rn = 1
    ),

    -- CTE 4: De-duplicate policy transactions.
    policy_transactions_deduped AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY tx_effective_date DESC, policy_transaction_id DESC) as rn
            FROM prod.dwh.policy_transactions
            WHERE transaction_type = 'BIND' AND lob = 'IM'
        )
        WHERE rn = 1
    ),

    -- CTE 5: Get the latest address for each business.
    latest_address AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY business_id ORDER BY creation_time DESC) as rnk
            FROM nimi_svc_prod.addresses
        )
        WHERE rnk = 1
    ),

    -- CTE 6: Get the latest credit score for each business.
    latest_credit_score AS (
        SELECT business_id, score AS credit_score
        FROM (
            SELECT business_id, score, rank() OVER (PARTITION BY business_id ORDER BY creation_time DESC) AS rnk
            FROM riskmgmt_svc_prod.risk_score_result
            WHERE score IS NOT NULL
        )
        WHERE rnk = 1
    ),

    -- CTE 7: Pull and de-duplicate Verisk crime score data.
    verisk_table AS (
        SELECT *
        FROM (
            SELECT *, row_number() OVER (PARTITION BY street, zip_code_5digit ORDER BY creation_time DESC) AS rnk
            FROM (
                SELECT
                    cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Arson,
                    cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Burglary,
                    cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Larceny,
                    cast(nullif(json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS AutoTheft,
                    lower(json_extract_path_text(f.response_data, 'Address', 'StreetAddress1', TRUE)) AS street,
                    cast(right('00000' + json_extract_path_text(f.response_data, 'Address', 'Zip', TRUE), 5) AS VARCHAR(5)) AS zip_code_5digit,
                    creation_time
                FROM insurance_data_gateway_svc_prod.third_parties_data AS f
                WHERE provider = 'Verisk' AND json_extract_path_text(f.response_data, 'Ms3', 'Crime', 'Arson', 'IndexValuesUpto10', 'Current', TRUE) IS NOT NULL
                UNION
                SELECT
                    cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Arson,
                    cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Burglary','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Burglary,
                    cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Larceny','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS Larceny,
                    cast(nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AutoTheft','IndexValuesUpto10', 'Current', TRUE), '') AS INTEGER) AS AutoTheft,
                    lower(street) AS street,
                    cast(right('00000' + zip_code, 5) AS VARCHAR(5)) AS zip_code_5digit,
                    creation_time
                FROM riskmgmt_svc_prod.verisk_property_risk_request_response
                WHERE json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'Arson', 'IndexValuesUpto10', 'Current', TRUE) IS NOT NULL
            )
        )
        WHERE rnk = 1
    ),

    -- *** NEW CTE FOR LOSS & CLAIMS START ***
    -- CTE 8: Calculate lifetime loss and claim counts for each policy.
    policy_loss_summary AS (
        SELECT
            policy_reference,
            SUM(incurred) AS incurred_loss,
            SUM(paid) AS paid_loss,
            COUNT(DISTINCT claim_id) AS claim_count,
            COUNT(DISTINCT CASE WHEN paid > 0 THEN claim_id ELSE NULL END) AS non_zero_claim_count
        FROM (
            SELECT
                a.policy_reference,
                a.claim_id,
                -- Incurred Loss Calculation
                (COALESCE(a.loss_paid_total, 0) + COALESCE(a.recovery_salvage_collected_total, 0) + COALESCE(a.recovery_subrogation_collected_total, 0) + COALESCE(a.expense_ao_paid_total, 0) + COALESCE(a.expense_dcc_paid_total, 0)) + COALESCE(a.loss_reserve_total, 0) + COALESCE(a.expense_ao_reserve_total, 0) + COALESCE(a.expense_dcc_reserve_total, 0) AS incurred,
                -- Paid Loss Calculation
                COALESCE(a.loss_paid_total, 0) + COALESCE(a.recovery_salvage_collected_total, 0) + COALESCE(a.recovery_subrogation_collected_total, 0) + COALESCE(a.expense_ao_paid_total, 0) + COALESCE(a.expense_dcc_paid_total, 0) AS paid,
                ROW_NUMBER() OVER (PARTITION BY a.claim_id ORDER BY a.date DESC) as rn
            FROM dwh.all_claims_financial_changes_ds AS a
            WHERE a.lob = 'IM'
        )
        WHERE rn = 1
        GROUP BY 1
    ),
    -- *** NEW CTE FOR LOSS & CLAIMS END ***

    -- CTE 9: Consolidate all policy attributes and detailed premium components.
    policy_attributes_pre AS (
        SELECT DISTINCT
            -- Core Identifiers
            a.policy_reference, a.policy_id, a.business_id,

            -- Policy Lifecycle & Terms
            a.new_renewal, pol.yearly_premium, pol.bind_date::DATE AS purch_date, a.start_date::DATE AS eff_date,
            a.end_date::DATE AS exp_date, EXTRACT(YEAR FROM a.start_date) * 100 + EXTRACT(MONTH FROM a.start_date) AS eff_period,

            -- Location & Business Details
            addr.county, addr.zip_code, a.state, clm.policy_status_name AS highest_status_name,
            a.cob_name, a.cob_group, COALESCE(NULLIF(a.cob_industry, ''), 'Construction') AS cob_industry,

            -- Carrier & Channel Information
            a.carrier_name AS carrier, a.carrier_name, a.channel,
            pt.package_name,

            -- Base Premium Components (Partially Optimized)
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'BASE', 'subCoverages', 'BLANKET_EQUIPMENT'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS blanket_equip_occ_lim,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'BASE', 'subCoverages', 'BLANKET_EQUIPMENT'), 'limits', 'DEDUCTIBLE'), '') AS NUMERIC) AS blanket_equip_ded,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketEquipmentPremiumCalculation', 'blanketFactor'),'') AS DECIMAL(10,4)), 1.5) AS blanket_factor,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketEquipmentPremiumCalculation', 'deductibleFactor'),'') AS DECIMAL(10,4)), 1) AS ded_factor,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketEquipmentPremiumCalculation', 'lossCost'),'') AS DECIMAL(10,4)), 2.67) AS base_premium_loss_cost,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketEquipmentPremiumCalculation', 'lcm'), '') AS DECIMAL(10,4)), 2) AS base_premium_lcm,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'blanketEquipmentPremiumCalculation', 'lossCostOver10K'), '') AS DECIMAL(10,4)), 1.84) AS loss_cost_10k,

            -- Additional Coverage (Omitted for brevity - same as original)
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f1 <- rewardsCoverage'), '') AS NUMERIC) AS rewards_coverage_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'rewardsCoverageCalclog', 'limitDelta'), '') AS NUMERIC) AS rewards_coverage_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'rewardsCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS rewards_coverage_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'rewardsCoverageCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.08) AS reward_coverage_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'REWARDS'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS rewards_coverage_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'rewardsCoverageCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS rewards_coverage_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f2 <- debrisRemoval'), '') AS NUMERIC) AS debris_removal_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'debrisRemovalCalclog', 'limitDelta'), '') AS NUMERIC) AS debris_removal_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'debrisRemovalCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS debris_removal_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'debrisRemovalCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.01) AS debris_removal_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'DEBRIS_REMOVAL'), 'limits', 'DEBRIS_REMOVAL'), '') AS NUMERIC) AS debris_removal_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'debrisRemovalCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS debris_removal_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f3 <- falsePretense'), '') AS NUMERIC) AS false_pretense_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'falsePretenseCalclog', 'limitDelta'), '') AS NUMERIC) AS false_pretense_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'falsePretenseCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS false_pretense_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'falsePretenseCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.05) AS false_pretense_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'FALSE_PRETENSE'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS false_pretense_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'falsePretenseCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS false_pretense_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f4 <- fireDepartmentService'), '') AS NUMERIC) AS fire_dept_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireDepartmentServiceCalclog', 'limitDelta'), '') AS NUMERIC) AS fire_dept_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireDepartmentServiceCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS fire_dept_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireDepartmentServiceCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.3) AS fire_dept_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'FIRE_DEPARTMENT_SERVICE_CHARGE'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS fire_dept_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireDepartmentServiceCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS fire_dept_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f5 <- fireExtinguishingSystems'), '') AS NUMERIC) AS fire_extinguishing_service_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireExtinguishingSystemsCalclog', 'limitDelta'), '') AS NUMERIC) AS fire_extinguishing_service_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireExtinguishingSystemsCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS fire_extinguishing_service_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireExtinguishingSystemsCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.05) AS fire_extinguishing_service_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'FIRE_EXTINGUISHING_SYSTEMS_EXPENSE'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS fire_extinguishing_service_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fireExtinguishingSystemsCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS fire_extinguishing_service_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f6 <- fuelAccessoriesSpareParts'), '') AS NUMERIC) AS fuel_accessory_parts_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fuelAccessoriesSparePartsCalclog', 'limitDelta'), '') AS NUMERIC) AS fuel_accessory_parts_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fuelAccessoriesSparePartsCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS fuel_accessory_parts_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fuelAccessoriesSparePartsCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.25) AS fuel_accessory_parts_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'FUEL_ACCESSORIES_AND_SPARE_PARTS'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS fuel_accessory_parts_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'fuelAccessoriesSparePartsCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS fuel_accessory_parts_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f7 <- inventoryAppraisal'), '') AS NUMERIC) AS inventory_appraisal_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'inventoryAppraisalCalclog', 'limitDelta'), '') AS NUMERIC) AS inventory_appraisal_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'inventoryAppraisalCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS inventory_appraisal_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'inventoryAppraisalCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.75) AS inventory_appraisal_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'INVENTORY_AND_APPRAISAL_EXPENSE'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS inventory_appraisal_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'inventoryAppraisalCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS inventory_appraisal_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f8 <- pollutantRemoval'), '') AS NUMERIC) AS pollutant_removal_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'pollutantRemovalCalclog', 'limitDelta'), '') AS NUMERIC) AS pollutant_removal_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'pollutantRemovalCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS pollutant_removal_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'pollutantRemovalCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.01) AS pollutant_removal_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'POLLUTANT_CLEANUP_AND_REMOVAL'), 'limits', 'AGGREGATE'), '') AS NUMERIC) AS pollutant_removal_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'pollutantRemovalCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS pollutant_removal_base_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f9 <- trailersContents'), '') AS NUMERIC) AS trailers_contents_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'trailersContentsCalclog', 'limitDelta'), '') AS NUMERIC) AS trailers_contents_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'trailersContentsCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS trailers_contents_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'trailersContentsCalclog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.25) AS trailers_contents_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'TRAILERS_AND_CONTENTS'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS trailers_contents_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'trailersContentsCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS trailers_contents_base_lim,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'f10 <- employeeToolsClothingPremium'), '') AS NUMERIC), 0) AS tools_clothes_prem,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'perEmployeeCalcLog', 'limitDelta'), '') AS NUMERIC),0) AS tools_clothes_emp_delta,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'perEmployeeCalcLog', 'lcm'), '') AS DECIMAL(10,4)) AS tools_clothes_emp_lcm,
            COALESCE(CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'perEmployeeCalcLog', 'lossCostValue'), '|', 1), '') AS DECIMAL(10,4)), 0.01) AS tools_clothes_emp_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'EMPLOYEE_TOOLS_AND_CLOTHING'), 'limits', 'PER_EMPLOYEE'), '') AS NUMERIC) AS tools_clothes_emp_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'perEmployeeCalcLog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS tools_clothes_emp_base_lim,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'perOccurrenceFinalRate'), '') AS DECIMAL(10,4)), 0.75) AS tools_clothes_occ_rate,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'limitDelta'), '') AS DECIMAL(10,4)), 0) AS tools_clothes_occ_delta,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'ADDITIONAL_COVERAGE', 'subCoverages', 'EMPLOYEE_TOOLS_AND_CLOTHING'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS tools_clothes_occ_package_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'additionalCoverageCalculation', 'employeeToolsClothingCalclog', 'limitDeltaCalclog', 'baseLimitValue'), '') AS NUMERIC) AS tools_clothes_occ_base_limit,
            -- Optional Coverage (Omitted for brevity - same as original)
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f1 <- borrowedEquipmentCoverage'), '') AS NUMERIC) AS borrowed_equip_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'borrowedEquipmentCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS borrowed_equip_lcm,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'borrowedEquipmentCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)) AS borrowed_equip_loss_cost,
            CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'OPTIONAL_COVERAGE', 'subCoverages', 'EQUIPMENT_BORROWED_FROM_OTHERS'), 'limits', 'OCCURRENCE'), '') AS NUMERIC) AS borrowed_equip_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f2 <- leasedFromEquipmentCoverage'), '') AS NUMERIC) AS leased_from_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedFromEquipmentCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS leased_from_lcm,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedFromEquipmentCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)) AS leased_from_loss_cost,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedFromEquipmentCoverageCalclog', 'limitAmount'), '') AS NUMERIC) AS leased_from_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f3 <- leasedToEquipmentCoverage'), '') AS NUMERIC) AS leased_to_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedToEquipmentCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS leased_to_lcm,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedToEquipmentCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)) AS leased_to_loss_cost,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'leasedToEquipmentCoverageCalclog', 'limitAmount'), '') AS NUMERIC) AS leased_to_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f4 <- loanedEquipmentCoverage'), '') AS NUMERIC) AS loaned_equip_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'loanedEquipmentCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS loaned_equip_lcm,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'loanedEquipmentCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)) AS loaned_equip_loss_cost,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'loanedEquipmentCoverageCalclog', 'limitAmount'), '') AS NUMERIC) AS loaned_equip_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f5 <- waterborneEquipmentCoverage'), '') AS NUMERIC) AS water_equip_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'waterborneEquipmentCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS water_equip_lcm,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'waterborneEquipmentCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)), 2.5) AS water_equip_loss_cost,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'waterborneEquipmentCoverageCalclog', 'limitAmount'), '') AS NUMERIC) AS water_equip_lim,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f6 <- miscToolsCoverage'), '') AS NUMERIC), 0) AS misc_tools_prem,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'miscToolsCoverageCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS misc_tools_lcm,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'miscToolsCoverageCalclog', 'lossCost'), '') AS DECIMAL(10,4)), 3.5)  AS misc_tools_loss_cost,
            COALESCE(CAST(NULLIF(json_extract_path_text(json_extract_path_text(iq.quote_package_data, 'coverages', 'BASE', 'subCoverages', 'BLANKET_MISC'), 'limits', 'OCCURRENCE'), '') AS NUMERIC), 0)  AS misc_tools_lim,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'f7 <- employeeToolsClothing'), '') AS NUMERIC), 0)  AS tools_clothes_ca_prem,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'employeeToolsClothingCalclog', 'limitAmount'), '') AS NUMERIC), 0)  AS tools_clothes_ca_emp_lim,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'employeeToolsClothingCalclog', 'deductibleFactor'), '') AS NUMERIC) AS tools_clothes_ca_emp_ded,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'employeeToolsClothingCalclog', 'lcm'), '') AS DECIMAL(10,4)) AS tools_clothes_ca_emp_lcm,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'optionalCoverageCalculation', 'employeeToolsClothingCalclog', 'lossCostValue'), '') AS DECIMAL(10,4)), 0.01) AS tools_clothes_ca_emp_loss_cost,

            -- Final Premium Calculation Components
            CASE
                WHEN NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'basePremium (f1)'),'') IS NULL
                THEN CAST(NULLIF(split_part(json_extract_path_text(s3.calculation, 'basePremiumCalculation', 'basePremium'), '|', 1), '') AS NUMERIC)
                ELSE COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'basePremium (f1)'),'') AS NUMERIC), 0)
            END AS base_premium,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'additionalCoveragePremium (f2)'),'') AS NUMERIC), 0) AS additional_coverage_premium,
            COALESCE(CAST(NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'optionalCoveragePremium (f3)'),'') AS NUMERIC), 0) AS optional_coverage_premium,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'minimumPremium'), '') AS NUMERIC) AS minimum_premium,
            pol.tria AS tria_premium,
            CAST(NULLIF(json_extract_path_text(s3.calculation, 'finalPremiumCalculation', 'stateSurchargeRate'), '') AS DECIMAL(10,6)) AS state_surcharge_rate,
            pol.surcharges AS state_surcharge,

            -- Risk & Crime Scores
            cs.credit_score,
            vt.Arson AS crime_score_arson,
            vt.Burglary AS crime_score_burglary,
            vt.Larceny AS crime_score_larceny,
            vt.AutoTheft AS crime_score_autotheft,

            -- Loss & Claim Information
            COALESCE(pls.incurred_loss, 0) AS incurred_loss,
            COALESCE(pls.paid_loss, 0) AS paid_loss,
            COALESCE(pls.claim_count, 0) AS claim_count,
            COALESCE(pls.non_zero_claim_count, 0) AS non_zero_claim_count

        FROM reporting.gaap_snapshots_asl AS a
        LEFT JOIN nimi_svc_prod.policies AS pol ON a.policy_id = pol.policy_id
        LEFT JOIN dwh.company_level_metrics_ds AS clm ON a.policy_reference = clm.policy_reference
        LEFT JOIN latest_address AS addr ON a.business_id = addr.business_id
        LEFT JOIN policy_transactions_deduped AS pt ON a.policy_id = pt.policy_id
        LEFT JOIN im_quotes_deduped AS iq ON pt.last_quote_id = iq.quote_id
        LEFT JOIN s3_im_deduped AS s3 ON iq.job_id = s3.job_id
        LEFT JOIN latest_credit_score AS cs ON a.business_id = cs.business_id
        LEFT JOIN verisk_table AS vt ON LOWER(addr.street_address) = vt.street AND cast(right('00000' + addr.zip_code, 5) AS VARCHAR(5)) = vt.zip_code_5digit
        LEFT JOIN policy_loss_summary AS pls ON a.policy_reference = pls.policy_reference
        WHERE a.lob = 'IM'
          AND a.carrier_name IN ('next-insurance', 'next-carrier', 'national-specialty')
          AND a.trans IN ('New', 'Renewal')
          AND a.date <= (SELECT MAX(eval_date_full) FROM dates_table)
          AND a.end_date > a.start_date
    ),

    -- CTE 10: De-duplicate the results to ensure one row per policy reference.
    policy_attributes AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY policy_reference ORDER BY eff_date DESC) as rn
        FROM policy_attributes_pre
    )

-- Final SELECT: Structure the output to match the requested column list.
SELECT
    policy_reference,
    policy_id,
    business_id,
    new_renewal,
    yearly_premium,
    purch_date,
    eff_date,
    exp_date,
    eff_period,
    county,
    zip_code,
    state,
    package_name AS highest_status_package,
    highest_status_name,
    cob_name,
    cob_group,
    cob_industry,
    carrier,
    carrier_name,
    channel,
    blanket_equip_occ_lim,
    blanket_equip_ded,
    blanket_factor,
    ded_factor,
    base_premium_loss_cost,
    base_premium_lcm,
    loss_cost_10k,
    rewards_coverage_prem,
    rewards_coverage_delta,
    rewards_coverage_lcm,
    reward_coverage_loss_cost,
    rewards_coverage_package_lim,
    rewards_coverage_base_lim,
    debris_removal_prem,
    debris_removal_delta,
    debris_removal_lcm,
    debris_removal_loss_cost,
    debris_removal_package_lim,
    debris_removal_base_lim,
    false_pretense_prem,
    false_pretense_delta,
    false_pretense_lcm,
    false_pretense_loss_cost,
    false_pretense_package_lim,
    false_pretense_base_lim,
    fire_dept_prem,
    fire_dept_delta,
    fire_dept_lcm,
    fire_dept_loss_cost,
    fire_dept_package_lim,
    fire_dept_base_lim,
    fire_extinguishing_service_prem,
    fire_extinguishing_service_delta,
    fire_extinguishing_service_lcm,
    fire_extinguishing_service_loss_cost,
    fire_extinguishing_service_package_lim,
    fire_extinguishing_service_base_lim,
    fuel_accessory_parts_prem,
    fuel_accessory_parts_delta,
    fuel_accessory_parts_lcm,
    fuel_accessory_parts_loss_cost,
    fuel_accessory_parts_package_lim,
    fuel_accessory_parts_base_lim,
    inventory_appraisal_prem,
    inventory_appraisal_delta,
    inventory_appraisal_lcm,
    inventory_appraisal_loss_cost,
    inventory_appraisal_package_lim,
    inventory_appraisal_base_lim,
    pollutant_removal_prem,
    pollutant_removal_delta,
    pollutant_removal_lcm,
    pollutant_removal_loss_cost,
    pollutant_removal_package_lim,
    pollutant_removal_base_lim,
    trailers_contents_prem,
    trailers_contents_delta,
    trailers_contents_lcm,
    trailers_contents_loss_cost,
    trailers_contents_package_lim,
    trailers_contents_base_lim,
    tools_clothes_prem,
    tools_clothes_emp_delta,
    tools_clothes_emp_lcm,
    tools_clothes_emp_loss_cost,
    tools_clothes_emp_package_lim,
    tools_clothes_emp_base_lim,
    tools_clothes_occ_rate,
    tools_clothes_occ_delta,
    tools_clothes_occ_package_lim,
    tools_clothes_occ_base_limit,
    borrowed_equip_prem,
    borrowed_equip_lcm,
    borrowed_equip_loss_cost,
    borrowed_equip_lim,
    leased_from_prem,
    leased_from_lcm,
    leased_from_loss_cost,
    leased_from_lim,
    leased_to_prem,
    leased_to_lcm,
    leased_to_loss_cost,
    leased_to_lim,
    loaned_equip_prem,
    loaned_equip_lcm,
    loaned_equip_loss_cost,
    loaned_equip_lim,
    water_equip_prem,
    water_equip_lcm,
    water_equip_loss_cost,
    water_equip_lim,
    misc_tools_prem,
    misc_tools_lcm,
    misc_tools_loss_cost,
    misc_tools_lim,
    tools_clothes_ca_prem,
    tools_clothes_ca_emp_lim,
    tools_clothes_ca_emp_ded,
    tools_clothes_ca_emp_lcm,
    tools_clothes_ca_emp_loss_cost,
    CASE WHEN tria_premium > 0 THEN 'Y' ELSE 'N' END AS tria_ind,
    tria_premium,
    state_surcharge_rate,
    base_premium,
    additional_coverage_premium,
    optional_coverage_premium,
    minimum_premium,
    credit_score,
    crime_score_arson,
    crime_score_burglary,
    crime_score_larceny,
    crime_score_autotheft,
    -- Added Loss & Claim Information
    incurred_loss,
    paid_loss,
    claim_count,
    non_zero_claim_count
FROM policy_attributes
WHERE rn = 1;