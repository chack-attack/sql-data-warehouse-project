/*
====================================================================
Quality Checks
====================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, 
  and standardization acoss the 'silver' schemas. It includes checks for:
  - NULLs or duplicate primary keys
  - Unwanted spaces in string fields
  - Data standardizations and consistency
  - Invalid Date ranges and orders
  - Data consistency between related fields

Usage Notes:
  - Run these checks after data loading silver layer.
  - Investigate and resolve any discrepancies found during the checks.
====================================================================
*/

--=========================================================
-- Checking silver.crm_cust_info
--=========================================================

SELECT
	prd_id, 
	REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,--Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		 --Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE  UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line, --Map product line codes to descriptive values
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
		AS DATE
	)AS prd_end_dt	 --Calculate end date as one day before the next start date
FROM bronze.crm_prd_info

--Check if prd_key can be joined with table crm_sales_details
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
SELECT sls_prd_key FROM bronze.crm_sales_details)

--Check if prd_key has unmatched data 
WHERE REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') NOT IN 
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

--Data Normalization
 SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--=========================================================
-- Checking silver.crm_prd_info
--=========================================================
--Check for null or duplicates in primary key
--Expectation: No Result

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


--Check for unwanted spaces
-- Expectation: No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check for NULLs or Negative Numbers
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt



--=========================================================
-- Checking silver.crm_sales_details
--=========================================================
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, --Recaulculate sales if original value is missing or incorrect
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price --Derive price if original value is invalid
FROM bronze.crm_sales_details
  
--Check if any sls_cust_id not matching with cst_id in crm_cust_info
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- Check if any sls_prd_key not matching with prd_key in crm_prd_info
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

--Check if order nm has unwanted space
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check for invalid dates
SELECT
NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101


--Check for invalid dates orders
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
-->> Sales = Quantity * Price
-->> Values must not be NULL, zero, or negative
/*
RULES
If sales is negative, zero, or null, derive it using quantity and price.
If price is zero or null, calculate it using sales and quantity.
If price is negative, convert it to a postive value.
*/
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity is NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price


SELECT * FROM silver.crm_sales_details

--=========================================================
-- Checking silver.erp_cust_az12
--=========================================================

--Check for an unmatched cid with cst_key from crm_cust_info
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

--Identify out of range dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE ()

--Data Standardization & Consistency
SELECT DISTINCT gen
  	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen -- Normalize gender values and handle unknown cases
FROM silver.erp_cust_az12


--=========================================================
-- Checking silver.erp_loc_a101
--=========================================================

SELECT
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry -- Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101 


--Check if and cid not match with cst_key
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)


--Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry




--=========================================================
-- Checking silver.erp_px_cat_g1v2
--=========================================================
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


--Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

SELECT *
FROM silver.erp_px_cat_g1v2
