/*
===============================================================================
Quality Checks (Aligned with Silver Layer DDL)
===============================================================================
Purpose:
    Perform quality checks for data consistency, accuracy, and standardization.
    Includes checks for:
    - Null or duplicate keys.
    - Unwanted spaces.
    - Data consistency and standardization.
    - Invalid date ranges and orders.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Null or Duplicate in 'cst_id' (Assumed as a business key)
SELECT 
    cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Unwanted Spaces in 'cst_key'
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Marital Status Standardization
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- Gender Standardization
SELECT DISTINCT 
    cst_gndr 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Null or Duplicate in 'prd_id'
SELECT 
    prd_id, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted Spaces in 'prd_nm'
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Nulls or Negative Values in 'prd_cost'
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Product Line Standardization
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Invalid Start/End Date Orders
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Invalid Order, Ship, or Due Dates
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Data Consistency: Sales = Quantity * Price
SELECT DISTINCT 
    sls_sales, sls_quantity, sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_CUST_AZ12'
-- ====================================================================

-- Birthdates Out of Range (before 1924 or in the future)
SELECT DISTINCT 
    BDATE 
FROM silver.erp_CUST_AZ12
WHERE BDATE < '1924-01-01' 
   OR BDATE > GETDATE();

-- Gender Standardization
SELECT DISTINCT 
    GEN 
FROM silver.erp_CUST_AZ12;

-- ====================================================================
-- Checking 'silver.erp_LOC_A101'
-- ====================================================================

-- Country Code Standardization
SELECT DISTINCT 
    CNTRY 
FROM silver.erp_LOC_A101
ORDER BY CNTRY;

-- ====================================================================
-- Checking 'silver.erp_PX_CAT_G1V2'
-- ====================================================================

-- Unwanted Spaces in Category Fields
SELECT 
    * 
FROM silver.erp_PX_CAT_G1V2
WHERE CAT != TRIM(CAT) 
   OR SUBCAT != TRIM(SUBCAT) 
   OR MAINTENANCE != TRIM(MAINTENANCE);

-- Maintenance Field Standardization
SELECT DISTINCT 
    MAINTENANCE 
FROM silver.erp_PX_CAT_G1V2;
