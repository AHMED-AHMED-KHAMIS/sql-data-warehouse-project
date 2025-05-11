
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
	SELECT 
		ROW_NUMBER() OVER(ORDER BY prd_key,prd_start_dt) AS product_key,-- To create a Surrogate key
		pri.[prd_id] AS product_id,
		pri.[prd_key] AS product_number,
		pri.[prd_nm]AS product_name,
		pri.[category_id],
		pcg.[CAT] AS category,
		pcg.[SUBCAT] AS subcategory,
		pcg.[MAINTENANCE] AS maintenance,
		pri.[prd_cost] AS product_cost,
		pri.[prd_line] AS product_line,
		pri.[prd_start_dt] AS Product_start_date,
		pri.[prd_end_dt] AS Product_end_date 
	FROM [silver].[crm_prd_info] AS pri
	LEFT JOIN [silver].[erp_PX_CAT_G1V2] AS pcg
	ON pri.category_id = pcg.ID;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT
	sd.[sls_ord_num] AS order_number,
	dp.product_key, -- using the Dim_product Surrogate Key 
	dc.customer_key, -- using the Dim_customer Surrogate Key 
	sd.[sls_order_dt] AS order_date,
	sd.[sls_ship_dt] AS shipping_date,
	sd.[sls_due_dt] AS due_date,
	sd.[sls_sales] AS sales_amount,
	sd.[sls_quantity] As quantity,
	sd.[sls_price] AS price
FROM [silver].[crm_sales_details] AS sd
LEFT JOIN gold.dim_products AS dp
	ON sd.sls_prd_key= dp.product_number
LEFT JOIN gold.dim_customers dc
	ON sd.sls_cust_id=dc.customer_id;
GO
