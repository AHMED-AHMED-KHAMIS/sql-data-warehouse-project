/*
===============================================================================
DDL Script: Create Silver Layer Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'Silver' schema, dropping existing tables 
    If they already exist.
	  Run this script to redefine the DDL structure of 'Silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL 
	DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR (50),
cst_marital_status NVARCHAR(12),
cst_gndr NVARCHAR(12),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE() -- this is a metadata column, used to track when a record was inserted into DWH. 
);


IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL 
	DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(60),
prd_cost INT,
prd_line NVARCHAR(7),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL 
	DROP TABLE silver.erp_CUST_AZ12;

CREATE TABLE silver.erp_CUST_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(20),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL 
	DROP TABLE silver.erp_LOC_A101;

CREATE TABLE silver.erp_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL 
	DROP TABLE silver.erp_PX_CAT_G1V2;

CREATE TABLE silver.erp_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(15),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
