/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC silver.Load_Silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.Load_Silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY -- SQL will Runs the TRY Block,and if it fails,it runs the CATCH block to handle the error
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Tables';
		PRINT '------------------------------------------------';
/* Here we check the Data before doing any transformations on it,
- Check for Nulls, or Duplicates in the Primary Key,
  Because the Primary Key must be Unique and Not Null 
- Check the Unwanted Spaces 
*/
--SELECT 
--	cst_id,
--	COUNT(*)
--FROM [bronze].[crm_cust_info]
--GROUP BY cst_id
--HAVING COUNT(*)>1 OR cst_id IS NULL; 


-- Checking the unwanted Spaces 

--SELECT
--	cst_id,
--	cst_firstname
--FROM [bronze].[crm_cust_info]
--WHERE [cst_firstname] != TRIM([cst_firstname]); -- So if the original value is not equal to 
-- the same value after Trimming, it means there are Spaces 

--
        SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE [silver].[crm_cust_info];
PRINT '>> Inserting Data Into: silver.crm_cust_info';
INSERT INTO [silver].[crm_cust_info](
	[cst_id],
	[cst_key],
	[cst_firstname],
	[cst_lastname],
	[cst_marital_status],
	[cst_gndr],
	[cst_create_date])
SELECT 
	[cst_id],
	[cst_key],
	TRIM([cst_firstname]) AS cst_firstname,
	TRIM([cst_lastname]) AS cst_lastname,
CASE WHEN UPPER([cst_marital_status]) = 'S' THEN 'Single'
	WHEN UPPER([cst_marital_status]) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER([cst_gndr]) = 'M' THEN 'Male'
	WHEN UPPER(cst_gndr)= 'F' THEN 'Female'
	ELSE 'n/a'
END cst_gndr,
	[cst_create_date]
FROM(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Latest
	FROM [bronze].[crm_cust_info]
	WHERE cst_id IS NOT NULL
)t  
WHERE Latest=1;
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';
/*
Since we will join (CID) from  [erp_CUST_AZ12] table 
with (cst_id) from  [crm_cust_info] table 
we need to check they are identical, put we fount that (CID) from [erp_CUST_AZ12] table
not identical so need to fix it.

- Sencond we will check the date if there're outlier values

*/
SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_CUST_AZ12';
TRUNCATE TABLE [silver].[erp_CUST_AZ12];
PRINT '>> Inserting Data Into: silver.erp_CUST_AZ12';
INSERT INTO silver.erp_CUST_AZ12 (
	[CID],
	[BDATE],
	[GEN] 
)
SELECT 
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(cid))
	ELSE CID 
END AS CID,
CASE WHEN [BDATE] >GETDATE() THEN NULL 
	ELSE [BDATE]
END AS [BDATE],
CASE 
	WHEN UPPER(TRIM([GEN])) IN ('M','MALE') THEN 'Male'
	WHEN UPPER(TRIM([GEN])) IN ('F','FEMALE') THEN 'Female'
	ELSE 'n/a'
END AS GEN
FROM [bronze].[erp_CUST_AZ12];
 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

--SELECT DISTINCT GEN
--FROM [bronze].[erp_CUST_AZ12];

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE [silver].[crm_sales_details];
PRINT '>> Inserting Data Into: silver.crm_sales_details';
INSERT INTO [silver].[crm_sales_details](
	[sls_ord_num],
	[sls_prd_key],
	[sls_cust_id],
	[sls_order_dt],
	[sls_ship_dt],
	[sls_due_dt],
	[sls_sales],
	[sls_quantity],
	[sls_price]
)
SELECT 
	[sls_ord_num],
	[sls_prd_key],
	[sls_cust_id],
CASE WHEN[sls_order_dt]=0 OR LEN([sls_order_dt])!=8 THEN NULL
	 ELSE CAST(CAST([sls_order_dt] AS VARCHAR)AS DATE)
END AS sls_order_dt,
	CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)AS sls_ship_dt,
	CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)AS [sls_due_dt],
CASE WHEN [sls_sales] IS NULL 
	 OR [sls_sales] <=0 
	 OR [sls_sales]!= [sls_quantity] * ABS([sls_price])
	 THEN [sls_quantity] * ABS([sls_price])
	 ELSE [sls_sales]
END AS [sls_sales],
	[sls_quantity],
CASE  WHEN [sls_price] IS NULL 
	 OR [sls_price] <=0 
	 THEN[sls_sales] / NULLIF([sls_quantity],0)
	 ELSE [sls_price]
END AS [sls_price]
FROM [bronze].[crm_sales_details]
-- WHERE [sls_order_dt] <=0 OR LEN ([sls_order_dt])!=8; -- we are checking if the length if the raw is less than 8 we consider it as a bad data	

-- In SQL Server to convert an INT Column To DATE you must first convert it to String then FROM string to DATE
 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE [silver].[crm_prd_info];
PRINT '>> Inserting Data Into: silver.crm_prd_info';
INSERT INTO [silver].[crm_prd_info](
	[prd_id],
	[category_id],
	[prd_key],
	[prd_nm],
	[prd_cost],
	[prd_line],
	[prd_start_dt],
	[prd_end_dt]
)
SELECT 
	[prd_id],
	REPLACE(SUBSTRING([prd_key],1,5),'-','_')AS category_id,
	SUBSTRING([prd_key],7,LEN([prd_key])) AS prd_key,
	[prd_nm],
	ISNULL([prd_cost],0)AS prd_cost,
CASE UPPER(TRIM([prd_line])) 
	 WHEN 'M' THEN 'Mountain' 
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'R' THEN 'Road'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS product_line,
	CAST([prd_start_dt]AS DATE)AS prd_start_dt,
	CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-2 AS DATE) AS prd_end_dt
	/*
	why we used LEAD here, cause we had I problem with the prd_start_dt column	
	which was greater than the prd_end_dt at some Rows so after brainstorming 
	we found this is the perfect solution 
	*/
FROM [bronze].[crm_prd_info];
 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_LOC_A101';
TRUNCATE TABLE silver.[erp_LOC_A101];
PRINT '>> Inserting Data Into: silver.erp_LOC_A101';
INSERT INTO [silver].[erp_LOC_A101](
	[CID],
	[CNTRY]
)
SELECT 
	REPLACE (CID,'-','') AS CID,
CASE WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
	 WHEN TRIM(CNTRY)='DE' THEN 'Germany'
	 WHEN TRIM(CNTRY)= '' OR CNTRY IS NULL THEN 'n/a'
	 ELSE TRIM(CNTRY)
END AS CNTRY
FROM [bronze].[erp_LOC_A101];

/*
Here we did Data transformation
- Handled Invalid Values (REPLACE (CID,'-','') AS CID)
- for CNTRY Column we've done Data Normalization
- Handled Missing values 
- we Removed the unwanted Spaces
*/ 
SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
/*
After checking the data we found that 
there's no need for any kind of transofmation
So we will Insert it immediately.
*/ 

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';
TRUNCATE TABLE silver.[erp_PX_CAT_G1V2];
PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';
INSERT INTO [silver].[erp_PX_CAT_G1V2](
[ID],
[CAT],
[SUBCAT],
[MAINTENANCE])
SELECT	
		[ID]
      ,[CAT]
      ,[SUBCAT]
      ,[MAINTENANCE]
  FROM [DWH].[bronze].[erp_PX_CAT_G1V2]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
