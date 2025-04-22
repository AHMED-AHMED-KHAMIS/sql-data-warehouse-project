/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY -- SQL will Runs the TRY Block,and if it fails,it runs the CATCH block to handle the error
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
	-- We are going to use BULK INSERT to Load the data from source System into the Database 
		SET @start_time = GETDATE(); -- Here we track ETL Duration,
						--so we can identify bottlenecks, optimize performance, and detect issues
		BULK INSERT [bronze].[crm_cust_info] -- here we put the table name
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- we put file path
		WITH (
			FIRSTROW = 2, -- means the data inside the file start from the second row (don't Load the first row)
			FIELDTERMINATOR =',', -- means the separator between Fields is (,)
			TABLOCK -- It enhance the performance Because we Lock the hole Table During Loading the Data
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		-- Then we do a quick test for the data to make sure everything it okay.
		--SELECT *
		--FROM [bronze].[crm_cust_info];

		--SELECT COUNT(*)
		--FROM [bronze].[crm_cust_info];

		-- 
		SET @start_time = GETDATE(); 
		BULK INSERT [bronze].[crm_prd_info] 
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',', 
			TABLOCK 
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		--
		SET @start_time = GETDATE();
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',', 
			TABLOCK 
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		--

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE(); 
		BULK INSERT [bronze].[erp_CUST_AZ12]
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',', 
			TABLOCK 
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		--
		SET @start_time = GETDATE(); 
		BULK INSERT [bronze].[erp_LOC_A101]
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',', 
			TABLOCK 
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		-- 
		SET @start_time = GETDATE(); 
		BULK INSERT [bronze].[erp_PX_CAT_G1V2]
		FROM 'D:\DWH Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',', 
			TABLOCK 
			);
			SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
	END TRY -- SQL will Runs the TRY Block,and if it fails,it runs the CATCH block to handle the error
	BEGIN CATCH -- SQL will Runs the TRY Block,and if it fails,it runs the CATCH block to handle the error
	END CATCH
END
