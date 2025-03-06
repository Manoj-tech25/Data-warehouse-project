CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @Start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=========================================';
	
		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';
	
		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT '>> Inserting table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info_fixed.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'


		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT '>> Inserting table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'

		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_sales_detail';
		TRUNCATE TABLE bronze.crm_sales_detail;
		PRINT '>> Inserting table: crm_sales_detail'; 
		BULK INSERT bronze.crm_sales_detail 
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'
		
		
		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';
	
		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		PRINT '>> Inserting table: bronze.erp_cust_az12'; 
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'

		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		PRINT '>> Inserting table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);

		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		PRINT '>> Inserting table: bronze.erp_px_cat_g1v2'; 
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\project\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------';

		SET @batch_end_time =GETDATE();
		PRINT '================================================';
		PRINT '>> Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '================================================';

	
	END TRY
	BEGIN CATCH
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH

END
