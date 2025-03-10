create or alter procedure silver.load_silver as
begin
	DECLARE @Start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading silver Layer';
		PRINT '=========================================';
	
		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';

		SET @Start_time = GETDATE();
		print '>> truncate the silver.crm_cust_info'
		truncate table silver.crm_cust_info;
		print '>> inserting the silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,

			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n\a'
			END  AS cst_marital_status,

			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN cst_gndr IS NULL OR TRIM(cst_gndr) = '' THEN 'n\a'
				ELSE 'n\a'
			END  AS cst_gndr,

		cst_create_date
		FROM(SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info where cst_id IS NOT NULL)t WHERE flag_last = 1;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'
		/*UPDATE silver.crm_cust_info
		SET cst_gndr = 
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN cst_gndr IS NULL OR TRIM(cst_gndr) = '' THEN 'n\a'
				ELSE 'n\a'
			END;

		UPDATE silver.crm_cust_info
		SET cst_material_status = 
			CASE 
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				WHEN cst_material_status IS NULL OR TRIM(cst_material_status) = '' THEN 'n\a'
				ELSE 'n\a'
			END;
			*/
		/*
		This is a multi-line comment.
		It can span multiple lines.
		*/


		SET @Start_time = GETDATE();
		print '>> truncate the silver.crm_prd_info'
		truncate table silver.crm_prd_info;
		print '>> inserting the silver.crm_prd_info'
		insert into silver.crm_prd_info(
		prd_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		select 
		prd_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case 
			when upper(TRIM(prd_line)) = 'M' then 'Mountain'
			when upper(TRIM(prd_line)) = 'R' then 'Road'
			when upper(TRIM(prd_line)) = 'S' then 'Other Sales'
			when upper(TRIM(prd_line)) = 'T' then 'Touring'
			else 'N\A'
		end as prd_line,
		prd_start_dt,
		prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'
		
		
		SET @Start_time = GETDATE();
		print '>> truncate the silver.crm_sales_detail'
		truncate table silver.crm_sales_detail;
		print '>> inserting the silver.crm_sales_detail'
		insert into silver.crm_sales_detail(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price

		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
				else cast(cast(sls_order_dt as nvarchar) as date)
			end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
				else cast(cast(sls_ship_dt as nvarchar) as date)
			end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
				else cast(cast(sls_due_dt as nvarchar) as date)
			end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
				then sls_quantity * ABS(sls_price)
				else sls_sales
			end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
			then sls_sales /nullif(sls_quantity,0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_detail;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'
		
		
		
		
		
		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';
		SET @Start_time = GETDATE();
		print '>> truncate the silver.erp_cust_az12'
		truncate table silver.erp_cust_az12;
		print '>> inserting the silver.erp_cust_az12'
		insert into silver.erp_cust_az12(cid,bdate,gen)select 
		case when cid like '%NAS%' then SUBSTRING(cid,4 ,len(cid))
			 else cid 
		end as cid,
		case when bdate > GETDATE() then null
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('M' , 'MALE') then 'Male'
			 when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
			 else 'N\A'
		end as gen
		from bronze.erp_cust_az12;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'



		SET @Start_time = GETDATE();
		print '>> truncate the silver.erp_loc_a101'
		truncate table silver.erp_loc_a101;
		print '>> inserting the silver.erp_loc_a101'
		insert into silver.erp_loc_a101(cid,cntry) select 
		replace (cid ,'-' , '' )cid ,
		case when trim(cntry) = 'DE' then 'germany'
		when trim(cntry) in ('us','USA') then 'United states'
		when trim(cntry) = '' or cntry is null then 'N\A'
		else cntry
		end as cntry
		from bronze.erp_loc_a101;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'


		SET @Start_time = GETDATE();
		print '>> truncate the silver.erp_loc_a101'
		truncate table silver.erp_px_cat_g1v2;
		print '>> inserting the silver.erp_loc_a101'
		insert into silver.erp_px_cat_g1v2(
		cat,
		subcat,
		maintenance)
		select cat,subcat,maintenance from bronze.erp_px_cat_g1v2;
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -----------------'

		SET @batch_end_time =GETDATE();
		PRINT '================================================';
		PRINT '>> Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '================================================';

	
	END TRY
	BEGIN CATCH
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH
end
