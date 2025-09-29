/* 
================
send bronze layer data to silver layer data
================
*/

CALL silver.load_silver();
/* 
================
CRM
================
*/
ROLLBACK;
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time        timestamptz;
    end_time          timestamptz;
    batch_start_time  timestamptz;
    batch_end_time    timestamptz;
BEGIN 
    batch_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Data Info: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Info: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id
    ,cst_key
    ,cst_firstname
    ,cst_lastname
    ,cst_material_status
    ,cst_gndr
    ,cst_create_date
    ,dwh_create_date)
    SELECT 
    cst_id
    ,cst_key
    ,TRIM(cst_firstname) AS cst_firstname
    ,TRIM(cst_lastname) AS cst_lastname
    ,CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
        ELSE 'n/a'
    END cst_material_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date,
    CURRENT_DATE
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_number
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    )t WHERE flag_number=1;

    RAISE NOTICE '>> Truncating Data Info: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Info: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
    prd_id
    ,cat_id
    ,prd_key
    ,prd_nm
    ,prd_cost
    ,prd_line
    ,prd_start_dt
    ,prd_end_dt,
    dwh_create_date
    )
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key FROM 7) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
    CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt,
    CURRENT_DATE
    FROM bronze.crm_prd_info;


    RAISE NOTICE '>> Truncating Data Info: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Info: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num
    ,sls_prd_key
    ,sls_cust_id
    ,sls_order_dt
    ,sls_ship_dt
    ,sls_due_dt
    ,sls_sales
    ,sls_quantity
    ,sls_price
    )
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0  OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0  OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0  OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
    FROM bronze.crm_sales_details;



    /* 
    ================
    ERP
    ================
    */
    RAISE NOTICE '>> Truncating Data Info: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Info: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(
    cid
    ,bdate
    ,gen)
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
        ELSE cid
    END cid,
    CASE WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
    FROM bronze.erp_cust_az12;


    RAISE NOTICE '>> Truncating Data Info: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Info: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(
        cid, cntry
    )
    SELECT
    REPLACE(cid, '-', '') cid,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
    FROM bronze.erp_loc_a101;


    RAISE NOTICE '>> Truncating Data Info: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Info: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(
        id, cat, subcat, maintenance
    )
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2;


    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze load finished. Duration: %', (batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

END;
$$;

