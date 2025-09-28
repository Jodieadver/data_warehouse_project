/* 
========================================
Store procedure: load bronze layer (source -> bronze)
========================================

Script Purpose:
    This store procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Copy the whole data from csv Files to bronze tables,

Parameters:
    None.

Use Examples:
    CALL bronze.load_bronze();

*/



CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time        timestamptz;
    end_time          timestamptz;
    batch_start_time  timestamptz;
    batch_end_time    timestamptz;
BEGIN
    
    batch_start_time := clock_timestamp();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==========================================';

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '==========================================';

    /* crm_cust_info */
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status,
        cst_gndr, cst_create_date
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'crm_cust_info Record count: %', (SELECT COUNT(*) FROM bronze.crm_cust_info);

    /* crm_prd_info */
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info(
        prd_id, prd_key, prd_nm, prd_cost,
        prd_line, prd_start_dt, prd_end_dt
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'crm_prd_info Record count: %', (SELECT COUNT(*) FROM bronze.crm_prd_info);

    /* crm_sales_details */
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
        sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'crm_sales_details Record count: %', (SELECT COUNT(*) FROM bronze.crm_sales_details);

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '==========================================';

    /* erp_cust_az12 */
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12(
        cid, bdate, gen
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'erp_cust_az12 Record count: %', (SELECT COUNT(*) FROM bronze.erp_cust_az12);

    /* erp_loc_a101 */
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101(
        cid, cntry
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'erp_loc_a101 Record count: %', (SELECT COUNT(*) FROM bronze.erp_loc_a101);

    /* erp_px_cat_g1v2 */
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2(
        id, cat, subcat, maintenance
    )
    FROM '/Users/xxscarlett/Desktop/data_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;
    RAISE NOTICE 'erp_px_cat_g1v2 Record count: %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze load finished. Duration: %', (batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '===================================';
    RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
    RAISE NOTICE '===================================';
END;
$$;

