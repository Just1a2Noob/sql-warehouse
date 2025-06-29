/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the COPY command to load data from CSV files to bronze tables.

Parameters:
@file_path VARCHAR - Base path to the CSV files directory

Usage Example:
CALL load_bronze('/home/user/Documents/github/data_engineer/sql-warehouse/datasets');
===============================================================================
*/

CREATE OR REPLACE PROCEDURE load_bronze(file_path VARCHAR DEFAULT '~/Documents/github/data_engineer/sql-warehouse/datasets')
LANGUAGE plpgsql
AS $$
DECLARE
    crm_path VARCHAR;
    erp_path VARCHAR;
BEGIN
    -- Set up file paths
    crm_path := file_path || '/source_crm/';
    erp_path := file_path || '/source_erp/';
    
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Starting Bronze Data Load Process';
    RAISE NOTICE '=================================================';

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading crm_cust_info';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE format('COPY bronze.crm_cust_info FROM %L CSV HEADER', crm_path || 'cust_info.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading crm_prd_info';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE format('COPY bronze.crm_prd_info FROM %L CSV HEADER', crm_path || 'prd_info.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading crm_sales_details';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE format('COPY bronze.crm_sales_details FROM %L CSV HEADER', crm_path || 'sales_details.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading erp_cust_az12';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE format('COPY bronze.erp_cust_az12 FROM %L CSV HEADER', erp_path || 'CUST_AZ12.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading erp_loc_a101';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE format('COPY bronze.erp_loc_a101 FROM %L CSV HEADER', erp_path || 'LOC_A101.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading erp_px_cat_g1v2';
    RAISE NOTICE '=================================================';
    
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE format('COPY bronze.erp_px_cat_g1v2 FROM %L CSV HEADER', erp_path || 'PX_CAT_G1V2.csv');

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Bronze Data Load Complete!';
    RAISE NOTICE '=================================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error loading bronze data: %', SQLERRM;
END
$$;