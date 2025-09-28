/* 
============================
Create Databse and Schemas
============================
Script Purpose;
    This scripts creates a new database named "DataWarehouse" and sets up three schemas wiithin the database: 'bronze', 'silver', 'gold'.

*/


-- delete if exists same name database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'DataWarehouse';

DROP DATABASE IF EXISTS "DataWarehouse";

-- create a new database using current existing database
CREATE DATABASE "DataWarehouse";

-- change to target database, create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


