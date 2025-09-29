# data_warehouse_project


This project involves:

Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.

ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.

Data Modeling: Developing fact and dimension tables optimized for analytical queries.

Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.


Details: 


## Data Architecture
<img width="1121" height="481" alt="data warehouse-Page-3 drawio (1)" src="https://github.com/user-attachments/assets/2ae5ca08-1b77-4d91-a785-3fd6be482fab" />

Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database. 

Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

