# Formula1-Analysis-Using-Azure-Databricks
## Project Overview:
Real World Project on Formula1 Racing using Azure Databricks, Delta Lake and orchestrated by Azure Data Factory.
This project aims to provide a data analysis solution for Formula-1 race results using Azure Databricks. 
This is an ETL pipeline to ingest Formula 1 motor racing data, transform and load it into our data warehouse for reporting 
and analysis purposes. The data is sourced from ergast.com, a website dedicated to Formula 1 statistics, and is stored in Azure Datalake Gen2 storage. 
Data transformation and analysis were performed using Azure Databricks. The entire process is orchestrated using Azure Data Factory.

## Solution Architecture 
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/Solution%20arch.PNG)

The structure of the database is shown in the following ER Diagram and explained in the [ERD Diagram](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/ER%20diagram.png)

## Project Requirements
The requirements for this project is broken down into six different parts which are;

### 1. Data Ingestion Requirements
Ingest all 8 files into Azure data lake.
1. Ingested data must have the same schema applied.
2. Ingested data must have audit columns.
3. Ingested data must be stored in columnar format (i.e., parquet).
4. We must be able to analyze the ingested data via SQL.
5. Ingestion Logic must be able to handle incremental load.
### 2. Data Transformation Requirements
1. Join the key information required for reporting to create a new table.
2. Join the key information required for analysis to create a new table.
3. Transformed tables must have audit columns.
5. Transformed data must be stored in columnar format (i.e parquet).
6. Transformation logic must be able to handle incremental load.
### 3. Data Reporting Requirements
1. Find Driver Standings.
2. Find Constructor/Team Standings.
### 4. Scheduling Requirements
1. Scheduled to run every sunday at 10pm.
2. Monitor pipelines.
3. Rerun failed pipelines.
4. Set up alerts on failures

## Databricks Reports

![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/Dominant%20Driver.PNG)

![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/Dominant%20Team.PNG)

## Data Factory Pipeline

#### pl_process_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/pl_process_formula1_data.PNG)

#### pl_ingest_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/pl_ingest_formula1_data.PNG)

#### pl_tranform_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/main/imgs/pl_transform_formula1_data.PNG)

