# Formula1-Analysis-Using-Azure-Databricks

## Project Overview:
Real World Project on Formula1 Racing using Azure Databricks, Delta Lake and orchestrated by Azure Data Factory.
This project aims to provide a data analysis solution for Formula-1 race results using Azure Databricks. 
This is an ETL pipeline to ingest Formula 1 motor racing data, transform and load it into our data warehouse for reporting 
and analysis purposes. The data is sourced from ergast.com, a website dedicated to Formula 1 statistics, and is stored in Azure Datalake Gen2 storage. 
Data transformation and analysis were performed using Azure Databricks. The entire process is orchestrated using Azure Data Factory.

## Solution Architecture 
![Solution Architecture ](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/solution_arch.png)

The structure of the database is shown in the following ER Diagram and explained in the [ERD Diagram](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/data_model.png)

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

### Dominant Driver

~~~~sql
SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC
~~~~

![dominant_drivers](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/dominant_drivers.png)

### Dominant Team

~~~~sql
SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC
~~~~

![dominant_teams](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/dominant_teams.png)

## Data Factory Pipeline

#### pl_process_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/pl_process_formula1_data.png)

#### pl_ingest_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/pl_ingest_formula1_data.png)

#### pl_tranform_formula1_data
![alt text](https://github.com/ahmedashraffcih/Formula1-Analysis-Using-Azure-Databricks/blob/Setup/imgs/pl_transform_formula1_data.png)

## Contributing
Contributions to enhance and expand the capabilities of this project are welcome! Please follow these guidelines:

- Fork the repository.
- Create a new branch for your feature or enhancement.
- Commit your changes with descriptive messages.
- Submit a pull request for review.

## Contact
For any inquiries or feedback, feel free to contact the project maintainer:

Email - ahmedashraffcih@gmail.com <br>
LinkedIn - [ahmedashraffcih](https://www.linkedin.com/in/ahmedashraffcih/)

Feel free to customize and expand upon this README to better suit the specifics of your project.