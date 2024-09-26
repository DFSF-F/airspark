<p align="center">
      <img src="https://i.ibb.co/SQ8FGrW/img.webp" width="726">
</p>

<p align="center">
   <img src="https://img.shields.io/badge/Airflow-2.7.1-brightgreen" alt="Airflow version">
   <img src="https://img.shields.io/badge/Spark-3.5.2-violet" alt="Spark version">
   <img src="https://img.shields.io/badge/PostgreSQL-14.0-blue" alt="Postgres version">
   <img src="https://img.shields.io/badge/Java-11-red" alt="Java version">
   <img src="https://img.shields.io/badge/Python-3.11-purple" alt="Python version">
</p>

## Project Description

_This project is designed to process a stream of data logs that record CRUD (Create, Read, Update, Delete) actions performed by users. The data is aggregated within a specified time window (7 days) to generate summary reports of the number of each type of action performed by each user._

_The project runs on **Apache Airflow** and **Apache Spark** within **Docker containers**. Each day, the system processes user logs stored in CSV format and calculates the sum of each action type (create, read, update, delete) performed over the last 7 days. The results are written into CSV files, with one row per user, showing the counts for each action type._


## Input Data

_The input consists of daily CSV files with the following columns:_

  - ***email***: User’s email address.
  - ***action***: The type of action performed (_CREATE, READ, UPDATE, DELETE_).
  - ***dt***: Date of the action (_in YYYY-MM-DD format_).
    
_Each CSV file contains logs for a specific day, and filenames follow the format_ **YYYY-MM-DD.csv**.


## Output Data

The output is a CSV file that contains the aggregated results for each user over the previous 7 days. The output schema includes:

  - ***email***: User's email address.
  - ***create_count***: The count of CREATE actions.
  - ***read_count***: The count of READ actions.
  - ***update_count***: The count of UPDATE actions.
  - ***delete_count***: The count of DELETE actions.
    
_Output files are named according to the date the aggregation is generated for, using the format ***YYYY-MM-DD.csv***, and are stored in the **output/ directory**._


## etc


