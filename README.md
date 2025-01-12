# Taxi data ETL
Simple ETL using airflow and pyspark in GCP environment, we create a simple data model to allow the data analysts to build BI reports.
Project can be deployed in a GCP environment.
## Technologies used.
- Python: We use pyspark as our framework to do the data transformations. also Pandas is used.
- SQL: To perform analytics in BigQuery
- Airflow: To orcheastate the ETL tasks.
- Google Cloud Platform:
  - Dataproc: Simple managed spark clusters to perform data engineering tasks.
  - Composer: Managed airflow environment to orchestate ETL tasks.
  - Bigquery: To perform analytics.
  - Google Cloud Storage: Here we store our raw files.
 
## Architecture

![architecture](https://github.com/cesarAndramart/uber_etl/blob/main/ETL-2.png)

## Dataset
We ingest the yellow trip data, downloading it from: 

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data dictionary is available here:
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
