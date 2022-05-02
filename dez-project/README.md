This folder hosts the final project assignment of [Data Engineering Zoomcamp by DataTalks.club]((with Apache Airflow)). The project is in line with the camp tutorials.

## Project Outline

The project is intended to build an end-to-end data pipeline for a machine learning task:
- Build the necessary GCP infrastructure as IAC (with Terraform),
- Extract data from a public dataset (with Apache Airflow),
- Transform its format locally (.csv to .parquet with Apache Airflow)
- Upload it to a GCP bucket (with Apache Airflow),
- Transfer from Bucket to Bigquery (with Apache Airflow),
- Transform the dataset on BigQuery (with dbt),
- Visualize raw data on a dashboard (on GCP DataStudio),
- Train a ML model with the proccessed data on BigQuery (with pySpark on GCP Dataproc)

This folder involves corresponding files and documentation for each step listed above. It is better to investigate folders following the order of building the pipeline:
1. terraform
2. airflow
3. dbt_cloud
4. GCP_DataStudio
5. pySpark

Each of these folders involve a PDF document that describe how to use the files included with corresponding tool.

## Project Topic

The project aims to build a pipeline to assess the performance of solar power systems. A common way to produce electricity from the sun is to use photovoltaic panels. These panels are brought together to meet desired power requirements. The overall system performance is monitored by capturing data for variety of parameters. Typically, the interdependence among environmental parameters (like temperature and irrediance), operational parameters (operating voltage, current, inverter temperature) and system output (produced power and energy) is investigated. Accordingly, the project aims to get data from a public dataset to investigate its performance and train a ML model to predict system performance.

It is important to note that the project is neither a solar engineering nor machine learning project. It solely focuses on developng a simple pipeline for investigating the PV system performance. So, the solar performance and machine learning (linear regression) approaches are just representative.

## The Dataset

The dataset of the project is extracted from Open Energy Data Initiative (OEDI) Data Lake. It is a repository of datasets aggregated from the U.S. Department of Energy’s Programs, Offices, and National Laboratories. The project uses PV system data from 4 NREL buildings (systems coded as 1430 to 1433) that are hosted on a [AWS bucket](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&limit=100&prefix=pvdaq%2Fcsv%2F)
