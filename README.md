# Python ETL Scripts

This repository contains Python-based ETL pipelines for extracting data from multiple Google Sheets, processing and consolidating the data, and loading it into Azure Data Lake Storage (ADLS).

## ETL Flow

Google Sheets → Python ETL → Data Processing → Azure ADLS

## Purpose

- Extract data from multiple Google Sheets
- Clean and transform the data
- Consolidate data from multiple sources
- Perform incremental data loading
- Store processed data in Azure ADLS
- Support automated execution using Airflow/Docker
