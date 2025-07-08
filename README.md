# NYC taxi data pipeline

This project demonstrates a modern data platform pipeline that ingests and processes [NYC taxi trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using **AWS**, **Databricks**, and **Delta Lake**. Designed as an ELT-style pipeline for batch analytics.

---

## Architecture Overview

```mermaid
graph LR
    B[S3] --> C["Delta Lake (Bronze)"]
    C["Delta Lake (Bronze)"] --> D["Delta Lake (Silver)"]
    D["Delta Lake (Silver)"] --> E["Delta Lake (Gold)"]
    E["Delta Lake (Gold)"] -->  F[Power BI]
```

## Tech stack
| Layer          | Technology                            |
| -------------- | ------------------------------------- |
| Ingestion      | AWS Lambda / Databricks Autoloader    |
| Storage        | Amazon S3 (Raw), Delta Lake (Bronze+) |
| Transformation | Databricks DLT (Python / SQL)         |
| Orchestration  | Databricks Workflows / Jobs           |
| BI Layer       | Power BI                              |


## Data Flow
- **Bronze Table**<br/>
    Ingest raw parquet data using DLT Autoloader into Delta format.

- **Silver Table**<br/>
    With refined schema, and filter data to only have valid trips inside New York city.

- **Gold Table** <br/>
    Aggregated view of the fare per minute for all pick up / drop off location combination.

- **Power BI** <br/>
    Connects to SQL Warehouse to see Gold Table.
