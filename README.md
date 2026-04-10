<img width="1536" height="1024" alt="Image" src="https://github.com/user-attachments/assets/94145222-bdf5-4443-a430-871f399791a4" />
Incremental ETL Pipeline with Apache Airflow

This project demonstrates a production-style ETL workflow using Apache Airflow to incrementally synchronize data from PostgreSQL to MySQL.

Key Features

Timestamp-based incremental extraction (last_update)

Idempotent upsert logic in MySQL

Airflow Variables for persistent state tracking

Task orchestration using PythonOperators

Fully containerized environment using Docker

Tech Stack

-Apache Airflow

-PostgreSQL

-MySQL

-Python

-Docker

Use Case

Efficiently process only new or updated records, reducing load, improving performance, and ensuring reliable data synchronization.
