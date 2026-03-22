✈️ Aviation Data Engineering & Analytics Platform

📌 Project Overview
This project is an end-to-end Data Engineering pipeline designed to process, transform, and visualize aviation data. It demonstrates how raw data can be converted into meaningful insights using modern data tools and cloud technologies.

The pipeline follows a Medallion Architecture (Bronze → Silver → Gold) approach and delivers insights through an interactive Streamlit dashboard.

🚀 Objectives
-> Build a scalable data pipeline for aviation datasets
-> Perform data cleaning and transformation
-> Store structured data in a cloud data warehouse
-> Create an interactive dashboard for analytics
-> Demonstrate real-world data engineering workflow

🏗️ Architecture

        Raw Data
           │
     (Bronze Layer - Data Lake)
           │
    Data Cleaning & Transformation
           │
        (Silver Layer)
           │
      Load into Snowflake
           │
        (Gold Layer)
           │
   Streamlit Dashboard (Visualization)

🛠️ Tech Stack
->Programming: Python
->Big Data: Apache Spark (PySpark)
->Cloud Storage: Azure Data Lake (ADLS Gen2)
->Data Warehouse: Snowflake
->Orchestration: Apache Airflow
->Visualization: Streamlit, Plotly
->Version Control: Git

📂 Project Structure
├── data/
│   ├── raw/                # Raw aviation data (Bronze)
│   ├── processed/         # Cleaned data (Silver)
│
├── scripts/
│   ├── bronze_to_silver.py
│   ├── transformations.py
│
├── airflow/
│   ├── dags/
│
├── snowflake/
│   ├── schema.sql
│   ├── queries.sql
│
├── dashboard/
│   ├── app.py             # Streamlit dashboard
│
├── utils/
│   ├── spark_session.py
│
├── requirements.txt
└── README.md


⚙️ Data Pipeline Flow
1️⃣ Bronze Layer (Raw Data)
Ingest raw aviation data into Azure Data Lake
Store data in its original format
2️⃣ Silver Layer (Cleaned Data)
Handle missing values
Remove duplicates
Apply schema and transformations using PySpark
3️⃣ Gold Layer (Analytics Ready)
Load transformed data into Snowflake
Create optimized tables for analytics
4️⃣ Visualization Layer
Build interactive dashboard using Streamlit
Generate insights like:
Flight delays
Airline performance
Airport traffic trends
