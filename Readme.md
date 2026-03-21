 Project Overview
This project is a comprehensive Data Engineering and Analytics platform designed to ingest, transform, and visualize multi-source data. It features a robust ETL pipeline that migrates raw data into a cloud data warehouse and provides an interactive dashboard for business stakeholders.

Currently, the hub supports two primary domains:

Retail Operations: KPI tracking for sales, profit margins, and category performance.

Aviation Insights: Real-time flight performance analysis, delay tracking, and carrier efficiency.

 Tech Stack
Language: Python 3.11

Libraries: Pandas (Data Wrangling), Plotly (Visuals), Streamlit (Frontend)

Data Warehouse: Google BigQuery / Snowflake

Version Control: Git & GitHub (SSH Authenticated)

Database Logic: Advanced SQL (Normalization, Window Functions, CTEs)

 Architecture & Data Flow
Data Ingestion: Python scripts extract data from CSV/API sources.

Transformation: Data is cleaned using Pandas (handling nulls, type casting).

Loading: Cleaned data is pushed to the Cloud Data Warehouse (BigQuery/Snowflake).

Modeling: SQL Views are created to normalize data (3NF) and calculate KPIs.

Visualization: A Streamlit dashboard queries the warehouse to display real-time metrics.

 Getting Started
1. Prerequisites
Python 3.11 (Ensure your environment is updated from 3.13)

Cloud Service Account Key (JSON for BigQuery or Snowflake Credentials)

2. Installation
Bash
# Clone the repository
git clone git@github.com:Sowmya273814/multi-source-data-hub.git

# Navigate to project folder
cd multi-source-data-hub

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
3. Running the Dashboard
Bash
streamlit run app.py
 Database Schema & SQL Logic
The project utilizes advanced SQL techniques to ensure data integrity and performance:

Normalization: Tables are organized into 3rd Normal Form (3NF) to reduce redundancy.

Primary/Alternate Keys: Robust indexing using Auto-incrementing IDs and Unique Alternate Keys (e.g., Email, SSN).

Analytics: Complex joins (Equi and Theta) and Window Functions (DENSE_RANK, PARTITION BY) for department-level and flight-level ranking.

 Key Features
Real-time KPI Tracking: Instant visibility into Total Sales and Profit.

Delay Analysis: Breakdown of aviation delays by carrier and origin.

Data Consistency: ACID-compliant transactions with COMMIT and ROLLBACK logic in the ingestion scripts.

 Contribution
This project was developed by Sowmya as part of a technical portfolio in Data Engineering. For inquiries or collaboration, please reach out via GitHub.