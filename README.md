<h1 align="center">💥🚗 California Crash Reporting System (CCRS) </br> Pipeline & Dashboard </h1>

<p align="center">
   <img src="https://img.shields.io/github/last-commit/nikitajakkam/ccrs-dashboard">
   <img src="https://img.shields.io/github/license/nikitajakkam/ccrs-dashboard">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" alt="Python (Pandas)">
  <img src="https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="GCP">
  <img src="https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white" alt="BigQuery">
  <img src="https://img.shields.io/badge/GCS-4285F4?style=for-the-badge&logo=googlestorage&logoColor=white" alt="Google Cloud Storage">
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white" alt="Terraform">
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt Cloud">
  <img src="https://img.shields.io/badge/-Looker-4285F4?style=for-the-badge&logo=looker&logoColor=white"/>
  <img src="https://img.shields.io/badge/-Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white"/>
</p>

This project creates an ETL pipeline for extracting, processing, and analyzing motor vehicle accident data from the [California Crash Reporting System (CCRS)](https://data.ca.gov/), part of the California Open Data Portal. I used this pipeline to build two dashboards to visualize crash data taken from **2016 to 2024** that you can view below:

- **[Tableau Public Dashboard](https://public.tableau.com/app/profile/nikita.jakkam/viz/CCRSDashboard/OverviewDashboard)** — A **static** Tableau Public dashboard that visualizes crash trends from 2016–2024. I built this version for a more polished and customized dashboard design.
- **[Looker Studio Dashboard](https://lookerstudio.google.com/s/houpEX8slXw)** — A **dynamic dashboard** connected directly to **BigQuery** using the data pipeline built for this project.

<p align="center">
<img src="images/tableau-dashboard.png" alt="Tableau Dashboard Overview Page Screenshot" width="700"/>
</p>

## ⚙️ Pipeline Overview & Architecture
This data pipeline was created using Google Cloud Platform (GCP) and is orchestrated via Apache Airflow to automate the ingestion and processing of crash data on a yearly basis. 

### 🛠 Tools & Technologies
- ⚡ **Workflow Orchestration**: Apache Airflow  
- 🏢 **Data Warehouse**: Google BigQuery  
- 💾 **Data Lake**: Google Cloud Storage (GCS)
- 🔄 **Data Modeling & Transformations**: dbt Cloud  
- 🐍 **Data Cleaning**: Python (Pandas)  
- 🏗️ **Infrastructure as Code (IaC)**: Terraform
     
<p align="center">
<img src="images/pipeline-diagram.svg" alt="Data Pipeline Diagram" width="1000"/>
</p>

### 🔄 Pipeline Steps
1. **Extraction**
   - Yearly crash CSV files are downloaded from the CCRS data set via the CKAN API.
   - These yearly files are stored in a **raw files data bucket in Google Cloud Storage (GCS)**.

2. **Loading & Data Cleaning**
   - Raw CSV files are processed using **Python (Pandas)** to standardize column names and data types.
   - Processed CSV files are converted to Parquet files and stored in a **processed data bucket in Google Cloud Storage (GCS)**. 

3. **Transformation**
   - The processed files are combined into one merged table called **crashes** which is then processed via **dbt Cloud** to create intermediary tables and a final reporting table used for the dashboards
   - Transformation steps include:
     - Data cleaning and standardization
     - Creating a fact table for crashes  
     - Creating a dimensional table for location-level data
     - Combining the fact and dimensional table into a reporting table used for the dashboards

5. **Workflow Orchestration**
   - An **Apache Airflow DAG** orchestrates a portion of the pipeline:
     - The DAG is scheduled to run yearly and ingests the current year's crash CSV file as well as backfills going back to 2016
     - The DAG orchestrates the downloading of crash files from the CKAN API, Python data cleaning, and merging of the yearly crash files into a final BigQuery table

This project isn't packaged to be setup locally, but for general instructions on how the project is setup please see the [setup.md File](setup.md).  
  
## 💐 Acknowledgements
I built this project after completing the modules in DataTalks.Club's [Data Engineering ZoomCamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). Huge thanks to the creators and DataTalks.Club community for the open-source curriculum that helped me learn the tools needed to create this data pipeline! A huge thanks to Manuel Guerra as well whose [detailed notes & Airflow dockerfile](https://github.com/ManuelGuerra1987/data-engineering-zoomcamp-notes) helped get up and running with Airflow. 
