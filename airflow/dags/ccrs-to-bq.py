import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import requests
import pandas as pd

# GCP Project Information
PROJECT_ID="ccrs-dashboard"
BUCKET_RAW="ccrs-dashboard-raw-data"
BUCKET_PROC="ccrs-dashboard-processed-data"
BIGQUERY_DATASET = "crashes"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

"""
    Helper Functions    
"""
# Uses the CKAN API to return the dataset url for a given year
def get_crash_file_url(year):
    url = "https://data.ca.gov/api/3/action/package_show?id=ccrs"
    response = requests.get(url)
    
    if response.status_code == 200:
        metadata = response.json()
        datasets = metadata["result"]["resources"]
        
        for dataset in datasets:
            if dataset['name'] == f"Crashes_{year}":
                print (f"Crashes_{year}: {dataset['url']}\n")
                return dataset['url']

        raise Exception(f"Error - Failed to access CKAN API. Status Code: {response.status_code}")

    else:
        raise Exception(f"Error - Failed to obtain dataset url for year {year}. Status Code: {response.status_code}")
  
# Downloads crash file data   
def download_crash_file_url(year):
    csv_file = f"raw_crashes_{year}.csv"
    url = get_crash_file_url(year)

    response = requests.get(url)
    if response.status_code == 200:
        with open(csv_file, 'wb') as f_out:
            f_out.write(response.content)
        print("Downloading crash file")
    else:
        raise Exception(f"Error - Failed to download dataset for year {year}. Status Code: {response.status_code}")
    
# Uploads files to GCS bucket
def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file,
        timeout=600
    )

# Cleans raw crash csv files, standardizes column names and data types
def clean_raw_crash_data(csv_file, proc_parquet_file):
    
    # Convert csv file to a dataframe
    df = pd.read_csv(csv_file)
    
    df.columns = (
        df.columns
        .str.lstrip(' \t')  # Remove leading tabs from column names
        .str.lower()  # Convert column names to lowercase
        .str.replace(r"[^\w\s]", "", regex=True)  # Remove special characters from column names
        .str.replace(r"\s+", "_", regex=True)  # Replace whitespace with underscores
    )
    
    print(df.columns)

    # Convert datetime columns to datetime64[ms] so it's easier for BigQuery to infer the datatype
    df["crash_date_time"] = pd.to_datetime(df["crash_date_time"], errors="coerce").astype("datetime64[ms]")
    
    df["prepareddate"] = pd.to_datetime(df["prepareddate"], errors="coerce").astype("datetime64[ms]")
    
    df["revieweddate"] = pd.to_datetime(df["revieweddate"], errors="coerce").astype("datetime64[ms]")
    
    df["createddate"] = pd.to_datetime(df["createddate"], errors="coerce").astype("datetime64[ms]")
    
    df["modifieddate"] = pd.to_datetime(df["modifieddate"], errors="coerce").astype("datetime64[ms]")
    
    df["notificationdate"] = pd.to_datetime(df["notificationdate"], errors="coerce").astype("datetime64[ms]")
    
    # Deal with a condition in the 2024 file where crash times contain a colon (:)
    df["crash_time_description"] = pd.to_numeric(
        df["crash_time_description"].astype(str).str.replace(":", "", regex=False),
        errors='coerce'
    ).astype("Int64")

    # Convert crash time to a time value
    df["crash_time_description"] = pd.to_datetime(
        df["crash_time_description"].apply(lambda x: str(x).zfill(4)),
        format='%H%M',
        errors='coerce'
    ).dt.time
    
    # Apply same condition for crash time data to notification time data
    df["notificationtimedescription"] = pd.to_numeric(
        df["notificationtimedescription"].astype(str).str.replace(":", "", regex=False),
        errors='coerce'
    ).astype("Int64")

    # # Convert notification time to a time value
    # df["notificationtimedescription"] = pd.to_datetime(
    #     df["notificationtimedescription"].apply(lambda x: str(x).zfill(4)),
    #     format='%H%M',
    #     errors='coerce'
    # ).dt.time
    
    expected_schema = {
        "collision_id": "Int64",
        "report_number": "string",
        "report_version": "Int64",
        "is_preliminary": "boolean",
        "ncic_code": "string",
        # "crash_time_description": "Int64",
        "beat": "string",
        "city_id": "Int64",
        "city_code": "string",
        "city_name": "string",
        "county_code": "Int64",
        "city_is_active": "boolean",
        "city_is_incorporated": "boolean",
        "collision_type_code": "string",
        "collision_type_description": "string",
        "collision_type_other_desc": "string",
        "day_of_week": "string",
        "dispatchnotified": "string",
        "hasphotographs": "boolean",
        "hitrun": "string",
        "isattachmentsmailed": "Int64",
        "isdeleted": "boolean",
        "ishighwayrelated": "boolean",
        "istowaway": "boolean",
        "judicialdistrict": "string",
        "motorvehicleinvolvedwithcode": "string",
        "motorvehicleinvolvedwithdesc": "string",
        "motorvehicleinvolvedwithother_desc": "string",
        "numberinjured": "Int64",
        "numberkilled": "Int64",
        "weather_1": "string",
        "weather_2": "string",
        "road_condition_1": "string",
        "road_condition_2": "string",
        "special_conditionlightingcode": "string",
        "lightingdescription": "string",
        "latitude": "float64",
        "longitude": "float64",
        "milepostdirection": "string",
        "milepostdistance": "float64",
        "milepostmarker": "string",
        "milepostunitofmeasure": "string",
        "pedestrianactioncode": "string",
        "pedestrianactiondesc": "string",
        "primary_collision_factor_code": "string",
        "primary_collision_factor_violation": "string",
        "primarycollisionfactoriscited": "boolean",
        "primarycollisionpartynumber": "Int64",
        "primaryroad": "string",
        "reportingdistrict": "string",
        "reportingdistrictcode": "string",
        "roadwaysurfacecode": "string",
        "secondarydirection": "string",
        "secondarydistance": "float64",
        "secondaryroad": "string",
        "secondaryunitofmeasure": "string",
        "sketchdesc": "string",
        "trafficcontroldevicecode": "string",
        "iscountyroad": "boolean",
        "isfreeway": "boolean",
        "chp555version": "Int64",
        "isadditonalobjectstruck": "Int64",
        # "notificationtimedescription": "Int64",
        "hasdigitalmediafiles": "boolean",
        "evidencenumber": "string",
        "islocationrefertonarrative": "boolean",
        "isaoionesameaslocation": "boolean"
    }
    
    for col, dtype in expected_schema.items():
        if col in df.columns and col not in ['crash_date_time', 'prepareddate', 'revieweddate', 'createddate', 'modifieddate', 'notificationdate', 'crash_time_description', 'notificationtimedescription']:
            df[col] = df[col].astype(dtype)
        
    print(f"Columns: {df.columns.tolist()}")

    df.to_parquet(proc_parquet_file, engine="pyarrow", index=False)

"""
    DAG 
"""

dag = DAG(
    "ccrs_gcp_ingestion",
    schedule_interval="@yearly",
    start_date=datetime(2016, 1, 1),
    catchup=True, 
    max_active_runs=1,
)

table_name_template = 'crashes_{{ execution_date.strftime("%Y") }}'
raw_file_template_csv = 'raw_crashes_{{ execution_date.strftime("%Y") }}.csv'
proc_file_template_parquet = 'crashes_{{ execution_date.strftime("%Y") }}.parquet'
consolidated_table_name = 'crashes_all'

# Task 1: Download crash file
download_task = PythonOperator(
    task_id="download_crash_data",
    python_callable=download_crash_file_url,
    op_kwargs={"year": "{{ execution_date.strftime('%Y') }}"},
    dag=dag
)

# Task 2: Upload raw csv file to GCS
upload_raw_files_task = PythonOperator(
    task_id="upload_raw_csv",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET_RAW,
        "object_name": "crashes/raw_crashes_{{ execution_date.strftime('%Y') }}.csv",
        "local_file": f"{path_to_local_home}/raw_crashes_{{{{ execution_date.strftime('%Y') }}}}.csv",
        "gcp_conn_id": "gcp-airflow"
    },
    dag=dag,
)

# Task 3: Clean and convert raw csv file to parquet
clean_raw_crash_data_task = PythonOperator(
    task_id="clean_raw_data",
    python_callable=clean_raw_crash_data,
    op_kwargs={
        "csv_file": f"{path_to_local_home}/raw_crashes_{{{{ execution_date.strftime('%Y') }}}}.csv",
        "proc_parquet_file": f"{path_to_local_home}/crashes_{{{{ execution_date.strftime('%Y') }}}}.parquet"
    },
    dag=dag,
)

# Task 4: Upload processed parquet file to GCS
upload_processed_files_task = PythonOperator(
    task_id="upload_processed_parquet",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET_PROC,
        "object_name": "crashes/crashes_{{ execution_date.strftime('%Y') }}.parquet",
        "local_file": f"{path_to_local_home}/crashes_{{{{ execution_date.strftime('%Y') }}}}.parquet",
        "gcp_conn_id": "gcp-airflow"
    },
    dag=dag,
)

# Task 5: Create final table
create_final_table_task = BigQueryInsertJobOperator(
    task_id="create_final_table",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{consolidated_table_name}`
                (
                    unique_row_id BYTES,
                    filename STRING,
                    collision_id INT64,
                    report_number STRING,
                    report_version INT64,
                    is_preliminary BOOLEAN,
                    ncic_code STRING,
                    crash_date_time TIMESTAMP,
                    crash_time_description TIME,
                    beat STRING,
                    city_id INT64,
                    city_code STRING,
                    city_name STRING,
                    county_code INT64,
                    city_is_active BOOLEAN,
                    city_is_incorporated BOOLEAN,
                    collision_type_code STRING,
                    collision_type_description STRING,
                    collision_type_other_desc STRING,
                    day_of_week STRING,
                    dispatchnotified STRING,
                    hasphotographs BOOLEAN,
                    hitrun STRING,
                    isattachmentsmailed INT64,
                    isdeleted BOOLEAN,
                    ishighwayrelated BOOLEAN,
                    istowaway BOOLEAN,
                    judicialdistrict STRING,
                    motorvehicleinvolvedwithcode STRING,
                    motorvehicleinvolvedwithdesc STRING,
                    motorvehicleinvolvedwithotherdesc STRING,
                    numberinjured INT64,
                    numberkilled INT64,
                    weather_1 STRING,
                    weather_2 STRING,
                    road_condition_1 STRING,
                    road_condition_2 STRING,
                    special_conditionlightingcode STRING,
                    lightingdescription STRING,
                    latitude FLOAT64,
                    longitude FLOAT64,
                    milepostdirection STRING,
                    milepostdistance FLOAT64,
                    milepostmarker STRING,
                    milepostunitofmeasure STRING,
                    pedestrianactioncode STRING,
                    pedestrianactiondesc STRING,
                    prepareddate TIMESTAMP,
                    primary_collision_factor_code STRING,
                    primary_collision_factor_violation STRING,
                    primarycollisionfactoriscited BOOLEAN,
                    primarycollisionpartynumber INT64,
                    primaryroad STRING,
                    reportingdistrict STRING,
                    reportingdistrictcode STRING,
                    revieweddate TIMESTAMP,
                    roadwaysurfacecode STRING,
                    secondarydirection STRING,
                    secondarydistance FLOAT64,
                    secondaryroad STRING,
                    secondaryunitofmeasure STRING,
                    sketchdesc STRING,
                    trafficcontroldevicecode STRING,
                    createddate TIMESTAMP,
                    modifieddate TIMESTAMP,
                    iscountyroad BOOLEAN,
                    isfreeway BOOLEAN,
                    chp555version INT64,
                    isadditonalobjectstruck INT64,
                    notificationdate TIMESTAMP,
                    notificationtimedescription INT64,
                    hasdigitalmediafiles BOOLEAN,
                    evidencenumber STRING,
                    islocationrefertonarrative BOOLEAN,
                    isaoionesameaslocation BOOLEAN
                )    
            """,
            "useLegacySql": False,
        }
    },
    retries=3,
    dag=dag,
)

# Task 6: Create external yearly table
create_external_yearly_table_task = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    gcp_conn_id="gcp-airflow",
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": f"{table_name_template}_ext"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET_PROC}/crashes/{proc_file_template_parquet}"],
            "autodetect": True
        }
    },
    dag=dag
)

# Task 7: Create native yearly table
create_native_yearly_table_task = BigQueryInsertJobOperator(
    task_id="create_native_yearly_table",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_native`
                AS
                SELECT
                    MD5(CONCAT(
                        COALESCE(CAST(collision_id AS STRING), ""),
                        COALESCE(CAST(report_number AS STRING), ""),
                        COALESCE(CAST(report_version AS STRING), ""),
                        COALESCE(CAST(ncic_code AS STRING), ""),
                        COALESCE(CAST(crash_date_time AS STRING), "")
                    )) AS unique_row_id,
                    "{proc_file_template_parquet}" AS filename,
                    *
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_ext`;
            """,
            "useLegacySql": False,
        }
    },
    retries=3,
    dag=dag,
)

# Task 8: Merge tables into one consolidated dataset
merge_to_final_table_task = BigQueryInsertJobOperator(
    task_id="merge_to_final_table",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{consolidated_table_name}` T
                USING `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_native` S
                ON T.unique_row_id = S.unique_row_id
                WHEN NOT MATCHED THEN
                    INSERT ROW;
            """,
            "useLegacySql": False,
        }
    },
    retries=3,
    dag=dag,
)

# # Task 9: Delete local files
cleanup_task = BashOperator(
    task_id="cleanup_files",
    bash_command=f"rm -f {path_to_local_home}/{raw_file_template_csv} {path_to_local_home}/{proc_file_template_parquet}",
    dag=dag,
)

download_task >> upload_raw_files_task >> clean_raw_crash_data_task >> upload_processed_files_task >> create_final_table_task >> create_external_yearly_table_task >> create_native_yearly_table_task >> merge_to_final_table_task >> cleanup_task