# California Crash Reporting System (CCRS)  
**Data Pipeline & Dashboard**

Data pipeline and dashboard for the California Crash Reporting System (CCRS)

## Data Pipeline Overview


## Setup Instructions

### 1. Setting up Terraform
Instructions for how to download and setup Terraform for your machine can be found here: [Terraform Install Tutorial](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)  

**For MacOS users (with Homebrew):**
```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

### 2. Setting Google Cloud
   1. Create an account on [Google Cloud Platform](https://cloud.google.com/) with your Google email.
   2. Setup a new Google Cloud project.
   3. Setup a Service Account in the project under **IAM & Admin > Service Accounts > + Create service account**. You can follow [Google's documentation](https://cloud.google.com/iam/docs/service-accounts-create) for setting up Service Accounts
      -  Assign the following IAM Roles to the Service Account: **Storage Admin, Storage Object Admin, BigQuery Admin and Viewer**.
      -  You do not need to assign any user roles.
   4. Click on your new Service Account and under Keys select **Add keys > JSON > Create**.
   8. Save this JSON Key to your computer. Take note of where it is saved and DO NOT commit this key anywhere to GitHub.
   11. Enable the following APIs:
        https://console.cloud.google.com/apis/library/iam.googleapis.com
        https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
   12. Download the [gCloud CLI](https://cloud.google.com/sdk/docs/install) and follow instructions to set it up locally.
   13. In Terminal, set an environment variable to point to your Service Account's JSON file:
       ```
       export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
       ```
   15. Run `gcloud auth application-default login` to verify you are logged in.
  
  ### 3. Setting up GCS (Data Lake) & BigQuery (Data Warehouse)
   This project uses GCS (Google Cloud Storage) buckets as a data lake and BigQuery as a data warehouse. Terraform is used to provision the infrastructure.
   1. Navigate to the `terraform/variables.tf` and replace the project ID, region, and location with the ones for your project. If you exported your Google application credentials in the step up above Terraform will be able to authenticate to the Google Cloud Project.
   2. Run the following commands to initialize 2 GCS buckets and a BigQuery dataset:
   ```
   terraform init
   terraform plan
   terraform apply
   ```

### 4. Setting up Airflow (Workflow Orchestration)
   1. Navigate to the `airflow/` folder and run:
      ```
       docker-compose build
       docker-compose -p ccrs up -d
      ```
   2. Once the image is built you can access Airflow on **localhost:8080**. Enter the following username/password combo:
      ```
      Username: airflow
      Password: airflow
      ```
   3. Once Airflow is open go to **Admin > Connections > Add Connection** and add in the following information:
      - Connection ID: `gcp-airflow`
      - Connection Type: `Google Cloud`
      - Project ID: Project ID from Google CLoud
      - KeyFile Path: `/home/airflow/.gcp/credentials.json`
   4. Click the toggle next to the DAG called **ccrs_gcp_ingestion**. The steps in the DAG should now run and populate the GCS buckets and crashes dataset in BigQuery that we created with Terraform.


## Acknowledgements
