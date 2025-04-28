# End-to-End Data Pipeline for Amazon Product Reviews Analysis

## Problem statement

In today’s data-driven world, monitoring and analyzing customer reviews, product ratings, and purchasing behavior is essential for making informed decisions in e-commerce, marketing, and product development. However, gathering and structuring this data can often be a complex and time-consuming task, especially when dealing with large volumes of unstructured data across multiple platforms. This project simplifies the process by automating the extraction, transformation, and organization of Amazon product review data, enabling businesses to gain valuable insights and make data-driven decisions with ease.

## Project Overview

This project builds an end-to-end data pipeline for Amazon Product Reviews to transform raw data into clean, structured, and trusted datasets. By designing a scalable and tested pipeline with dbt, BigQuery, and Power BI, this project enable fast, reliable, and insightful analysis of customer behaviors, product trends, and sales performance. This foundation is crucial because it empowers businesses to make data-driven decisions with confidence, optimize products and marketing strategies, and uncover hidden opportunities in massive datasets that would otherwise be too chaotic to analyze effectively.

This Amazon Review Data Pipeline project automates the workflow by:

Extracting Amazon product review and product metadata from raw datasets. 
**Thanks UCSD McAuley Lab collected this large-scale Amazon Reviews dataset in 2023. (https://amazon-reviews-2023.github.io/main.html)** 
**Bridging Language and Items for Retrieval and Recommendation -- Yupeng Hou, Jiacheng Li, Zhankui He, An Yan, Xiusi Chen, Julian McAuley. arXiv:2403.03952, 2024** 

Storing raw data in GCS (Google Cloud Storage) as a data lake.

Processing and cleaning the data using Apache Spark on Dataproc.

Building structured, partitioned, and clustered tables in BigQuery for efficient querying.

Modeling the data with dbt into dimension and fact tables (dim_items, dim_users, dim_time, fct_reviews).

Visualizing insights through an interactive dashboard in Power BI.

Here are the key data entities processed:

Reviews: Review ID, ratings, verified purchase status, timestamps, helpful votes

Users: Review activity frequency, total spending, review counts

Products: Categories, prices, average ratings

Time: Year, month, day, week, holiday, and weekend indicators

## Architecture
Architecture.png


## Infrastructure Setup (Terraform)

This project implemented Infrastructure as Code(IaC) to make the project deployment scalable and easy to maintain resource.

### Prerequisites
- Install Terraform 
- Install gcloud
- GCP service account key (For authentication)

```bash
vi ~/.bashrc

# Add GCP_CREDENTIALS
export GCP_CREDENTIALS="path/to/your/service-account-key.json" 

gcloud auth application-default login
```

Set up variable.tf for terraform

```bash
variable "google_credential" {
    description = "gcp service account credential"
    default = "path/to/your/service-account-key.json"}
```

Deploy the infrastructure setting

```bash
./deploy.sh
```

### Note: using the terrafrom to set up dataproc cluster, it needs to set up the service account auths as "serviceAccountUser", even it assign the same service account for terraform and dataproc resource 

```bash
    gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
    --member="serviceAccount:<GCP service account for terraform. eg:<your_service_account_name>@project-id.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser" 
```

## Extract Raw Data From Website To Data Lake

The [raw_to_gcs.py]<> python script subprocess and gcloud upload raw data from website to GCS data lake straight.  
Raw Data -> gs://de-amazon-product-review-bucket/raw/reviews/  
         -> gs://de-amazon-product-review-bucket/raw/meta/   

## Data Cleaning & Structuring with Apache Spark
The goal of this stage is to transform json form raw data into a clean, consistent, and structured format suitable for downstream analytics. Cleaning and structuring the data ensures data quality, enhances query performance, and makes later modeling and analysis much more reliable and efficient.

Tools Used:

1. Apache Spark (PySpark): Distributed processing of large datasets across a Dataproc cluster.

2. Google Cloud Dataproc: Managed Spark and Hadoop service to scale out data cleaning operations.

3. Google Cloud Storage (GCS): Storage for raw input files and cleaned output datasets.

Workflow:

1. Spark Data Cleaning script:

Parse and read raw data files (e.g., JSON, CSV).

Select columns to keep.

Clean invalid or corrupted records.

Standardize data types (e.g., converting strings to floats for prices, timestamps to datetime objects).

Remove duplicates and handle missing values.

2. Upload spark script [staging_meta_data.py]<> ,[staging_review_data.py]<> to gs://project-bucket/scripts/

```bash
gsutil cp staging_meta_data.py gs://de-amazon-product-review-bucket/scripts/

gsutil cp staging_review_data.py gs://de-amazon-product-review-bucket/scripts/
```

3. Submit Spark Job

The item dataset (product metadata) is relatively smaller and less complex, focusing mainly on cleaning and extracting key attributes (like price, average rating, categories, etc.). Therefore, it benefits from more executors for faster parallel processing but requires less memory per executor.  
The review dataset is much larger (571M records) and involves more intensive transformations (like timestamp handling, rating calculations, and filtering). It demands fewer but larger executors to avoid memory pressure during shuffles and aggregations.
Additionally, the number of shuffle partitions and default parallelism is increased to better distribute the heavy workload across the cluster and avoid bottlenecks during operations like joins, group-bys, and sorting.

```bash
gcloud dataproc jobs submit pyspark gs://de-amazon-product-review-bucket/scripts/staging_meta_data.py \
  --cluster=amazon-review-cluster \
  --region=australia-southeast1 \
  --properties=spark.executor.instances=8,\
spark.executor.memory=4g,\
spark.executor.cores=2,\
spark.driver.memory=4g 


gcloud dataproc jobs submit pyspark gs://de-amazon-product-review-bucket/scripts/staging_review_data.py \
  --cluster=amazon-review-cluster \
  --region=australia-southeast1 \
  --properties=spark.executor.instances=4,\
spark.executor.memory=6g,\
spark.executor.cores=2,\
spark.driver.memory=4g,\
spark.sql.shuffle.partitions=200,\
spark.default.parallelism=200

```

4. Read staing file from GCS locally.
The [read_items_locally.ipynb]<> and [read_reviews_locally.ipynb]<> allow us check the staging data locally.

## Data Warehouse Modeling
DBT (data build tool) is used to manage the transformation of cleaned staging data into a well-structured BigQuery data warehouse to support analytics and reporting.
The overall process consists of the following key steps:

1. Install dbt core and Bigquery extension

```bash
pip instll dbt-core
pip install dbt-bigquery
```

2. Build external table from busket as dbt source layer

```bash

gcloud beta interactive 


# stg_items_ext
bq mkdef --source_format=PARQUET \
    gs://de-amazon-product-review-bucket/staging/item/*.parquet > stg_items_def.json

bq mk --table \
  --external_table_definition=stg_items_def.json \
  de-amazon-product-review:de_amazon_product_reviews.stg_items_ext

# stg_reviews_ext
bq mkdef --source_format=PARQUET \
    gs://de-amazon-product-review-bucket/staging/review/*.parquet > stg_reviews_def.json

bq mk --table \
  --external_table_definition=stg_reviews_def.json \
  de-amazon-product-review:de_amazon_product_reviews.stg_reviews_ext

```

3. Staging modeling (stg_items, stg_reviews):

Raw source tables are further cleaned, standardized, and lightly transformed, include:

Standardizing column names and data types

Casting and cleaning fields (e.g., parsing price as FLOAT64)

Basic filtering of invalid or irrelevant records

These staging models serve as the clean foundation for further development.

4. Dimensional Modeling (dim_ models):

Build dimension tables such as:

dim_items: information about products

dim_users: summarized user behaviors

dim_time: date-related features for time-series analysis

Each dimension table is structured to support easy joins with fact tables and ensure consistency across reports.

Fact Modeling (fct_ models):

Build the main fact table fct_reviews, capturing individual review activities with rich context (user, item, time, ratings).

This fact table includes derived metrics such as:

An additional review_id_power_bi column, which converted from byte type to string. (Power BI does not support byte type.)

Rating differences from product average

Day name for weekly trend analysis


5. Deployment to BigQuery:

All models are materialized into BigQuery tables, using appropriate strategies:

Staging models are views, Dimensional and fact tables are materialized as tables for performance.

For fct_reviews table, partitioning(**by time, monthly**) and clustering(**by user and product asin**) strategies are applied where necessary to optimize query speed and reduce costs.

6. Star Schema Architecture
                        +----------------+
                        |   dim_users     |
                        +----------------+
                              ↑
                              |
+-------------+        +----------------+        +--------------+
|  dim_time   | ←────── |  fct_reviews   | ──────→ |  dim_items   |
+-------------+        +----------------+        +--------------+

![DAG]()

## Data Vizualization
Translate large-scale review data into intuitive charts and dashboards Power BI.

- Power BI (Visualization Tool):

  Connects directly to BigQuery datasets.

  Creates interactive dashboards, charts, KPIs, and reports.

  Enables real-time data refresh from BigQuery.

- Dashboard Design & Visualizations:

  - Create bar charts for review counts by year.

  - Create trend lines for average ratings across categories over time.

  - Create pie charts to visualize the review counts of each category proportion.

  - Create the top 10 most reviews users and their total spent.

  - Create bar charts for reviews counts and average price people spent in different months. 

  - Create filters/slicers for verified purchase.

![Report-1]<>

![Report-2]<>

