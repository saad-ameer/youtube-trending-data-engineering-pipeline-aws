# YouTube Trending Data Engineering Pipeline on AWS

End-to-end AWS data engineering pipeline for analyzing YouTube trending videos using S3, Glue, Lambda, Athena, and QuickSight.

---

## Project Goal & Use Case

The objective of this project is to analyze YouTube data for business insights, focusing on:

- Categorizing videos using metadata and comments  
- Identifying factors influencing video popularity  
- Optimizing ad targeting for specific regions and categories  

Why YouTube?  
- It is the second most visited website globally after Google  
- Provides rich engagement data such as views, likes, comments, and shares  

---

## Dataset

**Source:** [Kaggle – Trending YouTube Videos Dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new)

The dataset contains:  
1. **CSV files** – Video metadata per region (CA, US, IN, DE, FR, JP, etc.)  
   - Fields: `video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, description`  
2. **JSON files** – Category mappings (`category_id → category_name`)  

---

## Architecture

![AWS Data Lake Architecture](docs/architecture.jpeg)

---

## Architecture & Flow

### 1. Data Ingestion
- Data is downloaded from Kaggle and uploaded to **Amazon S3 (Raw Zone)**.  
- S3 folder structure:
  ```
  s3://<bucket-name>/youtube/raw_statistics/<region>/*.csv
  s3://<bucket-name>/youtube/raw_statistics_reference_data/*.json
  ```

### 2. Data Lake Setup
- Amazon S3 serves as the data lake.  
- Bucket naming conventions include company, environment, data type (raw/clean/analytics), and region.  

### 3. AWS Glue Crawler & Catalog
- Glue crawler scans raw JSON and CSV files to create metadata tables in Glue Data Catalog.  
- Exposed to **Athena** for SQL querying.  
- Issue identified: Pretty-printed JSON caused Athena errors (`Row is not a valid JSON Object`).  

### 4. ETL with AWS Lambda (JSON Transformation)
- Lambda function triggered on S3 `PUT` events for JSON files.  
- Converts multi-line JSON into **Parquet** format.  
- Writes output to the **Cleaned S3 Zone**.  
- Schema evolution enabled via **awswrangler**.  

### 5. CSV Processing with AWS Glue ETL
- Raw CSVs ingested with Glue ETL → converted to **Parquet** in Clean Zone.  
- Schema adjustments:  
  - `category_id` cast to **BIGINT**  
  - Partitioning applied on **region** for efficient queries  
- Glue jobs (PySpark) handle transformations and encoding issues.  

### 6. Data Integration
- Join operation:  
  - CSV (video stats) joined with JSON (categories).  
  - Key: `category_id (CSV)` ↔ `id (JSON)`  
  - Output: Each video enriched with its category name  

### 7. Analytical Zone
- Glue Studio ETL builds the **Analytics dataset** from Clean Zone.  
- Stored in Parquet, partitioned by `(region, category_id)`  
- Final schema optimized for BI queries.  

### 8. Querying with Athena
- Queries run on raw, clean, and analytics zones.  

Example:
```sql
SELECT a.title, a.views, b.snippet_title AS category
FROM raw_statistics a
JOIN cleaned_statistics_reference_data b
  ON a.category_id = CAST(b.id AS BIGINT)
WHERE a.region = 'CA'
ORDER BY views DESC
LIMIT 10;
```

### 9. Dashboarding
- Data exposed to **Amazon QuickSight** for visualization.  
- Can also integrate with Tableau, Power BI, or Jupyter notebooks.  

Example insights:
- Most popular categories by country  
- Engagement trends (likes vs dislikes)  
- Region-specific ad targeting recommendations  

---

## AWS Services Used
- **Amazon S3** – Data Lake storage (Raw, Clean, Analytics zones)  
- **AWS Glue Data Catalog** – Central metadata store  
- **AWS Glue Crawler** – Automated schema discovery  
- **AWS Glue ETL (PySpark)** – Data transformation and partitioning  
- **AWS Lambda** – JSON to Parquet transformation  
- **Amazon Athena** – Serverless SQL queries  
- **Amazon QuickSight** – Visualization and reporting  
- **IAM** – Secure access with least privilege  
- **Amazon CloudWatch** – Monitoring and logging  

---

## Project Workflow (Step by Step)
1. Download dataset from Kaggle → upload to **S3 Raw Zone**  
2. Run **Glue Crawler** → create metadata in Glue Catalog  
3. Transform **JSON (multi-line → Parquet)** with Lambda  
4. Process **CSV (partition by region, cast datatypes)** with Glue Spark Jobs  
5. Store outputs in **Clean Zone (Parquet)**  
6. Join video stats with categories → enrich dataset  
7. Store enriched data in **Analytics Zone**  
8. Query enriched dataset via **Athena**  
9. Visualize results in **QuickSight Dashboard**  

---

## Example Dashboard Insights
- Most popular categories by region  
- Engagement analysis: likes vs dislikes vs comments  
- Top performing creators per country  
- Ad-targeting recommendations by category popularity  

---

## Key Learnings
- Difference between **Data Lake** (flexible storage) and **Data Warehouse** (structured, reporting-ready)  
- Handling **multi-line JSON transformation** in AWS Lambda  
- Importance of **partitioning** (region, category) for query optimization  
- Schema evolution handling with Parquet in Glue/Athena  
- Cloud-native orchestration with **Lambda + Glue + Athena**  

---

## Final Outcome
- Fully automated end-to-end data engineering pipeline on AWS  
- Incremental ingestion supported via S3 triggers  
- Optimized analytics dataset stored in Parquet  
- Queryable in Athena and visualizable in QuickSight/Power BI/Tableau  

---

## Reference
- Dataset: [Trending YouTube Video Statistics on Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new)  
