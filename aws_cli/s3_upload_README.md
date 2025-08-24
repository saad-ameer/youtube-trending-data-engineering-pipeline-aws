# Uploading YouTube Dataset to Amazon S3

This guide explains how to upload the **YouTube Trending Dataset** into Amazon S3, following Hive-style partitioned folder structure for regions. Replace the bucket name with your own.

## 1. Upload JSON Reference Data

These files map category IDs to category names. Copy them into the `raw_statistics_reference_data` prefix.

```bash
aws s3 cp . s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics_reference_data/   --recursive --exclude "*" --include "*.json"
```

## 2. Upload CSV Data Files (Partitioned by Region)

Each regional CSV file should be placed into its own S3 prefix, using `region=<code>` style folders. Example commands:

```bash
aws s3 cp CAvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=ca/
aws s3 cp DEvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=de/
aws s3 cp FRvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=fr/
aws s3 cp GBvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=gb/
aws s3 cp INvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=in/
aws s3 cp JPvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=jp/
aws s3 cp KRvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=kr/
aws s3 cp MXvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=mx/
aws s3 cp RUvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=ru/
aws s3 cp USvideos.csv s3://<YOUR-BUCKET-NAME>/youtube/raw_statistics/region=us/
```

## Notes

- Ensure the bucket exists in the correct AWS region before uploading.
- The partitioned folder structure (`region=XX/`) ensures compatibility with Athena/Glue partitioned tables.
- Replace `<YOUR-BUCKET-NAME>` with your actual S3 bucket name, for example:  
  `saad-de-youtube-raw-us-east-1`
