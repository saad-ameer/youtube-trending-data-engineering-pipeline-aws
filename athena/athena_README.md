# AWS Athena Queries for YouTube Data Project

This file documents a collection of SQL queries used to interact with the Athena tables created in this YouTube Data Engineering pipeline.

## Example Queries

### 1. Preview raw reference data
```sql
SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics_reference_data" 
LIMIT 10;
```

### 2. Preview cleaned reference data
```sql
SELECT * FROM "AwsDataCatalog"."db_youtube_cleaned"."cleaned_statistics_reference_data" 
LIMIT 10;
```

### 3. Preview raw statistics
```sql
SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics" 
LIMIT 10;
```

### 4. Filter raw statistics by region
```sql
SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics" 
WHERE region = 'ca';
```

### 5. Join raw statistics with cleaned reference data (casting id)
```sql
SELECT * 
FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
  ON a.category_id = CAST(b.id AS int);
```

### 6. Join with selected columns
```sql
SELECT a.title, a.category_id, b.snippet_title 
FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
  ON a.category_id = b.id
WHERE a.region = 'ca';
```

### 7. General join across raw statistics and cleaned reference data
```sql
SELECT * 
FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
  ON a.category_id = b.id;
```

### 8. Join cleaned statistics with cleaned reference data
```sql
SELECT * 
FROM "db_youtube_cleaned"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
  ON a.category_id = b.id;
```

## DDL Statements

### Create external table for cleaned raw statistics
```sql
CREATE EXTERNAL TABLE db_youtube_cleaned.raw_statistics (
  video_id               string,
  trending_date          string,
  title                  string,
  channel_title          string,
  category_id            bigint,
  publish_time           string,
  tags                   string,
  views                  bigint,
  likes                  bigint,
  dislikes               bigint,
  comment_count          bigint,
  thumbnail_link         string,
  comments_disabled      boolean,
  ratings_disabled       boolean,
  video_error_or_removed boolean,
  description            string
)
PARTITIONED BY (region string)
STORED AS PARQUET
LOCATION 's3://saad-de-youtube-cleansed-us-east-1/youtube/raw_statistics/';
```

### Create analytics database
```sql
CREATE DATABASE db_youtube_analytics;
```

### Preview final analytics table
```sql
SELECT * FROM "db_youtube_analytics"."final_analytics" 
LIMIT 10;
```
