# Different SQL queries to interact with AWS Athena tables

SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics_reference_data" limit 10;

SELECT * FROM "AwsDataCatalog"."db_youtube_cleaned"."cleaned_statistics_reference_data" limit 10;

SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics" limit 10;

SELECT * FROM "AwsDataCatalog"."de_youtube_raw"."raw_statistics" where region='ca';

SELECT * FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
ON a.category_id = cast(b.id as int);

SELECT a.title, a.category_id, b.snippet_title FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
ON a.category_id = b.id
where a.region='ca';

SELECT * FROM "de_youtube_raw"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
ON a.category_id = b.id;

SELECT * FROM "db_youtube_cleaned"."raw_statistics" a
INNER JOIN "db_youtube_cleaned"."cleaned_statistics_reference_data" b 
ON a.category_id = b.id;

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


CREATE DATABASE db_youtube_analytics;


SELECT * FROM "db_youtube_analytics"."final_analytics" limit 10;