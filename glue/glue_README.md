# AWS Glue ETL: Raw YouTube CSV/JSON → Parquet (Partitioned by region)

This AWS Glue job reads YouTube trending data from the **raw zone** (via Glue Catalog if available, otherwise directly from S3), applies schema mapping, and writes optimized **Parquet** files to the **cleansed zone**, partitioned by `region`.

## What this job does

1. Attempts to read from the Glue Catalog table `de_youtube_raw.raw_statistics` with predicate pushdown on a region list (`REGIONS`).
2. If the catalog table is not found, falls back to reading from an S3 path.
3. Applies an explicit schema mapping to ensure datatypes are correct.
4. Writes Parquet to the cleansed S3 prefix, partitioned by `region`.
5. Coalesces output to a single file per run (optional, can be adjusted).

## Source and target

- **Catalog source (preferred):**
  - Database: `de_youtube_raw`
  - Table: `raw_statistics`

- **S3 fallback (if catalog missing):**
  - `s3://<YOUR-RAW-BUCKET>/youtube/raw_statistics/`

- **Cleansed target:**
  - `s3://saad-de-youtube-cleansed-us-east-1/youtube/raw_statistics/`

> Adjust bucket names and regions according to your environment.

## Key parameters and variables

Defined in the script:
- `RAW_DB = "de_youtube_raw"`
- `RAW_TABLE = "raw_statistics"`
- `REGIONS = ["ca", "gb", "us"]` (edit to the set of regions you ingest)
- `RAW_S3_PATH = "s3://<YOUR-RAW-BUCKET>/youtube/raw_statistics/"`
- `CLEANSED_S3_PATH = "s3://saad-de-youtube-cleansed-us-east-1/youtube/raw_statistics/"`

## Input schema (selected fields)

The job maps the following fields (source → target → type):

- `video_id` (string → string)
- `trending_date` (string → string)
- `title` (string → string)
- `channel_title` (string → string)
- `category_id` (long → bigint)
- `publish_time` (string → string)
- `tags` (string → string)
- `views` (long → bigint)
- `likes` (long → bigint)
- `dislikes` (long → bigint)
- `comment_count` (long → bigint)
- `thumbnail_link` (string → string)
- `comments_disabled` (boolean → boolean)
- `ratings_disabled` (boolean → boolean)
- `video_error_or_removed` (boolean → boolean)
- `description` (string → string)
- `region` (string → string)

## Output

- Format: **Parquet**
- Partitioned by: **`region`**
- Location: `CLEANSED_S3_PATH`

Example layout:
```
s3://.../youtube/raw_statistics/
  ├─ region=ca/part-....snappy.parquet
  ├─ region=gb/part-....snappy.parquet
  └─ region=us/part-....snappy.parquet
```

## How it chooses the source

- The helper `glue_table_exists()` checks for `de_youtube_raw.raw_statistics`.
- If present, the job uses `from_catalog()` with a predicate like:
  ```
  region in ('ca','gb','us')
  ```
- If absent, the job reads from `RAW_S3_PATH` with `from_options()`.
  - A best-effort filter keeps only records whose `region` is in `REGIONS`.

> If your raw data is CSV, set `format="csv"` and add the appropriate CSV options. The script currently uses `format="json"` in the fallback for JSON-structured raw data.

## Running the job

1. Upload the script to Glue or store it in S3 and reference it when creating a job.
2. Set the job name (Glue passes `--JOB_NAME`, which the script expects).
3. Ensure the IAM role attached to the job has:
   - Read access to the raw bucket/prefix
   - Write access to the cleansed bucket/prefix
   - Glue permissions to read the Catalog (if using Catalog source)
4. (Optional) Create or run a Glue Crawler beforehand to maintain the `de_youtube_raw.raw_statistics` table.

## IAM (minimal outline)

- `s3:GetObject`, `s3:ListBucket` on the raw bucket
- `s3:PutObject`, `s3:AbortMultipartUpload`, `s3:ListBucket` on the cleansed bucket
- `glue:GetDatabase`, `glue:GetTable` (if using the Catalog source)
- CloudWatch Logs permissions for the job run logs

## Tuning

- **Partitions**: The job writes partitions by `region`. Ensure downstream Athena table is created as partitioned.
- **Coalesce**: Currently set to `coalesce(1)` to write a single file per run. For larger datasets, consider removing or adjusting to improve parallelism and throughput.
- **REGIONS**: Update the list to match your ingestion scope (e.g., add `in`, `de`, `fr`, etc.).

## Troubleshooting

- **Catalog table not found**: The job will fall back to `RAW_S3_PATH`. Confirm the path and set the correct `format` (`json` or `csv`) as per your raw files.
- **Empty output**: Check the region filter and verify that the source data has the `region` column or partitioned folders (`region=XX/`). Remove the filter if needed to debug.
- **Slow writes or small files**: Remove `coalesce(1)` for parallel writes; use compaction later if needed.
- **Athena sees no partitions**: After the first successful write, run `MSCK REPAIR TABLE <db>.<table>` in Athena to load partitions.

## Related Athena DDL (example)

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS db_youtube_cleaned.raw_statistics (
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

MSCK REPAIR TABLE db_youtube_cleaned.raw_statistics;
```

## Files

- Script: `etl_raw_csv_to_parquet.py` (place under `glue/` in your repo)
- This README: `glue_README.md`

