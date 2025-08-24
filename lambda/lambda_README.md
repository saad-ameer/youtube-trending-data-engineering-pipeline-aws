# Lambda: JSON → Parquet (YouTube Categories)

This AWS Lambda function listens to S3 object-created events for YouTube category JSON files, normalizes them to a tabular structure, and writes Parquet files to the cleansed S3 zone. It also ensures the AWS Glue database/table exist (via `awswrangler`) so the data is queryable in Athena.

## What it does

1. Triggered by S3 `ObjectCreated:*` for files like:
   ```
   s3://<raw-bucket>/youtube/raw_statistics_reference_data/<COUNTRY>_category_id.json
   ```
2. Reads the JSON (expected shape: `{"items": [...]}`).
3. Normalizes and selects:
   - `kind`, `etag`, `id`, `snippet.channelId`, `snippet.title`, `snippet.assignable`
4. Writes Parquet to the cleansed S3 prefix and updates/creates the Glue Catalog table (schema evolution enabled).

## Required environment variables

Set in the Lambda console (Configuration → Environment variables):

| Key                      | Example                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| `s3_cleansed_layer`      | `s3://saad-de-youtube-cleansed-us-east-1/youtube/raw_statistics_reference_data/` |
| `glue_catalog_db_name`   | `db_youtube_cleaned`                                                    |
| `glue_catalog_table_name`| `cleaned_statistics_reference_data`                                     |
| `write_data_operation`   | `append` (or `overwrite_partitions`, `overwrite`)                       |

> Ensure `s3_cleansed_layer` ends with a trailing `/`.

## S3 event trigger

Configure the source bucket to invoke this function:

- Bucket: your raw bucket
- Prefix: `youtube/raw_statistics_reference_data/`
- Suffix: `.json`
- Event types: `s3:ObjectCreated:*`

## IAM permissions (minimum)

Attach to the Lambda execution role (adjust ARNs as needed):

- S3 read on raw, write on cleansed
- Glue database/table create and get
- CloudWatch Logs

Example statements:

```json
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:GetObject","s3:ListBucket"],
      "Resource":[
        "arn:aws:s3:::<RAW-BUCKET>",
        "arn:aws:s3:::<RAW-BUCKET>/*"
      ]
    },
    {
      "Effect":"Allow",
      "Action":["s3:PutObject","s3:AbortMultipartUpload","s3:ListBucket"],
      "Resource":[
        "arn:aws:s3:::<CLEANSED-BUCKET>",
        "arn:aws:s3:::<CLEANSED-BUCKET>/*"
      ]
    },
    {
      "Effect":"Allow",
      "Action":[
        "glue:GetDatabase","glue:GetDatabases","glue:CreateDatabase",
        "glue:GetTable","glue:CreateTable","glue:UpdateTable",
        "glue:GetPartition","glue:BatchCreatePartition","glue:BatchGetPartition"
      ],
      "Resource":"*"
    },
    {
      "Effect":"Allow",
      "Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
      "Resource":"*"
    }
  ]
}
```

If Lake Formation is enabled, grant this role the necessary LF permissions and S3 location access.

## Dependencies

The handler uses:

- `awswrangler`
- `pandas`
- `pyarrow`
- `boto3`

Provide these via a **Lambda layer** or a **container image**.

Example `requirements.txt`:

```
awswrangler==3.8.1
boto3>=1.28.0
pandas>=2.0.0
pyarrow>=14.0.0
```

## Deployment steps

1. Package dependencies into a Lambda layer (or build a container image).
2. Create the Lambda function with your handler code (`lambda_function.py`).
3. Attach the execution role with policies above.
4. Set environment variables.
5. Add the S3 trigger for the raw prefix.
6. Upload a sample JSON to the raw prefix and verify logs and output in the cleansed bucket.

## Manual test event

Use a real key present in your raw bucket:

```json
{
  "Records": [
    {
      "s3": {
        "bucket": {"name": "<RAW-BUCKET>"},
        "object": {"key": "youtube/raw_statistics_reference_data/CA_category_id.json"}
      }
    }
  ]
}
```

## Expected outputs

- Parquet files under:
  ```
  s3://<CLEANSED-BUCKET>/youtube/raw_statistics_reference_data/
  ```
- A Glue table:
  ```
  <glue_catalog_db_name>.<glue_catalog_table_name>
  ```

## Troubleshooting

- **No output files**: Check Lambda CloudWatch logs; confirm the event prefix/suffix and that the key ends with `.json`.
- **Glue errors**: Verify role permissions for Glue/Lake Formation and that the DB name is correct. The function creates the DB/table if missing.
- **Schema change errors**: `schema_evolution=True` is enabled; ensure the new columns map to valid Parquet/Glue types.
- **Access denied**: Confirm both S3 buckets and Glue permissions for the Lambda role.
