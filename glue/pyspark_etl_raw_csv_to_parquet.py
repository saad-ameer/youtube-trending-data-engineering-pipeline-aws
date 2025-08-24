import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ====== Params ======
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

RAW_DB = "de_youtube_raw"
RAW_TABLE = "raw_statistics"
REGIONS = ["ca", "gb", "us"]

RAW_S3_PATH = "s3://<YOUR-RAW-BUCKET>/youtube/raw_statistics/"
CLEANSED_S3_PATH = "s3://saad-de-youtube-cleansed-us-east-1/youtube/raw_statistics/"

# ====== Context ======
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ====== Helpers ======
def glue_table_exists(database: str, table: str) -> bool:
    glue = boto3.client("glue")
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

# ====== Read (Catalog if exists; otherwise S3 fallback) ======
if glue_table_exists(RAW_DB, RAW_TABLE):
    print(f"[INFO] Reading from Glue Catalog: {RAW_DB}.{RAW_TABLE}")
    predicate_pushdown = f"region in ({','.join([repr(r) for r in REGIONS])})"
    datasource0 = glueContext.create_dynamic_frame.from_catalog(
        database=RAW_DB,
        table_name=RAW_TABLE,
        transformation_ctx="datasource0",
        push_down_predicate=predicate_pushdown
    )
else:
    print(f"[WARN] Catalog table {RAW_DB}.{RAW_TABLE} not found. Fallback to S3 path: {RAW_S3_PATH}")
    datasource0 = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        connection_options={"paths": [RAW_S3_PATH], "recurse": True},
        transformation_ctx="datasource_path"
    )
    try:
        datasource0 = Filter.apply(
            frame=datasource0,
            f=lambda r: "region" in r and r["region"] in REGIONS
        )
    except Exception as e:
        print(f"[WARN] Could not filter by region in fallback: {e}")

# ====== Transform ======
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="applymapping1"
)

resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",
    transformation_ctx="resolvechoice2"
)

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3"
)

# ====== Coalesce & Write ======
df = dropnullfields3.toDF().coalesce(1)
df_final = DynamicFrame.fromDF(df, glueContext, "df_final")

datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=df_final,
    connection_type="s3",
    connection_options={
        "path": CLEANSED_S3_PATH,
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="datasink4"
)

job.commit()
