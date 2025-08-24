# lambda_function.py
import os
import json
import urllib.parse
import boto3
import pandas as pd
import awswrangler as wr

# ========= Environment variables (set in Lambda console) =========
# s3_cleansed_layer         -> e.g. s3://my-bucket/cleansed/
# glue_catalog_db_name      -> e.g. db_youtube_cleaned
# glue_catalog_table_name   -> e.g. youtube_categories
# write_data_operation      -> append | overwrite | overwrite_partitions  (default: append)

S3_CLEANSED = os.environ["s3_cleansed_layer"]
GLUE_DB     = os.environ["glue_catalog_db_name"]
GLUE_TABLE  = os.environ["glue_catalog_table_name"]
WRITE_MODE  = os.environ.get("write_data_operation", "append")

s3 = boto3.client("s3")

def ensure_glue_db(name: str) -> None:
    """Create the Glue DB if it doesn't exist (safe to call every run)."""
    try:
        wr.catalog.create_database(name=name, exist_ok=True)  # awswrangler >= 3.x
        print(f"[CONF] Glue DB ensured: {name}")
    except TypeError:
        try:
            wr.catalog.create_database(name=name)
            print(f"[CONF] Glue DB created: {name}")
        except Exception as e:
            if "AlreadyExistsException" in str(e):
                print(f"[CONF] Glue DB already exists: {name}")
            else:
                raise

def _load_df(bucket: str, key: str) -> pd.DataFrame:
    """
    Load YouTube category JSON and normalize to these 6 fields:
    kind, etag, id, snippet.channelId, snippet.title, snippet.assignable
    Output columns (renamed for Glue): kind, etag, id, snippet_channelid, snippet_title, snippet_assignable
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    print(f"[DEBUG] s3 object size={len(raw)} bytes for {key}")

    # Parse JSON
    try:
        doc = json.loads(raw)
    except Exception as e:
        print(f"[ERROR] json.loads failed: {e}")
        return pd.DataFrame()

    # Expect dict with "items"
    if not (isinstance(doc, dict) and isinstance(doc.get("items"), list)):
        print(f"[WARN] Unexpected JSON structure; returning empty. type={type(doc)} keys={list(doc.keys()) if isinstance(doc, dict) else None}")
        return pd.DataFrame()

    items = doc["items"]
    print(f"[DEBUG] items count={len(items)}")
    if not items:
        return pd.DataFrame()

    df = pd.json_normalize(items)

    # Select & rename to final column names
    wanted_map = {
        "kind": "kind",
        "etag": "etag",
        "id": "id",
        "snippet.channelId": "snippet_channelid",
        "snippet.title": "snippet_title",
        "snippet.assignable": "snippet_assignable",
    }
    present = [k for k in wanted_map if k in df.columns]
    missing = [k for k in wanted_map if k not in df.columns]
    if missing:
        print(f"[WARN] Missing expected keys: {missing}")

    if not present:
        print("[WARN] None of the expected columns present; returning empty.")
        return pd.DataFrame()

    df = df[present].rename(columns=wanted_map)

    # Types
    for col in ["kind", "etag", "id", "snippet_channelid", "snippet_title"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    if "snippet_assignable" in df.columns:
        df["snippet_assignable"] = df["snippet_assignable"].astype("boolean")

    print(f"[DEBUG] final DF shape={df.shape}")
    print(f"[DEBUG] dtypes:\n{df.dtypes}")
    return df

def lambda_handler(event, context):
    print("[START] JSON → Parquet (YouTube categories, 6 fields, schema evolution enabled)")
    records = event.get("Records", [])
    if not records:
        print("[WARN] No Records in event. Use an S3 Put event or trigger.")
        return {"ok": False, "reason": "no-records"}

    # Normalize destination path
    dest_path = S3_CLEANSED if S3_CLEANSED.endswith("/") else S3_CLEANSED + "/"
    print(f"[CONF] dest_path={dest_path} glue_db={GLUE_DB} glue_table={GLUE_TABLE} mode={WRITE_MODE}")

    # Ensure Glue DB exists
    ensure_glue_db(GLUE_DB)

    results = []
    for rec in records:
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"], encoding="utf-8")
        print(f"[INFO] Processing s3://{bucket}/{key}")

        df = _load_df(bucket, key)
        if df.empty:
            print(f"[WARN] Empty DataFrame for {key}; skipping write.")
            results.append({"key": key, "status": "empty"})
            continue

        # Write Parquet; allow schema evolution to add new columns to an existing table
        res = wr.s3.to_parquet(
            df=df,
            path=dest_path,
            dataset=True,
            database=GLUE_DB,
            table=GLUE_TABLE,
            mode=WRITE_MODE,  # usually "append" or "overwrite_partitions"
            dtype={
                "kind": "string",
                "etag": "string",
                "id": "string",
                "snippet_channelid": "string",
                "snippet_title": "string",
                "snippet_assignable": "boolean",
            },
            schema_evolution=True  # <-- key change to avoid "Schema change detected" error
        )
        print(f"[INFO] to_parquet result: {res}")
        results.append({"key": key, "status": "written", "rows": int(len(df))})

    return {"ok": True, "results": results}