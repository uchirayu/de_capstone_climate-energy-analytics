import json
import boto3
import os
from datetime import datetime

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET")  # Must be a real AWS S3 bucket

# Remove endpoint_url to use real AWS S3
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def write_batch_to_s3(records, source):
    if not records:
        return

    # Map stateid/sectorid to state/sector for eia_energy topic
    if source == "eia_energy":
        for r in records:
            if "stateid" in r:
                r["state"] = r.pop("stateid")
            if "sectorid" in r:
                r["sector"] = r.pop("sectorid")

    date_path = datetime.utcnow().strftime("%Y/%m/%d/%H")
    key = f"bronze/{source}/raw/{date_path}/{datetime.utcnow().isoformat()}.json"

    body = "\n".join(json.dumps(r) for r in records)

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=body
        )
        print(f"Wrote {len(records)} records to s3://{BUCKET}/{key}")
    except Exception as e:
        print(f"ERROR writing to S3: bucket={BUCKET}, key={key}, error={e}")
        # Optionally, log to file as well
        log_dir = "/opt/airflow/logs"
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "consumer_error.log")
        with open(log_path, "a") as f:
            f.write(f"ERROR writing to S3: bucket={BUCKET}, key={key}, error={e}\n")

    # Log written record
    log_dir = "/opt/airflow/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "consumer.log")
    with open(log_path, "a") as f:
        f.write(f"Wrote {len(records)} records to s3://{BUCKET}/bronze/{source}/raw/{date_path}/\n")