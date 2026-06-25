import subprocess
from dagster import (
    asset,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    MaterializeResult,
    MetadataValue,
)

@asset
def raw_layer_check():
    """Queries Trino to verify raw layer tables are available."""
    import trino
    conn = trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="dagster",
        catalog="nessie",
        schema="raw",
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES IN nessie.raw")
    tables = [row[0] for row in cursor.fetchall()]
    return MaterializeResult(
        metadata={"tables": MetadataValue.json(tables)}
    )

raw_layer_job = define_asset_job(
    name="raw_layer_job",
    selection="raw_layer_check"
)

raw_layer_schedule = ScheduleDefinition(
    job=raw_layer_job,
    cron_schedule="0 6 * * *",
    name="raw_layer_daily_schedule",
)

@sensor(job=raw_layer_job)
def minio_new_file_sensor(context: SensorEvaluationContext):
    import requests
    import hashlib
    try:
        response = requests.get(
            "http://localhost:9000/raw",
            params={"list-type": "2"},
            auth=("lakehouse", "lakehouse123"),
        )
        content_hash = hashlib.md5(response.content).hexdigest()
    except Exception as e:
        context.log.warning(f"Could not reach MinIO: {e}")
        return
    last_hash = context.cursor or ""
    if content_hash != last_hash:
        context.update_cursor(content_hash)
        yield RunRequest(run_key=content_hash)

defs = Definitions(
    assets=[raw_layer_check],
    jobs=[raw_layer_job],
    schedules=[raw_layer_schedule],
    sensors=[minio_new_file_sensor],
)