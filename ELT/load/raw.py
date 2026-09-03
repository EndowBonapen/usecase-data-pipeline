import io
import json

from google.cloud import bigquery

from config import BQ_DATASET_RAW, GCP_PROJECT_ID
from extract.land import RAW_DIR


def _rows_for_source(source: str, envelope: dict) -> dict[str, list[dict]]:
    """Return {table_name: rows} for one landed envelope, tagged with
    lineage metadata. Deliberately no flattening, no casting, no unnesting —
    that's dbt's job in this pipeline, not Python's (contrast ETL/transform/).
    """
    payload = envelope["payload"]
    lineage = {"_ingested_at": envelope["ingested_at"], "_batch_id": envelope["batch_id"]}

    if source == "relational_seed":
        tables = payload["data"]["tables"]
        return {name: [{**row, **lineage} for row in rows] for name, rows in tables.items()}

    return {source: [{**row, **lineage} for row in payload["data"]]}


def load_raw_source_date(client: bigquery.Client, source: str, date: str) -> dict[str, int]:
    """Load every file landed for `source` on `date`, as-is, into BigQuery.

    NDJSON + schema autodetect — nested objects/arrays (product{}, locations[],
    refund{}, items[], ...) become native BigQuery STRUCT/ARRAY columns rather
    than being flattened in Python first. dbt staging models UNNEST them.
    """
    day_dir = RAW_DIR / source / date
    files = sorted(day_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no landed files for '{source}' on {date} in {day_dir}")

    rows_by_table: dict[str, list[dict]] = {}
    for file in files:
        envelope = json.loads(file.read_text(encoding="utf-8"))
        for table_name, rows in _rows_for_source(source, envelope).items():
            rows_by_table.setdefault(table_name, []).extend(rows)

    row_counts = {}
    for table_name, rows in rows_by_table.items():
        buffer = io.BytesIO()
        for row in rows:
            buffer.write((json.dumps(row) + "\n").encode("utf-8"))
        buffer.seek(0)

        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )
        job = client.load_table_from_file(buffer, table_ref, job_config=job_config)
        job.result()
        row_counts[table_name] = job.output_rows

    return row_counts
