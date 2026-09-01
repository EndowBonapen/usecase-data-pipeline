import io

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from config import BQ_DATASET_RAW, GCP_PROJECT_ID


def load_table(
    client: bigquery.Client,
    table: pa.Table,
    table_name: str,
    dataset: str = BQ_DATASET_RAW,
    write_disposition: str = "WRITE_APPEND",
) -> bigquery.LoadJob:
    """Load a pyarrow Table into BigQuery via Parquet.

    Parquet carries an explicit schema (from the Arrow table itself), so
    BigQuery doesn't need to autodetect types from JSON — it just reads the
    schema Parquet already declares.
    """
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    table_ref = f"{GCP_PROJECT_ID}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )

    job = client.load_table_from_file(buffer, table_ref, job_config=job_config)
    job.result()  # blocks until the load finishes, raises on failure
    return job
