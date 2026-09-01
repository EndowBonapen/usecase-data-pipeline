import json
from pathlib import Path

import pyarrow as pa
from google.cloud import bigquery

from extract.land import RAW_DIR
from load.bigquery import load_table
from transform.clean import cast_money_columns, validate
from transform.flatten import FLATTENERS


class ValidationError(Exception):
    def __init__(self, violations: list[dict]):
        self.violations = violations
        super().__init__(f"{len(violations)} validation issue(s)")


def load_envelope_file(file: Path) -> tuple[str, dict[str, pa.Table]]:
    """Read one landed raw file and flatten it. Raises ValidationError if any
    derived-field check fails — callers decide what to do (abort, log, etc)."""
    envelope = json.loads(file.read_text(encoding="utf-8"))
    source = envelope["source"]

    flattener = FLATTENERS.get(source)
    if flattener is None:
        raise ValueError(f"no flattener registered for source '{source}'")

    tables = flattener(envelope["payload"])

    violations = validate(source, tables)
    if violations:
        raise ValidationError(violations)

    return source, tables


def load_source_date(client: bigquery.Client, source: str, date: str) -> dict[str, int]:
    """Load every file landed for `source` on `date` (raw/{source}/{date}/*.json).

    One full-refresh (WRITE_TRUNCATE) load per table, not per file — batches
    from the same day get concatenated first, so a second batch doesn't wipe
    out the first one. This is what an Airflow task should call: it only
    needs to know the source and the execution date, not an exact filename.
    """
    day_dir = RAW_DIR / source / date
    files = sorted(day_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no landed files for '{source}' on {date} in {day_dir}")

    tables_by_name: dict[str, list[pa.Table]] = {}
    for file in files:
        _, tables = load_envelope_file(file)
        for table_name, table in tables.items():
            tables_by_name.setdefault(table_name, []).append(table)

    row_counts = {}
    for table_name, parts in tables_by_name.items():
        combined = pa.concat_tables(parts)
        cleaned = cast_money_columns(combined)
        job = load_table(client, cleaned, table_name, write_disposition="WRITE_TRUNCATE")
        row_counts[table_name] = job.output_rows

    return row_counts
