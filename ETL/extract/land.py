import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Resolved relative to this file, not the current working directory — so
# landing works the same whether you run `python cli.py` from ETL/ or
# `python -m extract.land` from anywhere (see the GOOGLE_APPLICATION_CREDENTIALS
# relative-path bug we hit in Step 0).
RAW_DIR = Path(__file__).resolve().parent.parent / "raw"


def land_raw(source: str, batch_index: int, payload: dict) -> Path:
    """Write one extracted batch to the raw/bronze zone, untouched, with lineage metadata."""
    ingested_at = datetime.now(timezone.utc)

    envelope = {
        "source": source,
        "batch_index": batch_index,
        "batch_id": uuid.uuid4().hex,
        "ingested_at": ingested_at.isoformat(),
        "payload": payload,
    }

    date_partition = ingested_at.strftime("%Y-%m-%d")
    source_dir = RAW_DIR / source / date_partition
    source_dir.mkdir(parents=True, exist_ok=True)

    time_compact = ingested_at.strftime("%H%M%S")
    file_path = source_dir / f"{source}_{time_compact}_{batch_index:03d}.json"
    file_path.write_text(json.dumps(envelope, indent=2), encoding="utf-8")

    return file_path
