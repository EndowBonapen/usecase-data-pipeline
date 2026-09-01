import os
from pathlib import Path

from dotenv import load_dotenv

# Resolved relative to this file, not the current working directory — matters
# once this module is imported from somewhere other than ETL/ (e.g. Airflow
# running inside a container, cwd elsewhere). Same fix as land.py's RAW_DIR.
load_dotenv(Path(__file__).resolve().parent / ".env")

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_RAW = os.environ["BQ_DATASET_RAW"]
BQ_DATASET_MARTS = os.environ["BQ_DATASET_MARTS"]
RANDOMAPI_BASE_URL = os.environ["RANDOMAPI_BASE_URL"]
