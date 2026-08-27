import os
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_RAW = os.environ["BQ_DATASET_RAW"]
BQ_DATASET_MARTS = os.environ["BQ_DATASET_MARTS"]
RANDOMAPI_BASE_URL = os.environ["RANDOMAPI_BASE_URL"]
