from google.cloud import bigquery
from config import GCP_PROJECT_ID, BQ_DATASET_RAW, BQ_DATASET_MARTS

LOCATION = "US"


def ensure_dataset(client: bigquery.Client, dataset_id: str, description: str) -> None:
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    dataset.description = description
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {GCP_PROJECT_ID}.{dataset_id}")


if __name__ == "__main__":
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_dataset(client, BQ_DATASET_RAW, "ELT raw landing zone - unmodified JSON from randomapi.dev, transformed by dbt")
    ensure_dataset(client, BQ_DATASET_MARTS, "ELT data mart - dimensional model built entirely by dbt")
