from google.cloud import bigquery
from config import GCP_PROJECT_ID

client = bigquery.Client(project=GCP_PROJECT_ID)
result = list(client.query("SELECT 1 AS ok").result())
print("BigQuery connection OK:", result)
