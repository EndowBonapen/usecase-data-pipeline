FROM apache/airflow:2.10.4-python3.11

# Build context is the project root (see docker-compose.yaml), so this path
# is relative to there, not to this Dockerfile's own folder.
COPY ETL/requirements.txt /opt/airflow/etl-requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/etl-requirements.txt
