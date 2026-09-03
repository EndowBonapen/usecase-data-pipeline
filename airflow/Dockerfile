FROM apache/airflow:2.10.4-python3.11

# Build context is the project root (see docker-compose.yaml), so these paths
# are relative to there, not to this Dockerfile's own folder.
COPY ETL/requirements.txt /opt/airflow/etl-requirements.txt
COPY ELT/requirements.txt /opt/airflow/elt-requirements.txt

# Both ETL and ELT run inside the same Airflow instance (one scheduler
# imports both DAGs' task code directly), so both dependency sets have to
# coexist in one image. Not verified conflict-free (dbt-core pins its own
# click/jinja2/etc versions) — this hasn't been built and run, see
# airflow/docs/01-dag-reference.md.
RUN pip install --no-cache-dir -r /opt/airflow/etl-requirements.txt \
    && pip install --no-cache-dir -r /opt/airflow/elt-requirements.txt
