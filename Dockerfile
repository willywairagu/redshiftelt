FROM quay.io/astronomer/astro-runtime:12.4.0


WORKDIR /usr/local/airflow

COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_env && source dbt_venv/bin/activate && \ 
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate