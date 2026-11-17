# Airflow image with the project's Python dependencies baked in.
# The official constraints file pins every package to versions tested with Airflow 2.10.2,
# so installing our extras cannot break Airflow itself.
FROM apache/airflow:2.10.2-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.11.txt"
