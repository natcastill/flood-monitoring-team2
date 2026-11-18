# Airflow image with the project's Python dependencies baked in.
# The official constraints file pins every package to versions tested with Airflow 2.10.2,
# so installing our extras cannot break Airflow itself.
FROM apache/airflow:2.10.2-python3.11

# Workshop 3: PySpark needs a Java runtime. Spark 3.5 supports Java 8/11/17; Debian bookworm
# (the base of the Airflow image) ships OpenJDK 17. JAVA_HOME is linked to a fixed path so
# the image works on Intel/AMD (amd64) and Apple Silicon (arm64) laptops alike.
USER root
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.11.txt"
