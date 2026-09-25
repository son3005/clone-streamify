FROM apache/airflow:2.8.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR ${AIRFLOW_HOME}

WORKDIR $AIRFLOW_HOME