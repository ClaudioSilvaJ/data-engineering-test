FROM apache/airflow:2.10.3-python3.11

WORKDIR /opt/airflow

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libsqlite3-dev \
    libpq-dev \
    build-essential

USER airflow

RUN pip install --upgrade pip

COPY --chown=airflow:root requirements.txt .
COPY --chown=airflow:root airflow.cfg .
COPY --chown=airflow:root vendas-combustiveis-m3.xlsx .

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=airflow:root ./dags /opt/airflow/dags

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_db

COPY --chown=airflow:root entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]