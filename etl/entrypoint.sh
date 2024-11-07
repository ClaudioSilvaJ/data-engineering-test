#!/bin/bash
airflow db upgrade  
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

airflow webserver -p 8080 & airflow scheduler