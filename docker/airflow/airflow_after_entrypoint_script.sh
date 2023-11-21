#!/bin/bash

airflow db migrate
airflow users create --username admin --password admin --firstname FIRST_NAME --lastname LAST_NAME --role Admin --email admin@example.org
airflow webserver
airflow scheduler
