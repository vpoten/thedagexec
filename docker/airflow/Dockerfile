FROM apache/airflow:slim-latest-python3.9

COPY airflow_after_entrypoint_script.sh /
USER root
RUN chmod a+x /airflow_after_entrypoint_script.sh
USER airflow
