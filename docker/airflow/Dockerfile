FROM apache/airflow:slim-2.7.3-python3.10

ENV LOAD_EX="y"

COPY airflow_after_entrypoint_script.sh /
USER root
RUN chmod a+x /airflow_after_entrypoint_script.sh
USER airflow
