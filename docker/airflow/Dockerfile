FROM apache/airflow:2.7.3-python3.10

ENV LOAD_EX="y"

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-11-jre-headless gcc heimdal-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.4.0 \
    && pip install --no-cache-dir apache-airflow-providers-apache-hdfs==4.2.0

COPY airflow_after_entrypoint_script.sh /
USER root
RUN chmod a+x /airflow_after_entrypoint_script.sh
USER airflow
