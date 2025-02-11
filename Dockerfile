FROM apache/airflow:2.7.0

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install kafka-python websocket-client pyspark

USER airflow