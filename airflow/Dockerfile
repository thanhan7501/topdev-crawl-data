# Use the official Apache Airflow image
FROM apache/airflow:2.10.1-python3.11

# Install additional Airflow providers
USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y procps && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

USER airflow

RUN pip install \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \ 
    python-dotenv \
    pyspark \
