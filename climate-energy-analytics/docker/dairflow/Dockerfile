FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y gcc curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws && \
    apt-get clean

USER airflow
RUN pip install \
    apache-airflow-providers-amazon \
    apache-airflow-providers-apache-kafka \
    dbt-core \
    dbt-postgres \
    dbt-athena-community