FROM apache/airflow:2.6.3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64