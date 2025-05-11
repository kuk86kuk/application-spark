FROM apache/airflow:2.6.3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        procps \
        libhadoop3-native \  # Для Hadoop
        wget && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars/ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64