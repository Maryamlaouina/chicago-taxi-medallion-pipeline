FROM apache/airflow:3.1.8-python3.11

# --- Java ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# --- PySpark ---
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /requirements.txt


RUN SPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL -o $SPARK_JARS/hadoop-aws-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o $SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar