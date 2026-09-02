# =====================================================================
# AP2 — Spark Structured Streaming Job
#
# ⚠️ DHBWCloud: Spark ist der RAM-hungrigste Teil des Stacks.
#    Executor-/Driver-Memory werden über die Helm-values gesetzt
#    (spark.executor.memory=512m, 1 Executor). Vor Woche 2 mit dem
#    Spike aus Checkliste 0b.6 verifizieren!
#
# ⚠️ Versionsfalle: delta-spark, hadoop-aws und aws-java-sdk-bundle
#    müssen zur Spark-Version passen. Kombination unten ist für
#    Spark 3.5.x / Scala 2.12 getestet-kompatibel — beim Spike prüfen.
# =====================================================================
FROM apache/spark:3.5.1-python3

USER root

WORKDIR /opt/spark/work-dir

# --- JARs für Kafka-Source, Delta-Sink und S3A/MinIO ---
# TODO(AP2): im Spike bestätigen. Alternative: --packages beim spark-submit,
# braucht dann aber Internet-Egress von der VM — im Image ist robuster.
ARG DELTA_VERSION=3.1.0
ARG HADOOP_VERSION=3.3.4
ARG SPARK_VERSION=3.5.1
ENV SPARK_JARS=/opt/spark/jars

RUN set -eux; \
    for url in \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do curl -fsSL -o "${SPARK_JARS}/$(basename "$url")" "$url"; done

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER 185

ENV PYTHONUNBUFFERED=1

# Konfiguration (Broker, S3-Endpoint, Fenstergröße) kommt aus ConfigMap/Secret.
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "512m", \
     "streaming_job.py"]
