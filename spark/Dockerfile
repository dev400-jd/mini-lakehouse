# =============================================================================
# Spark Image mit Iceberg + Nessie + S3A Support
#
# Basis:   apache/spark:3.5.8-python3   (Hadoop 3.3.4 eingebettet)
#
# JAR-Kombination (geprüfte Versionskompatibilität):
#   iceberg-spark-runtime-3.5_2.12:1.7.1  — Iceberg Runtime inkl. Nessie-Catalog
#   iceberg-aws-bundle:1.7.1              — AWS SDK v2 für S3FileIO (seit Iceberg 1.3
#                                            separates JAR, nicht mehr im spark-runtime)
#   hadoop-aws:3.3.4                       — S3A FileSystem, exakt passend zu
#                                            hadoop-client-runtime-3.3.4.jar im Basisimage
#   aws-java-sdk-bundle:1.12.262           — AWS SDK v1, kompatibel mit hadoop-aws:3.3.4
#
# Hinweis: iceberg-aws-bundle enthält AWS SDK v2 (software.amazon.awssdk.*).
# aws-java-sdk-bundle enthält AWS SDK v1 (com.amazonaws.*). Kein Namenskonflikt.
# =============================================================================

FROM apache/spark:3.5.8-python3

ARG ICEBERG_VERSION=1.7.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ARG MAVEN=https://repo1.maven.org/maven2

USER root

RUN apt-get update -q && \
    apt-get install -y -q --no-install-recommends gettext-base && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    curl -fL -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    curl -fL -o /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# Eigenes Conf-Verzeichnis mit Schreibrechten für den spark-User.
# SPARK_CONF_DIR als Docker-ENV stellt sicher dass es für ALLE Prozesse
# im Container gilt — auch für spark-submit das via docker exec gestartet wird.
ENV SPARK_CONF_DIR=/opt/spark/custom-conf
RUN mkdir -p /opt/spark/custom-conf && chown spark:spark /opt/spark/custom-conf

COPY config/spark/entrypoint.sh /opt/spark-custom-entrypoint.sh
RUN chmod +x /opt/spark-custom-entrypoint.sh

RUN ln -s /opt/spark/bin/spark-sql /usr/local/bin/spark-sql && \
    ln -s /opt/spark/bin/pyspark /usr/local/bin/pyspark && \
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit

USER spark

ENTRYPOINT ["/opt/spark-custom-entrypoint.sh"]
