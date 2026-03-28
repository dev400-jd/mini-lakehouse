# =============================================================================
# Jupyter Image mit Iceberg + Nessie + S3A Support
#
# Basis: jupyter/pyspark-notebook:spark-3.5.3
#   Enthält: Python 3.11, Spark 3.5.3, Hadoop 3.3.4, JupyterLab
#
# Gleiche JAR-Kombination wie spark/Dockerfile (exakte Versionen):
#   iceberg-spark-runtime-3.5_2.12:1.7.1  — Iceberg Runtime für Spark 3.5.x
#   iceberg-aws-bundle:1.7.1              — AWS SDK v2 für S3FileIO
#   hadoop-aws:3.3.4                       — S3A FileSystem (passend zu Hadoop 3.3.4)
#   aws-java-sdk-bundle:1.12.262           — AWS SDK v1, kompatibel mit hadoop-aws:3.3.4
#
# Python-Packages (zusätzlich zu den bereits enthaltenen pandas/matplotlib):
#   trino                                  — Python-Client für Trino (Notebook 04)
#
# Credentials-Handling:
#   Gleicher envsubst-Ansatz wie spark/Dockerfile — before-spark-conf.sh
#   liest spark-defaults.conf.template und schreibt es mit substituierten
#   Credentials nach SPARK_CONF_DIR=/home/jovyan/spark-conf/.
# =============================================================================

ARG JUPYTER_VERSION=spark-3.5.3
FROM quay.io/jupyter/pyspark-notebook:${JUPYTER_VERSION}

ARG ICEBERG_VERSION=1.7.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ARG MAVEN=https://repo1.maven.org/maven2

USER root

# gettext-base für envsubst (wie im Spark-Image)
RUN apt-get update -q && \
    apt-get install -y -q --no-install-recommends gettext-base && \
    rm -rf /var/lib/apt/lists/*

# Iceberg + S3A JARs — exakt gleiche Versionen wie spark/Dockerfile
# Spark-Home in pyspark-notebook ist /usr/local/spark
RUN curl -fL -o /usr/local/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    curl -fL -o /usr/local/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    curl -fL -o /usr/local/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fL -o /usr/local/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# Spark-Conf-Dir mit Schreibrechten für jovyan (analog zu /opt/spark/custom-conf im Spark-Image)
RUN mkdir -p /home/jovyan/spark-conf && chown jovyan:users /home/jovyan/spark-conf

# Before-notebook.d: envsubst läuft vor dem JupyterLab-Start
COPY config/jupyter/before-spark-conf.sh /usr/local/bin/before-notebook.d/before-spark-conf.sh
RUN chmod +x /usr/local/bin/before-notebook.d/before-spark-conf.sh

# PySpark aus der Spark-Distribution importierbar machen.
# Das Basisimage installiert Spark als standalone unter /usr/local/spark,
# aber nicht als Python-Paket im conda-Environment.
# PYTHONPATH macht pyspark und py4j im Standard-Python3-Kernel importierbar.
ENV PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_HOME=/usr/local/spark

USER jovyan

# trino Python-Client (pandas + matplotlib bereits im Basisimage)
RUN pip install --no-cache-dir "trino>=0.328.0"
