"""
spark_init.py — Gemeinsame Lakehouse-Hilfsfunktionen für alle Notebooks.

Erste Zelle in jedem Notebook:
    import sys; sys.path.insert(0, "/home/jovyan/notebooks")
    from spark_init import get_spark_session, trino_query, show
    spark = get_spark_session("01-mein-notebook")
"""

import os
import pandas as pd
import trino.dbapi
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Konfiguration — aus Umgebungsvariablen (werden von docker-compose gesetzt)
# ---------------------------------------------------------------------------

_MINIO_USER     = os.environ.get("MINIO_ROOT_USER", "lakehouse")
_MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse123")
_MINIO_ENDPOINT = "http://minio:9000"
_NESSIE_URI     = "http://nessie:19120/api/v2"
_TRINO_HOST     = "trino"
_TRINO_PORT     = 8080


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def get_spark_session(app_name: str = "lakehouse-notebook") -> SparkSession:
    """
    Erstellt und gibt eine SparkSession im lokalen Modus zurück.

    Konfiguriert mit:
    - Iceberg Spark-Erweiterungen
    - Nessie als Iceberg-Katalog (Catalog-Name: "nessie")
    - MinIO als S3-kompatiblen Objektspeicher (S3FileIO)
    - S3A Hadoop Filesystem für s3a://-URIs

    Der lokale Modus (local[*]) läuft direkt im Jupyter-Container —
    kein Verbindungsaufbau zum Spark-Cluster nötig. Für die Demo-Notebooks
    ist das ausreichend; der Wow-Faktor liegt in Iceberg + Nessie, nicht
    im verteilten Rechnen.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # --- Iceberg Erweiterungen ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # --- Nessie Catalog ---
        .config("spark.sql.catalog.nessie",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type",          "nessie")
        .config("spark.sql.catalog.nessie.uri",           _NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",           "main")
        .config("spark.sql.catalog.nessie.warehouse",     "s3a://warehouse/")
        .config("spark.sql.catalog.nessie.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint",          _MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id",     _MINIO_USER)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", _MINIO_PASSWORD)
        .config("spark.sql.catalog.nessie.s3.region",            "us-east-1")

        # --- Hadoop S3A Filesystem (für s3a://-URI-Auflösung) ---
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",               _MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             _MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",             _MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Trino-Verbindung
# ---------------------------------------------------------------------------

def get_trino_connection(catalog: str = "nessie", schema: str = "raw"):
    """
    Erstellt und gibt eine Trino DBAPI-Verbindung zurück.

    Standardmäßig auf Catalog "nessie", Schema "raw" — kann überschrieben
    werden wenn Staging- oder Curated-Layer abgefragt werden sollen.
    """
    return trino.dbapi.connect(
        host=_TRINO_HOST,
        port=_TRINO_PORT,
        user="lakehouse",
        catalog=catalog,
        schema=schema,
    )


def trino_query(sql: str, catalog: str = "nessie", schema: str = "raw") -> pd.DataFrame:
    """
    Führt eine SQL-Abfrage via Trino aus und gibt das Ergebnis als
    pandas DataFrame zurück.

    Convenience-Funktion für schnelle Abfragen in Notebooks — kein
    manuelles Cursor-Handling nötig.

    Beispiel:
        df = trino_query("SELECT * FROM nessie.raw.fund_master")
        df = trino_query("SELECT count(*) FROM cdp_emissions")
    """
    conn = get_trino_connection(catalog=catalog, schema=schema)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description] if cursor.description else []
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Anzeige-Hilfsfunktionen
# ---------------------------------------------------------------------------

def show(df, n: int = 10) -> pd.DataFrame:
    """Zeigt einen Spark DataFrame als formatierte HTML-Tabelle in Jupyter."""
    return df.limit(n).toPandas()


# ---------------------------------------------------------------------------
# Modul geladen
# ---------------------------------------------------------------------------

print("Lakehouse Helpers geladen")
