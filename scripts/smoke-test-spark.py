"""
Iceberg Smoke Test — beweist die vollständige Integration:
  Spark → Nessie Catalog → Iceberg → MinIO (S3A)

Ausführung:
  docker compose exec spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /scripts/smoke-test-spark.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Iceberg Smoke Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("  Iceberg Smoke Test")
print("=" * 60)

# a) Namespace
print("\n[1/5] Namespace anlegen ...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
print("      OK: nessie.raw")

# b) Tabelle
print("\n[2/5] Tabelle anlegen ...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.raw.smoke_test (
        id    INT,
        name  STRING,
        value DOUBLE
    )
    USING iceberg
""")
print("      OK: nessie.raw.smoke_test")

# c) Daten schreiben
print("\n[3/5] Daten schreiben ...")
spark.sql("""
    INSERT INTO nessie.raw.smoke_test VALUES
        (1, 'test',      42.0),
        (2, 'lakehouse', 99.9),
        (3, 'iceberg',    1.5)
""")
print("      OK: 3 Zeilen geschrieben")

# d) Daten lesen
print("\n[4/5] Daten lesen:")
spark.sql("SELECT * FROM nessie.raw.smoke_test ORDER BY id").show()

# e) Snapshots
print("\n[5/5] Iceberg Snapshots:")
spark.sql("SELECT snapshot_id, committed_at, operation FROM nessie.raw.smoke_test.snapshots").show(truncate=False)

# Aufräumen
print("\n[6/6] Tabelle aufräumen ...")
spark.sql("DROP TABLE nessie.raw.smoke_test")
print("      OK: Tabelle gelöscht")

print("\n" + "=" * 60)
print("  Smoke Test BESTANDEN")
print("=" * 60 + "\n")

spark.stop()
