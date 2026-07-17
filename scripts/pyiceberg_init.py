"""
pyiceberg_init.py — Repo-Prep fuer DE1 (Engine-Right-Sizing): Catalog-Verbindung
fuer den Polars-Pfad.

Deckt NUR die Katalog-/S3-Anbindung ab (das "fummelige" Stueck laut
Briefing). Die eigentliche Landing-Logik (Parquet lesen, Schrott-Saetze
filtern, typisieren, nach raw.taxi_polars schreiben) ist Aufgabe der Gruppe.

Muss innerhalb des lakehouse-net-Docker-Netzes laufen (z.B. im jupyter-
Container) — 'nessie' und 'minio' sind nur dort per Hostname aufloesbar.

Nutzung — Catalog direkt (Tabellen anlegen/lesen):
    from pyiceberg_init import get_catalog
    catalog = get_catalog()
    print(catalog.list_tables("raw"))

Nutzung — mit Polars' write_iceberg (Polars nutzt pyiceberg intern fuer
Catalog-Zugriff und Commit; exakte Parameter-Namen je nach installierter
Polars-Version in deren Doku pruefen):
    import polars as pl
    df = pl.DataFrame(...)
    df.write_iceberg(get_catalog(), "raw.taxi_polars", mode="append")

Getestet gegen: Nessie 0.99.0, pyiceberg (REST-Catalog), Iceberg-REST-API
unter http://nessie:19120/iceberg/main.

Anders als bei DuckDB (siehe config/duckdb/attach-nessie.sql) ist hier KEIN
ACCESS_DELEGATION_MODE-Flag noetig: pyiceberg nutzt fuer den Datenschreibpfad
standardmaessig die unten uebergebenen S3-Credentials (s3.access-key-id /
s3.secret-access-key), nicht Nessies serverseitig vergebene ("vended")
Credentials, die in diesem Setup fehlschlagen wuerden.
"""

import os

from pyiceberg.catalog import Catalog, load_catalog

NESSIE_ICEBERG_URI = os.environ.get("NESSIE_ICEBERG_URI", "http://nessie:19120/iceberg/main")
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "lakehouse")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse123")
S3_REGION = "us-east-1"
WAREHOUSE = os.environ.get("S3_BUCKET_WAREHOUSE", "warehouse")


def get_catalog(name: str = "lakehouse") -> Catalog:
    """Gibt einen pyiceberg REST-Catalog zurueck, verbunden mit Nessies
    Iceberg-REST-API auf dem main-Branch."""
    return load_catalog(
        name,
        **{
            "uri": NESSIE_ICEBERG_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
            "s3.path-style-access": "true",
        },
    )


if __name__ == "__main__":
    catalog = get_catalog()
    print("Namespaces:", catalog.list_namespaces())
    print("Tabellen in raw:", catalog.list_tables("raw"))
