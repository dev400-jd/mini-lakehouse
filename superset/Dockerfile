ARG SUPERSET_VERSION

FROM apache/superset:${SUPERSET_VERSION}

USER root

RUN uv pip install \
    --python /app/.venv/bin/python \
    --no-cache \
    psycopg2-binary \
    sqlalchemy-trino \
    redis

RUN /app/.venv/bin/python -c \
    "import psycopg2; import trino; import redis; print('Superset drivers successfully installed')"

USER superset