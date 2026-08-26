# =============================================================================
# elt_pipeline — Multi-stage Dockerfile
# -----------------------------------------------------------------------------
# Stack (pinned for reproducibility):
#   Builder : python:3.11-slim  + uv  →  builds elt_pipeline wheel
#   Runtime : eclipse-temurin:23-jdk  +  Spark 4.1.2  +  Trino 468  +  wheel
# =============================================================================
# Usage:
#   docker build -t elt-pipeline:0.1.0 .
#   docker build --target runtime -t elt-pipeline:0.1.0 .
#
# Optional build args (defaults match the frozen manifest):
#   --build-arg PYTHON_VERSION=3.11
#   --build-arg JDK_BASE_IMAGE=eclipse-temurin:23-jdk
#   --build-arg SPARK_VERSION=4.1.2
#   --build-arg TRINO_VERSION=468
#   --build-arg ICEBERG_VERSION=1.11.0
#   --build-arg SCALA_BINARY=2.13
#   --build-arg EXTRAS=spark,s3  (comma-separated: spark,s3,gcs,adls,delta,emr,dataproc,synapse)
# =============================================================================

# -----------------------------------------------------------------------------
# STAGE 1 / 3 — Wheel builder
# -----------------------------------------------------------------------------
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install uv for reproducible wheel builds
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /bin/uv

WORKDIR /build

# Copy the project sources (keep order for layer caching)
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ ./src/
COPY examples/ ./examples/
COPY pipeline.yaml ./pipeline.yaml

# Build the wheel and install into an isolated prefix that we COPY later.
# --extras are selected at build time; the runtime install also pulls them.
ARG EXTRAS="spark,s3"
RUN uv build --wheel --out-dir /build/dist \
 && UV_PROJECT_ENVIRONMENT=/opt/elt_pipeline_venv \
    uv pip install \
        --python /usr/local/bin/python \
        --prefix /opt/elt_pipeline_venv \
        /build/dist/*.whl[${EXTRAS}]

# -----------------------------------------------------------------------------
# STAGE 2 / 3 — Spark + Trino downloader (separate to keep runtime clean)
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS dist-fetcher

ARG SPARK_VERSION=4.1.2
ARG TRINO_VERSION=468
ARG SCALA_BINARY=2.13

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tar \
        gzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dist

# Spark — pick the pre-built for Hadoop 3 (standard).
# Spark 4.x uses the same hadoop3 distribution variant as 3.x.
RUN SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
 && curl -fSL --retry 3 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" \
        -o "${SPARK_TGZ}" \
 && tar -xzf "${SPARK_TGZ}" \
 && mv "spark-${SPARK_VERSION}-bin-hadoop3" /opt/dist/spark \
 && rm -f "${SPARK_TGZ}"

# Trino server
RUN TRINO_TGZ="trino-server-${TRINO_VERSION}.tar.gz" \
 && curl -fSL --retry 3 \
        "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/${TRINO_TGZ}" \
        -o "${TRINO_TGZ}" \
 && tar -xzf "${TRINO_TGZ}" \
 && mv "trino-server-${TRINO_VERSION}" /opt/dist/trino \
 && rm -f "${TRINO_TGZ}"

# Trino CLI (useful for one-off queries inside the container)
RUN curl -fSL --retry 3 \
        "https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar" \
        -o /opt/dist/trino/bin/trino \
 && chmod +x /opt/dist/trino/bin/trino

# SQLite JDBC driver — injected into the Trino Iceberg plugin dir for the
# zero-service jdbc+sqlite workstation catalog (matches ops/trino_serving/run_trino.sh).
ARG SQLITE_JDBC_VERSION=3.46.0.0
RUN mkdir -p /opt/dist/trino/plugin/iceberg \
 && curl -fSL --retry 3 \
        "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_JDBC_VERSION}/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar" \
        -o "/opt/dist/trino/plugin/iceberg/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar"

# -----------------------------------------------------------------------------
# STAGE 3 / 3 — Final runtime
# -----------------------------------------------------------------------------
ARG JDK_BASE_IMAGE=eclipse-temurin:23-jdk
FROM ${JDK_BASE_IMAGE} AS runtime

ARG SPARK_VERSION=4.1.2
ARG TRINO_VERSION=468
ARG SCALA_BINARY=2.13
ARG ICEBERG_VERSION=1.11.0

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Pin versions in env so the running container reports what it was built with.
    ELT_PIPELINE_IMAGE_SPARK_VERSION=${SPARK_VERSION} \
    ELT_PIPELINE_IMAGE_TRINO_VERSION=${TRINO_VERSION} \
    ELT_PIPELINE_IMAGE_SCALA_BINARY=${SCALA_BINARY} \
    ELT_PIPELINE_IMAGE_ICEBERG_VERSION=${ICEBERG_VERSION}

# Minimal OS deps: Python 3 (needed for the CLI + PySpark), bash, curl for
# observability webhook emitters, procps for `ps` in debugging.
RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        bash \
        curl \
        ca-certificates \
        procps \
        tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/local/bin/python

# Copy wheel + venv install from builder stage
COPY --from=builder /opt/elt_pipeline_venv /opt/elt_pipeline_venv
ENV PATH="/opt/elt_pipeline_venv/bin:${PATH}" \
    VIRTUAL_ENV=/opt/elt_pipeline_venv

# Copy Spark + Trino from dist-fetcher
COPY --from=dist-fetcher /opt/dist/spark /opt/spark
COPY --from=dist-fetcher /opt/dist/trino /opt/trino

ENV SPARK_HOME=/opt/spark \
    TRINO_HOME=/opt/trino \
    PATH="/opt/spark/bin:/opt/trino/bin:${PATH}"

# Container layout convention (matches the pipeline.yaml "~" expansion semantics
# for the zero-config case, but kept absolute for container clarity).
ENV ELT_PIPELINE_REPO_RUN_DIR=/var/lib/elt_pipeline \
    ELT_PIPELINE_CONFIG_PATH=/etc/elt_pipeline/pipeline.yaml \
    ELT_PIPELINE_IVY_HOME=/var/cache/elt_pipeline/ivy2

# Project sources (examples + reference pipeline.yaml) copied into the standard
# container paths so `docker run … --config-path /etc/elt_pipeline/pipeline.yaml`
# works with zero bind-mounts for the smoke demo.
COPY pipeline.yaml /etc/elt_pipeline/pipeline.yaml
COPY pipeline.yaml /usr/share/elt_pipeline/pipeline.yaml
COPY examples/ /usr/share/elt_pipeline/examples/
COPY ops/ /usr/share/elt_pipeline/ops/
COPY docker/ /usr/share/elt_pipeline/docker/

RUN mkdir -p \
        /var/lib/elt_pipeline/results/elt_pipeline \
        /var/cache/elt_pipeline/ivy2 \
        /var/log/elt_pipeline \
 && chmod -R 775 /var/lib/elt_pipeline /var/cache/elt_pipeline /var/log/elt_pipeline

WORKDIR /var/lib/elt_pipeline

# Use tini so Ctrl-C / docker stop signal handling is correct.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/share/elt_pipeline/docker/entrypoint.sh"]

# Default: print the banner + version. Users override with `docker run … ingest run` etc.
CMD ["elt-pipeline", "--help"]

EXPOSE 8080
LABEL org.opencontainers.image.title="elt_pipeline" \
      org.opencontainers.image.description="5-layer configuration-driven ELT: L1 raw → L2 aligned → L3 canonical Iceberg → L4 marts → L5 static exports. Spark 4.1 + Trino 468 + JDK 23." \
      org.opencontainers.image.source="https://github.com/paterake/elt_pipeline" \
      org.opencontainers.image.licenses="MIT"
