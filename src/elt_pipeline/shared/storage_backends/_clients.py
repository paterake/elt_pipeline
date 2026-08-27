from __future__ import annotations

import sys
from typing import Any

from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import (
    StorageScheme,
    detect_scheme,
)

_FACADE_MOD = "elt_pipeline.shared.storage_backends"


def _get_s3_client() -> Any:
    facade = sys.modules[_FACADE_MOD]
    if facade._S3_CLIENT is not None:
        return facade._S3_CLIENT
    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ConfigValidationError(
            message=(
                "Using s3:// storage paths requires the 's3' optional extra to be installed, or "
                "boto3 must be available in your runtime (as it is by default on AWS EMR). "
                "Install via: uv sync --extra s3, or uv sync --extra emr to include spark deps too."
            ),
            context={
                "error": str(exc),
                "hint_emr": (
                    "On AWS EMR, boto3 is preinstalled and credentials "
                    "come from the cluster IAM role. No explicit "
                    "access key management required."
                ),
            },
        ) from exc
    facade._S3_CLIENT = boto3.client("s3")
    return facade._S3_CLIENT


def _split_s3_path(path: str) -> tuple[str, str]:
    scheme = detect_scheme(path)
    if scheme is not StorageScheme.s3:
        raise PipelineError(
            message=f"Expected s3:// URI for S3 split, got {path!r}",
            error_code="STORAGE_OPERATION_UNSUPPORTED",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context={"operation": "_split_s3_path", "path": path},
        )
    rest = path[len("s3://"):]
    if "/" in rest:
        bucket, key = rest.split("/", 1)
    else:
        bucket = rest
        key = ""
    if bucket == "":
        raise ConfigValidationError(
            message=f"S3 URI {path!r} does not specify a bucket name.",
            context={"path": path},
        )
    return bucket, key


def _get_gcs_client() -> Any:
    facade = sys.modules[_FACADE_MOD]
    if facade._GCS_CLIENT is not None:
        return facade._GCS_CLIENT
    try:
        from google.cloud import storage  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ConfigValidationError(
            message=(
                "Using gs:// storage paths requires the 'gcs' optional extra to be installed, or "
                "google-cloud-storage must be available in your runtime (as it is by default on "
                "GCP Dataproc / GKE with Workload Identity). "
                "Install via: uv sync --extra gcs, or uv sync --extra dataproc "
                "to include spark deps too."
            ),
            context={
                "error": str(exc),
                "hint_dataproc": (
                    "On GCP Dataproc, google-cloud-storage is preinstalled and credentials "
                    "come from the cluster service account. No explicit "
                    "service account key management required."
                ),
            },
        ) from exc
    facade._GCS_CLIENT = storage.Client()
    return facade._GCS_CLIENT


def _split_gcs_path(path: str) -> tuple[str, str]:
    scheme = detect_scheme(path)
    if scheme is not StorageScheme.gs:
        raise PipelineError(
            message=f"Expected gs:// URI for GCS split, got {path!r}",
            error_code="STORAGE_OPERATION_UNSUPPORTED",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context={"operation": "_split_gcs_path", "path": path},
        )
    rest = path[len("gs://"):]
    if "/" in rest:
        bucket, key = rest.split("/", 1)
    else:
        bucket = rest
        key = ""
    if bucket == "":
        raise ConfigValidationError(
            message=f"GCS URI {path!r} does not specify a bucket name.",
            context={"path": path},
        )
    return bucket, key


def _get_adls_client() -> Any:
    facade = sys.modules[_FACADE_MOD]
    if facade._ADLS_CLIENT is not None:
        return facade._ADLS_CLIENT
    try:
        from azure.storage.filedatalake import (  # type: ignore[import-not-found]
            DataLakeServiceClient,
        )
    except ImportError as exc:
        raise ConfigValidationError(
            message=(
                "Using abfss:// storage paths requires the 'adls' optional extra to be "
                "installed, or azure-storage-file-datalake must be available in your "
                "runtime (as it is by default on Azure Synapse / ADF IR with MSI). "
                "Install via: uv sync --extra adls, or uv sync --extra synapse to "
                "include spark deps too."
            ),
            context={
                "error": str(exc),
                "hint_synapse": (
                    "On Azure Synapse / ADF, azure-storage-file-datalake is "
                    "preinstalled and credentials come from the cluster Managed "
                    "Identity / linked service. No explicit key management required."
                ),
            },
        ) from exc
    facade._ADLS_CLIENT = DataLakeServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=placeholder;AccountKey=placeholder"
    )
    return facade._ADLS_CLIENT


def _split_adls_path(path: str) -> tuple[str, str, str]:
    scheme = detect_scheme(path)
    if scheme is not StorageScheme.abfss:
        raise PipelineError(
            message=f"Expected abfss:// URI for ADLS split, got {path!r}",
            error_code="STORAGE_OPERATION_UNSUPPORTED",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context={"operation": "_split_adls_path", "path": path},
        )
    rest = path[len("abfss://"):]
    if "@" not in rest:
        raise ConfigValidationError(
            message=(
                f"ADLS Gen2 URI {path!r} must use the form "
                f"abfss://<container>@<account>.dfs.core.windows.net/<path>"
            ),
            context={"path": path},
        )
    container_part, remainder = rest.split("@", 1)
    if "/" in remainder:
        account_host, key = remainder.split("/", 1)
    else:
        account_host = remainder
        key = ""
    if not container_part:
        raise ConfigValidationError(
            message=f"ADLS Gen2 URI {path!r} does not specify a container name.",
            context={"path": path},
        )
    if not account_host:
        raise ConfigValidationError(
            message=f"ADLS Gen2 URI {path!r} does not specify an account hostname.",
            context={"path": path},
        )
    account_name = account_host.split(".", 1)[0]
    return container_part, account_name, key


def _s3_infer_partition_subprefixes(
    *,
    staging_prefix: str,
    staging_keys: list[str],
) -> list[str]:
    subprefixes: set[str] = set()
    for key in staging_keys:
        rel = (
            key[len(staging_prefix):]
            if key.startswith(staging_prefix)
            else key
        )
        if "/" not in rel:
            continue
        segments = rel.split("/")[:-1]
        partition_segments: list[str] = []
        for segment in segments:
            if "=" not in segment:
                break
            partition_segments.append(segment)
        if partition_segments:
            subprefixes.add("/".join(partition_segments) + "/")
    return sorted(subprefixes)
