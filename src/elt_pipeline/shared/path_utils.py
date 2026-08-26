from __future__ import annotations

import re
from enum import Enum
from typing import Any

from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._/-]+")


class _StorageScheme(str, Enum):
    s3 = "s3"
    gs = "gs"
    abfss = "abfss"
    file = "file"
    local_unschemed = "local_unschemed"


StorageScheme = _StorageScheme


_SUPPORTED_SCHEME_PREFIXES: frozenset[str] = frozenset(
    {
        _StorageScheme.s3.value + "://",
        _StorageScheme.gs.value + "://",
        _StorageScheme.abfss.value + "://",
        _StorageScheme.file.value + "://",
    }
)
_SUPPORTED_SCHEMES_FOR_ERROR: tuple[str, ...] = (
    "s3:// (AWS S3)",
    "gs:// (Google Cloud Storage)",
    "abfss:// (Azure ADLS Gen2)",
    "file:// (explicit local POSIX)",
    "bare local POSIX path (no scheme)",
)


def _validate_root_is_string(path: Any) -> None:
    if not isinstance(path, str):
        raise ConfigValidationError(
            message=(
                "Storage root / path must be a string URI (e.g. s3://bucket/prefix, "
                "file:///abs/path, or a bare POSIX string). "
                f"Got value of type {type(path).__name__!r}: {path!r}"
            ),
            context={
                "provided_type": type(path).__name__,
                "hint": (
                    "Do not wrap root paths in pathlib.Path; use raw string URIs; "
                    "this preserves URI scheme semantics for s3:// and other object stores."
                ),
            },
        )


def detect_scheme(path: str) -> _StorageScheme:
    _validate_root_is_string(path)
    if path.startswith("s3://"):
        return _StorageScheme.s3
    if path.startswith("gs://"):
        return _StorageScheme.gs
    if path.startswith("abfss://"):
        return _StorageScheme.abfss
    if path.startswith("file://"):
        return _StorageScheme.file
    if path.startswith("wasbs://"):
        scheme_part = "wasbs://"
        raise ConfigValidationError(
            message=(
                f"Legacy Azure Blob scheme detected in path: {path!r}. "
                f"wasbs:// is out of scope — Azure Blob Storage legacy "
                f"is not on the recommended path. "
                f"Migrate to Azure Data Lake Storage Gen2 (abfss://) "
                f"instead: replace the URI scheme from "
                f"wasbs://container@account.blob.core.windows.net/path "
                f"to abfss://container@account.dfs.core.windows.net/path."
            ),
            context={
                "path": path,
                "detected_scheme": scheme_part,
                "recommended_scheme": "abfss:// (Azure ADLS Gen2)",
                "migration_guidance": (
                    "ADLS Gen2 uses the dfs.core.windows.net suffix "
                    "(not blob.core.windows.net) and the abfss:// scheme. "
                    "The same StorageBackend registry, Spark Hadoop FS "
                    "config surface, and B-2 production backend already "
                    "support abfss:// end-to-end. For existing wasbs-based "
                    "containers, enable Hierarchical Namespace on the "
                    "storage account (or create a new Gen2 account) then "
                    "rewrite the URI scheme + authority suffix — the "
                    "container name and relative path stay identical."
                ),
            },
        )
    if path.startswith("hdfs://"):
        scheme_part = "hdfs://"
        raise ConfigValidationError(
            message=(
                f"Hadoop HDFS scheme detected in path: {path!r}. "
                f"hdfs:// support is DEFUNCT. Industry reality: on-prem Hadoop/HDFS "
                f"clusters have been displaced by cloud-native object storage. Use "
                f"s3:// (AWS S3), gs:// (Google Cloud Storage), or abfss:// "
                f"(Azure ADLS Gen2). For legacy on-prem data, migrate payloads to a "
                f"cloud object store first (the platform's §2 object_storage connector "
                f"ingests from any of the 4 supported schemes), then run the standard "
                f"ELT pipeline against the object store."
            ),
            context={
                "path": path,
                "detected_scheme": scheme_part,
                "alternatives": list(_SUPPORTED_SCHEMES_FOR_ERROR),
                "note": (
                    "hdfs:// is intentionally NOT implementable via the B-6 "
                    "StorageBackend facade pattern for this project. Every modern "
                    "data platform (AWS EMR, GCP Dataproc, Azure Synapse, Databricks, "
                    "Snowflake, Trino/Iceberg) has converged on object storage as "
                    "the durable storage substrate; a bespoke HDFS backend adds "
                    "permanent maintenance surface for a shrinking legacy niche. "
                    "Reconsider only if a paying, signed-off customer contract "
                    "explicitly requires on-prem HDFS AND object storage is "
                    "genuinely unavailable at that site. If that day comes, "
                    "register HDFSStorageBackend via B-6 + add Spark Hadoop FS "
                    "config surface — but expect zero upstream support from this "
                    "project's maintainers for that bespoke fork."
                ),
            },
        )
    if "://" in path:
        scheme_part = path.split("://", 1)[0]
        raise ConfigValidationError(
            message=(
                f"Unsupported storage scheme in path: {path!r}. "
                f"Detected scheme {scheme_part + '://'!r}. "
                f"Supported schemes: {'; '.join(_SUPPORTED_SCHEMES_FOR_ERROR)}."
            ),
            context={
                "path": path,
                "detected_scheme": scheme_part + "://",
                "supported_schemes": list(_SUPPORTED_SCHEMES_FOR_ERROR),
                "note": (
                    "Spark internally may use s3a:// for some Hadoop configs; "
                    "on EMR use s3:// and let EMRFS handle it. "
                    "Never silently coerce schemes."
                ),
            },
        )
    return _StorageScheme.local_unschemed


def strip_file_scheme(path: str) -> str:
    _validate_root_is_string(path)
    scheme = detect_scheme(path)
    if scheme is not _StorageScheme.file:
        return path
    rest = path[len("file://"):]
    if rest.startswith("/") and not rest.startswith("//"):
        return rest
    return rest


def collapse_slashes_without_scheme(path_suffix: str) -> str:
    return re.sub(r"/+", "/", path_suffix)


def collapse_slashes(path: str) -> str:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        return "s3://" + collapse_slashes_without_scheme(path[len("s3://"):])
    if scheme is _StorageScheme.gs:
        return "gs://" + collapse_slashes_without_scheme(path[len("gs://"):])
    if scheme is _StorageScheme.abfss:
        return "abfss://" + collapse_slashes_without_scheme(path[len("abfss://"):])
    if scheme is _StorageScheme.file:
        stripped = strip_file_scheme(path)
        return "file://" + collapse_slashes_without_scheme(stripped)
    return collapse_slashes_without_scheme(path)


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def join_paths(root: str, *segments: str) -> str:
    return _get_backend(root).join_paths(root, *segments)


def path_parent(path: str) -> str:
    return _get_backend(path).path_parent(path)


def path_basename(path: str) -> str:
    return _get_backend(path).path_basename(path)


def path_with_suffix(path: str, suffix: str) -> str:
    return _get_backend(path).path_with_suffix(path, suffix)


def path_normalize(path: str) -> str:
    return _get_backend(path).path_normalize(path)


def path_relative_to(path: str, base: str) -> str:
    _validate_root_is_string(base)
    scheme_path = detect_scheme(path)
    scheme_base = detect_scheme(base)
    if scheme_path is not scheme_base:
        raise ConfigValidationError(
            message=(
                f"Cannot compute path_relative_to across different schemes: "
                f"path={path!r}, base={base!r}"
            ),
            context={
                "path": path,
                "base": base,
                "path_scheme": scheme_path.value,
                "base_scheme": scheme_base.value,
            },
        )
    norm_path = collapse_slashes(path)
    norm_base = collapse_slashes(base)
    norm_base_rstripped = norm_base.rstrip("/")
    if norm_path == norm_base_rstripped:
        return "."
    prefix = norm_base_rstripped + "/"
    if not norm_path.startswith(prefix):
        raise ConfigValidationError(
            message=(
                f"Path does not start with the required base prefix: "
                f"path={path!r}, base={base!r}"
            ),
            context={
                "path": path,
                "base": base,
                "normalized_path": norm_path,
                "normalized_base": norm_base,
            },
        )
    return norm_path[len(prefix):]


def path_exists(path: str) -> bool:
    return _get_backend(path).path_exists(path)


def path_is_dir(path: str) -> bool:
    return _get_backend(path).path_is_dir(path)


def path_mkdir(path: str, *, parents: bool = True, exist_ok: bool = True) -> None:
    _get_backend(path).path_mkdir(path, parents=parents, exist_ok=exist_ok)


def path_listdir(path: str) -> list[str]:
    return _get_backend(path).path_listdir(path)


def path_glob(base: str, pattern: str) -> list[str]:
    return _get_backend(base).path_glob(base, pattern)


def path_rglob(base: str, pattern: str) -> list[str]:
    return _get_backend(base).path_rglob(base, pattern)


def path_content_length(path: str) -> int:
    return _get_backend(path).path_content_length(path)


def path_read_bytes(path: str) -> bytes:
    return _get_backend(path).path_read_bytes(path)


def path_read_text(path: str, encoding: str = "utf-8") -> str:
    return path_read_bytes(path).decode(encoding)


def path_write_bytes(path: str, data: bytes, *, atomic: bool = True) -> None:
    _get_backend(path).path_write_bytes(path, data, atomic=atomic)


def path_write_text(
    path: str, data: str, encoding: str = "utf-8", *, atomic: bool = True
) -> None:
    path_write_bytes(path, data.encode(encoding), atomic=atomic)


def path_open_for_append(path: str, encoding: str = "utf-8"):
    return _get_backend(path).path_open_for_append(path, encoding=encoding)


def path_replace(src: str, dst: str) -> None:
    scheme_src = detect_scheme(src)
    scheme_dst = detect_scheme(dst)
    if scheme_src is not scheme_dst:
        replace_ctx = {
            "src": src,
            "dst": dst,
            "src_scheme": scheme_src.value,
            "dst_scheme": scheme_dst.value,
        }
        raise PipelineError(
            message=(
                f"Cannot path_replace across different schemes: "
                f"src={src!r} (scheme={scheme_src.value}) "
                f"dst={dst!r} (scheme={scheme_dst.value})"
            ),
            error_code="STORAGE_OPERATION_UNSUPPORTED",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context=replace_ctx,
        )
    _get_backend(src).path_replace(src, dst)


def path_delete_tree(path: str) -> None:
    _get_backend(path).path_delete_tree(path)


def _get_backend(path: str):
    from elt_pipeline.shared.storage_backends import get_backend

    return get_backend(path)


_S3_CLIENT = None


def _s3_client():
    from elt_pipeline.shared.storage_backends import _get_s3_client

    return _get_s3_client()


def _split_s3_path(path: str) -> tuple[str, str]:
    from elt_pipeline.shared.storage_backends import _split_s3_path as _route_split

    return _route_split(path)


_GCS_CLIENT = None


def _gcs_client():
    from elt_pipeline.shared.storage_backends import _get_gcs_client

    return _get_gcs_client()


def _split_gcs_path(path: str) -> tuple[str, str]:
    from elt_pipeline.shared.storage_backends import _split_gcs_path as _route_split

    return _route_split(path)


_ADLS_CLIENT = None


def _adls_client():
    from elt_pipeline.shared.storage_backends import _get_adls_client

    return _get_adls_client()


def _split_adls_path(path: str) -> tuple[str, str, str]:
    from elt_pipeline.shared.storage_backends import _split_adls_path as _route_split

    return _route_split(path)
