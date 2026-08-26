from __future__ import annotations

import fnmatch
import os
import posixpath
import shutil
from contextlib import contextmanager
from pathlib import Path as _Path
from typing import IO, Any, Iterator, Literal, Protocol, runtime_checkable

from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import (
    _SUPPORTED_SCHEMES_FOR_ERROR,
    StorageScheme,
    _dedupe_preserve_order,
    collapse_slashes,
    collapse_slashes_without_scheme,
    detect_scheme,
    strip_file_scheme,
)
from elt_pipeline.shared.runtime import StageName

SwapMode = Literal["full_refresh", "partition_overwrite"]

_S3_CLIENT: object | None = None
_GCS_CLIENT: object | None = None
_ADLS_CLIENT: object | None = None


def _get_s3_client() -> Any:
    global _S3_CLIENT
    if _S3_CLIENT is not None:
        return _S3_CLIENT
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
    _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


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
    global _GCS_CLIENT
    if _GCS_CLIENT is not None:
        return _GCS_CLIENT
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
    _GCS_CLIENT = storage.Client()
    return _GCS_CLIENT


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
    global _ADLS_CLIENT
    if _ADLS_CLIENT is not None:
        return _ADLS_CLIENT
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
    _ADLS_CLIENT = DataLakeServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=placeholder;AccountKey=placeholder"
    )
    return _ADLS_CLIENT


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


@runtime_checkable
class StorageBackend(Protocol):
    def join_paths(self, root: str, *segments: str) -> str: ...
    def path_parent(self, path: str) -> str: ...
    def path_basename(self, path: str) -> str: ...
    def path_with_suffix(self, path: str, suffix: str) -> str: ...
    def path_normalize(self, path: str) -> str: ...

    def path_exists(self, path: str) -> bool: ...
    def path_is_dir(self, path: str) -> bool: ...
    def path_mkdir(
        self, path: str, *, parents: bool = True, exist_ok: bool = True
    ) -> None: ...
    def path_listdir(self, path: str) -> list[str]: ...
    def path_glob(self, base: str, pattern: str) -> list[str]: ...
    def path_rglob(self, base: str, pattern: str) -> list[str]: ...
    def path_content_length(self, path: str) -> int: ...
    def path_read_bytes(self, path: str) -> bytes: ...
    def path_write_bytes(
        self, path: str, data: bytes, *, atomic: bool = True
    ) -> None: ...
    def path_open_for_append(
        self, path: str, encoding: str = "utf-8"
    ) -> Iterator[IO[str]]: ...
    def path_replace(self, src: str, dst: str) -> None: ...
    def path_delete_tree(self, path: str) -> None: ...

    def staging_swap_atomic(
        self,
        *,
        staging_path: str,
        target_path: str,
        mode: SwapMode,
    ) -> None: ...


class LocalBackend:
    def join_paths(self, root: str, *segments: str) -> str:
        cleaned_segments: list[str] = []
        for raw in segments:
            if not isinstance(raw, str):
                raise ConfigValidationError(
                    message=(
                        f"join_paths segments must be strings; got "
                        f"{type(raw).__name__!r}: {raw!r}"
                    ),
                    context={
                        "root": root,
                        "segment_type": type(raw).__name__,
                        "segment_value": repr(raw),
                    },
                )
            s = raw.strip()
            if s == "":
                continue
            cleaned = s.strip("/")
            if cleaned == "":
                continue
            cleaned_segments.append(
                collapse_slashes_without_scheme(cleaned)
            )
        if not cleaned_segments:
            return collapse_slashes(root)
        scheme = detect_scheme(root)
        if scheme is StorageScheme.file:
            stripped = strip_file_scheme(root)
            joined = posixpath.join(stripped, *cleaned_segments)
            return collapse_slashes(f"file://{joined}")
        joined = posixpath.join(root, *cleaned_segments)
        return collapse_slashes_without_scheme(joined)

    def path_parent(self, path: str) -> str:
        scheme = detect_scheme(path)
        if scheme is StorageScheme.file:
            stripped = strip_file_scheme(path)
            return "file://" + posixpath.dirname(stripped)
        return posixpath.dirname(path)

    def path_basename(self, path: str) -> str:
        scheme = detect_scheme(path)
        if scheme is StorageScheme.file:
            return posixpath.basename(strip_file_scheme(path).rstrip("/"))
        return posixpath.basename(path.rstrip("/"))

    def path_with_suffix(self, path: str, suffix: str) -> str:
        if not suffix.startswith("."):
            suffix = "." + suffix
        scheme = detect_scheme(path)
        if scheme is StorageScheme.file:
            return path + suffix
        return str(_Path(path).with_suffix(suffix))

    def path_normalize(self, path: str) -> str:
        scheme = detect_scheme(path)
        stripped = strip_file_scheme(path)
        resolved = os.path.realpath(stripped)
        if scheme is StorageScheme.file:
            return f"file://{resolved}"
        return resolved

    def path_exists(self, path: str) -> bool:
        stripped = strip_file_scheme(path)
        return os.path.exists(stripped)

    def path_is_dir(self, path: str) -> bool:
        stripped = strip_file_scheme(path)
        return os.path.isdir(stripped)

    def path_mkdir(
        self, path: str, *, parents: bool = True, exist_ok: bool = True
    ) -> None:
        stripped = strip_file_scheme(path)
        try:
            if parents:
                os.makedirs(stripped, exist_ok=exist_ok)
            else:
                os.mkdir(stripped)
        except OSError as exc:
            if exist_ok and isinstance(exc, FileExistsError):
                return
            err_ctx = {
                "operation": "mkdir",
                "path": path,
                "error": str(exc),
                "parents": parents,
                "exist_ok": exist_ok,
            }
            raise PipelineError(
                message=f"Failed mkdir on path {path!r}: {exc}",
                error_code="STORAGE_MKDIR_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context=err_ctx,
            ) from exc

    def path_listdir(self, path: str) -> list[str]:
        stripped = strip_file_scheme(path)
        try:
            names = os.listdir(stripped)
        except OSError as exc:
            raise PipelineError(
                message=f"Failed listdir on path {path!r}: {exc}",
                error_code="STORAGE_LIST_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={"operation": "listdir", "path": path, "error": str(exc)},
            ) from exc
        prefix = path if path.endswith("/") else path + "/"
        return [prefix + name for name in names]

    def path_glob(self, base: str, pattern: str) -> list[str]:
        stripped_base = strip_file_scheme(base)
        results_p = list(_Path(stripped_base).glob(pattern))
        scheme = detect_scheme(base)
        if scheme is StorageScheme.file:
            return [f"file://{p}" for p in results_p]
        return [str(p) for p in results_p]

    def path_rglob(self, base: str, pattern: str) -> list[str]:
        stripped_base = strip_file_scheme(base)
        results_p = list(_Path(stripped_base).rglob(pattern))
        scheme = detect_scheme(base)
        if scheme is StorageScheme.file:
            return [f"file://{p}" for p in results_p]
        return [str(p) for p in results_p]

    def path_content_length(self, path: str) -> int:
        stripped = strip_file_scheme(path)
        try:
            return os.stat(stripped).st_size
        except OSError as exc:
            raise PipelineError(
                message=f"Failed content_length on path {path!r}: {exc}",
                error_code="STORAGE_READ_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={"operation": "content_length", "path": path, "error": str(exc)},
            ) from exc

    def path_read_bytes(self, path: str) -> bytes:
        stripped = strip_file_scheme(path)
        try:
            with open(stripped, "rb") as f:
                return f.read()
        except OSError as exc:
            raise PipelineError(
                message=f"Failed read_bytes on path {path!r}: {exc}",
                error_code="STORAGE_READ_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={"operation": "read_bytes", "path": path, "error": str(exc)},
            ) from exc

    def path_write_bytes(
        self, path: str, data: bytes, *, atomic: bool = True
    ) -> None:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        stripped = strip_file_scheme(path)
        try:
            if atomic:
                dirname = os.path.dirname(stripped)
                basename = os.path.basename(stripped)
                tmp_path = os.path.join(dirname, basename + ".tmp")
                with open(tmp_path, "wb") as f:
                    f.write(data)
                os.replace(tmp_path, stripped)
                return
            with open(stripped, "wb") as f:
                f.write(data)
        except OSError as exc:
            raise PipelineError(
                message=f"Failed write_bytes on path {path!r}: {exc}",
                error_code="STORAGE_WRITE_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={
                    "operation": "write_bytes",
                    "path": path,
                    "error": str(exc),
                    "atomic": atomic,
                },
            ) from exc

    @contextmanager
    def path_open_for_append(
        self, path: str, encoding: str = "utf-8"
    ) -> Iterator[IO[str]]:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        stripped = strip_file_scheme(path)
        try:
            f = open(stripped, "a", encoding=encoding)
        except OSError as exc:
            raise PipelineError(
                message=f"Failed open_for_append on path {path!r}: {exc}",
                error_code="STORAGE_WRITE_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={
                    "operation": "open_for_append",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc
        try:
            yield f
        finally:
            f.close()

    def path_replace(self, src: str, dst: str) -> None:
        self.path_mkdir(self.path_parent(dst), parents=True, exist_ok=True)
        src_stripped = strip_file_scheme(src)
        dst_stripped = strip_file_scheme(dst)
        try:
            os.replace(src_stripped, dst_stripped)
        except OSError as exc:
            raise PipelineError(
                message=f"Failed path_replace src={src!r} dst={dst!r}: {exc}",
                error_code="STORAGE_WRITE_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={"operation": "replace", "src": src, "dst": dst, "error": str(exc)},
            ) from exc

    def path_delete_tree(self, path: str) -> None:
        stripped = strip_file_scheme(path)
        try:
            if os.path.isdir(stripped):
                shutil.rmtree(stripped)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise PipelineError(
                message=f"Failed delete_tree on path {path!r}: {exc}",
                error_code="STORAGE_WRITE_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=False,
                context={"operation": "delete_tree", "path": path, "error": str(exc)},
            ) from exc

    def staging_swap_atomic(
        self,
        *,
        staging_path: str,
        target_path: str,
        mode: SwapMode,
    ) -> None:
        staging_local = strip_file_scheme(staging_path)
        target_local = strip_file_scheme(target_path)
        if not os.path.isdir(staging_local):
            raise PipelineError(
                message=(
                    f"Staging path does not exist or is not a directory: {staging_path}"
                ),
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            )
        try:
            if mode == "full_refresh":
                if os.path.isdir(target_local):
                    shutil.rmtree(target_local)
                elif os.path.exists(target_local):
                    os.remove(target_local)
                os.makedirs(os.path.dirname(target_local) or ".", exist_ok=True)
                os.rename(staging_local, target_local)
                return
            self._swap_partition_tree_posix(staging_local, target_local)
        except OSError as exc:
            msg = (
                f"POSIX atomic swap failed: staging={staging_path!r} "
                f"target={target_path!r}: {exc}"
            )
            raise PipelineError(
                message=msg,
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "mode": mode,
                    "error": str(exc),
                },
            ) from exc

    def _swap_partition_tree_posix(
        self, staging_dir: str, target_dir: str
    ) -> None:
        partition_subdirs = [
            name
            for name in self._list_immediate_subdirs_posix(staging_dir)
            if "=" in name
        ]
        if not partition_subdirs:
            if os.path.isdir(target_dir):
                shutil.rmtree(target_dir)
            elif os.path.exists(target_dir):
                os.remove(target_dir)
            os.makedirs(os.path.dirname(target_dir) or ".", exist_ok=True)
            os.rename(staging_dir, target_dir)
            return
        os.makedirs(target_dir, exist_ok=True)
        for name in partition_subdirs:
            self._swap_partition_tree_posix(
                os.path.join(staging_dir, name),
                os.path.join(target_dir, name),
            )
        if not os.listdir(staging_dir):
            os.rmdir(staging_dir)

    def _list_immediate_subdirs_posix(
        self, local_path: str
    ) -> list[str]:
        try:
            names = os.listdir(local_path)
        except FileNotFoundError:
            return []
        result = []
        for name in names:
            full = os.path.join(local_path, name)
            if os.path.isdir(full):
                result.append(name)
        return sorted(result)


class S3Backend:
    def _get_client(self) -> Any:
        from elt_pipeline.shared.path_utils import _s3_client

        return _s3_client()

    def join_paths(self, root: str, *segments: str) -> str:
        cleaned_segments: list[str] = []
        for raw in segments:
            if not isinstance(raw, str):
                raise ConfigValidationError(
                    message=(
                        f"join_paths segments must be strings; got "
                        f"{type(raw).__name__!r}: {raw!r}"
                    ),
                    context={
                        "root": root,
                        "segment_type": type(raw).__name__,
                        "segment_value": repr(raw),
                    },
                )
            s = raw.strip()
            if s == "":
                continue
            cleaned = s.strip("/")
            if cleaned == "":
                continue
            cleaned_segments.append(
                collapse_slashes_without_scheme(cleaned)
            )
        if not cleaned_segments:
            return collapse_slashes(root)
        prefix_part = root[len("s3://"):]
        collapsed_prefix = collapse_slashes_without_scheme(prefix_part)
        if collapsed_prefix.endswith("/"):
            collapsed_prefix = collapsed_prefix[:-1]
        if collapsed_prefix.startswith("/"):
            collapsed_prefix = collapsed_prefix[1:]
        joined_suffix = "/".join(cleaned_segments)
        full_suffix = collapse_slashes_without_scheme(
            f"{collapsed_prefix}/{joined_suffix}"
        )
        return f"s3://{full_suffix}"

    def path_parent(self, path: str) -> str:
        suffix = path[len("s3://"):]
        parent_suffix = posixpath.dirname(suffix)
        return (
            f"s3://{parent_suffix}"
            if parent_suffix
            else f"s3://{''.join(suffix.split('/')[:1])}"
        )

    def path_basename(self, path: str) -> str:
        suffix = path[len("s3://"):].rstrip("/")
        return posixpath.basename(suffix)

    def path_with_suffix(self, path: str, suffix: str) -> str:
        if not suffix.startswith("."):
            suffix = "." + suffix
        return path + suffix

    def path_normalize(self, path: str) -> str:
        return collapse_slashes(path)

    def path_exists(self, path: str) -> bool:
        bucket, key = _split_s3_path(path)
        s3 = self._get_client()
        try:
            if key == "" or key.endswith("/"):
                resp = s3.list_objects_v2(
                    Bucket=bucket, Prefix=key, MaxKeys=1
                )
                return "Contents" in resp and len(resp["Contents"]) > 0
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "NoSuchBucket"):
                return False
            raise PipelineError(
                message=f"Failed path_exists on S3 for {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "exists", "path": path, "error": str(exc)},
            ) from exc

    def path_is_dir(self, path: str) -> bool:
        bucket, key = _split_s3_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        s3 = self._get_client()
        try:
            resp = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, MaxKeys=1, Delimiter="/"
            )
            return bool(resp.get("Contents") or resp.get("CommonPrefixes"))
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed path_is_dir on S3 for {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "is_dir", "path": path, "error": str(exc)},
            ) from exc

    def path_mkdir(
        self, path: str, *, parents: bool = True, exist_ok: bool = True
    ) -> None:
        _split_s3_path(path)
        return

    def path_listdir(self, path: str) -> list[str]:
        bucket, key = _split_s3_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        s3 = self._get_client()
        try:
            results: list[str] = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=bucket, Prefix=prefix, Delimiter="/"
            ):
                for cp in page.get("CommonPrefixes", []):
                    full_prefix = cp["Prefix"]
                    if not full_prefix.startswith(prefix):
                        continue
                    suffix = full_prefix[len(prefix):].rstrip("/")
                    if suffix:
                        results.append(f"s3://{bucket}/{full_prefix}")
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    if not k.startswith(prefix):
                        continue
                    suffix = k[len(prefix):]
                    if suffix == "":
                        continue
                    results.append(f"s3://{bucket}/{k}")
            return _dedupe_preserve_order(results)
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed listdir on S3 path {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "listdir", "path": path, "error": str(exc)},
            ) from exc

    def path_glob(self, base: str, pattern: str) -> list[str]:
        bucket, base_key = _split_s3_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        s3 = self._get_client()
        matches: list[str] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    suffix = (
                        k[len(prefix):] if k.startswith(prefix) else None
                    )
                    if (
                        suffix is not None
                        and "/" not in suffix
                        and fnmatch.fnmatch(suffix, pattern)
                    ):
                        matches.append(f"s3://{bucket}/{k}")
            return matches
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=(
                    f"Failed glob on S3 base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={
                    "operation": "glob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_rglob(self, base: str, pattern: str) -> list[str]:
        bucket, base_key = _split_s3_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        s3 = self._get_client()
        matches: list[str] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    suffix = (
                        k[len(prefix):] if k.startswith(prefix) else None
                    )
                    if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                        matches.append(f"s3://{bucket}/{k}")
                for cp in page.get("CommonPrefixes", []):
                    p = cp["Prefix"]
                    suffix = (
                        p[len(prefix):].rstrip("/")
                        if p.startswith(prefix)
                        else None
                    )
                    if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                        matches.append(f"s3://{bucket}/{p}")
            return _dedupe_preserve_order(matches)
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=(
                    f"Failed rglob on S3 base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={
                    "operation": "rglob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_content_length(self, path: str) -> int:
        bucket, key = _split_s3_path(path)
        s3 = self._get_client()
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            return int(resp.get("ContentLength", 0))
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "NoSuchBucket"):
                raise PipelineError(
                    message=(
                        f"Failed content_length on S3 path {path!r}: not found"
                    ),
                    error_code="STORAGE_S3_OP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "operation": "content_length",
                        "path": path,
                        "error": str(exc),
                    },
                ) from exc
            raise PipelineError(
                message=f"Failed content_length on S3 path {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={
                    "operation": "content_length",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc

    def path_read_bytes(self, path: str) -> bytes:
        bucket, key = _split_s3_path(path)
        s3 = self._get_client()
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read()
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed read_bytes on S3 path {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "read_bytes", "path": path, "error": str(exc)},
            ) from exc

    def path_write_bytes(
        self, path: str, data: bytes, *, atomic: bool = True
    ) -> None:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        bucket, key = _split_s3_path(path)
        s3 = self._get_client()
        if not atomic:
            try:
                s3.put_object(Bucket=bucket, Key=key, Body=data)
                return
            except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
                write_ctx = {
                    "operation": "write_bytes",
                    "path": path,
                    "error": str(exc),
                    "atomic": atomic,
                }
                raise PipelineError(
                    message=f"Failed write_bytes on S3 path {path!r}: {exc}",
                    error_code="STORAGE_S3_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=True,
                    context=write_ctx,
                ) from exc
        tmp_key = key + ".tmp"
        try:
            s3.put_object(Bucket=bucket, Key=tmp_key, Body=data)
            s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource={"Bucket": bucket, "Key": tmp_key},
            )
            s3.delete_object(Bucket=bucket, Key=tmp_key)
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=(
                    f"Failed atomic write_bytes on S3 path {path!r}: {exc}"
                ),
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=True,
                context={
                    "operation": "write_bytes.atomic",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc
        return

    @contextmanager
    def path_open_for_append(
        self, path: str, encoding: str = "utf-8"
    ) -> Iterator[IO[str]]:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        existing_parts: list[str] = []
        if self.path_exists(path):
            try:
                existing_parts.append(
                    self.path_read_bytes(path).decode(encoding)
                )
            except Exception as exc:  # noqa: BLE001
                append_read_ctx = {
                    "operation": "open_for_append.read_existing",
                    "path": path,
                    "error": str(exc),
                }
                raise PipelineError(
                    message=(
                        f"Failed to read prior contents during S3 append "
                        f"for path {path!r}: {exc}"
                    ),
                    error_code="STORAGE_S3_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=True,
                    context=append_read_ctx,
                ) from exc

        _outer_write_bytes = self.path_write_bytes

        class _S3AppendWriter:
            def __init__(
                self_w, target_path: str, initial: list[str], enc: str
            ) -> None:
                self_w._target = target_path
                self_w._initial = initial
                self_w._enc = enc
                self_w.closed = False
                self_w._buffer: list[str] = []

            def write(self_w, s: str) -> int:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    raise PipelineError(
                        message=(
                            f"Cannot write to closed S3 append writer "
                            f"for {self_w._target!r}"
                        ),
                        error_code="STORAGE_WRITE_FAILED",
                        error_category=ErrorCategory.storage_write_error,
                        retryable=False,
                        context={"path": self_w._target},
                    )
                self_w._buffer.append(s)
                return len(s)

            def close(self_w) -> None:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    return
                self_w.closed = True
                combined = "".join(self_w._initial + self_w._buffer)
                _outer_write_bytes(
                    self_w._target,
                    combined.encode(self_w._enc),
                    atomic=True,
                )

            def __enter__(self_w):  # type: ignore[no-untyped-def]
                return self_w

            def __exit__(self_w, exc_type, exc, tb):  # type: ignore[no-untyped-def]
                self_w.close()
                return False

        writer = _S3AppendWriter(path, existing_parts, encoding)
        try:
            yield writer  # type: ignore[arg-type]
        finally:
            writer.close()
        return

    def path_replace(self, src: str, dst: str) -> None:
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
        self.path_mkdir(self.path_parent(dst), parents=True, exist_ok=True)
        b_src, k_src = _split_s3_path(src)
        b_dst, k_dst = _split_s3_path(dst)
        s3 = self._get_client()
        try:
            s3.copy_object(
                Bucket=b_dst, Key=k_dst, CopySource={"Bucket": b_src, "Key": k_src}
            )
            s3.delete_object(Bucket=b_src, Key=k_src)
            return
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed path_replace on S3 src={src!r} dst={dst!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=True,
                context={"operation": "replace", "src": src, "dst": dst, "error": str(exc)},
            ) from exc

    def path_delete_tree(self, path: str) -> None:
        bucket, prefix = _split_s3_path(path)
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        s3 = self._get_client()
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                contents = page.get("Contents", [])
                if not contents:
                    continue
                keys = [{"Key": obj["Key"]} for obj in contents]
                s3.delete_objects(
                    Bucket=bucket, Delete={"Objects": keys, "Quiet": True}
                )
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                return
            raise PipelineError(
                message=f"Failed delete_tree on S3 path {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "delete_tree", "path": path, "error": str(exc)},
            ) from exc
        return

    def staging_swap_atomic(
        self,
        *,
        staging_path: str,
        target_path: str,
        mode: SwapMode,
    ) -> None:
        staging_bucket, staging_prefix = _split_s3_path(staging_path)
        target_bucket, target_prefix = _split_s3_path(target_path)
        if staging_bucket != target_bucket:
            raise PipelineError(
                message=(
                    "S3 staging-swap requires staging and target to share the same bucket. "
                    f"Got staging_bucket={staging_bucket!r} "
                    f"target_bucket={target_bucket!r}"
                ),
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            )
        bucket = staging_bucket
        if staging_prefix and not staging_prefix.endswith("/"):
            staging_prefix = staging_prefix + "/"
        if target_prefix and not target_prefix.endswith("/"):
            target_prefix = target_prefix + "/"

        s3 = self._get_client()
        try:
            if mode == "full_refresh":
                old_target_keys = _s3_list_keys(s3, bucket, target_prefix)
                staging_keys = _s3_list_keys(s3, bucket, staging_prefix)
                if not staging_keys:
                    raise PipelineError(
                        message=f"S3 staging prefix is empty: {staging_path}",
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=False,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                        },
                    )
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(staging_prefix):]
                        if s_key.startswith(staging_prefix)
                        else s_key
                    )
                    t_key = target_prefix + rel
                    src = {"Bucket": bucket, "Key": s_key}
                    s3.copy_object(Bucket=bucket, Key=t_key, CopySource=src)
                    copied_target_keys.append(t_key)
                confirm = _s3_list_keys(s3, bucket, target_prefix)
                expected_set = set(copied_target_keys)
                if not expected_set.issubset(set(confirm)):
                    missing = sorted(expected_set - set(confirm))[:5]
                    raise PipelineError(
                        message=(
                            "S3 copy to target prefix incomplete after staging copy"
                        ),
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=True,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                            "missing_copied_keys_sample": missing,
                        },
                    )
                stale_target = [
                    k for k in old_target_keys if k not in expected_set
                ]
                if stale_target:
                    _s3_batch_delete(s3, bucket, stale_target)
                _s3_batch_delete(s3, bucket, staging_keys)
                return

            staging_keys = _s3_list_keys(s3, bucket, staging_prefix)
            if not staging_keys:
                raise PipelineError(
                    message=f"S3 staging prefix is empty: {staging_path}",
                    error_code="SQL_ATOMIC_SWAP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "staging_path": staging_path,
                        "target_path": target_path,
                    },
                )
            partition_subprefixes = _s3_infer_partition_subprefixes(
                staging_prefix=staging_prefix,
                staging_keys=staging_keys,
            )
            if not partition_subprefixes:
                old_target_keys = _s3_list_keys(s3, bucket, target_prefix)
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(staging_prefix):]
                        if s_key.startswith(staging_prefix)
                        else s_key
                    )
                    t_key = target_prefix + rel
                    src = {"Bucket": bucket, "Key": s_key}
                    s3.copy_object(Bucket=bucket, Key=t_key, CopySource=src)
                    copied_target_keys.append(t_key)
                stale_target = [
                    k for k in old_target_keys
                    if k not in set(copied_target_keys)
                ]
                if stale_target:
                    _s3_batch_delete(s3, bucket, stale_target)
                _s3_batch_delete(s3, bucket, staging_keys)
                return

            for part_sub in partition_subprefixes:
                staging_part_prefix = staging_prefix + part_sub
                target_part_prefix = target_prefix + part_sub
                staging_part_keys = [
                    k for k in staging_keys
                    if k.startswith(staging_part_prefix)
                ]
                old_target_part_keys = _s3_list_keys(
                    s3, bucket, target_part_prefix
                )
                for s_key in staging_part_keys:
                    rel = (
                        s_key[len(staging_part_prefix):]
                        if s_key.startswith(staging_part_prefix)
                        else s_key
                    )
                    t_key = target_part_prefix + rel
                    src = {"Bucket": bucket, "Key": s_key}
                    s3.copy_object(Bucket=bucket, Key=t_key, CopySource=src)
                if old_target_part_keys:
                    _s3_batch_delete(s3, bucket, old_target_part_keys)
            _s3_batch_delete(s3, bucket, staging_keys)
        except PipelineError:
            raise
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            msg = (
                f"S3 atomic swap failed: staging={staging_path!r} "
                f"target={target_path!r}: {exc}"
            )
            raise PipelineError(
                message=msg,
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "mode": mode,
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:
            msg = (
                f"S3 atomic swap failed unexpectedly: staging={staging_path!r} "
                f"target={target_path!r}: {exc}"
            )
            raise PipelineError(
                message=msg,
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "mode": mode,
                    "error": str(exc),
                },
            ) from exc


class GCSBackend:
    def _get_client(self) -> Any:
        from elt_pipeline.shared.path_utils import _gcs_client

        return _gcs_client()

    def join_paths(self, root: str, *segments: str) -> str:
        cleaned_segments: list[str] = []
        for raw in segments:
            if not isinstance(raw, str):
                raise ConfigValidationError(
                    message=(
                        f"join_paths segments must be strings; got "
                        f"{type(raw).__name__!r}: {raw!r}"
                    ),
                    context={
                        "root": root,
                        "segment_type": type(raw).__name__,
                        "segment_value": repr(raw),
                    },
                )
            s = raw.strip()
            if s == "":
                continue
            cleaned = s.strip("/")
            if cleaned == "":
                continue
            cleaned_segments.append(
                collapse_slashes_without_scheme(cleaned)
            )
        if not cleaned_segments:
            return collapse_slashes(root)
        prefix_part = root[len("gs://"):]
        collapsed_prefix = collapse_slashes_without_scheme(prefix_part)
        if collapsed_prefix.endswith("/"):
            collapsed_prefix = collapsed_prefix[:-1]
        if collapsed_prefix.startswith("/"):
            collapsed_prefix = collapsed_prefix[1:]
        joined_suffix = "/".join(cleaned_segments)
        full_suffix = collapse_slashes_without_scheme(
            f"{collapsed_prefix}/{joined_suffix}"
        )
        return f"gs://{full_suffix}"

    def path_parent(self, path: str) -> str:
        suffix = path[len("gs://"):]
        parent_suffix = posixpath.dirname(suffix)
        return (
            f"gs://{parent_suffix}"
            if parent_suffix
            else f"gs://{''.join(suffix.split('/')[:1])}"
        )

    def path_basename(self, path: str) -> str:
        suffix = path[len("gs://"):].rstrip("/")
        return posixpath.basename(suffix)

    def path_with_suffix(self, path: str, suffix: str) -> str:
        if not suffix.startswith("."):
            suffix = "." + suffix
        return path + suffix

    def path_normalize(self, path: str) -> str:
        return collapse_slashes(path)

    def path_exists(self, path: str) -> bool:
        bucket_name, key = _split_gcs_path(path)
        gcs = self._get_client()
        try:
            bucket = gcs.bucket(bucket_name)
            if key == "" or key.endswith("/"):
                prefix = key
                iterator = gcs.list_blobs(bucket_name, prefix=prefix, max_results=1)
                for _ in iterator:
                    return True
                return False
            blob = bucket.blob(key)
            return blob.exists()
        except self._get_gcs_exc().NotFound:  # type: ignore[attr-defined]
            return False
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed path_exists on GCS for {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "exists", "path": path, "error": str(exc)},
            ) from exc

    def path_is_dir(self, path: str) -> bool:
        bucket_name, key = _split_gcs_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        gcs = self._get_client()
        try:
            iterator = gcs.list_blobs(
                bucket_name, prefix=prefix, delimiter="/", max_results=1
            )
            found = False
            for _ in iterator:
                found = True
                break
            if iterator.prefixes:
                found = True
            return found
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed path_is_dir on GCS for {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "is_dir", "path": path, "error": str(exc)},
            ) from exc

    def path_mkdir(
        self, path: str, *, parents: bool = True, exist_ok: bool = True
    ) -> None:
        _split_gcs_path(path)
        return

    def path_listdir(self, path: str) -> list[str]:
        bucket_name, key = _split_gcs_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        gcs = self._get_client()
        try:
            results: list[str] = []
            iterator = gcs.list_blobs(bucket_name, prefix=prefix, delimiter="/")
            for prefix_entry in iterator.prefixes or []:
                if not prefix_entry.startswith(prefix):
                    continue
                suffix = prefix_entry[len(prefix):].rstrip("/")
                if suffix:
                    results.append(f"gs://{bucket_name}/{prefix_entry}")
            for blob in iterator:
                k = blob.name
                if not k.startswith(prefix):
                    continue
                suffix = k[len(prefix):]
                if suffix == "":
                    continue
                results.append(f"gs://{bucket_name}/{k}")
            return _dedupe_preserve_order(results)
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed listdir on GCS path {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "listdir", "path": path, "error": str(exc)},
            ) from exc

    def path_glob(self, base: str, pattern: str) -> list[str]:
        bucket_name, base_key = _split_gcs_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        gcs = self._get_client()
        matches: list[str] = []
        try:
            iterator = gcs.list_blobs(bucket_name, prefix=prefix, delimiter="/")
            for blob in iterator:
                k = blob.name
                suffix = (
                    k[len(prefix):] if k.startswith(prefix) else None
                )
                if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                    matches.append(f"gs://{bucket_name}/{k}")
            return matches
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed glob on GCS base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "glob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_rglob(self, base: str, pattern: str) -> list[str]:
        bucket_name, base_key = _split_gcs_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        gcs = self._get_client()
        matches: list[str] = []
        try:
            iterator = gcs.list_blobs(bucket_name, prefix=prefix)
            for blob in iterator:
                k = blob.name
                leaf = k.rsplit("/", 1)[-1] if "/" in k else k
                if fnmatch.fnmatch(leaf, pattern):
                    matches.append(f"gs://{bucket_name}/{k}")
            return _dedupe_preserve_order(matches)
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed rglob on GCS base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "rglob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_content_length(self, path: str) -> int:
        bucket_name, key = _split_gcs_path(path)
        gcs = self._get_client()
        try:
            bucket = gcs.bucket(bucket_name)
            blob = bucket.get_blob(key)
            if blob is None:
                raise PipelineError(
                    message=(
                        f"Failed content_length on GCS path {path!r}: not found"
                    ),
                    error_code="STORAGE_GCS_OP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "operation": "content_length",
                        "path": path,
                    },
                )
            return int(blob.size or 0)
        except PipelineError:
            raise
        except self._get_gcs_exc().NotFound as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=(
                    f"Failed content_length on GCS path {path!r}: not found"
                ),
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=False,
                context={
                    "operation": "content_length",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed content_length on GCS path {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "content_length",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc

    def path_read_bytes(self, path: str) -> bytes:
        bucket_name, key = _split_gcs_path(path)
        gcs = self._get_client()
        try:
            bucket = gcs.bucket(bucket_name)
            blob = bucket.blob(key)
            return blob.download_as_bytes()
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed read_bytes on GCS path {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "read_bytes", "path": path, "error": str(exc)},
            ) from exc

    def path_write_bytes(
        self, path: str, data: bytes, *, atomic: bool = True
    ) -> None:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        bucket_name, key = _split_gcs_path(path)
        gcs = self._get_client()
        bucket = gcs.bucket(bucket_name)
        if not atomic:
            try:
                blob = bucket.blob(key)
                blob.upload_from_string(data)
                return
            except Exception as exc:  # noqa: BLE001
                retryable = self._is_gcs_retryable(exc)
                write_ctx = {
                    "operation": "write_bytes",
                    "path": path,
                    "error": str(exc),
                    "atomic": atomic,
                }
                raise PipelineError(
                    message=f"Failed write_bytes on GCS path {path!r}: {exc}",
                    error_code="STORAGE_GCS_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=retryable,
                    context=write_ctx,
                ) from exc
        tmp_key = key + ".tmp"
        try:
            tmp_blob = bucket.blob(tmp_key)
            tmp_blob.upload_from_string(data)
            src_blob = bucket.blob(tmp_key)
            bucket.copy_blob(src_blob, bucket, new_name=key)
            tmp_blob.delete()
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed atomic write_bytes on GCS path {path!r}: {exc}"
                ),
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=retryable,
                context={
                    "operation": "write_bytes.atomic",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc
        return

    @contextmanager
    def path_open_for_append(
        self, path: str, encoding: str = "utf-8"
    ) -> Iterator[IO[str]]:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        existing_parts: list[str] = []
        if self.path_exists(path):
            try:
                existing_parts.append(
                    self.path_read_bytes(path).decode(encoding)
                )
            except Exception as exc:  # noqa: BLE001
                append_read_ctx = {
                    "operation": "open_for_append.read_existing",
                    "path": path,
                    "error": str(exc),
                }
                raise PipelineError(
                    message=(
                        f"Failed to read prior contents during GCS append "
                        f"for path {path!r}: {exc}"
                    ),
                    error_code="STORAGE_GCS_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=self._is_gcs_retryable(exc),
                    context=append_read_ctx,
                ) from exc

        _outer_write_bytes = self.path_write_bytes

        class _GCSAppendWriter:
            def __init__(
                self_w, target_path: str, initial: list[str], enc: str
            ) -> None:
                self_w._target = target_path
                self_w._initial = initial
                self_w._enc = enc
                self_w.closed = False
                self_w._buffer: list[str] = []

            def write(self_w, s: str) -> int:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    raise PipelineError(
                        message=(
                            f"Cannot write to closed GCS append writer "
                            f"for {self_w._target!r}"
                        ),
                        error_code="STORAGE_WRITE_FAILED",
                        error_category=ErrorCategory.storage_write_error,
                        retryable=False,
                        context={"path": self_w._target},
                    )
                self_w._buffer.append(s)
                return len(s)

            def close(self_w) -> None:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    return
                self_w.closed = True
                combined = "".join(self_w._initial + self_w._buffer)
                _outer_write_bytes(
                    self_w._target,
                    combined.encode(self_w._enc),
                    atomic=True,
                )

            def __enter__(self_w):  # type: ignore[no-untyped-def]
                return self_w

            def __exit__(self_w, exc_type, exc, tb):  # type: ignore[no-untyped-def]
                self_w.close()
                return False

        writer = _GCSAppendWriter(path, existing_parts, encoding)
        try:
            yield writer  # type: ignore[arg-type]
        finally:
            writer.close()
        return

    def path_replace(self, src: str, dst: str) -> None:
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
        self.path_mkdir(self.path_parent(dst), parents=True, exist_ok=True)
        b_src, k_src = _split_gcs_path(src)
        b_dst, k_dst = _split_gcs_path(dst)
        gcs = self._get_client()
        try:
            bucket_src = gcs.bucket(b_src)
            bucket_dst = gcs.bucket(b_dst)
            src_blob = bucket_src.blob(k_src)
            bucket_src.copy_blob(src_blob, bucket_dst, new_name=k_dst)
            src_blob.delete()
            return
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed path_replace on GCS src={src!r} dst={dst!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=retryable,
                context={"operation": "replace", "src": src, "dst": dst, "error": str(exc)},
            ) from exc

    def path_delete_tree(self, path: str) -> None:
        bucket_name, prefix = _split_gcs_path(path)
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        gcs = self._get_client()
        try:
            blobs: list[Any] = []
            iterator = gcs.list_blobs(bucket_name, prefix=prefix)
            for blob in iterator:
                blobs.append(blob)
            if blobs:
                bucket = gcs.bucket(bucket_name)
                BATCH = 1000
                for i in range(0, len(blobs), BATCH):
                    chunk = blobs[i:i + BATCH]
                    bucket.delete_blobs(chunk)
        except self._get_gcs_exc().NotFound:  # type: ignore[attr-defined]
            return
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            raise PipelineError(
                message=f"Failed delete_tree on GCS path {path!r}: {exc}",
                error_code="STORAGE_GCS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "delete_tree", "path": path, "error": str(exc)},
            ) from exc
        return

    def staging_swap_atomic(
        self,
        *,
        staging_path: str,
        target_path: str,
        mode: SwapMode,
    ) -> None:
        staging_bucket, staging_prefix = _split_gcs_path(staging_path)
        target_bucket, target_prefix = _split_gcs_path(target_path)
        if staging_bucket != target_bucket:
            raise PipelineError(
                message=(
                    "GCS staging-swap requires staging and target to share the same bucket. "
                    f"Got staging_bucket={staging_bucket!r} "
                    f"target_bucket={target_bucket!r}"
                ),
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            )
        bucket_name = staging_bucket
        if staging_prefix and not staging_prefix.endswith("/"):
            staging_prefix = staging_prefix + "/"
        if target_prefix and not target_prefix.endswith("/"):
            target_prefix = target_prefix + "/"

        gcs = self._get_client()
        bucket = gcs.bucket(bucket_name)
        try:
            if mode == "full_refresh":
                old_target_keys = _gcs_list_blobs(gcs, bucket_name, target_prefix)
                staging_keys = _gcs_list_blobs(gcs, bucket_name, staging_prefix)
                if not staging_keys:
                    raise PipelineError(
                        message=f"GCS staging prefix is empty: {staging_path}",
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=False,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                        },
                    )
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(staging_prefix):]
                        if s_key.startswith(staging_prefix)
                        else s_key
                    )
                    t_key = target_prefix + rel
                    src_blob = bucket.blob(s_key)
                    bucket.copy_blob(src_blob, bucket, new_name=t_key)
                    copied_target_keys.append(t_key)
                confirm = _gcs_list_blobs(gcs, bucket_name, target_prefix)
                expected_set = set(copied_target_keys)
                if not expected_set.issubset(set(confirm)):
                    missing = sorted(expected_set - set(confirm))[:5]
                    raise PipelineError(
                        message=(
                            "GCS copy to target prefix incomplete after staging copy"
                        ),
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=True,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                            "missing_copied_keys_sample": missing,
                        },
                    )
                stale_target = [
                    k for k in old_target_keys if k not in expected_set
                ]
                if stale_target:
                    _gcs_batch_delete(gcs, bucket_name, stale_target)
                _gcs_batch_delete(gcs, bucket_name, staging_keys)
                return

            staging_keys = _gcs_list_blobs(gcs, bucket_name, staging_prefix)
            if not staging_keys:
                raise PipelineError(
                    message=f"GCS staging prefix is empty: {staging_path}",
                    error_code="SQL_ATOMIC_SWAP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "staging_path": staging_path,
                        "target_path": target_path,
                    },
                )
            partition_subprefixes = _s3_infer_partition_subprefixes(
                staging_prefix=staging_prefix,
                staging_keys=staging_keys,
            )
            if not partition_subprefixes:
                old_target_keys = _gcs_list_blobs(gcs, bucket_name, target_prefix)
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(staging_prefix):]
                        if s_key.startswith(staging_prefix)
                        else s_key
                    )
                    t_key = target_prefix + rel
                    src_blob = bucket.blob(s_key)
                    bucket.copy_blob(src_blob, bucket, new_name=t_key)
                    copied_target_keys.append(t_key)
                stale_target = [
                    k for k in old_target_keys
                    if k not in set(copied_target_keys)
                ]
                if stale_target:
                    _gcs_batch_delete(gcs, bucket_name, stale_target)
                _gcs_batch_delete(gcs, bucket_name, staging_keys)
                return

            for part_sub in partition_subprefixes:
                staging_part_prefix = staging_prefix + part_sub
                target_part_prefix = target_prefix + part_sub
                staging_part_keys = [
                    k for k in staging_keys
                    if k.startswith(staging_part_prefix)
                ]
                old_target_part_keys = _gcs_list_blobs(
                    gcs, bucket_name, target_part_prefix
                )
                for s_key in staging_part_keys:
                    rel = (
                        s_key[len(staging_part_prefix):]
                        if s_key.startswith(staging_part_prefix)
                        else s_key
                    )
                    t_key = target_part_prefix + rel
                    src_blob = bucket.blob(s_key)
                    bucket.copy_blob(src_blob, bucket, new_name=t_key)
                if old_target_part_keys:
                    _gcs_batch_delete(gcs, bucket_name, old_target_part_keys)
            _gcs_batch_delete(gcs, bucket_name, staging_keys)
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_gcs_retryable(exc)
            msg = (
                f"GCS atomic swap failed: staging={staging_path!r} "
                f"target={target_path!r}: {exc}"
            )
            raise PipelineError(
                message=msg,
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "mode": mode,
                    "error": str(exc),
                },
            ) from exc

    @staticmethod
    def _get_gcs_exc() -> Any:
        try:
            from google.api_core import exceptions as gexc  # type: ignore[import-not-found]
        except ImportError:
            class _FallbackExc:
                class NotFound(Exception):
                    pass
                class Forbidden(Exception):
                    pass
                class ServiceUnavailable(Exception):
                    pass
            return _FallbackExc
        return gexc

    @staticmethod
    def _is_gcs_retryable(exc: Exception) -> bool:
        gexc = GCSBackend._get_gcs_exc()
        if isinstance(exc, (gexc.ServiceUnavailable,)):
            return True
        msg = str(exc).lower()
        retry_kws = ("timeout", "retry", "temporary", "503", "rate limit", "throttle")
        if any(kw in msg for kw in retry_kws):
            return True
        return False


class ADLSBackend:
    def _get_client(self) -> Any:
        from elt_pipeline.shared.path_utils import _adls_client

        return _adls_client()

    def _get_fs_client(self, container: str, account: str) -> Any:
        _ = account
        svc = self._get_client()
        return svc.get_file_system_client(container)

    def join_paths(self, root: str, *segments: str) -> str:
        cleaned_segments: list[str] = []
        for raw in segments:
            if not isinstance(raw, str):
                raise ConfigValidationError(
                    message=(
                        f"join_paths segments must be strings; got "
                        f"{type(raw).__name__!r}: {raw!r}"
                    ),
                    context={
                        "root": root,
                        "segment_type": type(raw).__name__,
                        "segment_value": repr(raw),
                    },
                )
            s = raw.strip()
            if s == "":
                continue
            cleaned = s.strip("/")
            if cleaned == "":
                continue
            cleaned_segments.append(
                collapse_slashes_without_scheme(cleaned)
            )
        if not cleaned_segments:
            return collapse_slashes(root)
        prefix_part = root[len("abfss://"):]
        collapsed_prefix = collapse_slashes_without_scheme(prefix_part)
        if collapsed_prefix.endswith("/"):
            collapsed_prefix = collapsed_prefix[:-1]
        if collapsed_prefix.startswith("/"):
            collapsed_prefix = collapsed_prefix[1:]
        joined_suffix = "/".join(cleaned_segments)
        full_suffix = collapse_slashes_without_scheme(
            f"{collapsed_prefix}/{joined_suffix}"
        )
        return f"abfss://{full_suffix}"

    def path_parent(self, path: str) -> str:
        suffix = path[len("abfss://"):]
        parent_suffix = posixpath.dirname(suffix)
        return (
            f"abfss://{parent_suffix}"
            if parent_suffix
            else f"abfss://{''.join(suffix.split('/')[:1])}"
        )

    def path_basename(self, path: str) -> str:
        suffix = path[len("abfss://"):].rstrip("/")
        return posixpath.basename(suffix)

    def path_with_suffix(self, path: str, suffix: str) -> str:
        if not suffix.startswith("."):
            suffix = "." + suffix
        return path + suffix

    def path_normalize(self, path: str) -> str:
        return collapse_slashes(path)

    def path_exists(self, path: str) -> bool:
        container, account, key = _split_adls_path(path)
        adls = self._get_client()
        try:
            if key == "" or key.endswith("/"):
                prefix = key
                paths = _adls_list_paths(
                    adls, container, account, prefix,
                    recursive=False, max_results=1,
                )
                return len(paths) > 0
            fs = self._get_fs_client(container, account)
            fc = fs.get_file_client(key)
            try:
                fc.get_file_properties()
                return True
            except Exception as exc:  # noqa: BLE001
                if not self._is_not_found_exc(exc):
                    raise
                dc = fs.get_directory_client(key)
                try:
                    dc.get_directory_properties()
                    return True
                except Exception as exc2:  # noqa: BLE001
                    if not self._is_not_found_exc(exc2):
                        raise
                    return False
        except Exception as exc:  # noqa: BLE001
            if self._is_not_found_exc(exc):
                return False
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed path_exists on ADLS for {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "exists", "path": path, "error": str(exc)},
            ) from exc

    def path_is_dir(self, path: str) -> bool:
        container, account, key = _split_adls_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        adls = self._get_client()
        try:
            file_paths = _adls_list_paths(
                adls, container, account, prefix,
                recursive=False, max_results=1,
            )
            return len(file_paths) > 0
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed path_is_dir on ADLS for {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "is_dir", "path": path, "error": str(exc)},
            ) from exc

    def path_mkdir(
        self, path: str, *, parents: bool = True, exist_ok: bool = True
    ) -> None:
        _split_adls_path(path)
        return

    def path_listdir(self, path: str) -> list[str]:
        container, account, key = _split_adls_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        adls = self._get_client()
        try:
            results: list[str] = []
            all_paths = _adls_list_paths(adls, container, account, prefix, recursive=False)
            for p in all_paths:
                k = p["name"]
                if not k.startswith(prefix):
                    continue
                suffix = k[len(prefix):]
                if suffix == "":
                    continue
                if p.get("is_directory"):
                    results.append(f"abfss://{container}@{account}.dfs.core.windows.net/{k}/")
                else:
                    results.append(f"abfss://{container}@{account}.dfs.core.windows.net/{k}")
            return _dedupe_preserve_order(results)
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed listdir on ADLS path {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "listdir", "path": path, "error": str(exc)},
            ) from exc

    def path_glob(self, base: str, pattern: str) -> list[str]:
        container, account, base_key = _split_adls_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        adls = self._get_client()
        matches: list[str] = []
        try:
            all_paths = _adls_list_paths(adls, container, account, prefix, recursive=False)
            for p in all_paths:
                k = p["name"]
                suffix = (
                    k[len(prefix):] if k.startswith(prefix) else None
                )
                if (
                    suffix is not None
                    and not p.get("is_directory")
                    and fnmatch.fnmatch(suffix, pattern)
                ):
                    matches.append(f"abfss://{container}@{account}.dfs.core.windows.net/{k}")
            return matches
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed glob on ADLS base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "glob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_rglob(self, base: str, pattern: str) -> list[str]:
        container, account, base_key = _split_adls_path(base)
        prefix = (
            base_key
            if base_key.endswith("/") or base_key == ""
            else base_key + "/"
        )
        adls = self._get_client()
        matches: list[str] = []
        try:
            all_paths = _adls_list_paths(adls, container, account, prefix, recursive=True)
            for p in all_paths:
                k = p["name"]
                leaf = k.rsplit("/", 1)[-1] if "/" in k else k
                if not p.get("is_directory") and fnmatch.fnmatch(leaf, pattern):
                    matches.append(f"abfss://{container}@{account}.dfs.core.windows.net/{k}")
            return _dedupe_preserve_order(matches)
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed rglob on ADLS base {base!r} pattern {pattern!r}: {exc}"
                ),
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "rglob",
                    "base": base,
                    "pattern": pattern,
                    "error": str(exc),
                },
            ) from exc

    def path_content_length(self, path: str) -> int:
        container, account, key = _split_adls_path(path)
        try:
            fs = self._get_fs_client(container, account)
            fc = fs.get_file_client(key)
            props = fc.get_file_properties()
            try:
                return int(props.size or 0)
            except AttributeError:
                return int(getattr(props, "content_length", 0))
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001
            if self._is_not_found_exc(exc):
                raise PipelineError(
                    message=(
                        f"Failed content_length on ADLS path {path!r}: not found"
                    ),
                    error_code="STORAGE_ADLS_OP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "operation": "content_length",
                        "path": path,
                        "error": str(exc),
                    },
                ) from exc
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed content_length on ADLS path {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "operation": "content_length",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc

    def path_read_bytes(self, path: str) -> bytes:
        container, account, key = _split_adls_path(path)
        try:
            fs = self._get_fs_client(container, account)
            fc = fs.get_file_client(key)
            download = fc.download_file()
            try:
                return download.readall()
            except AttributeError:
                return download.content_as_bytes()
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed read_bytes on ADLS path {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "read_bytes", "path": path, "error": str(exc)},
            ) from exc

    def path_write_bytes(
        self, path: str, data: bytes, *, atomic: bool = True
    ) -> None:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        container, account, key = _split_adls_path(path)
        fs = self._get_fs_client(container, account)
        if not atomic:
            try:
                fc = fs.get_file_client(key)
                fc.upload_data(data, overwrite=True)
                return
            except Exception as exc:  # noqa: BLE001
                retryable = self._is_adls_retryable(exc)
                write_ctx = {
                    "operation": "write_bytes",
                    "path": path,
                    "error": str(exc),
                    "atomic": atomic,
                }
                raise PipelineError(
                    message=f"Failed write_bytes on ADLS path {path!r}: {exc}",
                    error_code="STORAGE_ADLS_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=retryable,
                    context=write_ctx,
                ) from exc
        tmp_key = key + ".tmp"
        try:
            tmp_fc = fs.get_file_client(tmp_key)
            tmp_fc.upload_data(data, overwrite=True)
            src_fc = fs.get_file_client(tmp_key)
            try:
                src_fc.rename_file(f"{container}/{key}")
            except AttributeError:
                pass
            try:
                del_fc = fs.get_file_client(tmp_key)
                del_fc.delete_file()
            except Exception:  # noqa: BLE001
                pass
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=(
                    f"Failed atomic write_bytes on ADLS path {path!r}: {exc}"
                ),
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=retryable,
                context={
                    "operation": "write_bytes.atomic",
                    "path": path,
                    "error": str(exc),
                },
            ) from exc
        return

    @contextmanager
    def path_open_for_append(
        self, path: str, encoding: str = "utf-8"
    ) -> Iterator[IO[str]]:
        self.path_mkdir(self.path_parent(path), parents=True, exist_ok=True)
        existing_parts: list[str] = []
        if self.path_exists(path):
            try:
                existing_parts.append(
                    self.path_read_bytes(path).decode(encoding)
                )
            except Exception as exc:  # noqa: BLE001
                append_read_ctx = {
                    "operation": "open_for_append.read_existing",
                    "path": path,
                    "error": str(exc),
                }
                raise PipelineError(
                    message=(
                        f"Failed to read prior contents during ADLS append "
                        f"for path {path!r}: {exc}"
                    ),
                    error_code="STORAGE_ADLS_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=self._is_adls_retryable(exc),
                    context=append_read_ctx,
                ) from exc

        _outer_write_bytes = self.path_write_bytes

        class _ADLSAppendWriter:
            def __init__(
                self_w, target_path: str, initial: list[str], enc: str
            ) -> None:
                self_w._target = target_path
                self_w._initial = initial
                self_w._enc = enc
                self_w.closed = False
                self_w._buffer: list[str] = []

            def write(self_w, s: str) -> int:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    raise PipelineError(
                        message=(
                            f"Cannot write to closed ADLS append writer "
                            f"for {self_w._target!r}"
                        ),
                        error_code="STORAGE_WRITE_FAILED",
                        error_category=ErrorCategory.storage_write_error,
                        retryable=False,
                        context={"path": self_w._target},
                    )
                self_w._buffer.append(s)
                return len(s)

            def close(self_w) -> None:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    return
                self_w.closed = True
                combined = "".join(self_w._initial + self_w._buffer)
                _outer_write_bytes(
                    self_w._target,
                    combined.encode(self_w._enc),
                    atomic=True,
                )

            def __enter__(self_w):  # type: ignore[no-untyped-def]
                return self_w

            def __exit__(self_w, exc_type, exc, tb):  # type: ignore[no-untyped-def]
                self_w.close()
                return False

        writer = _ADLSAppendWriter(path, existing_parts, encoding)
        try:
            yield writer  # type: ignore[arg-type]
        finally:
            writer.close()
        return

    def path_replace(self, src: str, dst: str) -> None:
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
        self.path_mkdir(self.path_parent(dst), parents=True, exist_ok=True)
        c_src, a_src, k_src = _split_adls_path(src)
        c_dst, a_dst, k_dst = _split_adls_path(dst)
        try:
            fs_src = self._get_fs_client(c_src, a_src)
            fs_dst = self._get_fs_client(c_dst, a_dst)
            try:
                src_fc = fs_src.get_file_client(k_src)
                data = src_fc.download_file()
                try:
                    content_bytes = data.readall()
                except AttributeError:
                    content_bytes = data.content_as_bytes()
                dst_fc = fs_dst.get_file_client(k_dst)
                dst_fc.upload_data(content_bytes, overwrite=True)
                src_fc.delete_file()
            except Exception:  # noqa: BLE001
                src_data = self.path_read_bytes(src)
                dst_fc2 = fs_dst.get_file_client(k_dst)
                dst_fc2.upload_data(src_data, overwrite=True)
                src_fc2 = fs_src.get_file_client(k_src)
                src_fc2.delete_file()
            return
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed path_replace on ADLS src={src!r} dst={dst!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=retryable,
                context={"operation": "replace", "src": src, "dst": dst, "error": str(exc)},
            ) from exc

    def path_delete_tree(self, path: str) -> None:
        container, account, prefix = _split_adls_path(path)
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        adls = self._get_client()
        try:
            all_keys = _adls_list_paths(adls, container, account, prefix, recursive=True)
            if all_keys:
                _adls_batch_delete(adls, container, account, all_keys)
        except Exception as exc:  # noqa: BLE001
            if self._is_not_found_exc(exc):
                return
            retryable = self._is_adls_retryable(exc)
            raise PipelineError(
                message=f"Failed delete_tree on ADLS path {path!r}: {exc}",
                error_code="STORAGE_ADLS_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={"operation": "delete_tree", "path": path, "error": str(exc)},
            ) from exc
        return

    def staging_swap_atomic(
        self,
        *,
        staging_path: str,
        target_path: str,
        mode: SwapMode,
    ) -> None:
        s_container, s_account, s_prefix = _split_adls_path(staging_path)
        t_container, t_account, t_prefix = _split_adls_path(target_path)
        if s_container != t_container or s_account != t_account:
            raise PipelineError(
                message=(
                    "ADLS staging-swap requires staging and target to share the same "
                    "container and account. "
                    f"Got staging_container={s_container!r} target_container={t_container!r} "
                    f"staging_account={s_account!r} target_account={t_account!r}"
                ),
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.config_error,
                retryable=False,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                },
            )
        container = s_container
        account = s_account
        if s_prefix and not s_prefix.endswith("/"):
            s_prefix = s_prefix + "/"
        if t_prefix and not t_prefix.endswith("/"):
            t_prefix = t_prefix + "/"

        adls = self._get_client()
        fs = self._get_fs_client(container, account)
        try:
            if mode == "full_refresh":
                old_target_keys = _adls_list_keys(adls, container, account, t_prefix)
                staging_keys = _adls_list_keys(adls, container, account, s_prefix)
                if not staging_keys:
                    raise PipelineError(
                        message=f"ADLS staging prefix is empty: {staging_path}",
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=False,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                        },
                    )
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(s_prefix):]
                        if s_key.startswith(s_prefix)
                        else s_key
                    )
                    t_key = t_prefix + rel
                    src_fc = fs.get_file_client(s_key)
                    data = src_fc.download_file()
                    try:
                        content_bytes = data.readall()
                    except AttributeError:
                        content_bytes = data.content_as_bytes()
                    dst_fc = fs.get_file_client(t_key)
                    dst_fc.upload_data(content_bytes, overwrite=True)
                    copied_target_keys.append(t_key)
                confirm = _adls_list_keys(adls, container, account, t_prefix)
                expected_set = set(copied_target_keys)
                if not expected_set.issubset(set(confirm)):
                    missing = sorted(expected_set - set(confirm))[:5]
                    raise PipelineError(
                        message=(
                            "ADLS copy to target prefix incomplete after staging copy"
                        ),
                        error_code="SQL_ATOMIC_SWAP_FAILED",
                        error_category=ErrorCategory.processing_error,
                        retryable=True,
                        context={
                            "staging_path": staging_path,
                            "target_path": target_path,
                            "missing_copied_keys_sample": missing,
                        },
                    )
                stale_target = [
                    k for k in old_target_keys if k not in expected_set
                ]
                if stale_target:
                    _adls_batch_delete(adls, container, account, stale_target)
                _adls_batch_delete(adls, container, account, staging_keys)
                return

            staging_keys = _adls_list_keys(adls, container, account, s_prefix)
            if not staging_keys:
                raise PipelineError(
                    message=f"ADLS staging prefix is empty: {staging_path}",
                    error_code="SQL_ATOMIC_SWAP_FAILED",
                    error_category=ErrorCategory.processing_error,
                    retryable=False,
                    context={
                        "staging_path": staging_path,
                        "target_path": target_path,
                    },
                )
            partition_subprefixes = _s3_infer_partition_subprefixes(
                staging_prefix=s_prefix,
                staging_keys=staging_keys,
            )
            if not partition_subprefixes:
                old_target_keys = _adls_list_keys(adls, container, account, t_prefix)
                copied_target_keys = []
                for s_key in staging_keys:
                    rel = (
                        s_key[len(s_prefix):]
                        if s_key.startswith(s_prefix)
                        else s_key
                    )
                    t_key = t_prefix + rel
                    src_fc = fs.get_file_client(s_key)
                    data = src_fc.download_file()
                    try:
                        content_bytes = data.readall()
                    except AttributeError:
                        content_bytes = data.content_as_bytes()
                    dst_fc = fs.get_file_client(t_key)
                    dst_fc.upload_data(content_bytes, overwrite=True)
                    copied_target_keys.append(t_key)
                stale_target = [
                    k for k in old_target_keys
                    if k not in set(copied_target_keys)
                ]
                if stale_target:
                    _adls_batch_delete(adls, container, account, stale_target)
                _adls_batch_delete(adls, container, account, staging_keys)
                return

            for part_sub in partition_subprefixes:
                s_part_prefix = s_prefix + part_sub
                t_part_prefix = t_prefix + part_sub
                s_part_keys = [
                    k for k in staging_keys
                    if k.startswith(s_part_prefix)
                ]
                old_t_part_keys = _adls_list_keys(
                    adls, container, account, t_part_prefix
                )
                for s_key in s_part_keys:
                    rel = (
                        s_key[len(s_part_prefix):]
                        if s_key.startswith(s_part_prefix)
                        else s_key
                    )
                    t_key = t_part_prefix + rel
                    src_fc = fs.get_file_client(s_key)
                    data = src_fc.download_file()
                    try:
                        content_bytes = data.readall()
                    except AttributeError:
                        content_bytes = data.content_as_bytes()
                    dst_fc = fs.get_file_client(t_key)
                    dst_fc.upload_data(content_bytes, overwrite=True)
                if old_t_part_keys:
                    _adls_batch_delete(adls, container, account, old_t_part_keys)
            _adls_batch_delete(adls, container, account, staging_keys)
        except PipelineError:
            raise
        except Exception as exc:  # noqa: BLE001
            retryable = self._is_adls_retryable(exc)
            msg = (
                f"ADLS atomic swap failed: staging={staging_path!r} "
                f"target={target_path!r}: {exc}"
            )
            raise PipelineError(
                message=msg,
                error_code="SQL_ATOMIC_SWAP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=retryable,
                context={
                    "staging_path": staging_path,
                    "target_path": target_path,
                    "mode": mode,
                    "error": str(exc),
                },
            ) from exc

    @staticmethod
    def _get_adls_exc() -> Any:
        try:
            from azure.core import exceptions as aexc  # type: ignore[import-not-found]
        except ImportError:
            class _FallbackExc:
                class ResourceNotFoundError(Exception):
                    pass
                class ClientAuthenticationError(Exception):
                    pass
                class ServiceRequestError(Exception):
                    pass
            return _FallbackExc
        return aexc

    @staticmethod
    def _is_adls_retryable(exc: Exception) -> bool:
        aexc = ADLSBackend._get_adls_exc()
        if isinstance(exc, (aexc.ServiceRequestError,)):
            return True
        msg = str(exc).lower()
        retry_kws = (
            "timeout", "retry", "temporary", "503",
            "rate limit", "throttle", "500", "gateway",
        )
        if any(kw in msg for kw in retry_kws):
            return True
        return False

    @staticmethod
    def _is_not_found_exc(exc: Exception) -> bool:
        aexc = ADLSBackend._get_adls_exc()
        if isinstance(exc, (aexc.ResourceNotFoundError,)):
            return True
        cls = type(exc)
        if cls.__name__ == "ResourceNotFoundError":
            return True
        # Hierarchy walk for SDK wrapper subclasses
        for base in cls.__mro__:
            if base.__name__ == "ResourceNotFoundError":
                return True
        msg = str(exc).lower()
        if "resource not found" in msg or "404" in msg or "not found" in msg:
            lowered = cls.__name__.lower()
            if "error" in lowered or "exception" in lowered:
                return True
        return False


def _s3_list_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _s3_batch_delete(s3, bucket: str, keys: list[str]) -> None:
    if not keys:
        return
    BATCH = 1000
    for i in range(0, len(keys), BATCH):
        chunk = [{"Key": k} for k in keys[i:i + BATCH]]
        s3.delete_objects(
            Bucket=bucket, Delete={"Objects": chunk, "Quiet": True}
        )


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


def _gcs_list_blobs(gcs, bucket_name: str, prefix: str) -> list[str]:
    iterator = gcs.list_blobs(bucket_name, prefix=prefix)
    keys: list[str] = []
    for blob in iterator:
        keys.append(blob.name)
    return keys


def _gcs_batch_delete(gcs, bucket_name: str, keys: list[str]) -> None:
    if not keys:
        return
    bucket = gcs.bucket(bucket_name)
    BATCH = 1000
    for i in range(0, len(keys), BATCH):
        chunk_keys = keys[i:i + BATCH]
        blobs = [bucket.blob(k) for k in chunk_keys]
        bucket.delete_blobs(blobs)


def _adls_list_paths(
    adls: Any,
    container: str,
    account: str,
    prefix: str,
    *,
    recursive: bool,
    max_results: int | None = None,
) -> list[dict[str, Any]]:
    _ = account
    fs = adls.get_file_system_client(container)
    results: list[dict[str, Any]] = []
    try:
        iterator = fs.list_paths(
            path=prefix or None,
            recursive=recursive,
            max_results=max_results,
        )
    except AttributeError:
        try:
            iterator = adls.list_paths(
                container,
                prefix or None,
                recursive=recursive,
                max_results=max_results,
            )
        except Exception:
            return results
    try:
        for p in iterator:
            try:
                name = p.name
                is_dir = bool(getattr(p, "is_directory", False))
            except AttributeError:
                if isinstance(p, dict):
                    name = p.get("name", "")
                    is_dir = bool(p.get("is_directory", False))
                else:
                    continue
            results.append({"name": name, "is_directory": is_dir})
    except Exception:
        pass
    return results


def _adls_list_keys(adls: Any, container: str, account: str, prefix: str) -> list[str]:
    all_paths = _adls_list_paths(
        adls, container, account, prefix, recursive=True
    )
    return [p["name"] for p in all_paths if not p.get("is_directory")]


def _adls_batch_delete(
    adls: Any, container: str, account: str, keys: list[str] | list[dict[str, Any]]
) -> None:
    _ = account
    if not keys:
        return
    fs = adls.get_file_system_client(container)
    for item in keys:
        if isinstance(item, dict):
            key = item["name"]
        else:
            key = item
        try:
            fc = fs.get_file_client(key)
            fc.delete_file()
        except AttributeError:
            try:
                adls.delete_file(container, key)
            except Exception:
                pass
        except Exception:
            pass


_BACKEND_REGISTRY: dict[StorageScheme, StorageBackend] = {
    StorageScheme.s3: S3Backend(),
    StorageScheme.gs: GCSBackend(),
    StorageScheme.abfss: ADLSBackend(),
    StorageScheme.file: LocalBackend(),
    StorageScheme.local_unschemed: LocalBackend(),
}


def get_backend(path: str) -> StorageBackend:
    scheme = detect_scheme(path)
    backend = _BACKEND_REGISTRY.get(scheme)
    if backend is None:
        raise PipelineError(
            message=(
                f"No storage backend registered for scheme {scheme.value!r} "
                f"on path {path!r}. "
                f"Supported schemes: {'; '.join(_SUPPORTED_SCHEMES_FOR_ERROR)}."
            ),
            error_code="STORAGE_BACKEND_NOT_FOUND",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context={"path": path, "scheme": scheme.value},
        )
    return backend


def register_backend(
    scheme: StorageScheme, backend: StorageBackend
) -> None:
    _BACKEND_REGISTRY[scheme] = backend


_NO_STAGING_MOVE_HINT = (
    "Staging-swap write protocol is only implemented for backends registered in the "
    "storage-backend facade. Currently registered backends cover POSIX (file:// or bare "
    "local), AWS S3 (s3://), Google Cloud Storage (gs://), and Azure ADLS Gen2 "
    "(abfss://). Use a supported scheme or declare the table load_mode='append' which "
    "does not require staging."
)


def validate_swap_scheme(target_path: str, model_id: str) -> StorageScheme:
    scheme = detect_scheme(target_path)
    if scheme not in _BACKEND_REGISTRY:
        raise PipelineError(
            message=(
                f"Cannot apply staging-swap protocol for model {model_id!r}: "
                f"target path scheme {scheme.value!r} has no registered backend."
            ),
            error_code="SQL_STAGING_SCHEME_UNSUPPORTED",
            error_category=ErrorCategory.config_error,
            retryable=False,
            context={
                "model_id": model_id,
                "target_path": target_path,
                "scheme": scheme.value,
                "operator_action": _NO_STAGING_MOVE_HINT,
            },
        )
    return scheme


def atomic_swap(
    *,
    staging_path: str,
    target_path: str,
    mode: SwapMode,
) -> None:
    scheme_s = detect_scheme(staging_path)
    scheme_t = detect_scheme(target_path)
    if scheme_s is not scheme_t:
        raise PipelineError(
            message=(
                f"Atomic swap requires matching scheme between staging and target. "
                f"staging={staging_path!r} (scheme={scheme_s.value}) "
                f"target={target_path!r} (scheme={scheme_t.value})"
            ),
            error_code="SQL_ATOMIC_SWAP_FAILED",
            error_category=ErrorCategory.input_contract_error,
            retryable=False,
            context={
                "staging_path": staging_path,
                "target_path": target_path,
                "staging_scheme": scheme_s.value,
                "target_scheme": scheme_t.value,
            },
        )
    backend = get_backend(staging_path)
    backend.staging_swap_atomic(
        staging_path=staging_path,
        target_path=target_path,
        mode=mode,
    )


def build_staging_path(
    *,
    staging_root: str,
    stage: StageName,
    target_table_name: str,
    run_id: str,
) -> str:
    from elt_pipeline.shared.path_utils import join_paths

    return join_paths(
        staging_root, stage.value, target_table_name, "run_id=" + run_id
    )


def best_effort_delete_staging(staging_path: str) -> None:
    from elt_pipeline.shared.path_utils import path_delete_tree

    try:
        path_delete_tree(staging_path)
    except PipelineError:
        return
    except Exception:
        return
