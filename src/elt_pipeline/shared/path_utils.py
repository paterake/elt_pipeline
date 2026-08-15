from __future__ import annotations

import os
import posixpath
import re
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import IO, Any, Iterator

from elt_pipeline.shared.errors import ConfigValidationError, ErrorCategory, PipelineError

_SAFE_PATH_FRAGMENT = re.compile(r"[^A-Za-z0-9._/-]+")


class _StorageScheme(str, Enum):
    s3 = "s3"
    file = "file"
    local_unschemed = "local_unschemed"


_SUPPORTED_SCHEME_PREFIXES: frozenset[str] = frozenset(
    {_StorageScheme.s3.value + "://", _StorageScheme.file.value + "://"}
)
_SUPPORTED_SCHEMES_FOR_ERROR: tuple[str, ...] = (
    "s3:// (AWS S3)",
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
    if path.startswith("file://"):
        return _StorageScheme.file
    # Anything else → treat as local POSIX (absolute or relative).
    # Unrecognized schemes MUST fail fast, sharp and clear.
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
    # file:///abs/path  →  /abs/path
    # file://rel/path   →  rel/path
    rest = path[len("file://"):]
    if rest.startswith("/") and not rest.startswith("//"):
        # Triple slash means absolute local path.
        return rest
    # file: without triple-slash is unusual but still valid relative; pass through.
    return rest


def join_paths(root: str, *segments: str) -> str:
    _validate_root_is_string(root)
    # Build a sanitized segments list (skip empties; strip leading/trailing slashes per segment)
    cleaned_segments: list[str] = []
    for raw in segments:
        if not isinstance(raw, str):
            raise ConfigValidationError(
                message=f"join_paths segments must be strings; got {type(raw).__name__!r}: {raw!r}",
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
        cleaned_segments.append(collapse_slashes_without_scheme(cleaned))
    # Short-circuit: no segments → return root with slash collapse (scheme preserved).
    if not cleaned_segments:
        return collapse_slashes(root)
    scheme = detect_scheme(root)
    if scheme is _StorageScheme.s3:
        # s3://bucket/prefix/segment1/segment2
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
    # file:// or local unschemed → build path, collapse slashes uniformly.
    if scheme is _StorageScheme.file:
        stripped = strip_file_scheme(root)
        joined = posixpath.join(stripped, *cleaned_segments)
        return collapse_slashes(f"file://{joined}")
    # Bare local POSIX (no scheme): use posixpath.join + collapse.
    joined = posixpath.join(root, *cleaned_segments)
    return collapse_slashes_without_scheme(joined)


def collapse_slashes(path: str) -> str:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        return "s3://" + collapse_slashes_without_scheme(path[len("s3://"):])
    if scheme is _StorageScheme.file:
        stripped = strip_file_scheme(path)
        return "file://" + collapse_slashes_without_scheme(stripped)
    return collapse_slashes_without_scheme(path)


def collapse_slashes_without_scheme(path_suffix: str) -> str:
    # Replace runs of '/' with a single '/'.
    return re.sub(r"/+", "/", path_suffix)


def path_parent(path: str) -> str:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        suffix = path[len("s3://"):]
        parent_suffix = posixpath.dirname(suffix)
        return f"s3://{parent_suffix}" if parent_suffix else f"s3://{''.join(suffix.split('/')[:1])}"
    if scheme is _StorageScheme.file:
        stripped = strip_file_scheme(path)
        return "file://" + posixpath.dirname(stripped)
    return posixpath.dirname(path)


def path_basename(path: str) -> str:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        suffix = path[len("s3://"):].rstrip("/")
        return posixpath.basename(suffix)
    if scheme is _StorageScheme.file:
        return posixpath.basename(strip_file_scheme(path).rstrip("/"))
    return posixpath.basename(path.rstrip("/"))


def path_with_suffix(path: str, suffix: str) -> str:
    scheme = detect_scheme(path)
    if not suffix.startswith("."):
        suffix = "." + suffix
    if scheme is _StorageScheme.s3:
        return path + suffix
    if scheme is _StorageScheme.file:
        return path + suffix
    # Bare local path: POSIX replace suffix using pathlib only on a stripped leaf.
    # NOTE: `path` stays a string; Path used only for suffix replace on the leaf.
    return str(Path(path).with_suffix(suffix))


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
    # Remove trailing '/' from base for consistent prefix matching.
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


def path_normalize(path: str) -> str:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        # For S3 there is no "resolve symlinks"; just collapse slashes and identity-return.
        return collapse_slashes(path)
    # For file and local_unschemed → resolve real path (symlinks, .., .).
    stripped = strip_file_scheme(path)
    resolved = os.path.realpath(stripped)
    if scheme is _StorageScheme.file:
        return f"file://{resolved}"
    return resolved


# ---------------------------------------------------------------------------
# Leaf I/O operations: dispatch by scheme, POSIX branch uses pathlib only locally
# ---------------------------------------------------------------------------

def _raise_unsupported_operation(scheme: _StorageScheme, operation: str, path: str) -> None:
    raise PipelineError(
        message=(
            f"Unsupported storage operation {operation!r} for "
            f"scheme {scheme.value!r} on path {path!r}"
        ),
        error_code="STORAGE_OPERATION_UNSUPPORTED",
        error_category=ErrorCategory.input_contract_error,
        retryable=False,
        context={"operation": operation, "path": path, "scheme": scheme.value},
    )


def path_exists(path: str) -> bool:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        s3 = _s3_client()
        try:
            # Check object first; if key ends with '/' or empty, check prefix listing.
            if key == "" or key.endswith("/"):
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
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
    stripped = strip_file_scheme(path)
    return os.path.exists(stripped)


def path_is_dir(path: str) -> bool:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        s3 = _s3_client()
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1, Delimiter="/")
            return bool(resp.get("Contents") or resp.get("CommonPrefixes"))
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed path_is_dir on S3 for {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "is_dir", "path": path, "error": str(exc)},
            ) from exc
    stripped = strip_file_scheme(path)
    return os.path.isdir(stripped)


def path_mkdir(path: str, *, parents: bool = True, exist_ok: bool = True) -> None:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        # S3 has no real directories; mkdir is a no-op. But for consistency, validate scheme.
        _ = _split_s3_path(path)
        return
    stripped = strip_file_scheme(path)
    try:
        os.makedirs(stripped, exist_ok=exist_ok) if parents else os.mkdir(stripped)
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


def path_listdir(path: str) -> list[str]:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        prefix = key if key.endswith("/") or key == "" else key + "/"
        s3 = _s3_client()
        try:
            results: list[str] = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    # CommonPrefixes entries end with '/'; strip trailing '/'
                    # to get a clean directory name for the listing.
                    full_prefix = cp["Prefix"]
                    if not full_prefix.startswith(prefix):
                        continue
                    suffix = full_prefix[len(prefix):].rstrip("/")
                    if suffix:
                        # full_prefix already has a trailing slash from S3.
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
    # Re-join to produce full absolute path strings; caller's responsibility.
    prefix = path if path.endswith("/") else path + "/"
    return [prefix + name for name in names]


def path_glob(base: str, pattern: str) -> list[str]:
    scheme = detect_scheme(base)
    if scheme is _StorageScheme.s3:
        # Use list-objects-prefix + fnmatch on the suffixes.
        import fnmatch
        bucket, base_key = _split_s3_path(base)
        prefix = base_key if base_key.endswith("/") or base_key == "" else base_key + "/"
        s3 = _s3_client()
        matches: list[str] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    suffix = k[len(prefix):] if k.startswith(prefix) else None
                    if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                        matches.append(f"s3://{bucket}/{k}")
            return matches
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed glob on S3 base {base!r} pattern {pattern!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "glob", "base": base, "pattern": pattern, "error": str(exc)},
            ) from exc
    stripped_base = strip_file_scheme(base)
    from pathlib import Path as _Path
    results_p = list(_Path(stripped_base).glob(pattern))
    if scheme is _StorageScheme.file:
        return [f"file://{p}" for p in results_p]
    return [str(p) for p in results_p]


def path_rglob(base: str, pattern: str) -> list[str]:
    scheme = detect_scheme(base)
    if scheme is _StorageScheme.s3:
        # Recursive glob on S3 = full prefix list + fnmatch.
        import fnmatch
        bucket, base_key = _split_s3_path(base)
        prefix = base_key if base_key.endswith("/") or base_key == "" else base_key + "/"
        s3 = _s3_client()
        matches: list[str] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    suffix = k[len(prefix):] if k.startswith(prefix) else None
                    if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                        matches.append(f"s3://{bucket}/{k}")
                # Include common prefixes as synthetic "dirs" so later
                # is_dir filtering on matches behaves consistently.
                for cp in page.get("CommonPrefixes", []):
                    p = cp["Prefix"]
                    suffix = p[len(prefix):].rstrip("/") if p.startswith(prefix) else None
                    if suffix is not None and fnmatch.fnmatch(suffix, pattern):
                        matches.append(f"s3://{bucket}/{p}")
            return _dedupe_preserve_order(matches)
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            raise PipelineError(
                message=f"Failed rglob on S3 base {base!r} pattern {pattern!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.processing_error,
                retryable=True,
                context={"operation": "rglob", "base": base, "pattern": pattern, "error": str(exc)},
            ) from exc
    stripped_base = strip_file_scheme(base)
    from pathlib import Path as _Path
    results_p = list(_Path(stripped_base).rglob(pattern))
    if scheme is _StorageScheme.file:
        return [f"file://{p}" for p in results_p]
    return [str(p) for p in results_p]


def path_content_length(path: str) -> int:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        s3 = _s3_client()
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            return int(resp.get("ContentLength", 0))
        except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "NoSuchBucket"):
                raise PipelineError(
                    message=f"Failed content_length on S3 path {path!r}: not found",
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
                context={"operation": "content_length", "path": path, "error": str(exc)},
            ) from exc
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


def path_read_bytes(path: str) -> bytes:
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        s3 = _s3_client()
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


def path_read_text(path: str, encoding: str = "utf-8") -> str:
    return path_read_bytes(path).decode(encoding)


def path_write_bytes(path: str, data: bytes, *, atomic: bool = True) -> None:
    scheme = detect_scheme(path)
    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    if scheme is _StorageScheme.s3:
        bucket, key = _split_s3_path(path)
        s3 = _s3_client()
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
        # Atomic S3: write to *.tmp → copy → delete tmp.
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
                message=f"Failed atomic write_bytes on S3 path {path!r}: {exc}",
                error_code="STORAGE_S3_OP_FAILED",
                error_category=ErrorCategory.storage_write_error,
                retryable=True,
                context={"operation": "write_bytes.atomic", "path": path, "error": str(exc)},
            ) from exc
        return
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
            context={"operation": "write_bytes", "path": path, "error": str(exc), "atomic": atomic},
        ) from exc


def path_write_text(path: str, data: str, encoding: str = "utf-8", *, atomic: bool = True) -> None:
    path_write_bytes(path, data.encode(encoding), atomic=atomic)


@contextmanager
def path_open_for_append(path: str, encoding: str = "utf-8") -> Iterator[IO[str]]:
    """Context manager for append writes (JSONL logs / event streams).

    For S3 there is no true append; we read existing bytes if any, concat in memory, and
    rewrite atomically on context exit. Acceptable for small audit/event files; not for
    large data payloads (which are written via path_write_bytes directly instead).
    """
    scheme = detect_scheme(path)
    path_mkdir(path_parent(path), parents=True, exist_ok=True)
    if scheme is _StorageScheme.s3:
        existing_parts: list[str] = []
        if path_exists(path):
            try:
                existing_parts.append(path_read_text(path, encoding=encoding))
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

        class _S3AppendWriter:
            def __init__(self, target_path: str, initial: list[str], enc: str) -> None:
                self._target = target_path
                self._initial = initial
                self._enc = enc
                self.closed = False
                self._buffer: list[str] = []

            def write(self, s: str) -> int:
                if self.closed:
                    raise PipelineError(
                        message=f"Cannot write to closed S3 append writer for {self._target!r}",
                        error_code="STORAGE_WRITE_FAILED",
                        error_category=ErrorCategory.storage_write_error,
                        retryable=False,
                        context={"path": self._target},
                    )
                self._buffer.append(s)
                return len(s)

            def close(self_w) -> None:  # type: ignore[no-untyped-def]
                if self_w.closed:
                    return
                self_w.closed = True
                combined = "".join(self_w._initial + self_w._buffer)
                path_write_text(self_w._target, combined, encoding=self_w._enc, atomic=True)

            def __enter__(self):
                return self

            def __exit__(self_w, exc_type, exc, tb):  # type: ignore[no-untyped-def]
                self_w.close()
                return False

        writer = _S3AppendWriter(path, existing_parts, encoding)
        try:
            yield writer  # type: ignore[arg-type]
        finally:
            writer.close()
        return
    stripped = strip_file_scheme(path)
    try:
        f = open(stripped, "a", encoding=encoding)
    except OSError as exc:
        raise PipelineError(
            message=f"Failed open_for_append on path {path!r}: {exc}",
            error_code="STORAGE_WRITE_FAILED",
            error_category=ErrorCategory.storage_write_error,
            retryable=False,
            context={"operation": "open_for_append", "path": path, "error": str(exc)},
        ) from exc
    try:
        yield f
    finally:
        f.close()


def path_replace(src: str, dst: str) -> None:
    """Atomic rename/replace.

    Equivalent to POSIX `os.replace(src, dst)`.
    On S3 this means CopyObject → DeleteObject on the source key.
    """
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
    path_mkdir(path_parent(dst), parents=True, exist_ok=True)
    if scheme_src is _StorageScheme.s3:
        b_src, k_src = _split_s3_path(src)
        b_dst, k_dst = _split_s3_path(dst)
        if b_src != b_dst:
            # Cross-bucket replace: S3 COPY cross-bucket then delete src.
            s3 = _s3_client()
            try:
                s3.copy_object(Bucket=b_dst, Key=k_dst, CopySource={"Bucket": b_src, "Key": k_src})
                s3.delete_object(Bucket=b_src, Key=k_src)
                return
            except s3.exceptions.ClientError as exc:  # type: ignore[attr-defined]
                cross_bucket_ctx = {
                    "operation": "replace.cross_bucket",
                    "src": src,
                    "dst": dst,
                    "error": str(exc),
                }
                raise PipelineError(
                    message=(
                        f"Failed path_replace (cross-bucket) S3 "
                        f"src={src!r} dst={dst!r}: {exc}"
                    ),
                    error_code="STORAGE_S3_OP_FAILED",
                    error_category=ErrorCategory.storage_write_error,
                    retryable=True,
                    context=cross_bucket_ctx,
                ) from exc
        s3 = _s3_client()
        try:
            s3.copy_object(Bucket=b_dst, Key=k_dst, CopySource={"Bucket": b_src, "Key": k_src})
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


def path_delete_tree(path: str) -> None:
    """Recursively delete a directory tree or S3 key prefix.

    For POSIX: equivalent to `shutil.rmtree(path, ignore_errors=False)`.
    For S3: paginates all keys under the prefix and batch-deletes them.
    Missing paths are tolerated (no-op) so callers can use this for
    best-effort cleanup without pre-checking path_exists.
    """
    scheme = detect_scheme(path)
    if scheme is _StorageScheme.s3:
        bucket, prefix = _split_s3_path(path)
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        s3 = _s3_client()
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                contents = page.get("Contents", [])
                if not contents:
                    continue
                keys = [{"Key": obj["Key"]} for obj in contents]
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
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
    import shutil

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


# ---------------------------------------------------------------------------
# S3 helpers + lazy client singleton
# ---------------------------------------------------------------------------

_S3_CLIENT: object | None = None


def _s3_client() -> Any:
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
    if scheme is not _StorageScheme.s3:
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


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
