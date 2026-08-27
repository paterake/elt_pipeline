from __future__ import annotations

import fnmatch
import posixpath
from contextlib import contextmanager
from typing import IO, Any, Iterator

from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import (
    _dedupe_preserve_order,
    collapse_slashes,
    collapse_slashes_without_scheme,
    detect_scheme,
)

from ._clients import (
    _s3_infer_partition_subprefixes,
    _split_gcs_path,
)
from ._protocol import SwapMode


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
