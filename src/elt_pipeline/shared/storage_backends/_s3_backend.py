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
    _split_s3_path,
)
from ._protocol import SwapMode


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
