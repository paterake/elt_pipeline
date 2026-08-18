from __future__ import annotations

import os
import shutil
from typing import Literal

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.path_utils import (
    _s3_client,
    _split_s3_path,
    _StorageScheme,
    detect_scheme,
    join_paths,
    path_delete_tree,
    strip_file_scheme,
)
from elt_pipeline.shared.runtime import StageName

SwapMode = Literal["full_refresh", "partition_overwrite"]


def build_staging_path(
    *,
    staging_root: str,
    stage: StageName,
    target_table_name: str,
    run_id: str,
) -> str:
    """Build staging directory path.

    Layout contract:  ``{staging_root}/{stage}/{table}/run_id={run_id}``

    Each run gets an isolated run_id-scoped directory so concurrent runs never
    collide on staging and cleanup can be per-run without touching siblings.
    """
    return join_paths(
        staging_root, stage.value, target_table_name, "run_id=" + run_id
    )

_NO_STAGING_MOVE_HINT = (
    "Staging-swap write protocol is only implemented for POSIX (file:// or bare local) "
    "and AWS S3 (s3://) storage. See PRD 08 for supported storage schemes. "
    "Use a supported scheme or declare the table load_mode='append' which does not "
    "require staging."
)


def validate_swap_scheme(target_path: str, model_id: str) -> _StorageScheme:
    scheme = detect_scheme(target_path)
    if scheme not in (
        _StorageScheme.file,
        _StorageScheme.local_unschemed,
        _StorageScheme.s3,
    ):
        raise PipelineError(
            message=(
                f"Cannot apply staging-swap protocol for model {model_id!r}: "
                f"target path scheme {scheme.value!r} is not supported."
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


def best_effort_delete_staging(
    staging_path: str, scheme: _StorageScheme
) -> None:
    _ = scheme
    try:
        path_delete_tree(staging_path)
    except PipelineError:
        return
    except Exception:
        return


def atomic_swap(
    *,
    staging_path: str,
    target_path: str,
    scheme: _StorageScheme,
    mode: SwapMode,
) -> None:
    if scheme in (_StorageScheme.file, _StorageScheme.local_unschemed):
        _atomic_swap_posix(
            staging_path=staging_path,
            target_path=target_path,
            mode=mode,
        )
    elif scheme is _StorageScheme.s3:
        _atomic_swap_s3(
            staging_path=staging_path,
            target_path=target_path,
            mode=mode,
        )
    else:
        raise PipelineError(
            message=(
                f"Atomic swap not implemented for scheme {scheme.value!r}"
            ),
            error_code="SQL_ATOMIC_SWAP_FAILED",
            error_category=ErrorCategory.processing_error,
            retryable=False,
            context={
                "staging_path": staging_path,
                "target_path": target_path,
                "scheme": scheme.value,
            },
        )


def _atomic_swap_posix(
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

        _swap_partition_tree_posix(staging_local, target_local)
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


def _swap_partition_tree_posix(staging_dir: str, target_dir: str) -> None:
    """Dynamic partition overwrite for a (possibly multi-level) Hive-partition tree.

    Descends through nested ``key=value`` partition directories and replaces only
    the **leaf** partition directories present in staging, leaving sibling
    partitions in the target untouched. This is what makes a multi-column default
    layout (e.g. L3 ``source_name=<src>/business_date=<date>``) obey dynamic
    partition-overwrite semantics: only the ``(source_name, business_date)`` tuples
    produced by this run are replaced — unrelated dates/sources survive.

    Empty staging directories are pruned as the recursion unwinds so a caller
    inspecting the staging root sees it gone once every partition has moved.
    """
    partition_subdirs = [
        name for name in _list_immediate_subdirs_posix(staging_dir) if "=" in name
    ]
    if not partition_subdirs:
        # Leaf partition (or unpartitioned output): replace the target subtree whole.
        if os.path.isdir(target_dir):
            shutil.rmtree(target_dir)
        elif os.path.exists(target_dir):
            os.remove(target_dir)
        os.makedirs(os.path.dirname(target_dir) or ".", exist_ok=True)
        os.rename(staging_dir, target_dir)
        return

    os.makedirs(target_dir, exist_ok=True)
    for name in partition_subdirs:
        _swap_partition_tree_posix(
            os.path.join(staging_dir, name),
            os.path.join(target_dir, name),
        )
    # Prune this staging level if nothing remains (a stray _SUCCESS marker or other
    # non-partition file legitimately keeps it around for best-effort cleanup later).
    if not os.listdir(staging_dir):
        os.rmdir(staging_dir)


def _list_immediate_subdirs_posix(local_path: str) -> list[str]:
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


def _atomic_swap_s3(
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

    s3 = _s3_client()
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
    """Infer the **leaf** partition subprefixes present in staging.

    Returns the full ``key=value/.../key=value/`` chain down to (but excluding) the
    object name, so a multi-column layout (e.g. ``source_name=X/business_date=Y/``)
    replaces only the exact leaf partitions written by this run and leaves sibling
    partitions in the target intact — the S3 analogue of dynamic partition overwrite.
    """
    subprefixes: set[str] = set()
    for key in staging_keys:
        rel = (
            key[len(staging_prefix):]
            if key.startswith(staging_prefix)
            else key
        )
        if "/" not in rel:
            continue
        # Leading run of consecutive key=value segments before the object name.
        segments = rel.split("/")[:-1]
        partition_segments: list[str] = []
        for segment in segments:
            if "=" not in segment:
                break
            partition_segments.append(segment)
        if partition_segments:
            subprefixes.add("/".join(partition_segments) + "/")
    return sorted(subprefixes)
