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
    _split_adls_path,
)
from ._protocol import SwapMode


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
        for base in cls.__mro__:
            if base.__name__ == "ResourceNotFoundError":
                return True
        msg = str(exc).lower()
        if "resource not found" in msg or "404" in msg or "not found" in msg:
            lowered = cls.__name__.lower()
            if "error" in lowered or "exception" in lowered:
                return True
        return False
