from __future__ import annotations

import os
import posixpath
import shutil
from contextlib import contextmanager
from pathlib import Path as _Path
from typing import IO, Iterator

from elt_pipeline.shared.errors import (
    ConfigValidationError,
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import (
    StorageScheme,
    collapse_slashes,
    collapse_slashes_without_scheme,
    detect_scheme,
    strip_file_scheme,
)

from ._protocol import SwapMode


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
