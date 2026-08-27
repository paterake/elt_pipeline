from __future__ import annotations

from elt_pipeline.shared.errors import (
    ErrorCategory,
    PipelineError,
)
from elt_pipeline.shared.path_utils import (
    _SUPPORTED_SCHEMES_FOR_ERROR,
    StorageScheme,
    detect_scheme,
)
from elt_pipeline.shared.runtime import StageName

from ._adls_backend import ADLSBackend
from ._gcs_backend import GCSBackend
from ._local_backend import LocalBackend
from ._protocol import StorageBackend, SwapMode
from ._s3_backend import S3Backend

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
