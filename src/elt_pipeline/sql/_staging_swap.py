from __future__ import annotations

from elt_pipeline.shared import path_utils
from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.path_utils import (
    StorageScheme as _StorageScheme,
)
from elt_pipeline.shared.path_utils import (
    detect_scheme,
    path_delete_tree,
)
from elt_pipeline.shared.runtime import StageName
from elt_pipeline.shared.storage_backends import (
    _BACKEND_REGISTRY as _BACKEND_REGISTRY_REF,
)
from elt_pipeline.shared.storage_backends import (
    SwapMode,
)
from elt_pipeline.shared.storage_backends import (
    atomic_swap as _atomic_swap_impl,
)
from elt_pipeline.shared.storage_backends import (
    build_staging_path as _build_staging_path_impl,
)

_s3_client = path_utils._s3_client
_S3_CLIENT = None

__all__ = [
    "SwapMode",
    "atomic_swap",
    "best_effort_delete_staging",
    "build_staging_path",
    "validate_swap_scheme",
]

_NO_STAGING_MOVE_HINT = (
    "Staging-swap write protocol is implemented for POSIX (file:// or bare local), "
    "AWS S3 (s3://), Google Cloud Storage (gs://), and Azure ADLS Gen2 (abfss://). "
    "See PRD 08 for supported storage schemes. Use a supported scheme or declare the "
    "table load_mode='append' which does not require staging."
)


def validate_swap_scheme(target_path: str, model_id: str) -> _StorageScheme:
    scheme = detect_scheme(target_path)
    if scheme not in _BACKEND_REGISTRY_REF:
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
    _ = scheme
    _atomic_swap_impl(
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
    return _build_staging_path_impl(
        staging_root=staging_root,
        stage=stage,
        target_table_name=target_table_name,
        run_id=run_id,
    )
