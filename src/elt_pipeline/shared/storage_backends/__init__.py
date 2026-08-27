from __future__ import annotations

from ._adls_backend import (
    ADLSBackend,
    _adls_batch_delete,
    _adls_list_keys,
    _adls_list_paths,
)
from ._clients import (
    _get_adls_client,
    _get_gcs_client,
    _get_s3_client,
    _s3_infer_partition_subprefixes,
    _split_adls_path,
    _split_gcs_path,
    _split_s3_path,
)
from ._gcs_backend import (
    GCSBackend,
    _gcs_batch_delete,
    _gcs_list_blobs,
)
from ._local_backend import (
    LocalBackend,
)
from ._protocol import (
    StorageBackend,
    SwapMode,
)
from ._registry import (
    _BACKEND_REGISTRY,
    _NO_STAGING_MOVE_HINT,
    atomic_swap,
    best_effort_delete_staging,
    build_staging_path,
    get_backend,
    register_backend,
    validate_swap_scheme,
)
from ._s3_backend import (
    S3Backend,
    _s3_batch_delete,
    _s3_list_keys,
)

_S3_CLIENT: object | None = None
_GCS_CLIENT: object | None = None
_ADLS_CLIENT: object | None = None

__all__ = [
    "ADLSBackend",
    "GCSBackend",
    "LocalBackend",
    "S3Backend",
    "StorageBackend",
    "SwapMode",
    "_ADLS_CLIENT",
    "_BACKEND_REGISTRY",
    "_GCS_CLIENT",
    "_NO_STAGING_MOVE_HINT",
    "_S3_CLIENT",
    "_adls_batch_delete",
    "_adls_list_keys",
    "_adls_list_paths",
    "_gcs_batch_delete",
    "_gcs_list_blobs",
    "_get_adls_client",
    "_get_gcs_client",
    "_get_s3_client",
    "_s3_batch_delete",
    "_s3_infer_partition_subprefixes",
    "_s3_list_keys",
    "_split_adls_path",
    "_split_gcs_path",
    "_split_s3_path",
    "atomic_swap",
    "best_effort_delete_staging",
    "build_staging_path",
    "get_backend",
    "register_backend",
    "validate_swap_scheme",
]
