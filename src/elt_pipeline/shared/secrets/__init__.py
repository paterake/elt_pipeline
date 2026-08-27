from __future__ import annotations

from ._errors import (
    SecretNotFoundError,
    SecretRefSyntaxError,
    SecretsError,
    SecretsNotImplementedError,
)
from ._models import (
    _REDACTED_PLACEHOLDER,
    _SCHEME_RE,
    _SECRET_MAX_PEEK_BYTES,
    _SUPPORTED_SCHEMES,
    ParsedSecretRef,
    SecretScheme,
    SecretValue,
    parse_secret_ref,
    redact_secret,
)
from ._protocol import SecretsProvider
from ._providers_cloud import (
    AWSSecretsManagerSecrets,
    AzureKeyVaultSecrets,
    GCPSecretManagerSecrets,
    VaultSecrets,
    _raise_sdk_missing,
)
from ._providers_env import EnvVarSecrets, FileSecrets
from ._registry import (
    _PROVIDER_REGISTRY,
    _bootstrap_default_registry,
    get_provider,
    register_provider,
    resolve_secret_ref,
    resolve_secret_refs,
)

__all__ = [
    "SecretScheme",
    "ParsedSecretRef",
    "parse_secret_ref",
    "SecretsProvider",
    "register_provider",
    "get_provider",
    "SecretValue",
    "redact_secret",
    "SecretsError",
    "SecretRefSyntaxError",
    "SecretNotFoundError",
    "SecretsNotImplementedError",
    "EnvVarSecrets",
    "FileSecrets",
    "AWSSecretsManagerSecrets",
    "AzureKeyVaultSecrets",
    "GCPSecretManagerSecrets",
    "VaultSecrets",
    "resolve_secret_ref",
    "resolve_secret_refs",
    "_PROVIDER_REGISTRY",
    "_bootstrap_default_registry",
    "_raise_sdk_missing",
    "_REDACTED_PLACEHOLDER",
    "_SCHEME_RE",
    "_SECRET_MAX_PEEK_BYTES",
    "_SUPPORTED_SCHEMES",
]
