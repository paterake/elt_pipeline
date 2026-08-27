from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from ._errors import SecretNotFoundError, SecretsError
from ._models import (
    ParsedSecretRef,
    SecretScheme,
    SecretValue,
    parse_secret_ref,
)
from ._protocol import SecretsProvider
from ._providers_cloud import (
    AWSSecretsManagerSecrets,
    AzureKeyVaultSecrets,
    GCPSecretManagerSecrets,
    VaultSecrets,
)
from ._providers_env import EnvVarSecrets, FileSecrets

_PROVIDER_REGISTRY: dict[SecretScheme, SecretsProvider] = {}


def register_provider(
    scheme: SecretScheme | str,
    provider: SecretsProvider,
) -> None:
    scheme_key = SecretScheme(scheme) if isinstance(scheme, str) else scheme
    if scheme_key in _PROVIDER_REGISTRY:
        raise SecretsError(
            message=(
                f"Secrets provider already registered for scheme '{scheme_key.value}://'. "
                f"Use register_provider() only once per scheme per process, or use a "
                f"different scheme name."
            ),
            error_code="SECRETS_PROVIDER_ALREADY_REGISTERED",
            context={"scheme": scheme_key.value},
        )
    if not isinstance(provider, SecretsProvider):
        raise SecretsError(
            message=(
                f"register_provider expected SecretsProvider Protocol implementor, "
                f"got {type(provider).__name__}.  Must have 'provider_type: str' and "
                f"'resolve(*, path: str) -> SecretValue'."
            ),
            error_code="SECRETS_PROVIDER_INVALID",
            context={"scheme": scheme_key.value, "type": type(provider).__name__},
        )
    _PROVIDER_REGISTRY[scheme_key] = provider


def get_provider(scheme: SecretScheme | str) -> SecretsProvider:
    scheme_key = SecretScheme(scheme) if isinstance(scheme, str) else scheme
    if scheme_key not in _PROVIDER_REGISTRY:
        raise SecretsError(
            message=(
                f"No SecretsProvider registered for scheme '{scheme_key.value}://'. "
                f"Register one via secrets.register_provider({scheme_key.value!r}, impl)."
            ),
            error_code="SECRETS_NO_PROVIDER",
            context={"scheme": scheme_key.value},
        )
    return _PROVIDER_REGISTRY[scheme_key]


def _bootstrap_default_registry() -> None:
    if _PROVIDER_REGISTRY:
        return
    register_provider(SecretScheme.env, EnvVarSecrets())
    register_provider(SecretScheme.file, FileSecrets())
    register_provider(SecretScheme.aws_secretsmanager, AWSSecretsManagerSecrets())
    register_provider(SecretScheme.azure_keyvault, AzureKeyVaultSecrets())
    register_provider(SecretScheme.gcp_secretmanager, GCPSecretManagerSecrets())
    register_provider(SecretScheme.vault, VaultSecrets())


def resolve_secret_ref(
    secret_ref: str,
    *,
    strict: bool = False,
    cwd: str | os.PathLike[str] | None = None,
) -> SecretValue:
    _bootstrap_default_registry()
    parsed: ParsedSecretRef = parse_secret_ref(secret_ref)

    cwd_path = Path(cwd).resolve() if cwd is not None else None

    if parsed.scheme is SecretScheme.env and not strict:
        provider = get_provider(parsed.scheme)
        try:
            return provider.resolve(path=parsed.path)
        except SecretNotFoundError:
            return SecretValue(parsed.original)

    provider = get_provider(parsed.scheme)
    if parsed.scheme is SecretScheme.file:
        if isinstance(provider, FileSecrets):
            return provider.resolve(path=parsed.path, cwd=cwd_path)
    return provider.resolve(path=parsed.path)


def resolve_secret_refs(
    secret_refs: Mapping[str, str],
    *,
    strict: bool = False,
    cwd: str | os.PathLike[str] | None = None,
) -> dict[str, SecretValue]:
    if not isinstance(secret_refs, Mapping):
        raise SecretsError(
            message=(
                f"resolve_secret_refs expected Mapping[str,str], got "
                f"{type(secret_refs).__name__}"
            ),
            error_code="SECRETS_BATCH_TYPE_ERROR",
            context={"type": type(secret_refs).__name__},
        )
    out: dict[str, SecretValue] = {}
    for name, ref in secret_refs.items():
        out[name] = resolve_secret_ref(ref, strict=strict, cwd=cwd)
    return out
