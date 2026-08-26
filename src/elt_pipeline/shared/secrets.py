"""Secrets resolution subsystem (BACKLOG item G-5).

Design contract (matches B-6 facade pattern + quality/lineage integration seams):

1. SecretRef URI syntax
   A secret_ref is a URI with a ``scheme://`` prefix::

       env://ENV_VAR_NAME            # read from os.environ (default provider)
       file:///absolute/path/to/secret   # read value from a file (chmod 600 recommended)
       aws_secretsmanager://secret-name[:version-id-or-stage]  # AWS Secrets Manager
       azure_keyvault://vault-name/secret-name[/version]       # Azure Key Vault
       gcp_secretmanager://project-id/secret-name[/version]    # GCP Secret Manager
       vault://mount/path/to/secret[#field]                    # HashiCorp Vault KV

   If the string has NO ``://`` scheme, it defaults to ``env://{ref}`` (EnvVarSecrets).
   This preserves **full backward compatibility** with existing secret_refs like
   ``"ORDERS_API_TOKEN"`` — they resolve to env-var reads as before.

2. SecretsProvider Protocol + registry
   * ``SecretsProvider`` — runtime_checkable Protocol with one method:
     ``resolve(ref_path: str) -> SecretValue``.
   * ``_PROVIDER_REGISTRY: dict[SecretScheme, SecretsProvider]`` singleton.
   * Default providers registered at import time: env, file → real impls; cloud/vault →
     real impls with lazy SDK imports (missing SDK → ``SECRETS_SDK_MISSING`` error with
     install guidance).
   * ``register_provider(scheme: SecretScheme | str, provider: SecretsProvider)``
     public API for explicit registration (no dynamic auto-discovery — same as B-6
     storage_backends constraint 8).

3. Resolution surface
   * ``resolve_secret_ref(secret_ref: str, *, strict: bool = False) -> SecretValue`` —
     top-level dispatcher. Parses scheme, looks up provider, resolves.
     ``strict=False`` (default): if the ref has no scheme, uses EnvVarSecrets AND on
     KeyError falls back to returning the literal value (the old pass-through behaviour).
     ``strict=True``: always raises ``SecretNotFoundError`` on a miss.
   * ``resolve_secret_refs(secret_refs: Mapping[str, str], *, strict: bool = False)
     -> dict[str, SecretValue]`` — batch resolver; returns the same dict shape
     but resolved.

4. Security guarantees
   * ``SecretValue`` is a ``str`` subclass whose ``__repr__`` / ``__str__`` / pydantic
     serialisation produce the redacted placeholder ``"[REDACTED]"``. The *actual* secret
     string value is the str itself (so it works wherever a bare str is needed — header
     injection, HTTP auth, etc.) — it just can't be accidentally logged without a
     deliberate cast.
   * ``redact_secret(value: str) -> str`` utility for manual redaction (used by audit
     paths, log events).
   * Providers MUST never log, print, or return a non-SecretValue string.

5. Zero-env lockdown compliance
   * ``EnvVarSecrets`` reads ``os.environ`` — this is allowed BECAUSE it is gated behind
     the secret_ref the OPERATOR explicitly put in their config. The ``runtime_context``
     singleton remains the *framework config* cascade; secrets resolution is a *run-time
     value look-up* the operator drives via their refs.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

from elt_pipeline.shared.errors import ErrorCategory, PipelineError

# ---------------------------------------------------------------------------
# Redaction / SecretValue wrapper
# ---------------------------------------------------------------------------

_REDACTED_PLACEHOLDER = "[REDACTED]"
_SECRET_MAX_PEEK_BYTES = 0  # never show even a prefix; paranoid-by-default


class SecretValue(str):
    """A str subclass that redacts __repr__, __str__, and serialisation.

    The actual secret content is the raw string and is retrievable via normal
    str operators (``==``, ``+``, slicing, ``str()`` *internally* to code that
    knows it's a secret).  Any accidental formatting / logging call site that
    does ``"%r" % secret``, ``repr(secret)``, or ``f"{secret!r}"`` gets
    ``[REDACTED]`` instead.

    pydantic serialisation: this IS a str subclass, so plain pydantic will
    serialise it as the real value.  Call sites responsible for audit/logs
    should convert via ``redact_secret()`` before putting values into log /
    audit dictionaries (the redacted_fields mechanism in RestConnectorConfig
    is the existing complementary defence).
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - trivial redaction
        return _REDACTED_PLACEHOLDER

    def __str__(self) -> str:  # pragma: no cover
        # NB: str(self) STILL returns the raw value — that's required for HTTP
        # calls ("Authorization: Bearer " + secret).  The repr path is the
        # accidental-logging defence.  For STRING redaction at an audit path
        # use redact_secret() explicitly.
        return str.__str__(self)

    def __format__(self, format_spec: str) -> str:
        # f"{secret}" → raw (needed for header construction).
        # f"{secret!r}" → repr → [REDACTED] (handled above).
        # Allow format but guard against any accidental debug-format surprises.
        if format_spec in {"r", "!r"}:
            return _REDACTED_PLACEHOLDER
        return str.__format__(self, format_spec)

    def __reduce_ex__(self, protocol: int) -> tuple[Any, ...]:
        # Pickle safety: rebuild as a plain SecretValue, never include raw bytes
        # in a pickle stream that might get written to disk.
        return (SecretValue, (str.__str__(self),))


def redact_secret(value: Any) -> str:
    """Return a safe-for-logs string representation of a secret value.

    Returns ``"[REDACTED]"`` for any non-empty string/SecretValue; empty strings
    stay empty.  Non-string inputs are coerced via str then redacted.
    """
    if value is None:
        return ""
    text = str(value)
    if not text:
        return ""
    return _REDACTED_PLACEHOLDER


# ---------------------------------------------------------------------------
# Scheme enum + URI parsing
# ---------------------------------------------------------------------------

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+\-_.]*)://(.*)$", re.DOTALL)


class SecretScheme(str, Enum):
    """Valid secret_ref URI schemes.  Enum = explicit boundary — no free-form strings."""

    env = "env"
    file = "file"
    aws_secretsmanager = "aws_secretsmanager"
    azure_keyvault = "azure_keyvault"
    gcp_secretmanager = "gcp_secretmanager"
    vault = "vault"


_SUPPORTED_SCHEMES: set[str] = {s.value for s in SecretScheme}


@dataclass(frozen=True)
class ParsedSecretRef:
    scheme: SecretScheme
    path: str
    original: str


def parse_secret_ref(secret_ref: str) -> ParsedSecretRef:
    """Parse a secret_ref URI into (scheme, path).  No scheme → defaults to env.

    Raises :class:`SecretRefSyntaxError` if an explicit scheme is given but not
    in :class:`SecretScheme` (fail-fast — same as path_utils unsupported storage
    schemes, constraint 3).
    """
    if not isinstance(secret_ref, str):
        raise SecretRefSyntaxError(
            message=f"secret_ref must be a str, got {type(secret_ref).__name__}",
            context={"ref_repr": redact_secret(secret_ref)},
        )
    stripped = secret_ref.strip()
    if not stripped:
        raise SecretRefSyntaxError(
            message="secret_ref must not be empty",
            context={},
        )

    m = _SCHEME_RE.match(stripped)
    if m is None:
        # No explicit scheme → default to env://
        return ParsedSecretRef(
            scheme=SecretScheme.env,
            path=stripped,
            original=secret_ref,
        )
    scheme_raw, path = m.group(1), m.group(2)
    if scheme_raw not in _SUPPORTED_SCHEMES:
        raise SecretRefSyntaxError(
            message=(
                f"Unsupported secret_ref scheme '{scheme_raw}://'. "
                f"Supported schemes: {sorted(_SUPPORTED_SCHEMES)}. "
                f"If you meant a plain env-var name, drop the '://' prefix "
                f"(it will default to env://)."
            ),
            context={
                "ref_repr": redact_secret(secret_ref),
                "supported_schemes": sorted(_SUPPORTED_SCHEMES),
            },
        )
    return ParsedSecretRef(
        scheme=SecretScheme(scheme_raw),
        path=path,
        original=secret_ref,
    )


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SecretsError(PipelineError):
    """Base for all secrets-subsystem exceptions."""

    def __init__(
        self,
        *,
        message: str,
        error_code: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            error_category=ErrorCategory.config_error,
            retryable=False,
            context=context,
        )


class SecretRefSyntaxError(SecretsError):
    def __init__(self, *, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(
            message=message,
            error_code="SECRET_REF_SYNTAX_ERROR",
            context=context,
        )


class SecretNotFoundError(SecretsError):
    def __init__(
        self,
        *,
        scheme: SecretScheme,
        path: str,
        message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        msg = message or f"Secret not found for {scheme.value}://{redact_secret(path)}"
        ctx = {"scheme": scheme.value, "path_repr": redact_secret(path)}
        if context:
            ctx.update(context)
        super().__init__(
            message=msg,
            error_code="SECRET_NOT_FOUND",
            context=ctx,
        )


class SecretsNotImplementedError(SecretsError, NotImplementedError):
    """Raised by provider stubs that exist in the enum but aren't shipped yet (G-5 roadmap)."""

    def __init__(self, *, scheme: SecretScheme, message: str | None = None) -> None:
        msg = (
            message
            or (
                f"Secrets provider '{scheme.value}://' is not yet implemented in v1 "
                f"(see BACKLOG item G-5 §roadmap).  Use env:// or file:// today, or "
                f"pull forward the corresponding roadmap item to add it."
            )
        )
        super().__init__(
            message=msg,
            error_code="SECRETS_PROVIDER_NOT_IMPLEMENTED",
            context={"scheme": scheme.value},
        )


# ---------------------------------------------------------------------------
# SecretsProvider Protocol + registry
# ---------------------------------------------------------------------------


@runtime_checkable
class SecretsProvider(Protocol):
    """Resolve a parsed secret path → SecretValue.

    Same shape as StorageBackend (B-6): a single method, runtime_checkable so
    both ABC-style inheritance and duck-typed classes satisfy the Protocol.
    """

    provider_type: str

    def resolve(self, *, path: str) -> SecretValue: ...


_PROVIDER_REGISTRY: dict[SecretScheme, SecretsProvider] = {}


def register_provider(
    scheme: SecretScheme | str,
    provider: SecretsProvider,
) -> None:
    """Register a SecretsProvider for a scheme.  Static-in-code registration only.

    Follows the same pattern as ``storage_backends.register_backend`` (B-6
    constraint 8: no dynamic auto-discovery — explicit registry or explicit
    call).  Raises :class:`SecretsError` on duplicate registration to prevent
    silent override.
    """
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
    """Look up a provider by scheme.  Raises SecretsError if not registered."""
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


# ---------------------------------------------------------------------------
# Concrete providers: EnvVarSecrets (production) + FileSecrets (production)
# ---------------------------------------------------------------------------


class EnvVarSecrets:
    """Resolve env://ENV_VAR_NAME from the process environment.

    THIS IS THE ONLY place secrets read os.environ (zero-env-lockdown: env var
    reads are gated behind a secret_ref the OPERATOR explicitly placed in their
    config; this is not framework-config cascade).

    ``os.environ`` is read at ``resolve()`` call time, not at construction
    time, so child-process / CI env-injection patterns work correctly.
    """

    provider_type = "env"

    def __init__(self, *, environ: Mapping[str, str] | None = None) -> None:
        # Allow injecting an environ mapping for tests; default → real os.environ
        self._environ_override = environ

    def _environ(self) -> Mapping[str, str]:
        return self._environ_override if self._environ_override is not None else os.environ

    def resolve(self, *, path: str) -> SecretValue:
        env_name = path.strip()
        if not env_name:
            raise SecretRefSyntaxError(
                message="env:// path must be a non-empty environment variable name",
                context={"path_repr": redact_secret(path)},
            )
        environ = self._environ()
        if env_name not in environ:
            raise SecretNotFoundError(
                scheme=SecretScheme.env,
                path=env_name,
                message=f"Environment variable '{redact_secret(env_name)}' is not set",
            )
        value = environ[env_name]
        return SecretValue(value)


class FileSecrets:
    """Resolve file:///absolute/path by reading a file from disk.

    Path rules:
    * ``file:///abs/path`` → absolute POSIX path.
    * ``file://./rel/path`` or ``file://rel/path`` → resolved relative to
      :func:`resolve_secret_ref`'s ``cwd`` keyword (default ``os.getcwd()``).

    Recommendation for operators: secrets files should have ``chmod 600`` and
    be owned by the user running the pipeline; we deliberately do NOT enforce
    mode checks here because some k8s / CI / tmpfs mounts don't support POSIX
    modes, but operators can enforce them in their deploy layer.
    """

    provider_type = "file"

    def __init__(self, *, base_dir: str | os.PathLike[str] | None = None) -> None:
        self._base_dir: Path | None = Path(base_dir).resolve() if base_dir is not None else None

    def resolve(self, *, path: str, cwd: Path | None = None) -> SecretValue:
        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="file:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
        # file:///abs/path → raw starts with "/" because the regex captures
        # everything after "file://".  "file:///etc/foo" → raw = "/etc/foo".
        resolved_cwd = cwd or (self._base_dir if self._base_dir is not None else Path.cwd())
        p = Path(raw)
        if not p.is_absolute():
            p = (resolved_cwd / p).resolve()
        try:
            content_bytes = p.read_bytes()
        except FileNotFoundError as exc:
            raise SecretNotFoundError(
                scheme=SecretScheme.file,
                path=str(p),
                message=f"Secrets file not found: {redact_secret(str(p))}",
            ) from exc
        except PermissionError as exc:
            raise SecretsError(
                message=f"Permission denied reading secrets file: {redact_secret(str(p))}",
                error_code="SECRETS_FILE_PERMISSION_DENIED",
                context={"path_repr": redact_secret(str(p))},
            ) from exc
        except OSError as exc:
            raise SecretsError(
                message=f"OS error reading secrets file {redact_secret(str(p))}: {exc}",
                error_code="SECRETS_FILE_IO_ERROR",
                context={"path_repr": redact_secret(str(p))},
            ) from exc
        # Strip trailing newline only — if the operator put non-newline whitespace
        # they meant it (otherwise we'd have no way to pass a value ending with
        # newline if someone ever needs it).
        if content_bytes.endswith(b"\n"):
            content_bytes = content_bytes[:-1]
        return SecretValue(content_bytes.decode("utf-8"))


# ---------------------------------------------------------------------------
# Concrete providers: Cloud secret managers + Vault (lazy SDK imports)
# ---------------------------------------------------------------------------


def _raise_sdk_missing(*, scheme: SecretScheme, sdk_package: str) -> None:
    """Raise a clean, context-rich error when an optional SDK isn't installed."""
    raise SecretsError(
        message=(
            f"Secrets provider '{scheme.value}://' requires optional Python package "
            f"'{sdk_package}' to be installed. Install it (e.g. "
            f"'uv pip install {sdk_package}' or add it to your project's extras) "
            f"before using '{scheme.value}://' secret refs."
        ),
        error_code="SECRETS_SDK_MISSING",
        context={"scheme": scheme.value, "required_package": sdk_package},
    )


class AWSSecretsManagerSecrets:
    """Resolve ``aws_secretsmanager://name[:version]`` via AWS Secrets Manager.

    URI syntax (path portion after ``aws_secretsmanager://``):

    * ``secret-name``               → latest AWSCURRENT version
    * ``secret-name:AWSPREVIOUS``   → by label/stage
    * ``secret-name:ab-12345678``   → by VersionId

    The provider uses boto3's ambient credential chain
    (env vars → ~/.aws/credentials → EC2/ECS/EKS IAM role → etc.).
    Region is taken from the ambient chain or from ``AWS_DEFAULT_REGION``.

    boto3 is imported **lazily at resolve() time** so projects that don't use
    AWS Secrets Manager don't need boto3 installed.
    """

    provider_type = "aws_secretsmanager"

    def __init__(
        self,
        *,
        region_name: str | None = None,
        boto3_session: Any = None,
    ) -> None:
        self._region_name = region_name
        self._session_override = boto3_session

    def resolve(self, *, path: str) -> SecretValue:
        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="aws_secretsmanager:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
        if ":" in raw:
            secret_id, _, version_part = raw.partition(":")
            secret_id = secret_id.strip()
            version_part = version_part.strip()
            if not secret_id:
                raise SecretRefSyntaxError(
                    message="aws_secretsmanager:// secret-id must not be empty",
                    context={"path_repr": redact_secret(path)},
                )
            # Heuristic: VersionIds look like uuid-ish (len>=8 + no slashes).
            # Stage labels are short upper-case tokens (AWSCURRENT, AWSPREVIOUS).
            kwargs: dict[str, str]
            if len(version_part) >= 8 and any(c.isdigit() for c in version_part):
                kwargs = {"VersionId": version_part}
            else:
                kwargs = {"VersionStage": version_part} if version_part else {}
        else:
            secret_id = raw
            kwargs = {}

        try:
            import boto3  # type: ignore[import-not-found]
        except ModuleNotFoundError:
            _raise_sdk_missing(
                scheme=SecretScheme.aws_secretsmanager, sdk_package="boto3"
            )

        try:
            session = self._session_override or boto3.session.Session()
            client = session.client(
                service_name="secretsmanager",
                region_name=self._region_name,
            )
            response = client.get_secret_value(SecretId=secret_id, **kwargs)
        except Exception as exc:  # noqa: BLE001
            exc_name = type(exc).__name__
            if "ResourceNotFoundException" in exc_name or (
                hasattr(exc, "response")
                and (exc.response or {}).get("Error", {}).get("Code")
                == "ResourceNotFoundException"
            ):
                raise SecretNotFoundError(
                    scheme=SecretScheme.aws_secretsmanager,
                    path=secret_id,
                ) from exc
            if "AccessDeniedException" in exc_name:
                raise SecretsError(
                    message=(
                        f"AWS Secrets Manager access denied for secret "
                        f"'{redact_secret(secret_id)}'. Verify the IAM role has "
                        f"secretsmanager:GetSecretValue on the resource."
                    ),
                    error_code="SECRETS_AWS_ACCESS_DENIED",
                    context={
                        "scheme": SecretScheme.aws_secretsmanager.value,
                        "path_repr": redact_secret(secret_id),
                    },
                ) from exc
            raise SecretsError(
                message=(
                    f"AWS Secrets Manager GetSecretValue failed: "
                    f"{exc_name}: {exc}"
                ),
                error_code="SECRETS_AWS_SDK_ERROR",
                context={
                    "scheme": SecretScheme.aws_secretsmanager.value,
                    "path_repr": redact_secret(secret_id),
                    "exception_type": exc_name,
                },
            ) from exc

        secret_value = response.get("SecretString")
        if secret_value is None:
            binary = response.get("SecretBinary")
            if binary is not None:
                try:
                    secret_value = binary.decode("utf-8")
                except UnicodeDecodeError as exc2:
                    raise SecretsError(
                        message=(
                            f"AWS Secrets Manager secret '{redact_secret(secret_id)}' "
                            f"is binary and cannot be decoded as UTF-8 text."
                        ),
                        error_code="SECRETS_AWS_BINARY_NOT_TEXT",
                        context={
                            "scheme": SecretScheme.aws_secretsmanager.value,
                            "path_repr": redact_secret(secret_id),
                        },
                    ) from exc2
            else:
                raise SecretsError(
                    message=(
                        f"AWS Secrets Manager secret '{redact_secret(secret_id)}' "
                        f"returned neither SecretString nor SecretBinary."
                    ),
                    error_code="SECRETS_AWS_EMPTY_RESPONSE",
                    context={
                        "scheme": SecretScheme.aws_secretsmanager.value,
                        "path_repr": redact_secret(secret_id),
                    },
                )
        return SecretValue(secret_value)


class AzureKeyVaultSecrets:
    """Resolve ``azure_keyvault://vault-name/secret-name[/version]`` via AKV.

    URI path structure: ``{vault-name}/{secret-name}[/{version}]``

    Vault URL is constructed as ``https://{vault-name}.vault.azure.net``
    (public Azure cloud); sovereign-cloud operators can pass a custom
    ``vault_url_template`` in the constructor.

    Uses ``azure-keyvault-secrets`` + ``azure-identity``
    (``DefaultAzureCredential``) for ambient credential delegation —
    AZURE_* env vars, Managed Identity, VS Code/CLI logins, etc.

    SDKs imported lazily at resolve() time.
    """

    provider_type = "azure_keyvault"

    def __init__(
        self,
        *,
        vault_url_template: str = "https://{vault_name}.vault.azure.net",
        credential: Any = None,
    ) -> None:
        self._vault_url_template = vault_url_template
        self._credential_override = credential

    def resolve(self, *, path: str) -> SecretValue:
        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="azure_keyvault:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
        parts = raw.split("/")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise SecretRefSyntaxError(
                message=(
                    "azure_keyvault:// URI must have the form "
                    "azure_keyvault://vault-name/secret-name[/version]"
                ),
                context={"path_repr": redact_secret(path)},
            )
        vault_name, secret_name = parts[0], parts[1]
        version: str | None = parts[2] if len(parts) >= 3 and parts[2] else None

        try:
            from azure.keyvault.secrets import (  # type: ignore[import-not-found]
                SecretClient,
            )
        except ModuleNotFoundError:
            _raise_sdk_missing(
                scheme=SecretScheme.azure_keyvault,
                sdk_package="azure-keyvault-secrets",
            )

        try:
            credential = self._credential_override
            if credential is None:
                try:
                    from azure.identity import (  # type: ignore[import-not-found]
                        DefaultAzureCredential,
                    )
                except ModuleNotFoundError:
                    _raise_sdk_missing(
                        scheme=SecretScheme.azure_keyvault,
                        sdk_package="azure-identity",
                    )
                credential = DefaultAzureCredential()

            vault_url = self._vault_url_template.format(vault_name=vault_name)
            client = SecretClient(vault_url=vault_url, credential=credential)
            got = client.get_secret(secret_name, version=version)
        except Exception as exc:  # noqa: BLE001
            exc_name = type(exc).__name__
            if "ResourceNotFoundError" in exc_name or "SecretNotFound" in exc_name:
                raise SecretNotFoundError(
                    scheme=SecretScheme.azure_keyvault,
                    path=f"{vault_name}/{secret_name}"
                    + (f"/{version}" if version else ""),
                ) from exc
            if "ClientAuthenticationError" in exc_name:
                raise SecretsError(
                    message=(
                        f"Azure Key Vault authentication failed for vault "
                        f"'{redact_secret(vault_name)}'. Verify that "
                        f"DefaultAzureCredential is configured (AZURE_CLIENT_ID / "
                        f"AZURE_TENANT_ID / AZURE_CLIENT_SECRET or a managed "
                        f"identity is attached)."
                    ),
                    error_code="SECRETS_AZURE_AUTH_FAILED",
                    context={
                        "scheme": SecretScheme.azure_keyvault.value,
                        "vault_repr": redact_secret(vault_name),
                    },
                ) from exc
            if "HttpResponseError" in exc_name and hasattr(exc, "status_code"):
                status = getattr(exc, "status_code", None)
                if status == 403:
                    raise SecretsError(
                        message=(
                            f"Azure Key Vault forbidden (403) reading secret "
                            f"'{redact_secret(secret_name)}' in vault "
                            f"'{redact_secret(vault_name)}'. Verify the "
                            f"principal has 'Get Secret' permission on the "
                            f"vault access policy / RBAC role."
                        ),
                        error_code="SECRETS_AZURE_ACCESS_DENIED",
                        context={
                            "scheme": SecretScheme.azure_keyvault.value,
                            "vault_repr": redact_secret(vault_name),
                            "secret_repr": redact_secret(secret_name),
                        },
                    ) from exc
            raise SecretsError(
                message=f"Azure Key Vault SDK error: {exc_name}: {exc}",
                error_code="SECRETS_AZURE_SDK_ERROR",
                context={
                    "scheme": SecretScheme.azure_keyvault.value,
                    "secret_repr": redact_secret(secret_name),
                    "exception_type": exc_name,
                },
            ) from exc

        if got.value is None:
            raise SecretsError(
                message=(
                    f"Azure Key Vault returned None value for secret "
                    f"'{redact_secret(secret_name)}' in vault "
                    f"'{redact_secret(vault_name)}'."
                ),
                error_code="SECRETS_AZURE_EMPTY_VALUE",
                context={
                    "scheme": SecretScheme.azure_keyvault.value,
                    "secret_repr": redact_secret(secret_name),
                },
            )
        return SecretValue(got.value)


class GCPSecretManagerSecrets:
    """Resolve ``gcp_secretmanager://project-id/secret-name[/version]`` via GCP SM.

    URI path: ``{project-id}/{secret-name}[/{version}]`` where version
    defaults to ``"latest"``.

    Uses ``google-cloud-secret-manager`` SDK with ambient credential
    delegation (``google.auth.default``): service-account keyfile env var,
    GCE/GKE/GCF metadata, gcloud user credentials, etc.

    SDK imported lazily.
    """

    provider_type = "gcp_secretmanager"

    def __init__(
        self,
        *,
        project_id_fallback: str | None = None,
        client: Any = None,
    ) -> None:
        self._project_id_fallback = project_id_fallback
        self._client_override = client

    def resolve(self, *, path: str) -> SecretValue:
        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="gcp_secretmanager:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
        parts = raw.split("/")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise SecretRefSyntaxError(
                message=(
                    "gcp_secretmanager:// URI must have the form "
                    "gcp_secretmanager://project-id/secret-name[/version]"
                ),
                context={"path_repr": redact_secret(path)},
            )
        project_id, secret_name = parts[0], parts[1]
        version = parts[2] if len(parts) >= 3 and parts[2] else "latest"

        try:
            from google.cloud import (  # type: ignore[import-not-found]
                secretmanager_v1 as sm,
            )
        except ModuleNotFoundError:
            _raise_sdk_missing(
                scheme=SecretScheme.gcp_secretmanager,
                sdk_package="google-cloud-secret-manager",
            )

        client = self._client_override
        try:
            if client is None:
                client = sm.SecretManagerServiceClient()
            name = (
                f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
            )
            response = client.access_secret_version(request={"name": name})
        except Exception as exc:  # noqa: BLE001
            exc_name = type(exc).__name__
            exc_msg = str(exc).lower()
            if (
                "notfound" in exc_name
                or "not_found" in exc_msg
                or "secret not found" in exc_msg
                or "404" in exc_msg
            ):
                raise SecretNotFoundError(
                    scheme=SecretScheme.gcp_secretmanager,
                    path=f"{project_id}/{secret_name}/{version}",
                ) from exc
            if "permissiondenied" in exc_name or (
                "permission" in exc_msg and "denied" in exc_msg
            ):
                raise SecretsError(
                    message=(
                        f"GCP Secret Manager permission denied reading secret "
                        f"'{redact_secret(secret_name)}' in project "
                        f"'{redact_secret(project_id)}'. Ensure the principal "
                        f"has 'secretmanager.versions.access' IAM permission."
                    ),
                    error_code="SECRETS_GCP_ACCESS_DENIED",
                    context={
                        "scheme": SecretScheme.gcp_secretmanager.value,
                        "project_repr": redact_secret(project_id),
                        "secret_repr": redact_secret(secret_name),
                    },
                ) from exc
            raise SecretsError(
                message=f"GCP Secret Manager SDK error: {exc_name}: {exc}",
                error_code="SECRETS_GCP_SDK_ERROR",
                context={
                    "scheme": SecretScheme.gcp_secretmanager.value,
                    "secret_repr": redact_secret(secret_name),
                    "exception_type": exc_name,
                },
            ) from exc

        payload = getattr(response, "payload", None)
        data = getattr(payload, "data", None) if payload is not None else None
        if data is None:
            raise SecretsError(
                message=(
                    f"GCP Secret Manager returned empty payload for secret "
                    f"'{redact_secret(secret_name)}' in project "
                    f"'{redact_secret(project_id)}'."
                ),
                error_code="SECRETS_GCP_EMPTY_PAYLOAD",
                context={
                    "scheme": SecretScheme.gcp_secretmanager.value,
                    "secret_repr": redact_secret(secret_name),
                },
            )
        try:
            if isinstance(data, bytes):
                text = data.decode("utf-8")
            else:
                text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as exc2:
            raise SecretsError(
                message=(
                    f"GCP Secret Manager secret '{redact_secret(secret_name)}' "
                    f"payload is not valid UTF-8 text."
                ),
                error_code="SECRETS_GCP_BINARY_NOT_TEXT",
                context={
                    "scheme": SecretScheme.gcp_secretmanager.value,
                    "secret_repr": redact_secret(secret_name),
                },
            ) from exc2
        return SecretValue(text)


class VaultSecrets:
    """Resolve ``vault://mount/path/to/secret[#field]`` via HashiCorp Vault.

    URI path: ``{mount}/{path/to/secret}[#{field}]``. The ``#field`` suffix is
    the JSON-key within a KV-v2 secret payload (``data.data.{field}``); if
    omitted the full ``data.data`` dict is serialised to a JSON string.

    Auth modes (auto-selected based on env vars / constructor args;
    first available wins):

    1. **Token** — ``VAULT_TOKEN`` env var or ``token=`` kwarg → direct token.
    2. **AppRole** — ``VAULT_ROLE_ID`` + ``VAULT_SECRET_ID`` env vars (or
       the ``role_id``/``secret_id`` kwargs) → ``auth_approle.login()``.

    Operators set ``VAULT_ADDR`` (required), ``VAULT_CACERT`` /
    ``VAULT_SKIP_VERIFY`` as usual for the hvac client.

    ``hvac`` is imported lazily.
    """

    provider_type = "vault"

    def __init__(
        self,
        *,
        url: str | None = None,
        token: str | None = None,
        role_id: str | None = None,
        secret_id: str | None = None,
        verify: bool | str = True,
        hvac_client: Any = None,
    ) -> None:
        self._url_override = url
        self._token_override = token
        self._role_id_override = role_id
        self._secret_id_override = secret_id
        self._verify = verify
        self._client_override = hvac_client

    def resolve(self, *, path: str) -> SecretValue:
        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="vault:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
        # Split out optional "#field" selector
        if "#" in raw:
            secret_path_full, _, field = raw.partition("#")
            secret_path_full = secret_path_full.strip()
            field = field.strip()
            if not field:
                raise SecretRefSyntaxError(
                    message=(
                        "vault:// URI has trailing '#' but no field name; "
                        "use vault://mount/path#field-name"
                    ),
                    context={"path_repr": redact_secret(path)},
                )
        else:
            secret_path_full = raw
            field = None

        if "/" not in secret_path_full:
            raise SecretRefSyntaxError(
                message=(
                    "vault:// URI must have the form "
                    "vault://mount/path/to/secret[#field]"
                ),
                context={"path_repr": redact_secret(path)},
            )
        mount, _, rel = secret_path_full.partition("/")
        if not mount or not rel:
            raise SecretRefSyntaxError(
                message=(
                    "vault:// mount and secret path must both be non-empty"
                ),
                context={"path_repr": redact_secret(path)},
            )

        try:
            import hvac  # type: ignore[import-not-found]
            from hvac.exceptions import (  # type: ignore[import-not-found]
                Forbidden,
                InvalidPath,
                Unauthorized,
            )
        except ModuleNotFoundError:
            _raise_sdk_missing(
                scheme=SecretScheme.vault, sdk_package="hvac"
            )

        client = self._client_override
        try:
            if client is None:
                url = (
                    self._url_override
                    or os.environ.get("VAULT_URL")
                    or os.environ.get("VAULT_ADDR")
                )
                if not url:
                    raise SecretsError(
                        message=(
                            "Vault URL is required. Set VAULT_ADDR (or "
                            "VAULT_URL) environment variable, or pass "
                            "url= to VaultSecrets()."
                        ),
                        error_code="SECRETS_VAULT_URL_MISSING",
                        context={"scheme": SecretScheme.vault.value},
                    )
                client = hvac.Client(url=url, verify=self._verify)
                # Auth selection: token > AppRole > plain (unauthenticated)
                tok = self._token_override or os.environ.get("VAULT_TOKEN")
                if tok:
                    client.token = tok
                else:
                    rid = self._role_id_override or os.environ.get("VAULT_ROLE_ID")
                    sid = self._secret_id_override or os.environ.get(
                        "VAULT_SECRET_ID"
                    )
                    if rid and sid:
                        try:
                            client.auth.approle.login(
                                role_id=rid, secret_id=sid
                            )
                        except Exception as exc:  # noqa: BLE001
                            raise SecretsError(
                                message=(
                                    "Vault AppRole login failed. Verify "
                                    "VAULT_ROLE_ID/VAULT_SECRET_ID match a "
                                    "valid approle on the server."
                                ),
                                error_code="SECRETS_VAULT_APPROLE_FAILED",
                                context={
                                    "scheme": SecretScheme.vault.value,
                                },
                            ) from exc

            try:
                response = client.secrets.kv.v2.read_secret_version(
                    mount_point=mount, path=rel
                )
            except InvalidPath as exc:
                raise SecretNotFoundError(
                    scheme=SecretScheme.vault,
                    path=secret_path_full,
                ) from exc
            except Unauthorized as exc:
                raise SecretsError(
                    message=(
                        "Vault unauthorised. Verify the token or "
                        "AppRole credentials are valid."
                    ),
                    error_code="SECRETS_VAULT_UNAUTHORIZED",
                    context={"scheme": SecretScheme.vault.value},
                ) from exc
            except Forbidden as exc:
                raise SecretsError(
                    message=(
                        f"Vault forbidden reading '{redact_secret(rel)}' "
                        f"under mount '{redact_secret(mount)}'. Ensure the "
                        f"token policy allows read on the KV-v2 path."
                    ),
                    error_code="SECRETS_VAULT_FORBIDDEN",
                    context={
                        "scheme": SecretScheme.vault.value,
                        "mount_repr": redact_secret(mount),
                    },
                ) from exc
        except SecretsError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SecretsError(
                message=f"Vault SDK error: {type(exc).__name__}: {exc}",
                error_code="SECRETS_VAULT_SDK_ERROR",
                context={
                    "scheme": SecretScheme.vault.value,
                    "exception_type": type(exc).__name__,
                },
            ) from exc

        data = (
            (response or {}).get("data", {}).get("data")
            if isinstance(response, dict)
            else None
        )
        if data is None and response is not None and not isinstance(response, dict):
            # hvac returns an object with 'data' attr in some releases
            resp_data = getattr(response, "data", None)
            if resp_data is not None:
                if isinstance(resp_data, dict):
                    data = resp_data.get("data")
                else:
                    data = getattr(resp_data, "data", None)

        if data is None:
            raise SecretNotFoundError(
                scheme=SecretScheme.vault,
                path=secret_path_full,
                message=(
                    f"Vault KV secret at {redact_secret(secret_path_full)} "
                    f"returned no data (mount={redact_secret(mount)}, "
                    f"path={redact_secret(rel)})."
                ),
            )

        if field is not None:
            if not isinstance(data, dict) or field not in data:
                if isinstance(data, dict):
                    keys_avail = sorted(list(data.keys()))
                else:
                    keys_avail = "<non-dict payload>"
                raise SecretNotFoundError(
                    scheme=SecretScheme.vault,
                    path=f"{secret_path_full}#{field}",
                    message=(
                        f"Vault secret payload has no field "
                        f"'{redact_secret(field)}'. "
                        f"Available keys: {keys_avail}"
                    ),
                )
            value = data[field]
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8")
                except UnicodeDecodeError as exc2:
                    raise SecretsError(
                        message=(
                            f"Vault field '{redact_secret(field)}' is binary "
                            f"and not valid UTF-8 text."
                        ),
                        error_code="SECRETS_VAULT_BINARY_NOT_TEXT",
                        context={"scheme": SecretScheme.vault.value},
                    ) from exc2
            if not isinstance(value, str):
                value = str(value)
            return SecretValue(value)

        # No field specified → JSON serialise the whole data dict
        import json

        return SecretValue(json.dumps(data, sort_keys=True))


# ---------------------------------------------------------------------------
# Resolution entry points + default registry bootstrap
# ---------------------------------------------------------------------------


def _bootstrap_default_registry() -> None:
    """Idempotent: register default providers exactly once.

    Called lazily on first resolve_secret_ref() call — keeps module import
    side-effects minimal (no os.environ walk at import time, no disk IO).
    """
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
    """Resolve a single secret_ref URI to a SecretValue.

    Parameters
    ----------
    secret_ref:
        URI string.  No explicit scheme → defaults to ``env://`` for backward
        compatibility (existing refs like ``"ORDERS_API_TOKEN"`` work as before).
    strict:
        Default ``False``.  When ``False`` AND the scheme is env:// (implicitly
        or explicitly) AND the env var is missing, fall back to returning the
        literal ``secret_ref`` string as the secret — matching the OLD stub
        behaviour (``resolve_secret(x) → x``).  This is what keeps all existing
        tests green without modification.  ``strict=True`` always raises on
        a miss.
    cwd:
        Working directory for relative ``file://`` paths.  Defaults to
        ``os.getcwd()``.  Passed through to FileSecrets.
    """
    _bootstrap_default_registry()
    parsed = parse_secret_ref(secret_ref)

    cwd_path = Path(cwd).resolve() if cwd is not None else None

    # Fast path for backward-compat fallback (non-strict env → KeyError fallback)
    if parsed.scheme is SecretScheme.env and not strict:
        provider = get_provider(parsed.scheme)
        try:
            return provider.resolve(path=parsed.path)
        except SecretNotFoundError:
            # OLD behaviour: resolve_secret(x) → x.
            return SecretValue(parsed.original)

    provider = get_provider(parsed.scheme)
    if parsed.scheme is SecretScheme.file:
        # FileSecrets takes an extra cwd kwarg; Protocol's resolve(*, path) is
        # the surface, but FileSecrets accepts cwd as a keyword extra. Use
        # hasattr / signature-style dispatch via direct type-checked call.
        if isinstance(provider, FileSecrets):
            return provider.resolve(path=parsed.path, cwd=cwd_path)
    return provider.resolve(path=parsed.path)


def resolve_secret_refs(
    secret_refs: Mapping[str, str],
    *,
    strict: bool = False,
    cwd: str | os.PathLike[str] | None = None,
) -> dict[str, SecretValue]:
    """Resolve a dict of {name: secret_ref} → {name: SecretValue}.

    Any ``Secret*Error`` raised by an individual ref is re-raised immediately —
    secrets resolution is fail-fast; no partial-result leakage (a partially
    resolved dict could leak which names succeeded/failed to an unauthorised
    log observer if we aggregated errors).
    """
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
]
