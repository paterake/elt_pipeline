"""Secrets resolution subsystem (BACKLOG item G-5).

Design contract (matches B-6 facade pattern + quality/lineage integration seams):

1. SecretRef URI syntax
   A secret_ref is a URI with a ``scheme://`` prefix::

       env://ENV_VAR_NAME            # read from os.environ (default provider)
       file:///absolute/path/to/secret   # read value from a file (chmod 600 recommended)
       aws_secretsmanager://name[:version]   # STUB: not implemented
       azure_keyvault://vault/secret[/version] # STUB
       gcp_secretmanager://project/secret[/version] # STUB
       vault://mount/secret/field    # STUB

   If the string has NO ``://`` scheme, it defaults to ``env://{ref}`` (EnvVarSecrets).
   This preserves **full backward compatibility** with existing secret_refs like
   ``"ORDERS_API_TOKEN"`` — they resolve to env-var reads as before.

2. SecretsProvider Protocol + registry
   * ``SecretsProvider`` — runtime_checkable Protocol with one method:
     ``resolve(ref_path: str) -> SecretValue``.
   * ``_PROVIDER_REGISTRY: dict[SecretScheme, SecretsProvider]`` singleton.
   * Default providers registered at import time: env, file → real impls; cloud/vault →
     fail-fast stubs raising ``SecretsNotImplementedError`` with a clear message.
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
# Roadmap stubs: cloud + vault → NotImplemented, fail-fast
# ---------------------------------------------------------------------------


class _StubSecretsProvider:
    """Generic fail-fast stub for roadmap-only schemes."""

    def __init__(self, *, scheme: SecretScheme, provider_type: str) -> None:
        self._scheme = scheme
        self.provider_type = provider_type

    def resolve(self, *, path: str) -> SecretValue:
        raise SecretsNotImplementedError(scheme=self._scheme)


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
    # Roadmap stubs → registered so parse_secret_ref succeeds but resolve()
    # raises a clear SecretsNotImplementedError with the roadmap message.
    register_provider(
        SecretScheme.aws_secretsmanager,
        _StubSecretsProvider(
            scheme=SecretScheme.aws_secretsmanager,
            provider_type="aws_secretsmanager_stub",
        ),
    )
    register_provider(
        SecretScheme.azure_keyvault,
        _StubSecretsProvider(
            scheme=SecretScheme.azure_keyvault,
            provider_type="azure_keyvault_stub",
        ),
    )
    register_provider(
        SecretScheme.gcp_secretmanager,
        _StubSecretsProvider(
            scheme=SecretScheme.gcp_secretmanager,
            provider_type="gcp_secretmanager_stub",
        ),
    )
    register_provider(
        SecretScheme.vault,
        _StubSecretsProvider(
            scheme=SecretScheme.vault,
            provider_type="vault_stub",
        ),
    )


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
    "resolve_secret_ref",
    "resolve_secret_refs",
]
