from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

_REDACTED_PLACEHOLDER = "[REDACTED]"
_SECRET_MAX_PEEK_BYTES = 0

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+\-_.]*)://(.*)$", re.DOTALL)


class SecretValue(str):
    __slots__ = ()

    def __repr__(self) -> str:
        return _REDACTED_PLACEHOLDER

    def __str__(self) -> str:
        return str.__str__(self)

    def __format__(self, format_spec: str) -> str:
        if format_spec in {"r", "!r"}:
            return _REDACTED_PLACEHOLDER
        return str.__format__(self, format_spec)

    def __reduce_ex__(self, protocol: int) -> tuple[Any, ...]:
        return (SecretValue, (str.__str__(self),))


def redact_secret(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if not text:
        return ""
    return _REDACTED_PLACEHOLDER


class SecretScheme(str, Enum):
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
    from ._errors import SecretRefSyntaxError

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
