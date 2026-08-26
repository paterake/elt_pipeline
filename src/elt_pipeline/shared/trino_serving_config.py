"""Pure, unit-testable builders for Trino serving config.properties lines.

Mirrors the build_spark_fs_hadoop_configs() pattern: zero JVM, zero shell,
zero env reads on the hot path — every input is an explicit kwarg so tests
can cover every branch deterministically.

The actual run_trino.sh write_configs() bash function mirrors the same
logic; this module is the single source of truth for the test surface.
"""
from __future__ import annotations

import secrets
from pathlib import Path
from typing import Any

from elt_pipeline.shared.errors import (
    ErrorCategory,
    PipelineError,
)

__all__ = [
    "TrinoHttpAuthType",
    "build_trino_serving_configs",
    "generate_trino_internal_shared_secret",
]

TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS = "TRINO_PASSWORD_AUTH_FILE_MISSING"
TRINO_KERBEROS_AUTH_INCOMPLETE = "TRINO_KERBEROS_AUTH_INCOMPLETE"
TRINO_SSL_KEYSTORE_REQUIRED = "TRINO_SSL_KEYSTORE_REQUIRED"


class TrinoHttpAuthType:
    """Valid Trino http-server.authentication.type values."""

    NONE = "none"
    PASSWORD = "password"
    CERTIFICATE = "certificate"
    KERBEROS = "kerberos"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    FORM = "form"

    _ALL = {NONE, PASSWORD, CERTIFICATE, KERBEROS, JWT, OAUTH2, FORM}
    _REQUIRES_SHARED_SECRET = _ALL - {NONE}


def _is_auth_enabled(auth_type: str | None) -> bool:
    if auth_type is None:
        return False
    at = auth_type.strip().lower()
    return at not in {"", "none", "disabled", "insecure"}


def generate_trino_internal_shared_secret() -> str:
    """16-byte cryptographically-random base64 token for internal-communication.shared-secret.

    Mirrors the bash `dd if=/dev/urandom bs=1 count=16 | base64` generator in
    run_trino.sh so tests can inject a deterministic value.
    """
    return secrets.token_urlsafe(16)


def _validate_password_auth(
    *,
    password_file_path: str | None,
    _enforce_file_existence: bool = True,
) -> None:
    """Fail-fast when password auth is configured but no password file."""
    pfp = (password_file_path or "").strip()
    if not pfp:
        raise PipelineError(
            message=(
                "Trino password authentication requires a password-file path. "
                "Set ELT_PIPELINE_TRINO_PASSWORD_FILE_PATH or the YAML key "
                "trino_serving.password_file_path to an absolute path of a "
                "Trino auth-properties (htpasswd-style) file."
            ),
            error_code=TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS,
            error_category=ErrorCategory.config_error,
        )
    if _enforce_file_existence:
        p = Path(pfp)
        if not p.is_file():
            raise PipelineError(
                message=(
                    f"Trino password file not found: {pfp}. The configured path "
                    "must exist before Trino starts; create it with `htpasswd` "
                    "or a compatible tool (username:password entries with BCrypt)."
                ),
                error_code=TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS,
                error_category=ErrorCategory.config_error,
                context={
                    "configured_path": str(p.resolve()) if p.is_absolute() else pfp
                },
            )


def _validate_kerberos_auth(
    *,
    kerberos_principal: str | None,
    kerberos_keytab: str | None,
) -> None:
    """Fail-fast when Kerberos auth is missing principal or keytab."""
    kp = (kerberos_principal or "").strip()
    kt = (kerberos_keytab or "").strip()
    missing = []
    if not kp:
        missing.append("kerberos_principal (ELT_PIPELINE_TRINO_KERBEROS_PRINCIPAL)")
    if not kt:
        missing.append("kerberos_keytab (ELT_PIPELINE_TRINO_KERBEROS_KEYTAB)")
    if missing:
        raise PipelineError(
            message=(
                "Trino Kerberos authentication requires all of: "
                f"{', '.join(missing)}."
            ),
            error_code=TRINO_KERBEROS_AUTH_INCOMPLETE,
            error_category=ErrorCategory.config_error,
            context={"missing_fields": missing},
        )


def _validate_ssl_keystore_for_https(
    *,
    https_enabled: bool,
    ssl_keystore_path: str | None,
    ssl_keystore_password: str | None,
    _enforce_file_existence: bool = True,
) -> None:
    """Fail-fast when HTTPS is enabled but keystore is missing."""
    if not https_enabled:
        return
    ksp = (ssl_keystore_path or "").strip()
    kspw = (ssl_keystore_password or "").strip()
    missing = []
    if not ksp:
        missing.append("ssl_keystore_path (ELT_PIPELINE_TRINO_SSL_KEYSTORE_PATH)")
    if not kspw:
        missing.append("ssl_keystore_password (ELT_PIPELINE_TRINO_SSL_KEYSTORE_PASSWORD)")
    if missing:
        raise PipelineError(
            message=(
                "Trino HTTPS (TLS) requires a keystore. Missing required: "
                f"{', '.join(missing)}."
            ),
            error_code=TRINO_SSL_KEYSTORE_REQUIRED,
            error_category=ErrorCategory.config_error,
            context={"missing_fields": missing},
        )
    if ksp and _enforce_file_existence:
        p = Path(ksp)
        if not p.is_file():
            raise PipelineError(
                message=(
                    f"Trino SSL keystore file not found: {ksp}. Generate one "
                    "with `keytool -genkeypair -alias trino -keyalg RSA -keystore …` "
                    "or mount your enterprise PKI-issued PKCS#12 / JKS keystore."
                ),
                error_code=TRINO_SSL_KEYSTORE_REQUIRED,
                error_category=ErrorCategory.config_error,
                context={
                    "configured_path": str(p.resolve()) if p.is_absolute() else ksp
                },
            )


def build_trino_serving_configs(
    *,
    coordinator: bool = True,
    include_coordinator: bool = True,
    http_port: int = 8080,
    trino_host: str = "127.0.0.1",
    node_environment: str = "elt_pipeline_iceberg",
    http_auth_type: str | None = None,
    https_enabled: bool = False,
    https_port: int = 8443,
    ssl_keystore_path: str | None = None,
    ssl_keystore_password: str | None = None,
    ssl_truststore_path: str | None = None,
    ssl_truststore_password: str | None = None,
    password_file_path: str | None = None,
    krb5_conf: str | None = None,
    kerberos_principal: str | None = None,
    kerberos_keytab: str | None = None,
    internal_shared_secret: str | None = None,
    validate: bool = True,
    _enforce_file_existence: bool = True,
) -> dict[str, Any]:
    """Build a flat dict of Trino config.properties key → value.

    Pure — no env reads, no file I/O (unless validate=True with
    _enforce_file_existence=True, in which case password-file and keystore
    paths are checked with Path.is_file()).

    Parameters are intentionally explicit so tests can cover every branch.
    The run_trino.sh bash function mirrors every conditional branch here
    so the two implementations are byte-for-byte equivalent in output.
    """
    cfg: dict[str, Any] = {}

    at = (http_auth_type or "").strip()
    if at.lower() in {"", "none", "disabled", "insecure"}:
        at = ""

    if validate:
        at_lc = at.lower()
        if at_lc == TrinoHttpAuthType.PASSWORD:
            _validate_password_auth(
                password_file_path=password_file_path,
                _enforce_file_existence=_enforce_file_existence,
            )
        elif at_lc == TrinoHttpAuthType.KERBEROS:
            _validate_kerberos_auth(
                kerberos_principal=kerberos_principal,
                kerberos_keytab=kerberos_keytab,
            )
        _validate_ssl_keystore_for_https(
            https_enabled=https_enabled,
            ssl_keystore_path=ssl_keystore_path,
            ssl_keystore_password=ssl_keystore_password,
            _enforce_file_existence=_enforce_file_existence,
        )

    cfg["coordinator"] = "true" if coordinator else "false"
    cfg["node-scheduler.include-coordinator"] = (
        "true" if include_coordinator else "false"
    )
    cfg["http-server.http.port"] = str(http_port)
    cfg["node.internal-address"] = trino_host
    cfg["node.environment"] = node_environment

    if at:
        cfg["http-server.authentication.type"] = at

    if https_enabled:
        cfg["http-server.https.enabled"] = "true"
        cfg["http-server.https.port"] = str(https_port)
        ksp = (ssl_keystore_path or "").strip()
        if ksp:
            cfg["http-server.https.keystore.path"] = ksp
        kspw = (ssl_keystore_password or "").strip()
        if kspw:
            cfg["http-server.https.keystore.key"] = kspw
        tsp = (ssl_truststore_path or "").strip()
        if tsp:
            cfg["http-server.https.truststore.path"] = tsp
        tspw = (ssl_truststore_password or "").strip()
        if tspw:
            cfg["http-server.https.truststore.key"] = tspw

    at_lc = at.lower()
    if at_lc == TrinoHttpAuthType.PASSWORD:
        pfp = (password_file_path or "").strip()
        if pfp:
            cfg["http-server.authentication.password.file"] = pfp
    elif at_lc == TrinoHttpAuthType.KERBEROS:
        kp = (kerberos_principal or "").strip()
        kt = (kerberos_keytab or "").strip()
        kc = (krb5_conf or "").strip()
        if kp:
            cfg["http-server.authentication.krb5.service-name"] = kp.split("/")[0].split("@")[0]
            cfg["http-server.authentication.krb5.principal-host"] = (
                kp.split("/")[1].split("@")[0] if "/" in kp else trino_host
            )
        if kt:
            cfg["http-server.authentication.krb5.keytab"] = kt
        if kc:
            cfg["http-server.authentication.krb5.config"] = kc
        cfg["http-server.authentication.krb5.user-mapping.pattern"] = "(.*)@.*"

    if _is_auth_enabled(at):
        secret = internal_shared_secret or generate_trino_internal_shared_secret()
        cfg["internal-communication.shared-secret"] = f"eltp-{secret}"

    return cfg
