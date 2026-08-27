from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from ._errors import SecretNotFoundError, SecretRefSyntaxError
from ._models import SecretScheme, SecretValue, redact_secret


class EnvVarSecrets:
    provider_type = "env"

    def __init__(self, *, environ: Mapping[str, str] | None = None) -> None:
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
    provider_type = "file"

    def __init__(self, *, base_dir: str | os.PathLike[str] | None = None) -> None:
        self._base_dir: Path | None = Path(base_dir).resolve() if base_dir is not None else None

    def resolve(self, *, path: str, cwd: Path | None = None) -> SecretValue:
        from ._errors import SecretsError

        raw = path.strip()
        if not raw:
            raise SecretRefSyntaxError(
                message="file:// path must not be empty",
                context={"path_repr": redact_secret(path)},
            )
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
        if content_bytes.endswith(b"\n"):
            content_bytes = content_bytes[:-1]
        return SecretValue(content_bytes.decode("utf-8"))
