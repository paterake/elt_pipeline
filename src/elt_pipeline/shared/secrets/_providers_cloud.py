from __future__ import annotations

import json
import os
from typing import Any

from ._errors import SecretNotFoundError, SecretRefSyntaxError, SecretsError
from ._models import SecretScheme, SecretValue, redact_secret


def _raise_sdk_missing(*, scheme: SecretScheme, sdk_package: str) -> None:
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

        return SecretValue(json.dumps(data, sort_keys=True))
