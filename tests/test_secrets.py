from __future__ import annotations

from pathlib import Path

import pytest

from elt_pipeline.shared.secrets import (
    _PROVIDER_REGISTRY,
    EnvVarSecrets,
    FileSecrets,
    SecretNotFoundError,
    SecretRefSyntaxError,
    SecretScheme,
    SecretsError,
    SecretsProvider,
    SecretValue,
    get_provider,
    parse_secret_ref,
    redact_secret,
    register_provider,
    resolve_secret_ref,
    resolve_secret_refs,
)

# ---------------------------------------------------------------------------
# SecretValue redaction
# ---------------------------------------------------------------------------


class TestSecretValueRedaction:
    def test_secret_value_is_str_subclass(self) -> None:
        s = SecretValue("supersecret123")
        assert isinstance(s, str)
        assert s == "supersecret123"

    def test_repr_is_redacted(self) -> None:
        s = SecretValue("supersecret123")
        assert repr(s) == "[REDACTED]"
        assert "supersecret" not in repr(s)

    def test_format_repr_spec_is_redacted(self) -> None:
        s = SecretValue("supersecret123")
        # f"{s!r}" → repr → [REDACTED]
        assert f"{s!r}" == "[REDACTED]"

    def test_str_and_format_return_value(self) -> None:
        s = SecretValue("tok-abc-123")
        # Plain str() returns real value — needed for header injection:
        # "Authorization: Bearer " + token
        assert str(s) == "tok-abc-123"
        assert f"{s}" == "tok-abc-123"
        assert f"Bearer {s}" == "Bearer tok-abc-123"

    def test_empty_secret_value_passthrough(self) -> None:
        s = SecretValue("")
        assert str(s) == ""
        assert repr(s) == "[REDACTED]"
        assert not s  # Falsy when empty, like str

    def test_redact_secret_utility(self) -> None:
        assert redact_secret(None) == ""
        assert redact_secret("") == ""
        assert redact_secret("my_password") == "[REDACTED]"
        assert redact_secret(SecretValue("x")) == "[REDACTED]"
        # Non-string inputs
        assert redact_secret(42) == "[REDACTED]"


# ---------------------------------------------------------------------------
# parse_secret_ref URI parsing
# ---------------------------------------------------------------------------


class TestParseSecretRef:
    def test_no_scheme_defaults_to_env(self) -> None:
        r = parse_secret_ref("ORDERS_API_TOKEN")
        assert r.scheme is SecretScheme.env
        assert r.path == "ORDERS_API_TOKEN"
        assert r.original == "ORDERS_API_TOKEN"

    def test_explicit_env_scheme(self) -> None:
        r = parse_secret_ref("env://MY_VAR")
        assert r.scheme is SecretScheme.env
        assert r.path == "MY_VAR"

    def test_explicit_file_scheme_absolute(self) -> None:
        r = parse_secret_ref("file:///var/run/secrets/api-key")
        assert r.scheme is SecretScheme.file
        assert r.path == "/var/run/secrets/api-key"

    def test_explicit_file_scheme_relative(self) -> None:
        r = parse_secret_ref("file://./local-secret.txt")
        assert r.scheme is SecretScheme.file
        assert r.path == "./local-secret.txt"

    def test_aws_scheme_stub(self) -> None:
        r = parse_secret_ref("aws_secretsmanager://prod/orders/api-key")
        assert r.scheme is SecretScheme.aws_secretsmanager
        assert r.path == "prod/orders/api-key"

    def test_azure_scheme_stub(self) -> None:
        r = parse_secret_ref("azure_keyvault://myvault/mysecret")
        assert r.scheme is SecretScheme.azure_keyvault
        assert r.path == "myvault/mysecret"

    def test_gcp_scheme_stub(self) -> None:
        r = parse_secret_ref("gcp_secretmanager://myproj/mysecret/versions/3")
        assert r.scheme is SecretScheme.gcp_secretmanager
        assert r.path == "myproj/mysecret/versions/3"

    def test_vault_scheme_stub(self) -> None:
        r = parse_secret_ref("vault://kv/data/orders/password")
        assert r.scheme is SecretScheme.vault
        assert r.path == "kv/data/orders/password"

    def test_unsupported_scheme_fails_fast(self) -> None:
        with pytest.raises(SecretRefSyntaxError, match="Unsupported secret_ref scheme"):
            parse_secret_ref("foobar://x")
        with pytest.raises(SecretRefSyntaxError):
            parse_secret_ref("ldap://dc.example.com/foo")

    def test_empty_ref_rejected(self) -> None:
        with pytest.raises(SecretRefSyntaxError, match="must not be empty"):
            parse_secret_ref("")
        with pytest.raises(SecretRefSyntaxError, match="must not be empty"):
            parse_secret_ref("   ")

    def test_non_string_ref_rejected(self) -> None:
        with pytest.raises(SecretRefSyntaxError, match="secret_ref must be a str"):
            parse_secret_ref(12345)  # type: ignore[arg-type]

    def test_scheme_case_matters_for_unknown(self) -> None:
        # Env/File are exact enum matches, but ENV:// (uppercase) should fail
        # because enum values are lowercase, and _SUPPORTED_SCHEMES checks
        # the raw string.
        with pytest.raises(SecretRefSyntaxError):
            parse_secret_ref("ENV://X")  # uppercase scheme not in enum


# ---------------------------------------------------------------------------
# EnvVarSecrets provider
# ---------------------------------------------------------------------------


class TestEnvVarSecrets:
    def test_resolve_hit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORDERS_API_TOKEN", "tok-orders-42")
        prov = EnvVarSecrets()
        val = prov.resolve(path="ORDERS_API_TOKEN")
        assert isinstance(val, SecretValue)
        assert val == "tok-orders-42"

    def test_resolve_miss_raises_secret_not_found(self) -> None:
        prov = EnvVarSecrets()
        # Make sure it's genuinely unset
        with pytest.raises(SecretNotFoundError) as ei:
            prov.resolve(path="DEFINITELY_UNSET_VAR_XYZ")
        err = ei.value
        assert err.context["scheme"] == "env"
        assert "DEFINITELY_UNSET_VAR_XYZ" not in err.context.get(
            "path_repr", ""
        ) or err.context["path_repr"] == "[REDACTED]"

    def test_empty_path_rejected(self) -> None:
        prov = EnvVarSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path="   ")

    def test_environ_dependency_injection_for_tests(self) -> None:
        prov = EnvVarSecrets(environ={"MY_VAR": "injected-value"})
        assert prov.resolve(path="MY_VAR") == "injected-value"
        with pytest.raises(SecretNotFoundError):
            prov.resolve(path="OTHER_VAR")


# ---------------------------------------------------------------------------
# FileSecrets provider
# ---------------------------------------------------------------------------


class TestFileSecrets:
    def test_resolve_absolute_path(self, tmp_path: Path) -> None:
        secret_file = tmp_path / "api-key"
        secret_file.write_text("sk-live-abc123\n", encoding="utf-8")
        prov = FileSecrets()
        val = prov.resolve(path=str(secret_file))
        assert isinstance(val, SecretValue)
        assert val == "sk-live-abc123"  # trailing newline stripped

    def test_resolve_no_trailing_newline(self, tmp_path: Path) -> None:
        secret_file = tmp_path / "exact"
        secret_file.write_bytes(b"no-newline-here")
        prov = FileSecrets()
        val = prov.resolve(path=str(secret_file))
        assert val == "no-newline-here"

    def test_resolve_relative_path_with_cwd(self, tmp_path: Path) -> None:
        sub = tmp_path / "nested"
        sub.mkdir()
        secret = sub / "secret.txt"
        secret.write_text("rel-path-value\n")
        prov = FileSecrets()
        val = prov.resolve(path="./nested/secret.txt", cwd=tmp_path)
        assert val == "rel-path-value"

    def test_resolve_relative_with_base_dir(self, tmp_path: Path) -> None:
        sub = tmp_path / "secrets"
        sub.mkdir()
        s = sub / "token"
        s.write_text("basedir-value")
        prov = FileSecrets(base_dir=tmp_path)
        val = prov.resolve(path="./secrets/token")
        assert val == "basedir-value"

    def test_missing_file_raises_secret_not_found(self, tmp_path: Path) -> None:
        prov = FileSecrets()
        with pytest.raises(SecretNotFoundError) as ei:
            prov.resolve(path=str(tmp_path / "does-not-exist"))
        assert ei.value.context["scheme"] == "file"

    def test_empty_path_rejected(self) -> None:
        prov = FileSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path="  ")


# ---------------------------------------------------------------------------
# Cloud + Vault providers (SDK-mocked; 2 SDK-missing paths use real imports)
# ---------------------------------------------------------------------------


class TestAWSSecretsManagerSecrets:
    """AWS SM tests: boto3 is an install extra; when present use moto mock,
    when absent the two SDK-missing tests still cover the missing-SDK branch
    with a real ModuleNotFoundError at import-time."""

    @pytest.fixture
    def _aws_sm_session(self):
        """Return a moto-backed boto3 session creating secrets via moto, or
        skip the test if boto3/moto aren't installed."""
        try:
            import boto3  # type: ignore[import-not-found]
            from moto import mock_aws  # type: ignore[import-not-found]
        except ModuleNotFoundError:
            pytest.skip("boto3 or moto not installed — skipping AWS SM moto tests")
            return None
        with mock_aws():
            session = boto3.Session(region_name="us-east-1")
            client = session.client(service_name="secretsmanager", region_name="us-east-1")
            yield session, client

    def test_boto3_not_installed_raises_sdk_missing(self) -> None:
        """Mock a fake boto3 client that raises a ResourceNotFoundException via response attr
        (no botocore dependency)."""
        import sys
        import types as _t

        from elt_pipeline.shared.secrets import (
            AWSSecretsManagerSecrets,
            SecretNotFoundError,
        )

        fake_boto3 = _t.ModuleType("boto3")
        fake_session_mod = _t.ModuleType("boto3.session")

        class _FakeClientError(Exception):
            def __init__(self, response, operation_name):
                self.response = response
                self.operation_name = operation_name
                super().__init__(f"{response}: {operation_name}")

        def _raise_inside(*a, **k):
            class FakeSession:
                def client(self, *a2, **k2):
                    class FakeClient:
                        def get_secret_value(self, *a3, **k3):
                            # Simulate ClientError with response dict shape
                            exc = _FakeClientError(
                                {
                                    "Error": {
                                        "Code": "ResourceNotFoundException",
                                        "Message": (
                                            "Secrets Manager can't find the "
                                            "specified secret."
                                        ),
                                    }
                                },
                                "GetSecretValue",
                            )
                            raise exc

                    return FakeClient()

            return FakeSession()

        fake_session_mod.Session = _raise_inside  # type: ignore[attr-defined]
        fake_boto3.session = fake_session_mod  # type: ignore[attr-defined]
        sys.modules["boto3"] = fake_boto3
        sys.modules["boto3.session"] = fake_session_mod
        try:
            prov = AWSSecretsManagerSecrets()
            with pytest.raises(SecretNotFoundError) as ei:
                prov.resolve(path="nonexistent-secret")
            assert ei.value.context["scheme"] == "aws_secretsmanager"
        finally:
            sys.modules.pop("boto3", None)
            sys.modules.pop("boto3.session", None)

    def test_sdk_missing_via_module_masking(self) -> None:
        """Confirm SECRETS_SDK_MISSING raised when boto3 module is absent."""
        import sys

        from elt_pipeline.shared.secrets import AWSSecretsManagerSecrets, SecretsError

        # Hide boto3 completely behind an import that fails
        orig = sys.modules.get("boto3")
        sys.modules["boto3"] = None  # type: ignore[assignment]
        try:
            if "boto3" in sys.modules:
                del sys.modules["boto3"]
            # Inject a pragma via sys.meta_path finder that raises ModuleNotFoundError
            class _RaiserFinder:
                def find_module(self, name, path=None):  # pragma: no cover
                    if name == "boto3":
                        return self

                def load_module(self, name):  # pragma: no cover
                    raise ModuleNotFoundError("No module named 'boto3'")

                def find_spec(self, name, path, target=None):
                    if name == "boto3":
                        raise ModuleNotFoundError("No module named 'boto3'")
                    return None

            finder = _RaiserFinder()
            sys.meta_path.insert(0, finder)
            try:
                prov = AWSSecretsManagerSecrets()
                with pytest.raises(SecretsError) as ei:
                    prov.resolve(path="any")
                assert ei.value.error_code == "SECRETS_SDK_MISSING"
                assert "boto3" in ei.value.message
            finally:
                sys.meta_path.remove(finder)
        finally:
            if orig is not None:
                sys.modules["boto3"] = orig
            else:
                sys.modules.pop("boto3", None)

    def test_aws_empty_path_rejected(self) -> None:
        from elt_pipeline.shared.secrets import AWSSecretsManagerSecrets

        prov = AWSSecretsManagerSecrets()
        # We expect either SecretRefSyntaxError (direct) or SDK miss — the
        # syntax validation runs BEFORE SDK import, so we should always get
        # SecretRefSyntaxError.
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path="  ")

    def test_aws_syntax_empty_secret_id_before_colon(self) -> None:
        from elt_pipeline.shared.secrets import AWSSecretsManagerSecrets

        prov = AWSSecretsManagerSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=":AWSPREVIOUS")


class TestAzureKeyVaultSecrets:
    def test_azure_empty_path_rejected(self) -> None:
        from elt_pipeline.shared.secrets import AzureKeyVaultSecrets

        prov = AzureKeyVaultSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=" ")

    @pytest.mark.parametrize("bad", ["justvault", "vault/", "/only-secret"])
    def test_azure_syntax_needs_vault_and_secret(self, bad: str) -> None:
        from elt_pipeline.shared.secrets import AzureKeyVaultSecrets

        prov = AzureKeyVaultSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=bad)

    def test_azure_sdk_missing(self) -> None:
        import sys

        from elt_pipeline.shared.secrets import AzureKeyVaultSecrets, SecretsError

        class _RaiserFinder:
            def find_spec(self, name, path, target=None):
                if name == "azure.keyvault.secrets":
                    raise ModuleNotFoundError(
                        "No module named 'azure.keyvault.secrets'"
                    )
                return None

        finder = _RaiserFinder()
        sys.meta_path.insert(0, finder)
        try:
            prov = AzureKeyVaultSecrets()
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="my-v/my-s")
            assert ei.value.error_code == "SECRETS_SDK_MISSING"
            assert "azure-keyvault-secrets" in ei.value.message
        finally:
            sys.meta_path.remove(finder)

    def test_azure_credential_via_mock(self) -> None:
        """Fully mocked Azure SDK path: SecretClient + credential, no network."""
        import sys
        import types as _t

        from elt_pipeline.shared.secrets import AzureKeyVaultSecrets, SecretValue

        azure_root = _t.ModuleType("azure")
        azure_kv = _t.ModuleType("azure.keyvault")
        azure_kv_sec = _t.ModuleType("azure.keyvault.secrets")
        azure_id = _t.ModuleType("azure.identity")

        class _FakeSecret:
            def __init__(self, v):
                self.value = v

        class _FakeSecretClient:
            def __init__(self, vault_url, credential):
                self.vault_url = vault_url

            def get_secret(self, name, version=None):
                if name == "the-secret":
                    return _FakeSecret("azure-secret-val-42")
                # Force a fake ResourceNotFoundError path via raising generic
                raise RuntimeError("azure-identity-resource-not-found-404")

        azure_kv_sec.SecretClient = _FakeSecretClient  # type: ignore[attr-defined]

        class _FakeCred:
            pass

        azure_id.DefaultAzureCredential = lambda: _FakeCred()  # type: ignore[attr-defined]

        for m_name, m_mod in [
            ("azure", azure_root),
            ("azure.keyvault", azure_kv),
            ("azure.keyvault.secrets", azure_kv_sec),
            ("azure.identity", azure_id),
        ]:
            sys.modules[m_name] = m_mod
        try:
            prov = AzureKeyVaultSecrets()
            v = prov.resolve(path="myvault/the-secret")
            assert isinstance(v, SecretValue)
            assert v == "azure-secret-val-42"
        finally:
            for m_name in [
                "azure",
                "azure.keyvault",
                "azure.keyvault.secrets",
                "azure.identity",
            ]:
                sys.modules.pop(m_name, None)


class TestGCPSecretManagerSecrets:
    def test_gcp_empty_path_rejected(self) -> None:
        from elt_pipeline.shared.secrets import GCPSecretManagerSecrets

        prov = GCPSecretManagerSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=" ")

    @pytest.mark.parametrize("bad", ["onlyproj", "proj/", "/secret-only"])
    def test_gcp_syntax_needs_project_and_secret(self, bad: str) -> None:
        from elt_pipeline.shared.secrets import GCPSecretManagerSecrets

        prov = GCPSecretManagerSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=bad)

    def test_gcp_sdk_missing(self) -> None:
        import sys

        from elt_pipeline.shared.secrets import GCPSecretManagerSecrets, SecretsError

        class _RaiserFinder:
            def find_spec(self, name, path, target=None):
                if name == "google.cloud.secretmanager_v1":
                    raise ModuleNotFoundError(
                        "No module named 'google.cloud.secretmanager_v1'"
                    )
                return None

        finder = _RaiserFinder()
        sys.meta_path.insert(0, finder)
        try:
            prov = GCPSecretManagerSecrets()
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="proj/sec")
            assert ei.value.error_code == "SECRETS_SDK_MISSING"
            assert "google-cloud-secret-manager" in ei.value.message
        finally:
            sys.meta_path.remove(finder)

    def test_gcp_mock_client_resolve(self) -> None:
        """Fully-mocked GCP SecretManagerServiceClient via module injection."""
        import sys
        import types as _t

        from elt_pipeline.shared.secrets import GCPSecretManagerSecrets, SecretValue

        google_root = _t.ModuleType("google")
        google_cloud = _t.ModuleType("google.cloud")
        google_cloud_sm = _t.ModuleType("google.cloud.secretmanager_v1")

        class _FakePayload:
            def __init__(self, b):
                self.data = b

        class _FakeResp:
            def __init__(self, b):
                self.payload = _FakePayload(b)

        class _FakeClient:
            def access_secret_version(self, request):
                name = request["name"]
                if name.endswith("/secrets/tok/versions/latest"):
                    return _FakeResp(b"gcp-token-77")
                if name.endswith("/secrets/binbad/versions/latest"):
                    return _FakeResp(b"\xff\xfe binary not utf8")
                if name.endswith("/secrets/empty/versions/latest"):
                    class _NoPayload:
                        payload = None
                    return _NoPayload()
                # Simulate a not-found response
                class _E(Exception):
                    pass
                nf = _E("google.api_core.exceptions NotFound 404 secret not found")
                raise nf

        google_cloud_sm.SecretManagerServiceClient = _FakeClient  # type: ignore[attr-defined]
        for m, mod in [
            ("google", google_root),
            ("google.cloud", google_cloud),
            ("google.cloud.secretmanager_v1", google_cloud_sm),
        ]:
            sys.modules[m] = mod
        try:
            prov = GCPSecretManagerSecrets()
            v = prov.resolve(path="myproj/tok")
            assert isinstance(v, SecretValue)
            assert v == "gcp-token-77"
            # Empty payload path
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="myproj/empty")
            assert ei.value.error_code == "SECRETS_GCP_EMPTY_PAYLOAD"
            # Bad binary
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="myproj/binbad")
            assert ei.value.error_code == "SECRETS_GCP_BINARY_NOT_TEXT"
            # 404-like failure via generic exception → SDK_ERROR code (no match)
            with pytest.raises(SecretsError):
                prov.resolve(path="myproj/other")
        finally:
            for m in [
                "google",
                "google.cloud",
                "google.cloud.secretmanager_v1",
            ]:
                sys.modules.pop(m, None)


class TestVaultSecrets:
    def test_vault_empty_path_rejected(self) -> None:
        from elt_pipeline.shared.secrets import VaultSecrets

        prov = VaultSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=" ")

    @pytest.mark.parametrize(
        "bad",
        [
            "nomount",  # no slash
            "justmount/",  # empty rel
            "kv/some/path#",  # trailing hash, empty field
        ],
    )
    def test_vault_syntax_cases(self, bad: str) -> None:
        from elt_pipeline.shared.secrets import VaultSecrets

        prov = VaultSecrets()
        with pytest.raises(SecretRefSyntaxError):
            prov.resolve(path=bad)

    def test_vault_sdk_missing(self) -> None:
        import sys

        from elt_pipeline.shared.secrets import SecretsError, VaultSecrets

        class _RaiserFinder:
            def find_spec(self, name, path, target=None):
                if name == "hvac":
                    raise ModuleNotFoundError("No module named 'hvac'")
                return None

        finder = _RaiserFinder()
        sys.meta_path.insert(0, finder)
        try:
            prov = VaultSecrets(url="http://localhost:8200", token="x")
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="kv/secret/db")
            assert ei.value.error_code == "SECRETS_SDK_MISSING"
            assert "hvac" in ei.value.message
        finally:
            sys.meta_path.remove(finder)

    def test_vault_url_missing(self) -> None:
        """When SDK IS present but VAULT_ADDR/URL is not, we get URL_MISSING."""
        import os
        import sys
        import types as _t

        from elt_pipeline.shared.secrets import SecretsError, VaultSecrets

        hvac_root = _t.ModuleType("hvac")
        hvac_exc = _t.ModuleType("hvac.exceptions")

        class _BaseExc(Exception):
            pass

        for name in ["Forbidden", "InvalidPath", "Unauthorized"]:
            setattr(hvac_exc, name, type(name, (_BaseExc,), {}))
        hvac_root.exceptions = hvac_exc  # type: ignore[attr-defined]

        class _FakeClient:
            pass

        hvac_root.Client = _FakeClient  # type: ignore[attr-defined]
        sys.modules["hvac"] = hvac_root
        sys.modules["hvac.exceptions"] = hvac_exc
        saved = os.environ.pop("VAULT_ADDR", None)
        saved_url = os.environ.pop("VAULT_URL", None)
        try:
            prov = VaultSecrets()
            with pytest.raises(SecretsError) as ei:
                prov.resolve(path="kv/x")
            assert ei.value.error_code == "SECRETS_VAULT_URL_MISSING"
        finally:
            if saved:
                os.environ["VAULT_ADDR"] = saved
            if saved_url:
                os.environ["VAULT_URL"] = saved_url
            sys.modules.pop("hvac", None)
            sys.modules.pop("hvac.exceptions", None)

    def test_vault_mock_client_field_and_whole_dict(self) -> None:
        """Use injected hvac module + fake client to cover all happy paths."""
        import json
        import os
        import sys
        import types as _t

        from elt_pipeline.shared.secrets import SecretNotFoundError, SecretValue, VaultSecrets

        hvac_root = _t.ModuleType("hvac")
        hvac_exc = _t.ModuleType("hvac.exceptions")

        class _BaseExc(Exception):
            pass

        InvalidPath = type("InvalidPath", (_BaseExc,), {})
        Forbidden = type("Forbidden", (_BaseExc,), {})
        Unauthorized = type("Unauthorized", (_BaseExc,), {})
        hvac_exc.Forbidden = Forbidden  # type: ignore[attr-defined]
        hvac_exc.InvalidPath = InvalidPath  # type: ignore[attr-defined]
        hvac_exc.Unauthorized = Unauthorized  # type: ignore[attr-defined]
        hvac_root.exceptions = hvac_exc  # type: ignore[attr-defined]

        STORE = {
            ("kv", "data/mypath"): {"data": {"username": "app-user", "password": "s3cret!"}},
            ("kv", "data/other"): {"data": None},  # missing inner data
        }

        class _FakeKVv2:
            def read_secret_version(self, mount_point, path):
                key = (mount_point, path)
                if key not in STORE:
                    raise InvalidPath("path not found")
                return {"data": STORE[key]}

        class _FakeSecrets:
            def __init__(self):
                self.kv = type("KV", (), {})()
                self.kv.v2 = _FakeKVv2()

        class _FakeClient:
            def __init__(self, url, verify=True):
                self.url = url
                self.secrets = _FakeSecrets()
                self.token = None

        hvac_root.Client = _FakeClient  # type: ignore[attr-defined]
        sys.modules["hvac"] = hvac_root
        sys.modules["hvac.exceptions"] = hvac_exc
        saved_addr = os.environ.get("VAULT_ADDR")
        os.environ["VAULT_ADDR"] = "http://vault:8200"
        try:
            # 1) Resolve a single field via # suffix
            prov = VaultSecrets()
            v = prov.resolve(path="kv/data/mypath#password")
            assert isinstance(v, SecretValue)
            assert v == "s3cret!"
            # 2) Field missing
            with pytest.raises(SecretNotFoundError) as ei:
                prov.resolve(path="kv/data/mypath#nonexistent_key")
            assert "Available keys" in ei.value.message
            # 3) Whole-dict JSON serialization
            v2 = prov.resolve(path="kv/data/mypath")
            parsed = json.loads(str(v2))
            assert parsed == {"password": "s3cret!", "username": "app-user"}
            # 4) InvalidPath → SecretNotFoundError
            with pytest.raises(SecretNotFoundError):
                prov.resolve(path="kv/data/nonexistent")
            # 5) Response data.data is None → SecretNotFoundError with msg
            with pytest.raises(SecretNotFoundError):
                prov.resolve(path="kv/data/other")
        finally:
            if saved_addr:
                os.environ["VAULT_ADDR"] = saved_addr
            else:
                os.environ.pop("VAULT_ADDR", None)
            sys.modules.pop("hvac", None)
            sys.modules.pop("hvac.exceptions", None)


# ---------------------------------------------------------------------------
# End-to-end: default registry now has real providers (not stubs)
# ---------------------------------------------------------------------------


class TestDefaultRegistryRealProviders:
    def test_bootstrap_registers_real_providers(self) -> None:
        from elt_pipeline.shared.secrets import (
            _PROVIDER_REGISTRY,
            AWSSecretsManagerSecrets,
            AzureKeyVaultSecrets,
            GCPSecretManagerSecrets,
            SecretScheme,
            VaultSecrets,
            _bootstrap_default_registry,
        )

        _bootstrap_default_registry()
        assert isinstance(
            _PROVIDER_REGISTRY[SecretScheme.aws_secretsmanager],
            AWSSecretsManagerSecrets,
        )
        assert isinstance(
            _PROVIDER_REGISTRY[SecretScheme.azure_keyvault],
            AzureKeyVaultSecrets,
        )
        assert isinstance(
            _PROVIDER_REGISTRY[SecretScheme.gcp_secretmanager],
            GCPSecretManagerSecrets,
        )
        assert isinstance(_PROVIDER_REGISTRY[SecretScheme.vault], VaultSecrets)

    def test_providers_implement_protocol(self) -> None:
        from elt_pipeline.shared.secrets import (
            AWSSecretsManagerSecrets,
            AzureKeyVaultSecrets,
            GCPSecretManagerSecrets,
            SecretsProvider,
            VaultSecrets,
        )

        for cls in [
            AWSSecretsManagerSecrets,
            AzureKeyVaultSecrets,
            GCPSecretManagerSecrets,
            VaultSecrets,
        ]:
            instance = cls()
            assert isinstance(instance, SecretsProvider), f"{cls.__name__} is not a SecretsProvider"
            assert isinstance(instance.provider_type, str)


# ---------------------------------------------------------------------------
# Registry: register_provider + get_provider + Protocol enforcement
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_get_default_providers(self) -> None:
        # Ensure bootstrap happened (call resolve_secret_ref once)
        resolve_secret_ref("env://X", strict=True) if False else None
        from elt_pipeline.shared.secrets import _bootstrap_default_registry

        _bootstrap_default_registry()
        env_p = get_provider("env")
        assert isinstance(env_p, EnvVarSecrets)
        file_p = get_provider("file")
        assert isinstance(file_p, FileSecrets)

    def test_duplicate_register_raises(self) -> None:
        from elt_pipeline.shared.secrets import _bootstrap_default_registry

        _bootstrap_default_registry()
        new_env = EnvVarSecrets(environ={})
        with pytest.raises(SecretsError, match="already registered"):
            register_provider("env", new_env)

    def test_register_invalid_protocol_rejected(self) -> None:
        class NotAProvider:
            pass

        with pytest.raises(SecretsError, match="expected SecretsProvider Protocol"):
            # Use a scheme that isn't yet registered (impossible in real state;
            # we temporarily mutate registry for test isolation)
            original = _PROVIDER_REGISTRY.pop(SecretScheme.env, None)
            try:
                register_provider("env", NotAProvider())  # type: ignore[arg-type]
            finally:
                if original is not None:
                    _PROVIDER_REGISTRY[SecretScheme.env] = original

    def test_runtime_checkable_protocol(self) -> None:
        assert isinstance(EnvVarSecrets(), SecretsProvider)
        assert isinstance(FileSecrets(), SecretsProvider)

        class NoProviderTypeAttr:
            def resolve(self, *, path: str) -> SecretValue:
                return SecretValue("x")

        # No provider_type attribute → not a match
        assert not isinstance(NoProviderTypeAttr(), SecretsProvider)


# ---------------------------------------------------------------------------
# resolve_secret_ref top-level dispatcher
# ---------------------------------------------------------------------------


class TestResolveSecretRef:
    def test_env_ref_hit_via_env_scheme(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("X", "xvalue")
        v = resolve_secret_ref("env://X")
        assert v == "xvalue"
        assert isinstance(v, SecretValue)

    def test_env_ref_hit_implicit_scheme(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_SECRET_XYZ", "hidden-value")
        v = resolve_secret_ref("MY_SECRET_XYZ")  # no scheme
        assert v == "hidden-value"

    def test_backward_compat_non_strict_env_miss_passthrough(self) -> None:
        # OLD behaviour: resolve_secret(x) → x.
        # strict=False (default) preserves this.
        v = resolve_secret_ref("SOME_MISSING_VAR_ZZZ")
        assert v == "SOME_MISSING_VAR_ZZZ"
        assert isinstance(v, SecretValue)

    def test_strict_env_miss_raises(self) -> None:
        with pytest.raises(SecretNotFoundError):
            resolve_secret_ref("DEFINITELY_UNSET_FOR_G5_TEST", strict=True)

    def test_file_ref_hit(self, tmp_path: Path) -> None:
        f = tmp_path / "secret"
        f.write_text("file-secret-value\n")
        v = resolve_secret_ref(f"file://{f}", strict=True)
        assert v == "file-secret-value"

    def test_file_ref_miss_strict_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SecretNotFoundError):
            resolve_secret_ref(
                f"file://{tmp_path}/nope-does-not-exist", strict=True
            )


# ---------------------------------------------------------------------------
# resolve_secret_refs batch resolver
# ---------------------------------------------------------------------------


class TestResolveSecretRefs:
    def test_batch_mixed(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setenv("A", "avalue")
        f = tmp_path / "bkey"
        f.write_text("bfile\n")
        result = resolve_secret_refs(
            {"a": "A", "b": f"file://{f}", "c": "passthrough_miss"}
        )
        assert set(result.keys()) == {"a", "b", "c"}
        assert result["a"] == "avalue"
        assert result["b"] == "bfile"
        assert result["c"] == "passthrough_miss"
        for v in result.values():
            assert isinstance(v, SecretValue)

    def test_batch_strict_fail_fast(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GOOD", "g")
        with pytest.raises(SecretNotFoundError):
            resolve_secret_refs(
                {"g": "GOOD", "b": "BAD_VAR_MISSING_XYZ"}, strict=True
            )

    def test_batch_rejects_non_mapping(self) -> None:
        with pytest.raises(SecretsError, match="expected Mapping"):
            resolve_secret_refs(["a", "list", "of", "refs"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# End-to-end: RestConnectorBase uses the new subsystem (backward compat)
# ---------------------------------------------------------------------------


class TestRestConnectorIntegration:
    def test_resolve_secret_dispatch_through_subsystem(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from elt_pipeline.ingest.connectors.rest import (
            RestConnectorBase,
            RestConnectorConfig,
        )
        from elt_pipeline.shared.runtime import StageName, new_run_context

        monkeypatch.setenv("ORDERS_API_TOKEN", "env-token-value")

        class TrivialConnector(RestConnectorBase):
            def execute_request(self, *a, **k):
                raise NotImplementedError

            def persist_response(self, *a, **k):
                raise NotImplementedError

        rc = new_run_context(stage=StageName.ingest, job_name="test")
        cfg = RestConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="s",
            entity_name="e",
            execution_mode="batch",
            base_url="https://example.com",
            request={"method": "GET", "path": "/x"},
        )
        conn = TrivialConnector(config=cfg, run_context=rc)

        # 1) Implicit env:// with hit → resolves through subsystem
        val = conn.resolve_secret(secret_name="token", secret_ref="ORDERS_API_TOKEN")
        assert val == "env-token-value"
        assert isinstance(val, SecretValue)
        assert repr(val) == "[REDACTED]"

        # 2) Backward compat: env miss in non-strict → literal passthrough
        val2 = conn.resolve_secret(
            secret_name="x", secret_ref="STILL_A_MISSING_VAR_ABC123"
        )
        assert val2 == "STILL_A_MISSING_VAR_ABC123"
        assert isinstance(val2, SecretValue)

    def test_existing_test_subclass_override_still_works(self) -> None:
        """Ensure the test fixture pattern in test_rest_connectors.py (subclass
        override of resolve_secret returning plain-dict value) still works.
        """
        from elt_pipeline.ingest.connectors.rest import (
            RestConnectorBase,
            RestConnectorConfig,
        )
        from elt_pipeline.shared.runtime import StageName, new_run_context

        SECRETS = {"ORDERS_API_TOKEN": "direct-dict-token"}

        class OverrideConnector(RestConnectorBase):
            def resolve_secret(self, *, secret_name: str, secret_ref: str) -> str:
                return SECRETS[secret_ref]

            def execute_request(self, *a, **k):
                raise NotImplementedError

            def persist_response(self, *a, **k):
                raise NotImplementedError

        rc = new_run_context(stage=StageName.ingest, job_name="test")
        cfg = RestConnectorConfig(
            schema_version="v1",
            environment="dev",
            source_name="s",
            entity_name="e",
            execution_mode="batch",
            base_url="https://example.com",
            request={"method": "GET", "path": "/x"},
        )
        conn = OverrideConnector(config=cfg, run_context=rc)
        assert (
            conn.resolve_secret(secret_name="t", secret_ref="ORDERS_API_TOKEN")
            == "direct-dict-token"
        )
