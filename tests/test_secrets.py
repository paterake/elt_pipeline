from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from elt_pipeline.shared.secrets import (
    EnvVarSecrets,
    FileSecrets,
    ParsedSecretRef,
    SecretNotFoundError,
    SecretRefSyntaxError,
    SecretScheme,
    SecretsError,
    SecretsNotImplementedError,
    SecretsProvider,
    SecretValue,
    _PROVIDER_REGISTRY,
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
# Roadmap provider stubs → NotImplemented fail-fast
# ---------------------------------------------------------------------------


class TestRoadmapStubsFailFast:
    @pytest.mark.parametrize(
        "ref",
        [
            "aws_secretsmanager://any",
            "azure_keyvault://v/s",
            "gcp_secretmanager://p/s",
            "vault://kv/x",
        ],
    )
    def test_stub_schemes_raise_not_implemented(self, ref: str) -> None:
        with pytest.raises(SecretsNotImplementedError) as ei:
            resolve_secret_ref(ref, strict=True)
        err = ei.value
        assert "not yet implemented" in err.message
        # Ensure the ref's value isn't in the message
        assert ref not in err.message


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
