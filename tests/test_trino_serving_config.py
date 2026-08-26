from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.trino_serving_config import (
    TRINO_KERBEROS_AUTH_INCOMPLETE,
    TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS,
    TRINO_SSL_KEYSTORE_REQUIRED,
    TrinoHttpAuthType,
    build_trino_serving_configs,
    generate_trino_internal_shared_secret,
)


class TestTrinoSharedSecret:
    def test_generate_returns_non_empty_urlsafe(self):
        s = generate_trino_internal_shared_secret()
        assert isinstance(s, str)
        assert len(s) >= 16

    def test_generates_unique_values(self):
        assert generate_trino_internal_shared_secret() != generate_trino_internal_shared_secret()


class TestBuildTrinoServingConfigsBackwardCompatDefaults:
    """Insecure / no-auth default path must match the pre-M-4 output shape."""

    def test_no_auth_no_https_returns_base_config_only(self):
        cfg = build_trino_serving_configs(
            validate=False,
            internal_shared_secret="should-be-ignored",
        )
        assert cfg["coordinator"] == "true"
        assert cfg["node-scheduler.include-coordinator"] == "true"
        assert cfg["http-server.http.port"] == "8080"
        assert cfg["node.environment"] == "elt_pipeline_iceberg"
        assert cfg["node.internal-address"] == "127.0.0.1"
        assert "http-server.authentication.type" not in cfg
        assert "http-server.https.enabled" not in cfg
        assert "internal-communication.shared-secret" not in cfg

    @pytest.mark.parametrize(
        "auth_type",
        ["", "none", "disabled", "insecure", "NONE", "InSecure"],
    )
    def test_disabled_auth_values_are_all_noop(self, auth_type):
        cfg = build_trino_serving_configs(
            http_auth_type=auth_type,
            validate=False,
        )
        assert "http-server.authentication.type" not in cfg
        assert "internal-communication.shared-secret" not in cfg


class TestBuildTrinoServingConfigsHttpsTls:
    """HTTPS / TLS surface — keystore required, truststore optional."""

    def test_https_disabled_by_default(self):
        cfg = build_trino_serving_configs(validate=False)
        assert "http-server.https.enabled" not in cfg

    def test_https_enabled_emits_keystore_and_port(self, tmp_path: Path):
        ks = tmp_path / "trino.p12"
        ks.write_bytes(b"fake-keystore-bytes")
        cfg = build_trino_serving_configs(
            https_enabled=True,
            https_port=8444,
            ssl_keystore_path=str(ks),
            ssl_keystore_password="changeit",
            validate=True,
        )
        assert cfg["http-server.https.enabled"] == "true"
        assert cfg["http-server.https.port"] == "8444"
        assert cfg["http-server.https.keystore.path"] == str(ks)
        assert cfg["http-server.https.keystore.key"] == "changeit"

    def test_https_with_truststore_emits_both(self, tmp_path: Path):
        ks = tmp_path / "keystore.jks"
        ts = tmp_path / "truststore.jks"
        ks.write_text("x")
        ts.write_text("x")
        cfg = build_trino_serving_configs(
            https_enabled=True,
            ssl_keystore_path=str(ks),
            ssl_keystore_password="ks-pw",
            ssl_truststore_path=str(ts),
            ssl_truststore_password="ts-pw",
            validate=True,
        )
        assert cfg["http-server.https.truststore.path"] == str(ts)
        assert cfg["http-server.https.truststore.key"] == "ts-pw"

    def test_https_enabled_missing_keystore_raises(self):
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                https_enabled=True,
                ssl_keystore_password="pw-only-no-path",
                validate=True,
                _enforce_file_existence=False,
            )
        assert exc_info.value.error_code == TRINO_SSL_KEYSTORE_REQUIRED
        assert exc_info.value.error_category == ErrorCategory.config_error
        assert any(
            f.startswith("ssl_keystore_path")
            for f in exc_info.value.context["missing_fields"]
        )

    def test_https_enabled_missing_keystore_password_raises(self):
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                https_enabled=True,
                ssl_keystore_path="/tmp/does-not-matter-for-this-check.p12",
                validate=True,
                _enforce_file_existence=False,
            )
        assert exc_info.value.error_code == TRINO_SSL_KEYSTORE_REQUIRED
        assert any(
            f.startswith("ssl_keystore_password")
            for f in exc_info.value.context["missing_fields"]
        )

    def test_https_enabled_keystore_file_not_found_raises(self, tmp_path: Path):
        missing = tmp_path / "absent.jks"
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                https_enabled=True,
                ssl_keystore_path=str(missing),
                ssl_keystore_password="pw",
                validate=True,
                _enforce_file_existence=True,
            )
        assert exc_info.value.error_code == TRINO_SSL_KEYSTORE_REQUIRED


class TestBuildTrinoServingConfigsPasswordAuth:
    """auth_type=password — password file required; emits file + shared-secret."""

    def test_password_auth_with_file_emits_type_and_file_and_shared_secret(
        self, tmp_path: Path
    ):
        pf = tmp_path / "htpasswd"
        pf.write_text("alice:$2y$05$...")
        cfg = build_trino_serving_configs(
            http_auth_type=TrinoHttpAuthType.PASSWORD,
            password_file_path=str(pf),
            internal_shared_secret="static-test-secret",
            validate=True,
        )
        assert cfg["http-server.authentication.type"] == TrinoHttpAuthType.PASSWORD
        assert cfg["http-server.authentication.password.file"] == str(pf)
        assert cfg["internal-communication.shared-secret"] == "eltp-static-test-secret"

    def test_password_auth_missing_path_raises(self):
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                http_auth_type=TrinoHttpAuthType.PASSWORD,
                validate=True,
                _enforce_file_existence=False,
            )
        assert exc_info.value.error_code == TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS

    def test_password_auth_file_missing_on_disk_raises(self, tmp_path: Path):
        missing = tmp_path / "nope"
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                http_auth_type=TrinoHttpAuthType.PASSWORD,
                password_file_path=str(missing),
                validate=True,
            )
        assert exc_info.value.error_code == TRINO_PASSWORD_AUTH_VALID_FILE_EXISTS


class TestBuildTrinoServingConfigsKerberosAuth:
    """auth_type=kerberos — principal + keytab required; user-mapping pattern emitted."""

    def test_kerberos_with_full_principal_parses_svc_and_host(
        self, tmp_path: Path
    ):
        kt = tmp_path / "http.keytab"
        kt.write_text("x")
        cfg = build_trino_serving_configs(
            http_auth_type=TrinoHttpAuthType.KERBEROS,
            kerberos_principal="HTTP/trino.example.com@EXAMPLE.COM",
            kerberos_keytab=str(kt),
            krb5_conf="/etc/krb5.conf",
            validate=True,
        )
        assert cfg["http-server.authentication.type"] == TrinoHttpAuthType.KERBEROS
        assert cfg["http-server.authentication.krb5.service-name"] == "HTTP"
        assert (
            cfg["http-server.authentication.krb5.principal-host"]
            == "trino.example.com"
        )
        assert cfg["http-server.authentication.krb5.keytab"] == str(kt)
        assert cfg["http-server.authentication.krb5.config"] == "/etc/krb5.conf"
        assert (
            cfg["http-server.authentication.krb5.user-mapping.pattern"]
            == "(.*)@.*"
        )
        assert "internal-communication.shared-secret" in cfg

    def test_kerberos_bare_principal_falls_back_to_trino_host(
        self, tmp_path: Path
    ):
        kt = tmp_path / "http.keytab"
        kt.write_text("x")
        cfg = build_trino_serving_configs(
            http_auth_type=TrinoHttpAuthType.KERBEROS,
            kerberos_principal="HTTP@EXAMPLE.COM",
            kerberos_keytab=str(kt),
            trino_host="analytics.internal",
            validate=True,
        )
        assert (
            cfg["http-server.authentication.krb5.principal-host"]
            == "analytics.internal"
        )

    def test_kerberos_missing_principal_and_keytab_raises(self):
        with pytest.raises(PipelineError) as exc_info:
            build_trino_serving_configs(
                http_auth_type=TrinoHttpAuthType.KERBEROS,
                validate=True,
            )
        assert exc_info.value.error_code == TRINO_KERBEROS_AUTH_INCOMPLETE
        missing = set(exc_info.value.context["missing_fields"])
        assert "kerberos_principal (ELT_PIPELINE_TRINO_KERBEROS_PRINCIPAL)" in missing
        assert "kerberos_keytab (ELT_PIPELINE_TRINO_KERBEROS_KEYTAB)" in missing


class TestBuildTrinoServingConfigsPassThroughAuthTypes:
    """JWT / OAuth2 / Form / Certificate — type line emitted; no extra file lines."""

    @pytest.mark.parametrize(
        "auth_type",
        [
            TrinoHttpAuthType.JWT,
            TrinoHttpAuthType.OAUTH2,
            TrinoHttpAuthType.FORM,
            TrinoHttpAuthType.CERTIFICATE,
        ],
    )
    def test_passthrough_types_emit_type_line_and_shared_secret(
        self, auth_type
    ):
        cfg = build_trino_serving_configs(
            http_auth_type=auth_type,
            internal_shared_secret="s",
            validate=True,
        )
        assert cfg["http-server.authentication.type"] == auth_type
        assert cfg["internal-communication.shared-secret"] == "eltp-s"
        assert "http-server.authentication.password.file" not in cfg
        assert "http-server.authentication.krb5.service-name" not in cfg


class TestEnvVarNames:
    """Centralized env var names must be registered with expected strings."""

    def test_env_var_names_exist_in_manifest(self):
        from elt_pipeline.config.runtime_manifest import EnvVarNames

        names = EnvVarNames()
        expected = {
            "trino_http_auth_type": "ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE",
            "trino_https_enabled": "ELT_PIPELINE_TRINO_HTTPS_ENABLED",
            "trino_https_port": "ELT_PIPELINE_TRINO_HTTPS_PORT",
            "trino_ssl_keystore_path": "ELT_PIPELINE_TRINO_SSL_KEYSTORE_PATH",
            "trino_ssl_keystore_password": "ELT_PIPELINE_TRINO_SSL_KEYSTORE_PASSWORD",
            "trino_ssl_truststore_path": "ELT_PIPELINE_TRINO_SSL_TRUSTSTORE_PATH",
            "trino_ssl_truststore_password": "ELT_PIPELINE_TRINO_SSL_TRUSTSTORE_PASSWORD",
            "trino_password_file_path": "ELT_PIPELINE_TRINO_PASSWORD_FILE_PATH",
            "trino_krb5_conf": "ELT_PIPELINE_TRINO_KRB5_CONF",
            "trino_kerberos_principal": "ELT_PIPELINE_TRINO_KERBEROS_PRINCIPAL",
            "trino_kerberos_keytab": "ELT_PIPELINE_TRINO_KERBEROS_KEYTAB",
        }
        for attr, env_name in expected.items():
            assert getattr(names, attr) == env_name, attr


class TestEnvRoundtripViaRuntimeContext:
    """Integration: env vars → runtime_context → builder produces expected keys."""

    def test_password_auth_env_roundtrip(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        from elt_pipeline.config import runtime_context

        pf = tmp_path / "htpasswd"
        pf.write_text("a:b")
        env = {
            "ELT_PIPELINE_TRINO_HTTP_AUTH_TYPE": "password",
            "ELT_PIPELINE_TRINO_PASSWORD_FILE_PATH": str(pf),
        }
        with patch.dict("os.environ", env, clear=False):
            runtime_context._reset_for_tests()
            runtime_context.initialize(config_path_arg=None)
            cfg = build_trino_serving_configs(
                http_auth_type=runtime_context.get(
                    "trino_serving.http_authentication_type"
                ),
                password_file_path=runtime_context.get(
                    "trino_serving.password_file_path"
                ),
                internal_shared_secret="stamp",
                validate=True,
            )
        assert cfg["http-server.authentication.type"] == "password"
        assert cfg["http-server.authentication.password.file"] == str(pf)
        assert cfg["internal-communication.shared-secret"] == "eltp-stamp"
        runtime_context._reset_for_tests()
