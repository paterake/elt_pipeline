from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from elt_pipeline.config import runtime_context
from elt_pipeline.shared.errors import ErrorCategory, PipelineError
from elt_pipeline.shared.secrets import SecretNotFoundError
from elt_pipeline.spark.session import build_spark_fs_hadoop_configs


class TestBuildSparkFsHadoopConfigsS3:
    """S3 (s3a://) branch of build_spark_fs_hadoop_configs — pure unit, no JVM."""

    def test_nothing_configured_returns_empty(self):
        assert build_spark_fs_hadoop_configs() == {}

    def test_s3_region_only_emits_impl_and_region_no_creds(self):
        out = build_spark_fs_hadoop_configs(s3_region="eu-west-1")
        assert out["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        assert out["spark.hadoop.fs.s3a.endpoint.region"] == "eu-west-1"
        assert "spark.hadoop.fs.s3a.access.key" not in out
        assert "spark.hadoop.fs.s3a.secret.key" not in out

    def test_s3_endpoint_only_emits_impl_and_endpoint(self):
        out = build_spark_fs_hadoop_configs(s3_endpoint="https://minio.local:9000")
        assert out["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        assert out["spark.hadoop.fs.s3a.endpoint"] == "https://minio.local:9000"

    def test_s3_access_and_secret_via_env_refs_resolved(self):
        env = {
            "MY_AWS_AK": "AKIAIOSFODNN7EXAMPLE",
            "MY_AWS_SK": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }
        with patch.dict(os.environ, env, clear=False):
            out = build_spark_fs_hadoop_configs(
                s3_access_key_ref="env://MY_AWS_AK",
                s3_secret_key_ref="env://MY_AWS_SK",
                s3_region="us-east-2",
            )
        assert out["spark.hadoop.fs.s3a.access.key"] == "AKIAIOSFODNN7EXAMPLE"
        assert out["spark.hadoop.fs.s3a.secret.key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert out["spark.hadoop.fs.s3a.endpoint.region"] == "us-east-2"

    def test_s3_access_key_without_secret_raises_config_error(self):
        env = {"MY_AWS_AK": "AKIAIOSFODNN7EXAMPLE"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(PipelineError) as exc_info:
                build_spark_fs_hadoop_configs(
                    s3_access_key_ref="env://MY_AWS_AK",
                    s3_region="us-east-1",
                )
        assert exc_info.value.error_code == "SPARK_FS_S3_CRED_MISMATCH"
        assert exc_info.value.error_category == ErrorCategory.config_error

    def test_s3_secret_key_without_access_raises_config_error(self):
        env = {"MY_AWS_SK": "secret"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(PipelineError) as exc_info:
                build_spark_fs_hadoop_configs(
                    s3_secret_key_ref="env://MY_AWS_SK",
                )
        assert exc_info.value.error_code == "SPARK_FS_S3_CRED_MISMATCH"
        assert exc_info.value.error_category == ErrorCategory.config_error

    def test_s3_missing_env_ref_strict_fail_fast(self):
        with patch.dict(os.environ, {}, clear=False):
            with pytest.raises(SecretNotFoundError):
                build_spark_fs_hadoop_configs(
                    s3_access_key_ref="env://NO_SUCH_VAR",
                    s3_secret_key_ref="env://NO_SUCH_VAR_EITHER",
                )

    def test_s3_plain_ref_implicit_env_scheme_resolved(self):
        """Bare ref without scheme → env:// (default) for both keys."""
        env = {"BARE_AK": "bare-ak", "BARE_SK": "bare-sk"}
        with patch.dict(os.environ, env, clear=False):
            out = build_spark_fs_hadoop_configs(
                s3_access_key_ref="BARE_AK",
                s3_secret_key_ref="BARE_SK",
            )
        assert out["spark.hadoop.fs.s3a.access.key"] == "bare-ak"
        assert out["spark.hadoop.fs.s3a.secret.key"] == "bare-sk"

    def test_s3_file_ref_resolved(self, tmp_path: Path):
        ak = tmp_path / "ak.txt"
        sk = tmp_path / "sk.txt"
        ak.write_text("file-ak-value")
        sk.write_text("file-sk-value")
        out = build_spark_fs_hadoop_configs(
            s3_access_key_ref=f"file://{ak}",
            s3_secret_key_ref=f"file://{sk}",
        )
        assert out["spark.hadoop.fs.s3a.access.key"] == "file-ak-value"
        assert out["spark.hadoop.fs.s3a.secret.key"] == "file-sk-value"


class TestBuildSparkFsHadoopConfigsGCS:
    """GCS (gs://) branch of build_spark_fs_hadoop_configs."""

    def test_gcs_project_only(self):
        out = build_spark_fs_hadoop_configs(gcs_project_id="my-gcp-project-123")
        assert (
            out["spark.hadoop.fs.gs.impl"]
            == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        assert (
            out["spark.hadoop.fs.AbstractFileSystem.gs.impl"]
            == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        assert out["spark.hadoop.fs.gs.project.id"] == "my-gcp-project-123"
        assert "spark.hadoop.google.cloud.auth.service.account.enable" not in out

    def test_gcs_sa_keyfile_via_file_ref(self, tmp_path: Path):
        keyfile = tmp_path / "sa-key.json"
        keyfile.write_text('{"type":"service_account","private_key_id":"xxx"}')
        out = build_spark_fs_hadoop_configs(
            gcs_sa_keyfile_ref=f"file://{keyfile}",
            gcs_project_id="proj",
        )
        assert out["spark.hadoop.google.cloud.auth.service.account.enable"] == "true"
        assert out["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] == str(keyfile)
        assert out["spark.hadoop.fs.gs.project.id"] == "proj"

    def test_gcs_missing_keyfile_via_env_ref_fails(self):
        with patch.dict(os.environ, {}, clear=False):
            with pytest.raises(SecretNotFoundError):
                build_spark_fs_hadoop_configs(
                    gcs_sa_keyfile_ref="env://MISSING_VAR",
                )


class TestBuildSparkFsHadoopConfigsADLS:
    """ADLS Gen2 (abfss://) branch of build_spark_fs_hadoop_configs."""

    def test_adls_shared_key_via_env_refs(self):
        env = {"ACCT_KEY": "c29tZS1iYXNlNjQtYWNjb3VudC1rZXk="}
        with patch.dict(os.environ, env, clear=False):
            out = build_spark_fs_hadoop_configs(
                adls_account_name="mystorage",
                adls_account_key_ref="env://ACCT_KEY",
            )
        assert (
            out["spark.hadoop.fs.azure.account.key.mystorage.dfs.core.windows.net"]
            == "c29tZS1iYXNlNjQtYWNjb3VudC1rZXk="
        )
        assert "spark.hadoop.fs.azure.account.auth.type" not in out

    def test_adls_requires_account_name_when_creds_set(self):
        env = {"ACCT_KEY": "key"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(PipelineError) as exc_info:
                build_spark_fs_hadoop_configs(adls_account_key_ref="env://ACCT_KEY")
        assert exc_info.value.error_code == "SPARK_FS_ADLS_ACCOUNT_REQUIRED"
        assert exc_info.value.error_category == ErrorCategory.config_error

    def test_adls_msi_mode(self):
        out = build_spark_fs_hadoop_configs(
            adls_account_name="myacct",
            adls_use_msi=True,
        )
        assert out["spark.hadoop.fs.azure.account.auth.type.myacct.dfs.core.windows.net"] == "OAuth"
        assert (
            out["spark.hadoop.fs.azure.account.oauth.provider.type.myacct.dfs.core.windows.net"]
            == "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        )

    def test_adls_service_principal_via_env_refs(self):
        env = {"CID": "sp-client-id", "CSEC": "sp-client-secret"}
        with patch.dict(os.environ, env, clear=False):
            out = build_spark_fs_hadoop_configs(
                adls_account_name="acct1",
                adls_tenant_id="a1b2c3d4-tenant",
                adls_client_id_ref="env://CID",
                adls_client_secret_ref="env://CSEC",
            )
        host = "acct1.dfs.core.windows.net"
        assert out[f"spark.hadoop.fs.azure.account.auth.type.{host}"] == "OAuth"
        assert (
            out[f"spark.hadoop.fs.azure.account.oauth.provider.type.{host}"]
            == "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        assert out[f"spark.hadoop.fs.azure.account.oauth2.client.id.{host}"] == (
            "sp-client-id"
        )
        assert out[f"spark.hadoop.fs.azure.account.oauth2.client.secret.{host}"] == (
            "sp-client-secret"
        )
        assert (
            out[f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{host}"]
            == "https://login.microsoftonline.com/a1b2c3d4-tenant/oauth2/token"
        )

    def test_adls_sp_partial_missing_client_id(self):
        env = {"CSEC": "sec"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(PipelineError) as exc_info:
                build_spark_fs_hadoop_configs(
                    adls_account_name="x",
                    adls_tenant_id="tid",
                    adls_client_secret_ref="env://CSEC",
                )
        assert exc_info.value.error_code == "SPARK_FS_ADLS_SP_INCOMPLETE"
        assert exc_info.value.error_category == ErrorCategory.config_error

    def test_adls_sp_partial_missing_tenant(self):
        env = {"CID": "cid", "CSEC": "csec"}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(PipelineError) as exc_info:
                build_spark_fs_hadoop_configs(
                    adls_account_name="x",
                    adls_client_id_ref="env://CID",
                    adls_client_secret_ref="env://CSEC",
                )
        assert exc_info.value.error_code == "SPARK_FS_ADLS_SP_INCOMPLETE"
        assert exc_info.value.error_category == ErrorCategory.config_error

    def test_adls_msi_without_creds_no_cred_keys(self):
        """MSI mode sets auth provider but NO secret keys — default MSI chain."""
        out = build_spark_fs_hadoop_configs(
            adls_account_name="msistorage",
            adls_use_msi=True,
        )
        secret_keys = [k for k in out if "secret" in k or "key." in k]
        assert secret_keys == []


class TestRuntimeContextSparkFsCascade:
    """Spark FS config materialization through the 4-tier runtime_context cascade."""

    @staticmethod
    def _reset_rc():
        runtime_context._reset_for_tests()

    def test_manifest_floor_all_empty(self):
        self._reset_rc()
        with patch.dict(os.environ, {}, clear=True):
            runtime_context.initialize(config_path_arg=None, environment_arg=None)
            fs = runtime_context.get_dict("spark_fs")
        assert fs["s3_access_key_ref"] == ""
        assert fs["s3_secret_key_ref"] == ""
        assert fs["s3_region"] == ""
        assert fs["s3_endpoint"] == ""
        assert fs["gcs_sa_keyfile_ref"] == ""
        assert fs["gcs_project_id"] == ""
        assert fs["adls_account_name"] == ""
        assert fs["adls_account_key_ref"] == ""
        assert fs["adls_tenant_id"] == ""
        assert fs["adls_client_id_ref"] == ""
        assert fs["adls_client_secret_ref"] == ""
        assert fs["adls_use_msi"] == ""
        self._reset_rc()

    def test_env_vars_materialize(self):
        self._reset_rc()
        env = {
            "ELT_PIPELINE_SPARK_FS_S3_REGION": "ap-southeast-1",
            "ELT_PIPELINE_SPARK_FS_S3_ENDPOINT": "https://custom-s3.example",
            "ELT_PIPELINE_SPARK_FS_GCS_PROJECT_ID": "env-project-789",
            "ELT_PIPELINE_SPARK_FS_ADLS_ACCOUNT_NAME": "envacct",
            "ELT_PIPELINE_SPARK_FS_ADLS_USE_MSI": "true",
        }
        with patch.dict(os.environ, env, clear=True):
            runtime_context.initialize(config_path_arg=None, environment_arg=None)
            fs = runtime_context.get_dict("spark_fs")
        assert fs["s3_region"] == "ap-southeast-1"
        assert fs["s3_endpoint"] == "https://custom-s3.example"
        assert fs["gcs_project_id"] == "env-project-789"
        assert fs["adls_account_name"] == "envacct"
        assert fs["adls_use_msi"] == "true"
        self._reset_rc()

    def test_dotted_keys_are_flat_accessible(self):
        self._reset_rc()
        env = {"ELT_PIPELINE_SPARK_FS_S3_REGION": "us-west-2"}
        with patch.dict(os.environ, env, clear=True):
            runtime_context.initialize(config_path_arg=None, environment_arg=None)
            assert runtime_context.get("spark_fs.s3_region") == "us-west-2"
            assert runtime_context.get("spark_fs.nonexistent") is None
        self._reset_rc()

    def test_as_runtime_overrides_includes_spark_fs(self):
        self._reset_rc()
        env = {"ELT_PIPELINE_SPARK_FS_ADLS_ACCOUNT_NAME": "inro"}
        with patch.dict(os.environ, env, clear=True):
            runtime_context.initialize(config_path_arg=None, environment_arg=None)
            ro = runtime_context.as_runtime_overrides()
        assert "spark_fs" in ro
        assert ro["spark_fs"]["adls_account_name"] == "inro"
        self._reset_rc()


class TestBuildSparkSessionFsConfigsIntegration:
    """Build-spark-session integration for the FS config path.

    Pattern: follow test_iceberg_catalog_config.py — we CAN'T actually boot
    a JVM here, but we CAN confirm that build_spark_session() raises no
    validation-level ValueError / PipelineError for valid FS configs before
    it hits getOrCreate() (the JVM failure), and that it DOES raise the
    expected PipelineError for invalid FS configs.
    """

    def _hit_jvm(self, exc: Exception) -> bool:
        return any(
            tok in str(exc).lower()
            for tok in ("java", "jvm", "spark", "getorcreate", "gateway")
        )

    def test_valid_s3_region_no_creds_passes_validation(self):
        """Region-only S3 config: no cred mismatch, should pass validation."""
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                from elt_pipeline.spark.session import build_spark_session

                build_spark_session(
                    app_name="test-fs-s3-region",
                    iceberg_enabled=False,
                    runtime_overrides={"spark_fs": {"s3_region": "us-east-1"}},
                )
        assert not any(
            code in (str(type(exc_info.value)) + str(exc_info.value))
            for code in ("SPARK_FS_S3_CRED", "SPARK_FS_ADLS", "SPARK_FS_")
        )
        assert self._hit_jvm(exc_info.value)

    def test_invalid_s3_cred_mismatch_raises_before_jvm(self):
        """Secret-key-only → PipelineError SPARK_FS_S3_CRED_MISMATCH, not a JVM error."""
        env = {"ONLY_SK": "sk-only"}
        with pytest.raises(PipelineError) as exc_info:
            with patch.dict(os.environ, env, clear=False):
                from elt_pipeline.spark.session import build_spark_session

                build_spark_session(
                    app_name="test-fs-bad",
                    iceberg_enabled=False,
                    runtime_overrides={
                        "spark_fs": {"s3_secret_key_ref": "env://ONLY_SK"}
                    },
                )
        assert exc_info.value.error_code == "SPARK_FS_S3_CRED_MISMATCH"

    def test_valid_adls_msi_passes_validation(self):
        """MSI-only ADLS config: should pass and hit JVM, not a validation error."""
        with pytest.raises(Exception) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                from elt_pipeline.spark.session import build_spark_session

                build_spark_session(
                    app_name="test-fs-adls-msi",
                    iceberg_enabled=False,
                    runtime_overrides={
                        "spark_fs": {
                            "adls_account_name": "validacct",
                            "adls_use_msi": True,
                        }
                    },
                )
        err_blurb = str(type(exc_info.value)) + str(exc_info.value)
        assert "SPARK_FS_ADLS" not in err_blurb
        assert self._hit_jvm(exc_info.value)

    def test_invalid_adls_no_account_name_raises_before_jvm(self):
        with pytest.raises(PipelineError) as exc_info:
            with patch.dict(os.environ, {}, clear=True):
                from elt_pipeline.spark.session import build_spark_session

                build_spark_session(
                    app_name="test-fs-adls-bad",
                    iceberg_enabled=False,
                    runtime_overrides={
                        "spark_fs": {"adls_use_msi": True}
                    },
                )
        assert exc_info.value.error_code == "SPARK_FS_ADLS_ACCOUNT_REQUIRED"
