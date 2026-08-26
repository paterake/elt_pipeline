from __future__ import annotations

from typing import Any, Iterator

import pytest

from elt_pipeline.shared import path_utils as pu
from elt_pipeline.shared import storage_backends as _sb
from elt_pipeline.shared.errors import PipelineError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_all_backend_singletons() -> None:
    """Clear cached SDK client singletons before every emulator test.

    The path_utils and storage_backends modules cache SDK clients at module
    load time (lazy on first call). An emulator test must start with a fresh
    client pointed at the emulator; if a previous test (or import-time side
    effect) cached a real-production client or a stale fake, it would leak
    across and cause false failures.
    """
    pu._S3_CLIENT = None
    pu._GCS_CLIENT = None
    pu._ADLS_CLIENT = None
    _sb._S3_CLIENT = None
    _sb._GCS_CLIENT = None
    _sb._ADLS_CLIENT = None


# ---------------------------------------------------------------------------
# 1. S3  —  moto (pure-Python in-process, no Docker required)
# ---------------------------------------------------------------------------


S3_BUCKET = "elt-pipeline-emulator-test"
S3_ROOT = f"s3://{S3_BUCKET}/elt_root"


@pytest.fixture()
def _s3_moto_env(moto_s3) -> Iterator[None]:
    """Set up a moto-backed S3 bucket + StorageBackend routing.

    moto's ``mock_aws`` patches boto3 globally so any call to
    ``boto3.client("s3")`` returns a client against moto's in-memory service.
    This fixture also creates the test bucket so tests can start writing
    immediately.
    """
    _reset_all_backend_singletons()
    import boto3

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=S3_BUCKET)
    yield
    _reset_all_backend_singletons()


@pytest.mark.emulator
@pytest.mark.usefixtures("_s3_moto_env")
class TestS3EmulatorLeafOps:
    """Category A — all 18 StorageBackend leaf IO ops against a real boto3
    client talking to moto's in-memory S3 service (not a handwritten Fake)."""

    def test_write_and_read_bytes_roundtrip(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/data/file.bin", b"hello s3 world")
        assert pu.path_read_bytes(f"{S3_ROOT}/data/file.bin") == b"hello s3 world"

    def test_write_read_text_roundtrip(self) -> None:
        pu.path_write_text(f"{S3_ROOT}/logs/app.log", "line1\nline2\n")
        assert pu.path_read_text(f"{S3_ROOT}/logs/app.log") == "line1\nline2\n"

    def test_path_exists_for_key_and_missing(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/present/key.parquet", b"data")
        assert pu.path_exists(f"{S3_ROOT}/present/key.parquet") is True
        assert pu.path_exists(f"{S3_ROOT}/missing/key.parquet") is False

    def test_path_exists_for_directory_via_prefix(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/nested/deep/key.bin", b"x")
        assert pu.path_exists(f"{S3_ROOT}/nested/deep/") is True
        assert pu.path_exists(f"{S3_ROOT}/nested/") is True

    def test_path_is_dir(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/dir1/child/file.bin", b"y")
        assert pu.path_is_dir(f"{S3_ROOT}/dir1/") is True
        assert pu.path_is_dir(f"{S3_ROOT}/dir1/child/file.bin") is False

    def test_path_mkdir_is_noop(self) -> None:
        pu.path_mkdir(f"{S3_ROOT}/new_dir/sub")

    def test_path_content_length(self) -> None:
        payload = b"1234567890" * 50
        pu.path_write_bytes(f"{S3_ROOT}/sized/payload.bin", payload)
        assert pu.path_content_length(f"{S3_ROOT}/sized/payload.bin") == len(payload)

    def test_path_content_length_missing_raises(self) -> None:
        with pytest.raises(PipelineError) as exc_info:
            pu.path_content_length(f"{S3_ROOT}/does/not/exist.bin")
        assert "STORAGE_S3_OP_FAILED" in str(exc_info.value.error_code)

    def test_path_listdir_with_delimiter(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/top/a.bin", b"a")
        pu.path_write_bytes(f"{S3_ROOT}/top/b.bin", b"b")
        pu.path_write_bytes(f"{S3_ROOT}/top/sub/c.bin", b"c")
        entries = pu.path_listdir(f"{S3_ROOT}/top/")
        assert f"{S3_ROOT}/top/a.bin" in entries
        assert f"{S3_ROOT}/top/b.bin" in entries
        assert f"{S3_ROOT}/top/sub/" in entries
        assert f"{S3_ROOT}/top/sub/c.bin" not in entries

    def test_path_glob_suffix_filter(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/glob/file1.json", b"{}")
        pu.path_write_bytes(f"{S3_ROOT}/glob/file2.parquet", b"pq")
        pu.path_write_bytes(f"{S3_ROOT}/glob/nested/file3.json", b"[]")
        matched = pu.path_glob(f"{S3_ROOT}/glob/", "*.json")
        assert f"{S3_ROOT}/glob/file1.json" in matched
        assert f"{S3_ROOT}/glob/file2.parquet" not in matched
        assert f"{S3_ROOT}/glob/nested/file3.json" not in matched

    def test_path_rglob_recursive(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/rglob/a.parquet", b"1")
        pu.path_write_bytes(f"{S3_ROOT}/rglob/sub/b.parquet", b"2")
        pu.path_write_bytes(f"{S3_ROOT}/rglob/sub/deep/c.parquet", b"3")
        pu.path_write_bytes(f"{S3_ROOT}/rglob/skip.txt", b"nope")
        matched = pu.path_rglob(f"{S3_ROOT}/rglob/", "*.parquet")
        assert len(matched) == 3
        assert all(m.endswith(".parquet") for m in matched)

    def test_path_replace_intra_bucket(self) -> None:
        pu.path_write_bytes(f"{S3_ROOT}/replace/src.bin", b"original")
        pu.path_replace(
            f"{S3_ROOT}/replace/src.bin", f"{S3_ROOT}/replace/dst.bin"
        )
        assert pu.path_exists(f"{S3_ROOT}/replace/dst.bin") is True
        assert pu.path_read_bytes(f"{S3_ROOT}/replace/dst.bin") == b"original"

    def test_path_delete_tree_batch(self) -> None:
        for i in range(5):
            pu.path_write_bytes(f"{S3_ROOT}/tree/sub/file{i}.bin", str(i).encode())
        pu.path_write_bytes(f"{S3_ROOT}/tree/root.bin", b"r")
        pu.path_write_bytes(f"{S3_ROOT}/tree_sibling/safe.bin", b"safe")
        pu.path_delete_tree(f"{S3_ROOT}/tree/")
        assert pu.path_exists(f"{S3_ROOT}/tree/") is False
        assert pu.path_exists(f"{S3_ROOT}/tree_sibling/safe.bin") is True

    def test_path_open_for_append_buffer(self) -> None:
        with pu.path_open_for_append(f"{S3_ROOT}/append/log.txt") as fh:
            fh.write("alpha\n")
            fh.write("beta\n")
        with pu.path_open_for_append(f"{S3_ROOT}/append/log.txt") as fh:
            fh.write("gamma\n")
        assert (
            pu.path_read_text(f"{S3_ROOT}/append/log.txt")
            == "alpha\nbeta\ngamma\n"
        )

    def test_path_write_bytes_atomic_uses_tmp_copy_delete(self) -> None:
        """Atomic mode writes a .tmp key, copies, then deletes the tmp."""
        pu.path_write_bytes(
            f"{S3_ROOT}/atomic/data.bin", b"atomic payload", atomic=True
        )
        assert pu.path_read_bytes(f"{S3_ROOT}/atomic/data.bin") == b"atomic payload"
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        resp = client.list_objects_v2(Bucket=S3_BUCKET, Prefix="elt_root/atomic/")
        keys = [o["Key"] for o in resp.get("Contents", [])]
        assert all(not k.endswith(".tmp") for k in keys)

    def test_path_string_helpers_join_parent_basename_suffix_normalize(
        self,
    ) -> None:
        joined = pu.join_paths(S3_ROOT, "seg1", "seg2", "table=abc/")
        assert joined == f"{S3_ROOT}/seg1/seg2/table=abc"
        parent = pu.path_parent(f"{S3_ROOT}/a/b/c.bin")
        assert parent == f"{S3_ROOT}/a/b"
        base = pu.path_basename(f"{S3_ROOT}/a/b/c.bin")
        assert base == "c.bin"
        with_suffix = pu.path_with_suffix(f"{S3_ROOT}/a/file", ".parquet")
        assert with_suffix == f"{S3_ROOT}/a/file.parquet"
        normalized = pu.path_normalize(f"{S3_ROOT}//dup//slashes//")
        assert normalized == f"{S3_ROOT}/dup/slashes/"


@pytest.mark.emulator
@pytest.mark.usefixtures("_s3_moto_env")
class TestS3EmulatorStagingSwap:
    """Staging-swap protocol (full_refresh + partition_overwrite) end-to-end
    against moto S3, using the real S3Backend.staging_swap_atomic method."""

    def _seed(self, prefix: str, files: dict[str, bytes]) -> None:
        for rel, data in files.items():
            pu.path_write_bytes(f"{prefix}/{rel}", data)

    def test_full_refresh_replaces_all_and_preserves_siblings(self) -> None:
        staging = f"{S3_ROOT}/swap/staging"
        target = f"{S3_ROOT}/swap/target"
        self._seed(
            staging,
            {
                "dt=2026-01-01/entity=A/part-0001.parquet": b"new_A1",
                "dt=2026-01-01/entity=B/part-0001.parquet": b"new_B1",
                "_SUCCESS": b"",
            },
        )
        self._seed(
            target,
            {
                "dt=2025-12-31/entity=OLD/part-0001.parquet": b"old_stale",
                "dt=2026-01-01/entity=B/part-0001.parquet": b"old_B",
                "_SUCCESS": b"old",
            },
        )
        sibling_root = f"{S3_ROOT}/swap/sibling"
        self._seed(sibling_root, {"keep.parquet": b"untouched"})

        _sb.atomic_swap(staging_path=staging, target_path=target, mode="full_refresh")

        # Staging is purged
        assert pu.path_exists(staging) is False
        # Old stale keys are removed; new keys present
        assert (
            pu.path_exists(f"{target}/dt=2025-12-31/entity=OLD/part-0001.parquet")
            is False
        )
        assert pu.path_read_bytes(
            f"{target}/dt=2026-01-01/entity=A/part-0001.parquet"
        ) == b"new_A1"
        assert pu.path_read_bytes(
            f"{target}/dt=2026-01-01/entity=B/part-0001.parquet"
        ) == b"new_B1"
        # Sibling table never touched
        assert pu.path_read_bytes(f"{sibling_root}/keep.parquet") == b"untouched"

    def test_partition_overwrite_leaf_only_no_sibling_blast_radius(self) -> None:
        staging = f"{S3_ROOT}/p_swap/staging"
        target = f"{S3_ROOT}/p_swap/target"
        self._seed(
            staging,
            {
                "dt=2026-01-01/entity=A/part-0001.parquet": b"A_new",
                "dt=2026-01-02/entity=C/part-0001.parquet": b"C_new",
                "_SUCCESS": b"",
            },
        )
        self._seed(
            target,
            {
                "dt=2026-01-01/entity=A/part-old.parquet": b"A_old_stale",
                "dt=2026-01-01/entity=B/part-0001.parquet": b"B_preserved",
                "dt=2026-01-02/entity=C/part-old.parquet": b"C_old_stale",
                "dt=2025-12-31/entity=Z/part-0001.parquet": b"Z_untouched",
            },
        )

        _sb.atomic_swap(
            staging_path=staging, target_path=target, mode="partition_overwrite"
        )

        # Staging cleared
        assert pu.path_exists(staging) is False
        # dt=2026-01-01/entity=A: old stale gone, new present
        assert (
            pu.path_exists(f"{target}/dt=2026-01-01/entity=A/part-old.parquet")
            is False
        )
        assert pu.path_read_bytes(
            f"{target}/dt=2026-01-01/entity=A/part-0001.parquet"
        ) == b"A_new"
        # SIBLING entity=B on SAME dt untouched — leaf-only guarantee
        assert pu.path_read_bytes(
            f"{target}/dt=2026-01-01/entity=B/part-0001.parquet"
        ) == b"B_preserved"
        # dt=2026-01-02/entity=C: old gone, new present
        assert (
            pu.path_exists(f"{target}/dt=2026-01-02/entity=C/part-old.parquet")
            is False
        )
        assert pu.path_read_bytes(
            f"{target}/dt=2026-01-02/entity=C/part-0001.parquet"
        ) == b"C_new"
        # UNRELATED dt=2025-12-31 never touched
        assert pu.path_read_bytes(
            f"{target}/dt=2025-12-31/entity=Z/part-0001.parquet"
        ) == b"Z_untouched"


@pytest.mark.emulator
@pytest.mark.usefixtures("_s3_moto_env")
class TestS3EmulatorL1Landing:
    """Category B — L1 raw-landing roundtrip: write a raw payload and its
    manifest using the real path_utils dispatcher against moto S3, then list
    and verify integrity end-to-end."""

    def test_l1_land_raw_plus_manifest_and_list_back(self) -> None:
        import hashlib

        job = "orders_api"
        window = "dt=2026-01-01"
        raw_bytes = (
            b'{"id":1,"customer":"alice","amount":99.0}\n'
            b'{"id":2,"customer":"bob","amount":150.0}\n'
        )
        raw_key = f"{S3_ROOT}/l1/{job}/{window}/raw_00001.jsonl"
        pu.path_write_bytes(raw_key, raw_bytes)

        checksum = hashlib.sha256(raw_bytes).hexdigest()
        manifest_text = (
            "source_id,file_path,byte_count,record_count,sha256\n"
            f"{job},{raw_key},{len(raw_bytes)},2,{checksum}\n"
        )
        manifest_key = f"{S3_ROOT}/l1/{job}/{window}/_manifest.csv"
        pu.path_write_text(manifest_key, manifest_text)

        listings = pu.path_listdir(f"{S3_ROOT}/l1/{job}/{window}/")
        assert raw_key in listings
        assert manifest_key in listings

        roundtrip = pu.path_read_bytes(raw_key)
        assert roundtrip == raw_bytes
        manifest_rt = pu.path_read_text(manifest_key)
        assert manifest_rt == manifest_text
        assert hashlib.sha256(roundtrip).hexdigest() == checksum


# ---------------------------------------------------------------------------
# 2. GCS  —  fake-gcs-server (via testcontainers; requires Docker)
# ---------------------------------------------------------------------------


GCS_BUCKET = "elt-pipeline-emulator-bucket"
GCS_ROOT = f"gs://{GCS_BUCKET}/elt_root"


@pytest.fixture()
def _gcs_emulator(monkeypatch) -> Iterator[None]:
    pytest.importorskip("testcontainers")
    from testcontainers.gcs import GCSContainer

    _reset_all_backend_singletons()
    try:
        container = GCSContainer("fsouza/fake-gcs-server:1.47.6")
    except Exception:
        pytest.skip("Docker unavailable or fake-gcs-server image pull failed")
    with container as gcs:
        endpoint = gcs.get_exposed_url()
        from google.api_core.client_options import ClientOptions
        from google.cloud import storage

        client = storage.Client(
            project="test-project",
            client_options=ClientOptions(api_endpoint=endpoint),
            use_auth_w_custom_endpoint=False,
        )
        client.create_bucket(GCS_BUCKET, location="US")

        def _emulator_gcs_client_factory() -> Any:
            return client

        monkeypatch.setattr(pu, "_gcs_client", _emulator_gcs_client_factory)
        yield
    _reset_all_backend_singletons()


@pytest.mark.emulator
@pytest.mark.usefixtures("_gcs_emulator")
class TestGCSEmulatorLeafOps:
    def test_write_read_bytes_roundtrip(self) -> None:
        pu.path_write_bytes(f"{GCS_ROOT}/a/b/c.bin", b"gcs real sdk roundtrip")
        assert pu.path_read_bytes(f"{GCS_ROOT}/a/b/c.bin") == b"gcs real sdk roundtrip"

    def test_exists_dir_and_key(self) -> None:
        pu.path_write_bytes(f"{GCS_ROOT}/p/key.txt", b"x")
        assert pu.path_exists(f"{GCS_ROOT}/p/key.txt") is True
        assert pu.path_exists(f"{GCS_ROOT}/p/") is True
        assert pu.path_exists(f"{GCS_ROOT}/missing/") is False

    def test_listdir_and_rglob(self) -> None:
        for k in (
            "top/f1.json",
            "top/f2.parquet",
            "top/sub/f3.json",
            "top/sub/deep/f4.parquet",
        ):
            pu.path_write_bytes(f"{GCS_ROOT}/{k}", k.encode())
        listed = pu.path_listdir(f"{GCS_ROOT}/top/")
        assert f"{GCS_ROOT}/top/f1.json" in listed
        assert f"{GCS_ROOT}/top/sub/" in listed
        parquets = pu.path_rglob(f"{GCS_ROOT}/top/", "*.parquet")
        assert len(parquets) == 2

    def test_delete_tree_preserves_siblings(self) -> None:
        pu.path_write_bytes(f"{GCS_ROOT}/killme/a.bin", b"x")
        pu.path_write_bytes(f"{GCS_ROOT}/killme/sub/b.bin", b"y")
        pu.path_write_bytes(f"{GCS_ROOT}/keepme/c.bin", b"z")
        pu.path_delete_tree(f"{GCS_ROOT}/killme/")
        assert pu.path_exists(f"{GCS_ROOT}/killme/") is False
        assert pu.path_read_bytes(f"{GCS_ROOT}/keepme/c.bin") == b"z"

    def test_staging_swap_full_refresh(self) -> None:
        staging = f"{GCS_ROOT}/swap/staging"
        target = f"{GCS_ROOT}/swap/target"
        pu.path_write_bytes(f"{staging}/part-0001.parquet", b"fresh")
        pu.path_write_bytes(f"{target}/stale.parquet", b"old")
        _sb.atomic_swap(staging_path=staging, target_path=target, mode="full_refresh")
        assert pu.path_exists(staging) is False
        assert pu.path_read_bytes(f"{target}/part-0001.parquet") == b"fresh"
        assert pu.path_exists(f"{target}/stale.parquet") is False


# ---------------------------------------------------------------------------
# 3. ADLS Gen2  —  Azurite (via testcontainers; requires Docker)
# ---------------------------------------------------------------------------


ADLS_ACCOUNT = "devstoreaccount1"
ADLS_CONTAINER = "elt-test-container"
ADLS_ROOT = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/elt_root"
# Azurite well-known dev credentials (public constant, not a secret)
_AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


@pytest.fixture()
def _adls_emulator(monkeypatch) -> Iterator[None]:
    pytest.importorskip("testcontainers")
    try:
        from testcontainers.azurite import AzuriteContainer
    except Exception:
        pytest.skip("Azurite testcontainer not available in current testcontainers version")
    _reset_all_backend_singletons()
    try:
        container = AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.30.0")
    except Exception:
        pytest.skip("Docker unavailable or Azurite image pull failed")
    with container as azurite:
        blob_port = azurite.get_exposed_port(10000)
        host = azurite.get_container_host_ip()
        blob_endpoint = f"http://{host}:{blob_port}/{ADLS_ACCOUNT}"

        from azure.core.credentials import AzureNamedKeyCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        credential = AzureNamedKeyCredential(ADLS_ACCOUNT, _AZURITE_ACCOUNT_KEY)
        service_client = DataLakeServiceClient(
            account_url=blob_endpoint, credential=credential, api_version="2023-11-03"
        )
        service_client.create_file_system(file_system=ADLS_CONTAINER)

        def _emulator_adls_client_factory() -> Any:
            return service_client

        monkeypatch.setattr(pu, "_adls_client", _emulator_adls_client_factory)
        yield
    _reset_all_backend_singletons()


@pytest.mark.emulator
@pytest.mark.usefixtures("_adls_emulator")
class TestADLSEmulatorLeafOps:
    def test_write_read_bytes_roundtrip(self) -> None:
        pu.path_write_bytes(
            f"{ADLS_ROOT}/lake/landing/data.bin", b"azure adls real sdk roundtrip"
        )
        assert (
            pu.path_read_bytes(f"{ADLS_ROOT}/lake/landing/data.bin")
            == b"azure adls real sdk roundtrip"
        )

    def test_exists_and_content_length(self) -> None:
        payload = b"hello" * 1000
        pu.path_write_bytes(f"{ADLS_ROOT}/sized/payload.bin", payload)
        assert pu.path_exists(f"{ADLS_ROOT}/sized/payload.bin") is True
        assert pu.path_content_length(f"{ADLS_ROOT}/sized/payload.bin") == len(payload)

    def test_listdir_glob_and_delete_tree(self) -> None:
        for k in (
            "a/x.parquet",
            "a/y.json",
            "a/sub/z.parquet",
            "b/keep.parquet",
        ):
            pu.path_write_bytes(f"{ADLS_ROOT}/{k}", k.encode())
        listed = pu.path_listdir(f"{ADLS_ROOT}/a/")
        assert f"{ADLS_ROOT}/a/x.parquet" in listed
        assert f"{ADLS_ROOT}/a/sub/" in listed
        globs = pu.path_glob(f"{ADLS_ROOT}/a/", "*.parquet")
        assert f"{ADLS_ROOT}/a/x.parquet" in globs
        assert f"{ADLS_ROOT}/a/sub/z.parquet" not in globs
        pu.path_delete_tree(f"{ADLS_ROOT}/a/")
        assert pu.path_exists(f"{ADLS_ROOT}/a/") is False
        assert pu.path_read_bytes(f"{ADLS_ROOT}/b/keep.parquet") == b"b/keep.parquet"

    def test_staging_swap_partition_overwrite(self) -> None:
        staging = f"{ADLS_ROOT}/p_swap/staging"
        target = f"{ADLS_ROOT}/p_swap/target"
        pu.path_write_bytes(
            f"{staging}/region=us/part-0001.parquet", b"us_new"
        )
        pu.path_write_bytes(
            f"{target}/region=us/part-old.parquet", b"us_stale"
        )
        pu.path_write_bytes(
            f"{target}/region=eu/part-0001.parquet", b"eu_untouched"
        )
        _sb.atomic_swap(
            staging_path=staging, target_path=target, mode="partition_overwrite"
        )
        assert pu.path_exists(staging) is False
        assert pu.path_exists(f"{target}/region=us/part-old.parquet") is False
        assert (
            pu.path_read_bytes(f"{target}/region=us/part-0001.parquet") == b"us_new"
        )
        assert (
            pu.path_read_bytes(f"{target}/region=eu/part-0001.parquet")
            == b"eu_untouched"
        )
