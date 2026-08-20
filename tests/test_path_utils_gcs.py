from __future__ import annotations

from typing import Any, Iterator

import pytest

import elt_pipeline.sql._staging_swap as _swap_mod
from elt_pipeline.shared import path_utils as pu
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError


class _FakeBlob:
    def __init__(
        self,
        bucket_name: str,
        name: str,
        size: int = 0,
        client: "_FakeGCSClient | None" = None,
    ) -> None:
        self.bucket = type("B", (), {"name": bucket_name})()
        self.name = name
        self.size = size
        self._client = client

    def exists(self) -> bool:
        assert self._client is not None
        self._client.calls.append(
            ("blob.exists", {"bucket": self.bucket.name, "key": self.name})
        )
        return (self.bucket.name, self.name) in self._client.objects

    def download_as_bytes(self) -> bytes:
        assert self._client is not None
        self._client.calls.append(
            (
                "blob.download_as_bytes",
                {"bucket": self.bucket.name, "key": self.name},
            )
        )
        key = (self.bucket.name, self.name)
        if key not in self._client.objects:
            raise self._client.exceptions.NotFound("blob not found")
        return self._client.objects[key]

    def upload_from_string(self, data: bytes | str) -> None:
        assert self._client is not None
        self._client.calls.append(
            (
                "blob.upload_from_string",
                {"bucket": self.bucket.name, "key": self.name},
            )
        )
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._client.objects[(self.bucket.name, self.name)] = data
        self.size = len(data)

    def delete(self) -> None:
        assert self._client is not None
        self._client.calls.append(
            ("blob.delete", {"bucket": self.bucket.name, "key": self.name})
        )
        self._client.objects.pop((self.bucket.name, self.name), None)


class _FakeBucket:
    def __init__(self, bucket_name: str, client: "_FakeGCSClient") -> None:
        self.name = bucket_name
        self._client = client

    def blob(self, key: str) -> _FakeBlob:
        return _FakeBlob(self.name, key, client=self._client)

    def get_blob(self, key: str) -> _FakeBlob | None:
        self._client.calls.append(("bucket.get_blob", {"bucket": self.name, "key": key}))
        if (self.name, key) in self._client.objects:
            size = len(self._client.objects[(self.name, key)])
            return _FakeBlob(self.name, key, size=size, client=self._client)
        return None

    def copy_blob(
        self,
        source_blob: _FakeBlob,
        destination_bucket: "_FakeBucket",
        new_name: str | None = None,
    ) -> _FakeBlob:
        dst_key = new_name if new_name is not None else source_blob.name
        self._client.calls.append(
            (
                "bucket.copy_blob",
                {
                    "src_bucket": source_blob.bucket.name,
                    "src_key": source_blob.name,
                    "dst_bucket": destination_bucket.name,
                    "dst_key": dst_key,
                },
            )
        )
        src_data = self._client.objects.get((source_blob.bucket.name, source_blob.name), b"")
        self._client.objects[(destination_bucket.name, dst_key)] = src_data
        return _FakeBlob(destination_bucket.name, dst_key, size=len(src_data), client=self._client)

    def delete_blobs(self, blobs: list[_FakeBlob]) -> None:
        self._client.calls.append(
            (
                "bucket.delete_blobs",
                {
                    "bucket": self.name,
                    "keys": sorted(b.name for b in blobs),
                },
            )
        )
        for b in blobs:
            self._client.objects.pop((self.name, b.name), None)


class _FakeBlobIterator:
    def __init__(
        self,
        client: "_FakeGCSClient",
        bucket_name: str,
        prefix: str,
        delimiter: str | None,
        max_results: int | None,
    ) -> None:
        self._client = client
        self._bucket = bucket_name
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results
        self._prefixes: set[str] = set()
        self._content_keys: list[str] = []
        matched = sorted(
            k for (b, k) in self._client.objects.keys()
            if b == bucket_name and k.startswith(prefix)
        )
        for k in matched:
            rest = k[len(prefix):]
            if delimiter and delimiter in rest:
                first_seg = rest.split(delimiter, 1)[0] + delimiter
                cp = prefix + first_seg
                self._prefixes.add(cp)
            else:
                self._content_keys.append(k)
        if max_results is not None:
            self._content_keys = self._content_keys[:max_results]

    def __iter__(self) -> Iterator[_FakeBlob]:
        for k in self._content_keys:
            size = len(self._client.objects.get((self._bucket, k), b""))
            yield _FakeBlob(self._bucket, k, size=size, client=self._client)

    @property
    def prefixes(self) -> set[str]:
        return self._prefixes


class _FakeGCSClient:
    """Record-keeping fake GCS client; mimics google-cloud-storage API surface we use."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    class exceptions:  # noqa: N801 - mirror GCS SDK shape
        class NotFound(Exception):
            pass
        class Forbidden(Exception):
            pass
        class ServiceUnavailable(Exception):
            pass

    def bucket(self, bucket_name: str) -> _FakeBucket:
        self.calls.append(("client.bucket", {"bucket": bucket_name}))
        return _FakeBucket(bucket_name, self)

    def list_blobs(
        self,
        bucket_or_name: str,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int | None = None,
    ) -> _FakeBlobIterator:
        bucket_name = bucket_or_name if isinstance(bucket_or_name, str) else bucket_or_name.name
        p = prefix or ""
        self.calls.append(
            (
                "client.list_blobs",
                {
                    "bucket": bucket_name,
                    "prefix": p,
                    "delimiter": delimiter,
                    "max_results": max_results,
                },
            )
        )
        return _FakeBlobIterator(
            self,
            bucket_name,
            prefix=p,
            delimiter=delimiter,
            max_results=max_results,
        )


class TestMockedGCSRouting:
    """Scheme-dispatch tests.

    The fake client replaces _gcs_client() via fixture monkeypatch, so any
    scheme=gs call flows through the fake. Correct routing is verified by
    asserting fake GCS call shapes and object store contents — if a call
    ever fell through to the POSIX branch, the fake client would have
    zero recorded calls and the assertions would fail.
    """

    @pytest.fixture()
    def fake_gcs(self, monkeypatch: pytest.MonkeyPatch) -> _FakeGCSClient:
        fake = _FakeGCSClient()
        monkeypatch.setattr(pu, "_GCS_CLIENT", None)
        monkeypatch.setattr(pu, "_gcs_client", lambda: fake)
        return fake

    def test_gs_write_atomic_uses_tmp_then_copy_then_delete(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        target = "gs://my-bucket/path/to/file.parquet"
        pu.path_write_bytes(target, b"data!")
        assert fake_gcs.objects[("my-bucket", "path/to/file.parquet")] == b"data!"
        upload_calls = [c for c in fake_gcs.calls if c[0] == "blob.upload_from_string"]
        copy_calls = [c for c in fake_gcs.calls if c[0] == "bucket.copy_blob"]
        del_calls = [c for c in fake_gcs.calls if c[0] == "blob.delete"]
        assert len(upload_calls) == 1
        assert upload_calls[0][1]["key"] == "path/to/file.parquet.tmp"
        assert len(copy_calls) == 1
        cp = copy_calls[0][1]
        assert cp["src_bucket"] == "my-bucket"
        assert cp["src_key"] == "path/to/file.parquet.tmp"
        assert cp["dst_bucket"] == "my-bucket"
        assert cp["dst_key"] == "path/to/file.parquet"
        assert len(del_calls) == 1
        assert del_calls[0][1]["key"] == "path/to/file.parquet.tmp"
        assert ("my-bucket", "path/to/file.parquet.tmp") not in fake_gcs.objects

    def test_gs_write_non_atomic_skips_tmp(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        target = "gs://b/key.bin"
        pu.path_write_bytes(target, b"v", atomic=False)
        uploads = [c for c in fake_gcs.calls if c[0] == "blob.upload_from_string"]
        assert len(uploads) == 1
        assert uploads[0][1]["key"] == "key.bin"
        copies = [c for c in fake_gcs.calls if c[0] == "bucket.copy_blob"]
        assert copies == []

    def test_gs_listdir_uses_list_blobs_and_returns_gs_uris(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("bkt", "p/a.txt")] = b"a"
        fake_gcs.objects[("bkt", "p/b.csv")] = b"b"
        fake_gcs.objects[("bkt", "p/sub/nested.parquet")] = b"n"
        listing = pu.path_listdir("gs://bkt/p")
        list_calls = [c for c in fake_gcs.calls if c[0] == "client.list_blobs"]
        assert len(list_calls) >= 1
        assert list_calls[0][1]["delimiter"] == "/"
        basenames = sorted(pu.path_basename(x) for x in listing)
        assert "a.txt" in basenames
        assert "b.csv" in basenames
        assert "sub" in basenames
        for entry in listing:
            assert entry.startswith("gs://bkt/"), entry

    def test_gs_exists_uses_blob_exists_for_keys(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "k")] = b"x"
        assert pu.path_exists("gs://b/k") is True
        assert pu.path_exists("gs://b/missing") is False
        exists_calls = [c for c in fake_gcs.calls if c[0] == "blob.exists"]
        assert len(exists_calls) >= 1

    def test_gs_exists_for_empty_prefix_uses_list_blobs(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "prefix/inside/file.txt")] = b"data"
        assert pu.path_exists("gs://b/prefix/") is True
        list_calls = [c for c in fake_gcs.calls if c[0] == "client.list_blobs"]
        assert any(c[1]["max_results"] == 1 for c in list_calls)

    def test_gs_mkdir_is_noop_but_validates_scheme(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        pu.path_mkdir("gs://b/virtual/dir")
        assert not any(
            c[0] in ("blob.upload_from_string", "bucket.copy_blob", "blob.delete")
            for c in fake_gcs.calls
        )

    def test_gs_read_bytes(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "k")] = b"\x00\x01\x02\xff"
        assert pu.path_read_bytes("gs://b/k") == b"\x00\x01\x02\xff"
        read_calls = [c for c in fake_gcs.calls if c[0] == "blob.download_as_bytes"]
        assert len(read_calls) == 1

    def test_gs_content_length_via_get_blob(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        data = b"hello world"
        fake_gcs.objects[("b", "f.bin")] = data
        assert pu.path_content_length("gs://b/f.bin") == len(data)
        get_blob_calls = [c for c in fake_gcs.calls if c[0] == "bucket.get_blob"]
        assert len(get_blob_calls) == 1
        assert get_blob_calls[0][1]["key"] == "f.bin"

    def test_gs_content_length_missing_raises(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            pu.path_content_length("gs://b/missing")
        assert "STORAGE_GCS_OP_FAILED" in exc_info.value.error_code
        assert "not found" in exc_info.value.message.lower()

    def test_gs_is_dir_checks_prefixes_and_contents(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "p/sub/f.bin")] = b"data"
        assert pu.path_is_dir("gs://b/p") is True
        assert pu.path_is_dir("gs://b/empty") is False

    def test_gs_replace_uses_copy_then_delete(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "src")] = b"hello"
        pu.path_replace("gs://b/src", "gs://b/dst")
        assert ("b", "src") not in fake_gcs.objects
        assert fake_gcs.objects[("b", "dst")] == b"hello"
        copy_calls = [c for c in fake_gcs.calls if c[0] == "bucket.copy_blob"]
        del_calls = [c for c in fake_gcs.calls if c[0] == "blob.delete"]
        assert len(copy_calls) == 1
        assert copy_calls[0][1]["src_key"] == "src"
        assert copy_calls[0][1]["dst_key"] == "dst"
        assert len(del_calls) == 1

    def test_gs_glob_filtered_by_fnmatch(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "data/a.parquet")] = b"a"
        fake_gcs.objects[("b", "data/b.csv")] = b"b"
        fake_gcs.objects[("b", "data/sub/nested.parquet")] = b"n"
        matches = sorted(pu.path_glob("gs://b/data", "*.parquet"))
        assert len(matches) == 1
        assert matches[0].endswith("/a.parquet")

    def test_gs_rglob_recursive_includes_subprefixes(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        fake_gcs.objects[("b", "root/top.parquet")] = b"t"
        fake_gcs.objects[("b", "root/dt=2025-01-01/p.parquet")] = b"d1"
        fake_gcs.objects[("b", "root/dt=2025-01-02/p.parquet")] = b"d2"
        matches = sorted(pu.path_rglob("gs://b/root", "*.parquet"))
        assert len(matches) == 3

    def test_gs_delete_tree_batch_deletes(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        prefix = "wh/sql/"
        for i in range(5):
            fake_gcs.objects[("b", f"{prefix}file_{i}.parquet")] = b"x" * i
        fake_gcs.objects[("b", "other/keep.parquet")] = b"keep"
        pu.path_delete_tree("gs://b/wh/sql")
        remaining = [k for (b, k) in fake_gcs.objects.keys() if b == "b"]
        assert remaining == ["other/keep.parquet"]
        delete_blobs_calls = [c for c in fake_gcs.calls if c[0] == "bucket.delete_blobs"]
        assert len(delete_blobs_calls) == 1

    def test_gs_open_for_append_reads_existing_then_rewrites(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        target = "gs://b/events.jsonl"
        with pu.path_open_for_append(target) as f:
            f.write('{"a": 1}\n')
        with pu.path_open_for_append(target) as f:
            f.write('{"b": 2}\n')
        assert fake_gcs.objects[("b", "events.jsonl")] == b'{"a": 1}\n{"b": 2}\n'

    def test_gs_relative_to(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        assert pu.path_relative_to("gs://b/p1/p2/k", "gs://b/p1") == "p2/k"

    def test_gs_split_gcs_path_rejects_missing_bucket(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        with pytest.raises(ConfigValidationError):
            pu._split_gcs_path("gs://")

    def test_gs_split_gcs_path_root_only_bucket(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        assert pu._split_gcs_path("gs://mybucket") == ("mybucket", "")


class TestStagingSwapGCS:
    """Staging-swap protocol tests for GCS backend mirroring S3 patterns."""

    @pytest.fixture()
    def fake_gcs(self, monkeypatch: pytest.MonkeyPatch) -> _FakeGCSClient:
        fake = _FakeGCSClient()
        monkeypatch.setattr(pu, "_GCS_CLIENT", None)
        monkeypatch.setattr(pu, "_gcs_client", lambda: fake)
        monkeypatch.setattr(_swap_mod, "_s3_client", lambda: fake)
        return fake

    def test_full_refresh_replaces_target_with_staging(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        staging_prefix = "wh/_staging/sql/tbl/run_id=r1/"
        target_prefix = "wh/sql/tbl/"
        for i in range(3):
            fake_gcs.objects[("b", f"{staging_prefix}part-{i}.parquet")] = f"new_{i}".encode()
        fake_gcs.objects[("b", f"{target_prefix}old.parquet")] = b"stale"
        fake_gcs.objects[("b", "wh/sql/other.parquet")] = b"keep_other"

        _swap_mod.atomic_swap(
            staging_path=f"gs://b/{staging_prefix.rstrip('/')}",
            target_path=f"gs://b/{target_prefix.rstrip('/')}",
            scheme=_swap_mod._StorageScheme.gs,
            mode="full_refresh",
        )

        all_keys = sorted(k for (b, k) in fake_gcs.objects.keys() if b == "b")
        assert all_keys == sorted([
            "wh/sql/other.parquet",
            "wh/sql/tbl/part-0.parquet",
            "wh/sql/tbl/part-1.parquet",
            "wh/sql/tbl/part-2.parquet",
        ])
        for i in range(3):
            body = fake_gcs.objects[("b", f"wh/sql/tbl/part-{i}.parquet")]
            assert body == f"new_{i}".encode()

        op_names = [c[0] for c in fake_gcs.calls]
        assert "bucket.copy_blob" in op_names

    def test_partition_overwrite_replaces_only_matching_partition_dirs(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        staging = "stg/run"
        target = "wh/tbl"
        sp = f"{staging}/" if not staging.endswith("/") else staging
        tp = f"{target}/" if not target.endswith("/") else target

        fake_gcs.objects[("b", f"{sp}dt=2025-01-02/part-0.parquet")] = b"new_jan2"
        fake_gcs.objects[("b", f"{sp}dt=2025-01-02/part-1.parquet")] = b"new_jan2_b"

        fake_gcs.objects[("b", f"{tp}dt=2024-12-31/part-0.parquet")] = b"keep_dec"
        fake_gcs.objects[("b", f"{tp}dt=2025-01-02/part-stale.parquet")] = b"stale_jan2"
        fake_gcs.objects[("b", f"{tp}dt=2025-01-01/part-0.parquet")] = b"keep_jan1"

        _swap_mod.atomic_swap(
            staging_path=f"gs://b/{staging}",
            target_path=f"gs://b/{target}",
            scheme=_swap_mod._StorageScheme.gs,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (b, k) in fake_gcs.objects.keys() if b == "b")
        assert all_keys == sorted([
            "wh/tbl/dt=2024-12-31/part-0.parquet",
            "wh/tbl/dt=2025-01-01/part-0.parquet",
            "wh/tbl/dt=2025-01-02/part-0.parquet",
            "wh/tbl/dt=2025-01-02/part-1.parquet",
        ])

    def test_nested_partition_overwrite_preserves_siblings(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        staging = "stg/run"
        target = "wh/tbl"
        sp = f"{staging}/"
        tp = f"{target}/"

        fake_gcs.objects[("b", f"{sp}src=web/business_date=2026-07-31/p.parquet")] = b"jul_new"
        fake_gcs.objects[("b", f"{sp}src=web/business_date=2026-08-10/p.parquet")] = b"aug_new"

        fake_gcs.objects[("b", f"{tp}src=web/business_date=2026-06-01/p.parquet")] = b"jun_keep"
        stale_key = f"{tp}src=web/business_date=2026-07-31/p_stale.parquet"
        fake_gcs.objects[("b", stale_key)] = b"jul_stale"
        fake_gcs.objects[("b", f"{tp}src=api/business_date=2026-08-10/p.parquet")] = b"api_aug_keep"

        _swap_mod.atomic_swap(
            staging_path=f"gs://b/{staging}",
            target_path=f"gs://b/{target}",
            scheme=_swap_mod._StorageScheme.gs,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (b, k) in fake_gcs.objects.keys() if b == "b")
        assert all_keys == sorted([
            "wh/tbl/src=web/business_date=2026-06-01/p.parquet",
            "wh/tbl/src=web/business_date=2026-07-31/p.parquet",
            "wh/tbl/src=web/business_date=2026-08-10/p.parquet",
            "wh/tbl/src=api/business_date=2026-08-10/p.parquet",
        ])

    def test_staging_empty_raises(self, fake_gcs: _FakeGCSClient) -> None:
        with pytest.raises(PipelineError) as exc_info:
            _swap_mod.atomic_swap(
                staging_path="gs://b/empty_staging",
                target_path="gs://b/wh/tbl",
                scheme=_swap_mod._StorageScheme.gs,
                mode="full_refresh",
            )
        assert "SQL_ATOMIC_SWAP_FAILED" in exc_info.value.error_code

    def test_cross_bucket_swap_rejected(self, fake_gcs: _FakeGCSClient) -> None:
        fake_gcs.objects[("b1", "stg/p")] = b"x"
        with pytest.raises(PipelineError) as exc_info:
            _swap_mod.atomic_swap(
                staging_path="gs://b1/stg",
                target_path="gs://b2/wh",
                scheme=_swap_mod._StorageScheme.gs,
                mode="full_refresh",
            )
        assert "SQL_ATOMIC_SWAP_FAILED" in exc_info.value.error_code
        assert "share the same bucket" in exc_info.value.message

    def test_validate_swap_accepts_gs_scheme(self) -> None:
        scheme = _swap_mod.validate_swap_scheme("gs://bucket/wh/sql/orders", "orders")
        assert scheme is _swap_mod._StorageScheme.gs

    def test_unsupported_scheme_rejected_by_detect_scheme(self) -> None:
        from elt_pipeline.shared.errors import ConfigValidationError

        bad_scheme = "abfs://container/path"
        with pytest.raises(ConfigValidationError) as exc_info:
            _swap_mod.validate_swap_scheme(bad_scheme, "model")
        assert "Unsupported storage scheme" in exc_info.value.message
        assert "abfs://" in exc_info.value.message

    def test_best_effort_delete_staging_missing_is_safe(
        self, fake_gcs: _FakeGCSClient
    ) -> None:
        _swap_mod.best_effort_delete_staging("gs://b/missing_stg", _swap_mod._StorageScheme.gs)
