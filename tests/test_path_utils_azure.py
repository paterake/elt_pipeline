from __future__ import annotations

from typing import Any

import pytest

import elt_pipeline.sql._staging_swap as _swap_mod
from elt_pipeline.shared import path_utils as pu
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError

# ---------------------------------------------------------------------------
# Fake ADLS SDK client — mirrors azure.storage.filedatalake API surface we use
# ---------------------------------------------------------------------------


class _FakePathProperties:
    def __init__(self, name: str, is_directory: bool = False) -> None:
        self.name = name
        self.is_directory = is_directory


class _FakeFileDownloader:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def readall(self) -> bytes:
        return self._data

    def content_as_bytes(self) -> bytes:
        return self._data


class _FakeFileProps:
    def __init__(self, size: int) -> None:
        self.size = size
        self.content_length = size


class _FakeDataLakeFileClient:
    def __init__(
        self,
        container: str,
        key: str,
        svc: "_FakeADLSClient",
    ) -> None:
        self._container = container
        self._key = key
        self._svc = svc

    def get_file_properties(self) -> _FakeFileProps:
        self._svc.calls.append(
            ("file.get_properties", {"container": self._container, "key": self._key})
        )
        k = (self._container, self._key)
        if k not in self._svc.objects:
            raise self._svc.exceptions.ResourceNotFoundError("file not found")
        return _FakeFileProps(len(self._svc.objects[k]))

    def download_file(self) -> _FakeFileDownloader:
        self._svc.calls.append(
            ("file.download", {"container": self._container, "key": self._key})
        )
        k = (self._container, self._key)
        if k not in self._svc.objects:
            raise self._svc.exceptions.ResourceNotFoundError("file not found")
        return _FakeFileDownloader(self._svc.objects[k])

    def upload_data(self, data: bytes | str, overwrite: bool = False) -> None:
        self._svc.calls.append(
            ("file.upload", {"container": self._container, "key": self._key})
        )
        if isinstance(data, str):
            data = data.encode("utf-8")
        k = (self._container, self._key)
        if k in self._svc.objects and not overwrite:
            raise self._svc.exceptions.ServiceRequestError("already exists")
        self._svc.objects[k] = data

    def delete_file(self) -> None:
        self._svc.calls.append(
            ("file.delete", {"container": self._container, "key": self._key})
        )
        self._svc.objects.pop((self._container, self._key), None)

    def rename_file(self, new_name: str) -> None:
        self._svc.calls.append(
            (
                "file.rename",
                {
                    "container": self._container,
                    "key": self._key,
                    "new_name": new_name,
                },
            )
        )
        if "/" not in new_name:
            return
        dst_container, _, dst_key = new_name.partition("/")
        _ = dst_container
        src_k = (self._container, self._key)
        if src_k in self._svc.objects:
            self._svc.objects[(self._container, dst_key)] = self._svc.objects[src_k]
            del self._svc.objects[src_k]


class _FakeDataLakeDirectoryClient:
    def __init__(
        self,
        container: str,
        key: str,
        svc: "_FakeADLSClient",
    ) -> None:
        self._container = container
        self._key = key
        self._svc = svc

    def get_directory_properties(self) -> _FakeFileProps:
        self._svc.calls.append(
            (
                "dir.get_properties",
                {"container": self._container, "key": self._key},
            )
        )
        prefix = self._key
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        for (c, k) in self._svc.objects.keys():
            if c == self._container and k.startswith(prefix):
                return _FakeFileProps(0)
        raise self._svc.exceptions.ResourceNotFoundError("dir not found")


class _FakeFileSystemClient:
    def __init__(self, container: str, svc: "_FakeADLSClient") -> None:
        self._container = container
        self._svc = svc

    def get_file_client(self, key: str) -> _FakeDataLakeFileClient:
        return _FakeDataLakeFileClient(self._container, key, self._svc)

    def get_directory_client(self, key: str) -> _FakeDataLakeDirectoryClient:
        return _FakeDataLakeDirectoryClient(self._container, key, self._svc)

    def list_paths(
        self,
        path: str | None = None,
        recursive: bool = False,
        max_results: int | None = None,
    ) -> list[_FakePathProperties]:
        prefix = path or ""
        self._svc.calls.append(
            (
                "fs.list_paths",
                {
                    "container": self._container,
                    "prefix": prefix,
                    "recursive": recursive,
                    "max_results": max_results,
                },
            )
        )
        results: list[_FakePathProperties] = []
        matched = sorted(
            k
            for (c, k) in self._svc.objects.keys()
            if c == self._container and k.startswith(prefix)
        )
        if recursive:
            for k in matched:
                results.append(_FakePathProperties(k, is_directory=False))
        else:
            dir_prefixes: set[str] = set()
            for k in matched:
                rest = k[len(prefix):]
                if "/" in rest:
                    first_seg = rest.split("/", 1)[0] + "/"
                    dp = prefix + first_seg
                    if dp not in dir_prefixes:
                        dir_prefixes.add(dp)
                        results.append(_FakePathProperties(dp[:-1], is_directory=True))
                else:
                    results.append(_FakePathProperties(k, is_directory=False))
        if max_results is not None:
            results = results[:max_results]
        return results


class _FakeADLSClient:
    """Record-keeping fake ADLS client; mirrors azure-storage-file-datalake API."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    class exceptions:  # noqa: N801 - mirror SDK shape
        class ResourceNotFoundError(Exception):
            pass

        class ClientAuthenticationError(Exception):
            pass

        class ServiceRequestError(Exception):
            pass

    def get_file_system_client(self, container: str) -> _FakeFileSystemClient:
        self.calls.append(("client.get_file_system_client", {"container": container}))
        return _FakeFileSystemClient(container, self)

    def list_paths(
        self,
        container: str,
        prefix: str | None = None,
        recursive: bool = False,
        max_results: int | None = None,
    ) -> list[_FakePathProperties]:
        fs = self.get_file_system_client(container)
        return fs.list_paths(path=prefix, recursive=recursive, max_results=max_results)

    def delete_file(self, container: str, key: str) -> None:
        self.objects.pop((container, key), None)


# ---------------------------------------------------------------------------
# TestMockedADLSRouting — 18 scheme-dispatch tests mirroring GCS
# ---------------------------------------------------------------------------


class TestMockedADLSRouting:
    """Scheme-dispatch tests for abfss:// routing through ADLSBackend."""

    @pytest.fixture()
    def fake_adls(self, monkeypatch: pytest.MonkeyPatch) -> _FakeADLSClient:
        fake = _FakeADLSClient()
        monkeypatch.setattr(pu, "_ADLS_CLIENT", None)
        monkeypatch.setattr(pu, "_adls_client", lambda: fake)
        return fake

    ABFSS_ROOT = "abfss://mycontainer@myacct.dfs.core.windows.net"

    def test_abfss_write_atomic_uses_tmp_then_rename_then_delete(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        target = f"{self.ABFSS_ROOT}/path/to/file.parquet"
        pu.path_write_bytes(target, b"data!")
        assert fake_adls.objects[("mycontainer", "path/to/file.parquet")] == b"data!"
        upload_calls = [c for c in fake_adls.calls if c[0] == "file.upload"]
        rename_calls = [c for c in fake_adls.calls if c[0] == "file.rename"]
        del_calls = [c for c in fake_adls.calls if c[0] == "file.delete"]
        assert len(upload_calls) == 1
        assert upload_calls[0][1]["key"] == "path/to/file.parquet.tmp"
        if rename_calls:
            r = rename_calls[0][1]
            assert r["key"] == "path/to/file.parquet.tmp"
            assert r["new_name"].endswith("/path/to/file.parquet")
        leftover_tmp = ("mycontainer", "path/to/file.parquet.tmp")
        assert leftover_tmp not in fake_adls.objects
        assert ("mycontainer", "path/to/file.parquet") in fake_adls.objects
        _ = del_calls

    def test_abfss_write_non_atomic_skips_tmp(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        target = f"{self.ABFSS_ROOT}/key.bin"
        pu.path_write_bytes(target, b"v", atomic=False)
        uploads = [c for c in fake_adls.calls if c[0] == "file.upload"]
        assert len(uploads) == 1
        assert uploads[0][1]["key"] == "key.bin"

    def test_abfss_listdir_returns_abfss_uris(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "p/a.txt")] = b"a"
        fake_adls.objects[("mycontainer", "p/b.csv")] = b"b"
        fake_adls.objects[("mycontainer", "p/sub/nested.parquet")] = b"n"
        listing = pu.path_listdir(f"{self.ABFSS_ROOT}/p")
        list_calls = [c for c in fake_adls.calls if c[0] == "fs.list_paths"]
        assert len(list_calls) >= 1
        basenames = sorted(pu.path_basename(x) for x in listing)
        assert "a.txt" in basenames
        assert "b.csv" in basenames
        assert "sub" in basenames
        for entry in listing:
            assert entry.startswith("abfss://mycontainer@myacct.dfs.core.windows.net/"), entry

    def test_abfss_exists_file_uses_get_properties(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "k")] = b"x"
        assert pu.path_exists(f"{self.ABFSS_ROOT}/k") is True
        assert pu.path_exists(f"{self.ABFSS_ROOT}/missing") is False
        get_props_calls = [
            c for c in fake_adls.calls if c[0] == "file.get_properties"
        ]
        assert len(get_props_calls) >= 1

    def test_abfss_exists_for_empty_prefix_uses_list_paths(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "prefix/inside/file.txt")] = b"data"
        assert pu.path_exists(f"{self.ABFSS_ROOT}/prefix/") is True
        list_calls = [c for c in fake_adls.calls if c[0] == "fs.list_paths"]
        assert any(c[1].get("max_results") == 1 for c in list_calls)

    def test_abfss_mkdir_is_noop(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        pu.path_mkdir(f"{self.ABFSS_ROOT}/virtual/dir")
        assert not any(
            c[0] in ("file.upload", "file.delete", "file.rename")
            for c in fake_adls.calls
        )

    def test_abfss_read_bytes(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "k")] = b"\x00\x01\x02\xff"
        assert pu.path_read_bytes(f"{self.ABFSS_ROOT}/k") == b"\x00\x01\x02\xff"
        read_calls = [c for c in fake_adls.calls if c[0] == "file.download"]
        assert len(read_calls) == 1

    def test_abfss_content_length(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        data = b"hello world"
        fake_adls.objects[("mycontainer", "f.bin")] = data
        assert pu.path_content_length(f"{self.ABFSS_ROOT}/f.bin") == len(data)
        get_props_calls = [
            c for c in fake_adls.calls if c[0] == "file.get_properties"
        ]
        assert len(get_props_calls) == 1

    def test_abfss_content_length_missing_raises(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            pu.path_content_length(f"{self.ABFSS_ROOT}/missing")
        assert "STORAGE_ADLS_OP_FAILED" in exc_info.value.error_code
        assert "not found" in exc_info.value.message.lower()

    def test_abfss_is_dir_checks_list_paths(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "p/sub/f.bin")] = b"data"
        assert pu.path_is_dir(f"{self.ABFSS_ROOT}/p") is True
        assert pu.path_is_dir(f"{self.ABFSS_ROOT}/empty") is False

    def test_abfss_replace_download_upload_delete(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "src")] = b"hello"
        pu.path_replace(
            f"{self.ABFSS_ROOT}/src",
            f"{self.ABFSS_ROOT}/dst",
        )
        assert ("mycontainer", "src") not in fake_adls.objects
        assert fake_adls.objects[("mycontainer", "dst")] == b"hello"
        dl_calls = [c for c in fake_adls.calls if c[0] == "file.download"]
        ul_calls = [c for c in fake_adls.calls if c[0] == "file.upload"]
        del_calls = [c for c in fake_adls.calls if c[0] == "file.delete"]
        assert len(dl_calls) >= 1
        assert len(ul_calls) >= 1
        assert len(del_calls) >= 1

    def test_abfss_glob_filtered_by_fnmatch(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "data/a.parquet")] = b"a"
        fake_adls.objects[("mycontainer", "data/b.csv")] = b"b"
        fake_adls.objects[("mycontainer", "data/sub/nested.parquet")] = b"n"
        matches = sorted(pu.path_glob(f"{self.ABFSS_ROOT}/data", "*.parquet"))
        assert len(matches) == 1
        assert matches[0].endswith("/a.parquet")

    def test_abfss_rglob_recursive_includes_subprefixes(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("mycontainer", "root/top.parquet")] = b"t"
        fake_adls.objects[("mycontainer", "root/dt=2025-01-01/p.parquet")] = b"d1"
        fake_adls.objects[("mycontainer", "root/dt=2025-01-02/p.parquet")] = b"d2"
        matches = sorted(pu.path_rglob(f"{self.ABFSS_ROOT}/root", "*.parquet"))
        assert len(matches) == 3

    def test_abfss_delete_tree_batch_deletes(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        prefix = "wh/sql/"
        for i in range(5):
            fake_adls.objects[("mycontainer", f"{prefix}file_{i}.parquet")] = b"x" * i
        fake_adls.objects[("mycontainer", "other/keep.parquet")] = b"keep"
        pu.path_delete_tree(f"{self.ABFSS_ROOT}/wh/sql")
        remaining = sorted(k for (c, k) in fake_adls.objects.keys() if c == "mycontainer")
        assert remaining == ["other/keep.parquet"]
        delete_calls = [c for c in fake_adls.calls if c[0] == "file.delete"]
        assert len(delete_calls) == 5

    def test_abfss_open_for_append_reads_existing_then_rewrites(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        target = f"{self.ABFSS_ROOT}/events.jsonl"
        with pu.path_open_for_append(target) as f:
            f.write('{"a": 1}\n')
        with pu.path_open_for_append(target) as f:
            f.write('{"b": 2}\n')
        assert (
            fake_adls.objects[("mycontainer", "events.jsonl")]
            == b'{"a": 1}\n{"b": 2}\n'
        )

    def test_abfss_relative_to(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        assert (
            pu.path_relative_to(
                f"{self.ABFSS_ROOT}/p1/p2/k",
                f"{self.ABFSS_ROOT}/p1",
            )
            == "p2/k"
        )
        _ = fake_adls

    def test_abfss_split_path_rejects_missing_container(self) -> None:
        with pytest.raises(ConfigValidationError):
            pu._split_adls_path("abfss://")

    def test_abfss_split_path_root_only(self) -> None:
        container, account, key = pu._split_adls_path(
            "abfss://mycontainer@myacct.dfs.core.windows.net"
        )
        assert container == "mycontainer"
        assert account == "myacct"
        assert key == ""


# ---------------------------------------------------------------------------
# TestStagingSwapADLS — 10 staging-swap protocol tests mirroring GCS
# ---------------------------------------------------------------------------


class TestStagingSwapADLS:
    """Staging-swap protocol tests for ADLS backend mirroring S3/GCS patterns."""

    @pytest.fixture()
    def fake_adls(self, monkeypatch: pytest.MonkeyPatch) -> _FakeADLSClient:
        fake = _FakeADLSClient()
        monkeypatch.setattr(pu, "_ADLS_CLIENT", None)
        monkeypatch.setattr(pu, "_adls_client", lambda: fake)
        monkeypatch.setattr(_swap_mod, "_s3_client", lambda: fake)
        return fake

    ABFSS_ROOT = "abfss://cnt@acct.dfs.core.windows.net"

    def test_full_refresh_replaces_target_with_staging(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        staging_prefix = "wh/_staging/sql/tbl/run_id=r1/"
        target_prefix = "wh/sql/tbl/"
        for i in range(3):
            fake_adls.objects[
                ("cnt", f"{staging_prefix}part-{i}.parquet")
            ] = f"new_{i}".encode()
        fake_adls.objects[("cnt", f"{target_prefix}old.parquet")] = b"stale"
        fake_adls.objects[("cnt", "wh/sql/other.parquet")] = b"keep_other"

        _swap_mod.atomic_swap(
            staging_path=f"{self.ABFSS_ROOT}/{staging_prefix.rstrip('/')}",
            target_path=f"{self.ABFSS_ROOT}/{target_prefix.rstrip('/')}",
            scheme=_swap_mod._StorageScheme.abfss,
            mode="full_refresh",
        )

        all_keys = sorted(k for (c, k) in fake_adls.objects.keys() if c == "cnt")
        assert all_keys == sorted(
            [
                "wh/sql/other.parquet",
                "wh/sql/tbl/part-0.parquet",
                "wh/sql/tbl/part-1.parquet",
                "wh/sql/tbl/part-2.parquet",
            ]
        )
        for i in range(3):
            body = fake_adls.objects[("cnt", f"wh/sql/tbl/part-{i}.parquet")]
            assert body == f"new_{i}".encode()

    def test_partition_overwrite_replaces_only_matching_partition_dirs(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        staging = "stg/run"
        target = "wh/tbl"
        sp = f"{staging}/"
        tp = f"{target}/"

        fake_adls.objects[("cnt", f"{sp}dt=2025-01-02/part-0.parquet")] = b"new_jan2"
        fake_adls.objects[("cnt", f"{sp}dt=2025-01-02/part-1.parquet")] = b"new_jan2_b"

        fake_adls.objects[("cnt", f"{tp}dt=2024-12-31/part-0.parquet")] = b"keep_dec"
        fake_adls.objects[("cnt", f"{tp}dt=2025-01-02/part-stale.parquet")] = b"stale_jan2"
        fake_adls.objects[("cnt", f"{tp}dt=2025-01-01/part-0.parquet")] = b"keep_jan1"

        _swap_mod.atomic_swap(
            staging_path=f"{self.ABFSS_ROOT}/{staging}",
            target_path=f"{self.ABFSS_ROOT}/{target}",
            scheme=_swap_mod._StorageScheme.abfss,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (c, k) in fake_adls.objects.keys() if c == "cnt")
        assert all_keys == sorted(
            [
                "wh/tbl/dt=2024-12-31/part-0.parquet",
                "wh/tbl/dt=2025-01-01/part-0.parquet",
                "wh/tbl/dt=2025-01-02/part-0.parquet",
                "wh/tbl/dt=2025-01-02/part-1.parquet",
            ]
        )

    def test_nested_partition_overwrite_preserves_siblings(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        staging = "stg/run"
        target = "wh/tbl"
        sp = f"{staging}/"
        tp = f"{target}/"

        fake_adls.objects[
            ("cnt", f"{sp}src=web/business_date=2026-07-31/p.parquet")
        ] = b"jul_new"
        fake_adls.objects[
            ("cnt", f"{sp}src=web/business_date=2026-08-10/p.parquet")
        ] = b"aug_new"

        fake_adls.objects[
            ("cnt", f"{tp}src=web/business_date=2026-06-01/p.parquet")
        ] = b"jun_keep"
        stale_key = f"{tp}src=web/business_date=2026-07-31/p_stale.parquet"
        fake_adls.objects[("cnt", stale_key)] = b"jul_stale"
        fake_adls.objects[
            ("cnt", f"{tp}src=api/business_date=2026-08-10/p.parquet")
        ] = b"api_aug_keep"

        _swap_mod.atomic_swap(
            staging_path=f"{self.ABFSS_ROOT}/{staging}",
            target_path=f"{self.ABFSS_ROOT}/{target}",
            scheme=_swap_mod._StorageScheme.abfss,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (c, k) in fake_adls.objects.keys() if c == "cnt")
        assert all_keys == sorted(
            [
                "wh/tbl/src=web/business_date=2026-06-01/p.parquet",
                "wh/tbl/src=web/business_date=2026-07-31/p.parquet",
                "wh/tbl/src=web/business_date=2026-08-10/p.parquet",
                "wh/tbl/src=api/business_date=2026-08-10/p.parquet",
            ]
        )

    def test_staging_empty_raises(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            _swap_mod.atomic_swap(
                staging_path=f"{self.ABFSS_ROOT}/empty_staging",
                target_path=f"{self.ABFSS_ROOT}/wh/tbl",
                scheme=_swap_mod._StorageScheme.abfss,
                mode="full_refresh",
            )
        assert "SQL_ATOMIC_SWAP_FAILED" in exc_info.value.error_code

    def test_cross_container_swap_rejected(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        fake_adls.objects[("c1", "stg/p")] = b"x"
        with pytest.raises(PipelineError) as exc_info:
            _swap_mod.atomic_swap(
                staging_path="abfss://c1@a.dfs.core.windows.net/stg",
                target_path="abfss://c2@a.dfs.core.windows.net/wh",
                scheme=_swap_mod._StorageScheme.abfss,
                mode="full_refresh",
            )
        assert "SQL_ATOMIC_SWAP_FAILED" in exc_info.value.error_code
        assert "share the same container" in exc_info.value.message

    def test_cross_account_swap_rejected(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            _swap_mod.atomic_swap(
                staging_path="abfss://c@acct1.dfs.core.windows.net/stg",
                target_path="abfss://c@acct2.dfs.core.windows.net/wh",
                scheme=_swap_mod._StorageScheme.abfss,
                mode="full_refresh",
            )
        assert "SQL_ATOMIC_SWAP_FAILED" in exc_info.value.error_code
        assert "account" in exc_info.value.message.lower()

    def test_validate_swap_accepts_abfss_scheme(self) -> None:
        scheme = _swap_mod.validate_swap_scheme(
            "abfss://container@account.dfs.core.windows.net/wh/sql/orders",
            "orders",
        )
        assert scheme is _swap_mod._StorageScheme.abfss

    def test_best_effort_delete_staging_missing_is_safe(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        _swap_mod.best_effort_delete_staging(
            f"{self.ABFSS_ROOT}/missing_stg",
            _swap_mod._StorageScheme.abfss,
        )

    def test_partition_overwrite_no_prefixes_falls_back_full(
        self, fake_adls: _FakeADLSClient
    ) -> None:
        staging = "stg/np"
        target = "wh/np"
        sp = f"{staging}/"
        tp = f"{target}/"
        fake_adls.objects[("cnt", f"{sp}part-0.parquet")] = b"np_new_0"
        fake_adls.objects[("cnt", f"{sp}part-1.parquet")] = b"np_new_1"
        fake_adls.objects[("cnt", f"{tp}old.parquet")] = b"np_stale"

        _swap_mod.atomic_swap(
            staging_path=f"{self.ABFSS_ROOT}/{staging}",
            target_path=f"{self.ABFSS_ROOT}/{target}",
            scheme=_swap_mod._StorageScheme.abfss,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (c, k) in fake_adls.objects.keys() if c == "cnt")
        assert all_keys == sorted(
            [
                "wh/np/part-0.parquet",
                "wh/np/part-1.parquet",
            ]
        )
