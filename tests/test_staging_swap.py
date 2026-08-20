from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import pytest

import elt_pipeline.shared.path_utils as pu
from elt_pipeline.shared.errors import PipelineError
from elt_pipeline.shared.runtime import StageName
from elt_pipeline.sql._staging_swap import (
    _StorageScheme,  # noqa: PLC2701 - friend import
    atomic_swap,
    best_effort_delete_staging,
    build_staging_path,
    validate_swap_scheme,
)

# ---------------------------------------------------------------------------
# Shared Fake S3 client — matches _FakeS3Client shape from test_path_utils,
# but kept local to avoid cross-test-module imports.
# ---------------------------------------------------------------------------


class _FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("head_object", kwargs))
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects:
            return {"Key": key[1]}
        exc = self.exceptions.ClientError("404")
        exc.response = {"Error": {"Code": "404"}}
        raise exc

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_object", kwargs))
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            exc = self.exceptions.ClientError("NoSuchKey")
            exc.response = {"Error": {"Code": "NoSuchKey"}}
            raise exc
        return {
            "Body": SimpleNamespace(read=lambda: self.objects[key])  # type: ignore[attr-defined]
        }

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("put_object", kwargs))
        body = kwargs.get("Body", b"")
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = body
        return {}

    def copy_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("copy_object", kwargs))
        src = kwargs["CopySource"]
        src_key = (src["Bucket"], src["Key"])
        data = self.objects.get(src_key, b"")
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = data
        return {}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("delete_object", kwargs))
        self.objects.pop((kwargs["Bucket"], kwargs["Key"]), None)
        return {}

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("delete_objects", kwargs))
        bucket = kwargs["Bucket"]
        for entry in kwargs.get("Delete", {}).get("Objects", []):
            key = entry["Key"]
            self.objects.pop((bucket, key), None)
        return {}

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list_objects_v2", kwargs))
        bucket = kwargs["Bucket"]
        prefix = kwargs.get("Prefix", "")
        max_keys = kwargs.get("MaxKeys")
        matched_keys = sorted(
            k for (b, k) in self.objects.keys() if b == bucket and k.startswith(prefix)
        )
        delimiter = kwargs.get("Delimiter")
        common_prefixes: list[dict[str, str]] = []
        content_keys: list[str] = []
        for k in matched_keys:
            rest = k[len(prefix):]
            if delimiter and delimiter in rest:
                first_seg = rest.split(delimiter, 1)[0] + delimiter
                cp = prefix + first_seg
                if not any(c["Prefix"] == cp for c in common_prefixes):
                    common_prefixes.append({"Prefix": cp})
            else:
                content_keys.append(k)
        contents = [
            {"Key": k, "Size": len(self.objects[(bucket, k)])} for k in content_keys
        ]
        if max_keys is not None:
            contents = contents[:max_keys]
        result: dict[str, Any] = {}
        if contents:
            result["Contents"] = contents
        if common_prefixes:
            result["CommonPrefixes"] = common_prefixes
        return result

    class exceptions:  # noqa: N801
        class ClientError(Exception):  # type: ignore[override]
            pass

    def get_paginator(self, name: str) -> Any:
        assert name == "list_objects_v2"

        class _Paginator:
            def __init__(self_2, fake: "_FakeS3Client") -> None:
                self_2._fake = fake

            def paginate(self_2, **kwargs: Any) -> Any:  # type: ignore[override]
                yield self_2._fake.list_objects_v2(**kwargs)

        return _Paginator(self)


@pytest.fixture()
def fake_s3(monkeypatch: pytest.MonkeyPatch) -> _FakeS3Client:
    fake = _FakeS3Client()
    import elt_pipeline.sql._staging_swap as _swap_mod

    monkeypatch.setattr(pu, "_S3_CLIENT", None)
    monkeypatch.setattr(pu, "_s3_client", lambda: fake)
    monkeypatch.setattr(_swap_mod, "_s3_client", lambda: fake)
    return fake


# ---------------------------------------------------------------------------
# validate_swap_scheme — scheme guard (PRD 08 operator hint on failure)
# ---------------------------------------------------------------------------


class TestValidateSwapScheme:
    def test_accepts_local_unschemed_posix(self) -> None:
        s = validate_swap_scheme("/data/warehouse/sql/orders", "orders")
        assert s is _StorageScheme.local_unschemed

    def test_accepts_file_scheme(self) -> None:
        s = validate_swap_scheme("file:///data/wh/sql/orders", "orders")
        assert s is _StorageScheme.file

    def test_accepts_s3_scheme(self) -> None:
        s = validate_swap_scheme("s3://my-bucket/wh/sql/orders", "orders")
        assert s is _StorageScheme.s3

    def test_accepts_gs_scheme(self) -> None:
        s = validate_swap_scheme("gs://my-bucket/wh/sql/orders", "orders")
        assert s is _StorageScheme.gs

    @pytest.mark.parametrize(
        "bad",
        [
            "s3a://bucket/p",
            "s3n://bucket/p",
            "abfs://container/p",
            "wasbs://container/p",
            "https://example.com/p",
        ],
    )
    def test_detect_scheme_blocks_unknown_schemes_early(self, bad: str) -> None:
        from elt_pipeline.shared.errors import ConfigValidationError

        # detect_scheme() raises ConfigValidationError before validate_swap_scheme
        # can emit the SQL-layer error for schemes that are structurally unknown.
        with pytest.raises(ConfigValidationError):
            validate_swap_scheme(bad, "mymodel")

    def test_rejects_known_but_unsupported_scheme_enum_via_sql_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import enum

        # Simulate a future scheme enum value that detect_scheme returns
        # but validate_swap_scheme still rejects.
        existing = [(m.name, m.value) for m in _StorageScheme]
        future_values = existing + [("azure_blob", "azure_blob")]
        FakeScheme = enum.Enum("FakeScheme", future_values)  # type: ignore[misc]

        def fake_detect(_path: str) -> Any:
            return FakeScheme.azure_blob

        import elt_pipeline.sql._staging_swap as _swap_mod

        monkeypatch.setattr(_swap_mod, "detect_scheme", fake_detect)
        with pytest.raises(PipelineError) as exc_info:
            validate_swap_scheme(
                "azure_blob://container/path", "mymodel"
            )
        err = exc_info.value
        assert err.error_code == "SQL_STAGING_SCHEME_UNSUPPORTED"
        assert "mymodel" in err.message
        context = err.context or {}
        assert context["model_id"] == "mymodel"
        assert context["target_path"] == "azure_blob://container/path"
        assert "PRD 08" in context["operator_action"]
        assert "load_mode='append'" in context["operator_action"]


# ---------------------------------------------------------------------------
# build_staging_path — layout contract
# ---------------------------------------------------------------------------


class TestBuildStagingPath:
    def test_layout_contract(self) -> None:
        path = build_staging_path(
            staging_root="/warehouse/_staging",
            stage=StageName.sql,
            target_table_name="orders_summary",
            run_id="r_20260115_abc123",
        )
        assert path == (
            "/warehouse/_staging/sql/orders_summary/"
            "run_id=r_20260115_abc123"
        )

    def test_s3_root_produces_s3_staging_uri(self) -> None:
        path = build_staging_path(
            staging_root="s3://bucket/wh/_staging",
            stage=StageName.sql,
            target_table_name="line_items",
            run_id="run001",
        )
        assert path == "s3://bucket/wh/_staging/sql/line_items/run_id=run001"

    def test_normalize_stage_is_supported(self) -> None:
        path = build_staging_path(
            staging_root="/wh/_staging",
            stage=StageName.normalize,
            target_table_name="t",
            run_id="rx",
        )
        assert path == "/wh/_staging/normalize/t/run_id=rx"


# ---------------------------------------------------------------------------
# best_effort_delete_staging — never raises even on garbage input
# ---------------------------------------------------------------------------


class TestBestEffortDeleteStaging:
    def test_tolerates_missing_posix_path(self, tmp_path) -> None:
        missing = os.path.join(str(tmp_path), "nope", "nope")
        best_effort_delete_staging(missing, _StorageScheme.local_unschemed)

    def test_tolerates_invalid_s3_prefix(
        self, fake_s3: _FakeS3Client
    ) -> None:
        best_effort_delete_staging(
            "s3://bkt/does/not/exist", _StorageScheme.s3
        )

    def test_tolerates_bare_str_paths_with_posix_scheme(self) -> None:
        # Even with complete garbage, best_effort must not raise.
        best_effort_delete_staging(
            "/not/even/close/to/reality", _StorageScheme.file
        )


# ---------------------------------------------------------------------------
# atomic_swap — POSIX
# ---------------------------------------------------------------------------


def _write(path: str, content: str = "x") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


class TestAtomicSwapPosix:
    def test_full_refresh_removes_old_target_and_moves_staging(
        self, tmp_path
    ) -> None:
        root = str(tmp_path)
        staging = os.path.join(root, "staging")
        target = os.path.join(root, "target")
        _write(os.path.join(staging, "part-0000.parquet"), "new_data")
        # pre-existing target content that must be removed
        _write(os.path.join(target, "old.parquet"), "stale")
        assert os.path.isfile(os.path.join(target, "old.parquet"))

        atomic_swap(
            staging_path=staging,
            target_path=target,
            scheme=_StorageScheme.local_unschemed,
            mode="full_refresh",
        )

        assert os.path.isdir(target)
        assert not os.path.isdir(staging)
        files = sorted(os.listdir(target))
        assert files == ["part-0000.parquet"]
        with open(os.path.join(target, "part-0000.parquet")) as f:
            assert f.read() == "new_data"

    def test_partition_overwrite_merges_unrelated_partitions(
        self, tmp_path
    ) -> None:
        root = str(tmp_path)
        staging = os.path.join(root, "staging")
        target = os.path.join(root, "target")
        # Staging contains overwrite for dt=2025-01-02
        _write(
            os.path.join(staging, "dt=2025-01-02", "part-0.parquet"),
            "jan2_new",
        )
        # Pre-existing target: unrelated dt=2024-12-31 + stale dt=2025-01-02
        _write(
            os.path.join(target, "dt=2024-12-31", "part-0.parquet"),
            "dec31_keep",
        )
        _write(
            os.path.join(target, "dt=2025-01-02", "part-0.parquet"),
            "jan2_stale",
        )

        atomic_swap(
            staging_path=staging,
            target_path=target,
            scheme=_StorageScheme.local_unschemed,
            mode="partition_overwrite",
        )

        kept_dec = os.path.join(target, "dt=2024-12-31", "part-0.parquet")
        new_jan = os.path.join(target, "dt=2025-01-02", "part-0.parquet")
        assert os.path.isfile(kept_dec)
        with open(kept_dec) as f:
            assert f.read() == "dec31_keep"
        assert os.path.isfile(new_jan)
        with open(new_jan) as f:
            assert f.read() == "jan2_new"
        # staging cleaned up when empty after partition moves
        assert not os.path.isdir(staging)

    def test_partition_overwrite_multi_level_preserves_sibling_leaves(
        self, tmp_path
    ) -> None:
        # L3 default layout is two-level: source_name=<src>/business_date=<date>.
        # Dynamic partition overwrite must replace only the leaf (source, date)
        # tuples produced by this run, leaving unrelated dates/sources intact.
        root = str(tmp_path)
        staging = os.path.join(root, "staging")
        target = os.path.join(root, "target")
        src = "source_name=orders_source"
        # This run writes two business_date leaves under orders_source.
        _write(os.path.join(staging, src, "business_date=2026-07-31", "p.parquet"), "jul_new")
        _write(os.path.join(staging, src, "business_date=2026-08-10", "p.parquet"), "aug_new")
        # Pre-existing target: an unrelated date leaf (keep), a stale copy of a
        # written leaf (replace), and a whole other source (keep).
        _write(os.path.join(target, src, "business_date=2026-06-01", "p.parquet"), "jun_keep")
        _write(os.path.join(target, src, "business_date=2026-07-31", "p.parquet"), "jul_stale")
        _write(
            os.path.join(target, "source_name=other", "business_date=2026-06-01", "p.parquet"),
            "other_keep",
        )

        atomic_swap(
            staging_path=staging,
            target_path=target,
            scheme=_StorageScheme.local_unschemed,
            mode="partition_overwrite",
        )

        def _read(*parts: str) -> str:
            with open(os.path.join(target, *parts)) as f:
                return f.read()

        # Unrelated date under the same source survives untouched.
        assert _read(src, "business_date=2026-06-01", "p.parquet") == "jun_keep"
        # A wholly different source survives untouched.
        assert _read("source_name=other", "business_date=2026-06-01", "p.parquet") == "other_keep"
        # Written leaves are replaced with fresh data (stale jul copy gone).
        assert _read(src, "business_date=2026-07-31", "p.parquet") == "jul_new"
        assert _read(src, "business_date=2026-08-10", "p.parquet") == "aug_new"

    def test_file_scheme_posix_also_works(self, tmp_path) -> None:
        root = str(tmp_path)
        staging = os.path.join(root, "staging")
        target = os.path.join(root, "target")
        _write(os.path.join(staging, "p.parquet"), "ok")

        atomic_swap(
            staging_path="file://" + staging,
            target_path="file://" + target,
            scheme=_StorageScheme.file,
            mode="full_refresh",
        )
        assert os.path.isdir(target)
        assert sorted(os.listdir(target)) == ["p.parquet"]

    def test_missing_staging_raises_with_context(self, tmp_path) -> None:
        root = str(tmp_path)
        staging = os.path.join(root, "no_such_staging")
        target = os.path.join(root, "target")
        with pytest.raises(PipelineError) as exc_info:
            atomic_swap(
                staging_path=staging,
                target_path=target,
                scheme=_StorageScheme.local_unschemed,
                mode="full_refresh",
            )
        err = exc_info.value
        assert err.error_code == "SQL_ATOMIC_SWAP_FAILED"
        assert err.context["staging_path"] == staging
        assert err.context["target_path"] == target


# ---------------------------------------------------------------------------
# atomic_swap — S3 mocked
# ---------------------------------------------------------------------------


class TestAtomicSwapS3:
    @pytest.fixture()
    def seed_full_refresh(
        self, fake_s3: _FakeS3Client
    ) -> tuple[str, str]:
        staging_prefix = "wh/_staging/sql/tbl/run_id=r1/"
        target_prefix = "wh/sql/tbl/"
        for i in range(3):
            fake_s3.objects[("b", f"{staging_prefix}part-{i}.parquet")] = (
                f"new_{i}".encode()
            )
        # stale target key (must be removed) + unrelated key outside prefix
        fake_s3.objects[("b", f"{target_prefix}old.parquet")] = b"stale"
        fake_s3.objects[("b", "wh/sql/other.parquet")] = b"keep_other"
        return ("s3://b/" + staging_prefix.rstrip("/"), "s3://b/" + target_prefix.rstrip("/"))

    def test_full_refresh_copies_staging_deletes_stale_and_purges_staging(
        self,
        seed_full_refresh: tuple[str, str],
        fake_s3: _FakeS3Client,
    ) -> None:
        staging_path, target_path = seed_full_refresh
        atomic_swap(
            staging_path=staging_path,
            target_path=target_path,
            scheme=_StorageScheme.s3,
            mode="full_refresh",
        )
        all_keys = sorted(
            k for (b, k) in fake_s3.objects.keys() if b == "b"
        )
        # 3 new parts landed at target, unrelated key preserved
        assert all_keys == [
            "wh/sql/other.parquet",
            "wh/sql/tbl/part-0.parquet",
            "wh/sql/tbl/part-1.parquet",
            "wh/sql/tbl/part-2.parquet",
        ]
        # Content check for one migrated key
        body = fake_s3.objects[("b", "wh/sql/tbl/part-0.parquet")]
        assert body == b"new_0"
        # Confirm call shapes: list x3 (old_target + staging + confirm),
        # copy per key, batch-deletes (stale + staging)
        op_names = [c[0] for c in fake_s3.calls]
        assert op_names.count("copy_object") == 3
        assert op_names.count("delete_objects") >= 2

    def test_s3_different_buckets_rejected(
        self, fake_s3: _FakeS3Client
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            atomic_swap(
                staging_path="s3://bucket-a/wh/stage/x",
                target_path="s3://bucket-b/wh/sql/x",
                scheme=_StorageScheme.s3,
                mode="full_refresh",
            )
        err = exc_info.value
        assert err.error_code == "SQL_ATOMIC_SWAP_FAILED"
        assert "same bucket" in err.message

    def test_empty_staging_prefix_raises(
        self, fake_s3: _FakeS3Client
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            atomic_swap(
                staging_path="s3://b/wh/empty/",
                target_path="s3://b/wh/sql/empty/",
                scheme=_StorageScheme.s3,
                mode="full_refresh",
            )
        assert exc_info.value.error_code == "SQL_ATOMIC_SWAP_FAILED"
        assert "empty" in exc_info.value.message

    def test_partition_overwrite_replaces_only_selected_partitions(
        self, fake_s3: _FakeS3Client
    ) -> None:
        staging = "wh/_staging/sql/tbl/run_id=r2"
        target = "wh/sql/tbl"
        fake_s3.objects[("b", f"{staging}/dt=2025-01-02/part-0.parquet")] = b"new_jan2"
        fake_s3.objects[("b", f"{staging}/dt=2025-01-02/part-1.parquet")] = b"new_jan2_b"
        # Pre-existing target: dt=2024-12-31 (keep), dt=2025-01-02 (overwrite)
        fake_s3.objects[("b", f"{target}/dt=2024-12-31/part-0.parquet")] = b"keep_dec"
        fake_s3.objects[("b", f"{target}/dt=2025-01-02/part-stale.parquet")] = b"stale_jan2"
        fake_s3.objects[("b", f"{target}/dt=2025-01-01/part-0.parquet")] = b"keep_jan1"

        atomic_swap(
            staging_path="s3://b/" + staging,
            target_path="s3://b/" + target,
            scheme=_StorageScheme.s3,
            mode="partition_overwrite",
        )

        all_keys = sorted(
            k for (b, k) in fake_s3.objects.keys() if b == "b"
        )
        assert f"{target}/dt=2024-12-31/part-0.parquet" in all_keys
        assert f"{target}/dt=2025-01-01/part-0.parquet" in all_keys
        assert f"{target}/dt=2025-01-02/part-0.parquet" in all_keys
        assert f"{target}/dt=2025-01-02/part-1.parquet" in all_keys
        # Stale jan2 part must be gone
        assert f"{target}/dt=2025-01-02/part-stale.parquet" not in all_keys
        # Staging run_id keys must be purged
        assert not any(k.startswith(staging) for k in all_keys)


    def test_partition_overwrite_multi_level_preserves_sibling_leaves(
        self, fake_s3: _FakeS3Client
    ) -> None:
        # S3 analogue of the two-level L3 layout: only the exact leaf
        # source_name=/business_date= prefixes written this run are replaced.
        staging = "wh/_staging/sql/canonical/run_id=r2"
        target = "wh/sql/canonical"
        src = "source_name=orders_source"
        fake_s3.objects[("b", f"{staging}/{src}/business_date=2026-07-31/p.parquet")] = b"jul_new"
        fake_s3.objects[("b", f"{staging}/{src}/business_date=2026-08-10/p.parquet")] = b"aug_new"
        # Pre-existing: unrelated date (keep), stale copy of written leaf (replace),
        # a whole other source (keep).
        fake_s3.objects[("b", f"{target}/{src}/business_date=2026-06-01/p.parquet")] = b"jun_keep"
        fake_s3.objects[
            ("b", f"{target}/{src}/business_date=2026-07-31/stale.parquet")
        ] = b"jul_stale"
        fake_s3.objects[
            ("b", f"{target}/source_name=other/business_date=2026-06-01/p.parquet")
        ] = b"other_keep"

        atomic_swap(
            staging_path="s3://b/" + staging,
            target_path="s3://b/" + target,
            scheme=_StorageScheme.s3,
            mode="partition_overwrite",
        )

        all_keys = sorted(k for (b, k) in fake_s3.objects.keys() if b == "b")
        # Unrelated date and unrelated source survive.
        assert f"{target}/{src}/business_date=2026-06-01/p.parquet" in all_keys
        assert f"{target}/source_name=other/business_date=2026-06-01/p.parquet" in all_keys
        # Written leaves present; stale jul copy purged.
        assert f"{target}/{src}/business_date=2026-07-31/p.parquet" in all_keys
        assert f"{target}/{src}/business_date=2026-08-10/p.parquet" in all_keys
        assert f"{target}/{src}/business_date=2026-07-31/stale.parquet" not in all_keys
        # Staging purged.
        assert not any(k.startswith(staging) for k in all_keys)


# ---------------------------------------------------------------------------
# best_effort_delete_staging — swap failure path leaves staging for forensics
# ---------------------------------------------------------------------------


class TestBestEffortSwapFailureCleanup:
    def test_failing_posix_swap_still_triggers_cleanup_path(
        self, tmp_path
    ) -> None:
        root = str(tmp_path)
        staging = os.path.join(root, "staging_run")
        target = os.path.join(root, "target_run")
        # No staging dir — swap will raise. best_effort_delete_staging
        # after that must not raise even on nonexistent paths.
        with pytest.raises(PipelineError):
            atomic_swap(
                staging_path=staging,
                target_path=target,
                scheme=_StorageScheme.local_unschemed,
                mode="full_refresh",
            )
        best_effort_delete_staging(staging, _StorageScheme.local_unschemed)
