from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import pytest

from elt_pipeline.shared import path_utils as pu
from elt_pipeline.shared.errors import ConfigValidationError, PipelineError

# ---------------------------------------------------------------------------
# Scheme detection + root-is-string validation (Gate 0.c.i, 0.c.ii)
# ---------------------------------------------------------------------------

class TestDetectScheme:
    def test_detect_s3(self) -> None:
        assert pu.detect_scheme("s3://bucket/prefix") == pu._StorageScheme.s3
        assert pu.detect_scheme("s3://b") == pu._StorageScheme.s3

    def test_detect_file(self) -> None:
        assert pu.detect_scheme("file:///abs/path") == pu._StorageScheme.file
        assert pu.detect_scheme("file://rel/path") == pu._StorageScheme.file

    def test_detect_local_unschemed(self) -> None:
        assert pu.detect_scheme("/abs/path") == pu._StorageScheme.local_unschemed
        assert pu.detect_scheme("rel/path") == pu._StorageScheme.local_unschemed
        assert pu.detect_scheme("file.parquet") == pu._StorageScheme.local_unschemed

    def test_reject_unknown_schemes_sharp(self) -> None:
        for bad in (
            "s3a://bucket/prefix",
            "s3n://bucket/prefix",
            "gs://bucket/prefix",
            "abfs://container/path",
            "https://example.com/foo",
        ):
            with pytest.raises(ConfigValidationError) as exc_info:
                pu.detect_scheme(bad)
            msg = exc_info.value.message
            assert "Unsupported storage scheme" in msg
            assert bad.split("://", 1)[0] + "://" in msg
            assert "s3:// (AWS S3)" in msg
            assert "Never silently coerce schemes." in exc_info.value.context["note"]

    def test_validate_root_rejects_pathlib(self) -> None:
        from pathlib import Path

        with pytest.raises(ConfigValidationError) as exc_info:
            pu._validate_root_is_string(Path("/tmp"))
        assert "Storage root / path must be a string URI" in exc_info.value.message
        assert "Do not wrap root paths in pathlib.Path" in exc_info.value.context["hint"]

    def test_validate_root_rejects_non_string(self) -> None:
        with pytest.raises(ConfigValidationError):
            pu._validate_root_is_string(123)
        with pytest.raises(ConfigValidationError):
            pu._validate_root_is_string(None)


# ---------------------------------------------------------------------------
# strip_file_scheme
# ---------------------------------------------------------------------------

class TestStripFileScheme:
    def test_strips_file_triple_slash(self) -> None:
        assert pu.strip_file_scheme("file:///abs/path") == "/abs/path"

    def test_preserves_others(self) -> None:
        assert pu.strip_file_scheme("s3://b/k") == "s3://b/k"
        assert pu.strip_file_scheme("/local") == "/local"


# ---------------------------------------------------------------------------
# join_paths — the core contract (Gate 0.c.iii matrix)
# ---------------------------------------------------------------------------

class TestJoinPaths:
    # --- s3:// ---
    def test_s3_basic(self) -> None:
        assert (
            pu.join_paths("s3://bucket/prefix", "level2", "entity=x")
            == "s3://bucket/prefix/level2/entity=x"
        )

    def test_s3_trailing_slash_on_root(self) -> None:
        assert pu.join_paths("s3://b/p/", "seg") == "s3://b/p/seg"

    def test_s3_leading_slash_on_segment(self) -> None:
        assert pu.join_paths("s3://b/p", "/seg/") == "s3://b/p/seg"

    def test_s3_double_slashes_collapse(self) -> None:
        assert pu.join_paths("s3://b//p1//p2", "s1//s2") == "s3://b/p1/p2/s1/s2"

    def test_s3_no_segments_returns_root_collapsed(self) -> None:
        assert pu.join_paths("s3://b//p//") == "s3://b/p/"

    def test_s3_empty_strings_skipped(self) -> None:
        assert pu.join_paths("s3://b/p", "", "  ", "s1", "", "s2") == "s3://b/p/s1/s2"

    def test_s3_root_only_bucket(self) -> None:
        assert pu.join_paths("s3://bucket", "key") == "s3://bucket/key"

    # --- file:// ---
    def test_file_basic(self) -> None:
        assert (
            pu.join_paths("file:///data/root", "l1", "src=foo")
            == "file:///data/root/l1/src=foo"
        )

    def test_file_slashes_collapse(self) -> None:
        assert pu.join_paths("file:///a//b/", "/c/") == "file:///a/b/c"

    # --- bare local POSIX ---
    def test_local_abs_basic(self) -> None:
        assert pu.join_paths("/data/root", "l1", "e=1") == "/data/root/l1/e=1"

    def test_local_rel_basic(self) -> None:
        assert pu.join_paths("rel/root", "a", "b") == "rel/root/a/b"

    def test_local_double_slashes(self) -> None:
        assert pu.join_paths("/a//b", "c//d") == "/a/b/c/d"

    # --- error cases ---
    def test_non_string_segment_raises(self) -> None:
        from pathlib import Path

        with pytest.raises(ConfigValidationError):
            pu.join_paths("/root", Path("bad"))  # type: ignore[arg-type]
        with pytest.raises(ConfigValidationError):
            pu.join_paths("/root", 42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# String helpers: parent, basename, suffix, relative_to, normalize
# ---------------------------------------------------------------------------

class TestPathStringHelpers:
    def test_parent_local(self) -> None:
        assert pu.path_parent("/a/b/c") == "/a/b"

    def test_parent_s3(self) -> None:
        assert pu.path_parent("s3://b/p1/p2/k") == "s3://b/p1/p2"

    def test_basename_all_schemes(self) -> None:
        assert pu.path_basename("/a/b/file.parquet") == "file.parquet"
        assert pu.path_basename("file:///a/b/f.csv") == "f.csv"
        assert pu.path_basename("s3://b/p/k.json") == "k.json"

    def test_with_suffix(self) -> None:
        assert pu.path_with_suffix("s3://b/k", ".tmp") == "s3://b/k.tmp"
        assert pu.path_with_suffix("/a/b/foo.csv", "parquet") == "/a/b/foo.parquet"

    def test_relative_to_same_scheme(self) -> None:
        assert pu.path_relative_to("/a/b/c/d", "/a/b") == "c/d"
        assert pu.path_relative_to("s3://b/p1/p2", "s3://b/p1") == "p2"

    def test_relative_to_equal_base(self) -> None:
        assert pu.path_relative_to("/a/b", "/a/b") == "."

    def test_relative_to_cross_scheme_rejected(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            pu.path_relative_to("s3://b/p", "/local/p")
        assert "Cannot compute path_relative_to across different schemes" in exc_info.value.message

    def test_relative_to_bad_prefix(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            pu.path_relative_to("/a/b/c", "/x/y")
        assert "does not start with the required base prefix" in exc_info.value.message

    def test_normalize_local_resolves(self, tmp_path) -> None:
        # Use a real tmp path so os.path.realpath can verify canonicalization
        base = str(tmp_path)
        target = pu.join_paths(base, "a", "b")
        os.makedirs(target, exist_ok=True)
        # Resolve a ".." relative variant
        alt = pu.join_paths(base, "a", "b", "..", "b")
        assert pu.path_normalize(alt) == pu.path_normalize(target)

    def test_normalize_s3_just_collapses(self) -> None:
        assert pu.path_normalize("s3://b//p1///p2/") == "s3://b/p1/p2/"


# ---------------------------------------------------------------------------
# Local POSIX round-trip (Gate 0.c.iv)
# ---------------------------------------------------------------------------

class TestLocalPosixRoundTrip:
    def test_write_read_text_roundtrip(self, tmp_path) -> None:
        root = str(tmp_path)
        target = pu.join_paths(root, "subdir", "hello.txt")
        pu.path_write_text(target, "hello world")
        assert pu.path_read_text(target) == "hello world"
        assert pu.path_exists(target)
        assert not pu.path_is_dir(target)

    def test_write_read_bytes_roundtrip(self, tmp_path) -> None:
        root = str(tmp_path)
        target = pu.join_paths(root, "data.bin")
        pu.path_write_bytes(target, b"\x00\x01\x02\xff")
        assert pu.path_read_bytes(target) == b"\x00\x01\x02\xff"

    def test_mkdir_exists_isdir(self, tmp_path) -> None:
        root = str(tmp_path)
        d = pu.join_paths(root, "a", "b", "c")
        assert not pu.path_exists(d)
        pu.path_mkdir(d, parents=True, exist_ok=True)
        assert pu.path_exists(d)
        assert pu.path_is_dir(d)

    def test_atomic_write_creates_tmp_then_replaces(self, tmp_path) -> None:
        root = str(tmp_path)
        target = pu.join_paths(root, "atomic.txt")
        pu.path_write_text(target, "v1", atomic=True)
        assert pu.path_read_text(target) == "v1"
        # No leftover .tmp
        listing = pu.path_listdir(root)
        leftover = [n for n in listing if n.endswith(".tmp")]
        assert leftover == []

    def test_listdir(self, tmp_path) -> None:
        root = str(tmp_path)
        for name in ("a.txt", "b.csv"):
            pu.path_write_text(pu.join_paths(root, name), "x")
        pu.path_mkdir(pu.join_paths(root, "sub"))
        names = sorted(os.path.basename(p) for p in pu.path_listdir(root))
        assert names == sorted(["a.txt", "b.csv", "sub"])

    def test_glob_and_rglob(self, tmp_path) -> None:
        root = str(tmp_path)
        pu.path_write_text(pu.join_paths(root, "top.parquet"), "t")
        pu.path_mkdir(pu.join_paths(root, "a"))
        pu.path_write_text(pu.join_paths(root, "a", "nested.parquet"), "n")
        # glob is single-level
        g = sorted(os.path.basename(p) for p in pu.path_glob(root, "*.parquet"))
        assert g == ["top.parquet"]
        # rglob is recursive
        rg = sorted(os.path.basename(p) for p in pu.path_rglob(root, "*.parquet"))
        assert rg == sorted(["top.parquet", "nested.parquet"])

    def test_replace_atomic_rename(self, tmp_path) -> None:
        root = str(tmp_path)
        src = pu.join_paths(root, "src.csv")
        dst = pu.join_paths(root, "dst.csv")
        pu.path_write_text(src, "payload")
        pu.path_replace(src, dst)
        assert not pu.path_exists(src)
        assert pu.path_read_text(dst) == "payload"

    def test_open_for_append(self, tmp_path) -> None:
        root = str(tmp_path)
        target = pu.join_paths(root, "events.jsonl")
        with pu.path_open_for_append(target) as f:
            f.write('{"a": 1}\n')
        with pu.path_open_for_append(target) as f:
            f.write('{"b": 2}\n')
        assert pu.path_read_text(target) == '{"a": 1}\n{"b": 2}\n'


# ---------------------------------------------------------------------------
# Mocked S3 branch — verify routing, call shape, and atomic path (Gate 0.c.v)
# ---------------------------------------------------------------------------

class _FakeS3Client:
    """Record-keeping fake S3 client; mimics boto3 client API surface we use."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        # Object store: (bucket, key) -> bytes
        self.objects: dict[tuple[str, str], bytes] = {}

    # --- direct API ---
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
        return {"Body": SimpleNamespace(read=lambda: self.objects[key])}  # type: ignore[attr-defined]

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

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list_objects_v2", kwargs))
        bucket = kwargs["Bucket"]
        prefix = kwargs.get("Prefix", "")
        max_keys = kwargs.get("MaxKeys")
        delimiter = kwargs.get("Delimiter")
        matched_keys = sorted(
            k for (b, k) in self.objects.keys() if b == bucket and k.startswith(prefix)
        )
        common_prefixes: list[dict[str, str]] = []
        content_keys: list[str] = []
        for k in matched_keys:
            rest = k[len(prefix):]
            if delimiter and delimiter in rest:
                # Belongs in a common prefix, not Contents
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

    class exceptions:  # noqa: N801 - mirror boto3 shape
        class ClientError(Exception):  # type: ignore[override]
            pass

    def get_paginator(self, name: str) -> Any:  # noqa: D401 - fake
        assert name == "list_objects_v2"

        class _Paginator:
            def __init__(self_2, fake: "_FakeS3Client") -> None:
                self_2._fake = fake

            def paginate(self_2, **kwargs: Any) -> Any:  # type: ignore[override]
                # Single page: call list_objects_v2 and wrap in a 1-element list
                yield self_2._fake.list_objects_v2(**kwargs)

        return _Paginator(self)


class TestMockedS3Routing:
    """Scheme-dispatch tests.

    The fake client replaces _s3_client() via fixture monkeypatch, so any
    scheme=s3 call flows through the fake. Correct routing is verified by
    asserting fake S3 call shapes and object store contents — if a call
    ever fell through to the POSIX branch, the fake client would have
    zero recorded calls and the assertions would fail.
    """

    @pytest.fixture()
    def fake_s3(self, monkeypatch: pytest.MonkeyPatch) -> _FakeS3Client:
        fake = _FakeS3Client()
        # Reset the singleton cache so _s3_client() returns our fake
        monkeypatch.setattr(pu, "_S3_CLIENT", None)
        monkeypatch.setattr(pu, "_s3_client", lambda: fake)
        return fake

    def test_s3_write_atomic_uses_tmp_then_copy_then_delete(
        self, fake_s3: _FakeS3Client
    ) -> None:
        target = "s3://my-bucket/path/to/file.parquet"
        pu.path_write_bytes(target, b"data!")
        # Verify object landed at real key
        assert fake_s3.objects[("my-bucket", "path/to/file.parquet")] == b"data!"
        # Verify call order: put(.tmp), copy, delete(.tmp)
        put_calls = [c for c in fake_s3.calls if c[0] == "put_object"]
        copy_calls = [c for c in fake_s3.calls if c[0] == "copy_object"]
        del_calls = [c for c in fake_s3.calls if c[0] == "delete_object"]
        assert len(put_calls) == 1
        assert put_calls[0][1]["Key"] == "path/to/file.parquet.tmp"
        assert len(copy_calls) == 1
        cp_src = copy_calls[0][1]["CopySource"]
        assert cp_src["Bucket"] == "my-bucket"
        assert cp_src["Key"] == "path/to/file.parquet.tmp"
        assert copy_calls[0][1]["Key"] == "path/to/file.parquet"
        assert len(del_calls) == 1
        assert del_calls[0][1]["Key"] == "path/to/file.parquet.tmp"
        # tmp key must no longer exist
        assert ("my-bucket", "path/to/file.parquet.tmp") not in fake_s3.objects

    def test_s3_listdir_uses_list_objects_v2_and_returns_s3_uris(
        self, fake_s3: _FakeS3Client
    ) -> None:
        # Seed fake store
        fake_s3.objects[("bkt", "p/a.txt")] = b"a"
        fake_s3.objects[("bkt", "p/b.csv")] = b"b"
        fake_s3.objects[("bkt", "p/sub/nested.parquet")] = b"n"
        listing = pu.path_listdir("s3://bkt/p")
        # listing goes through paginator -> list_objects_v2 with Delimiter="/"
        paginated_calls = [
            c for c in fake_s3.calls if c[0] == "list_objects_v2"
        ]
        assert len(paginated_calls) >= 1
        assert paginated_calls[0][1]["Delimiter"] == "/"
        # Results: s3:// URIs for files + synthetic dir
        basenames = sorted(pu.path_basename(x) for x in listing)
        assert "a.txt" in basenames
        assert "b.csv" in basenames
        assert "sub" in basenames
        # Every entry MUST be a full s3:// URI
        for entry in listing:
            assert entry.startswith("s3://bkt/"), entry

    def test_s3_exists_uses_head_object_for_keys(
        self, fake_s3: _FakeS3Client
    ) -> None:
        fake_s3.objects[("b", "k")] = b"x"
        assert pu.path_exists("s3://b/k") is True
        assert pu.path_exists("s3://b/missing") is False
        # Confirm HEAD was the probe mechanism for object-key path
        head_calls = [c for c in fake_s3.calls if c[0] == "head_object"]
        assert len(head_calls) >= 1

    def test_s3_mkdir_is_noop_but_validates_scheme(
        self, fake_s3: _FakeS3Client
    ) -> None:
        pu.path_mkdir("s3://b/virtual/dir")
        # Zero put/copy — S3 has no real dirs
        assert not any(
            c[0] in ("put_object", "copy_object", "delete_object")
            for c in fake_s3.calls
        )

    def test_s3_replace_copy_then_delete_same_bucket(
        self, fake_s3: _FakeS3Client
    ) -> None:
        fake_s3.objects[("b", "src")] = b"hello"
        pu.path_replace("s3://b/src", "s3://b/dst")
        assert ("b", "src") not in fake_s3.objects
        assert fake_s3.objects[("b", "dst")] == b"hello"
        copy_calls = [c for c in fake_s3.calls if c[0] == "copy_object"]
        del_calls = [c for c in fake_s3.calls if c[0] == "delete_object"]
        assert len(copy_calls) == 1 and len(del_calls) == 1

    def test_s3_append_reads_existing_then_rewrites(
        self, fake_s3: _FakeS3Client
    ) -> None:
        # Write v1 through the append path (creates new)
        with pu.path_open_for_append("s3://b/events.jsonl") as w:
            w.write('{"id": 1}\n')
        # Append v2 through the same path (reads existing + rewrites)
        with pu.path_open_for_append("s3://b/events.jsonl") as w:
            w.write('{"id": 2}\n')
        # Final content must contain both lines
        data = pu.path_read_text("s3://b/events.jsonl")
        assert data == '{"id": 1}\n{"id": 2}\n'

    def test_split_s3_path_rejects_missing_bucket(self) -> None:
        with pytest.raises(ConfigValidationError):
            pu._split_s3_path("s3:///onlykey")

    def test_cross_scheme_replace_raises(
        self, fake_s3: _FakeS3Client
    ) -> None:
        with pytest.raises(PipelineError) as exc_info:
            pu.path_replace("s3://b/k", "/local/path")
        assert "Cannot path_replace across different schemes" in exc_info.value.message

    def test_unsupported_scheme_propagates_through_io(self) -> None:
        # The unsupported scheme detection lives in detect_scheme; confirm
        # it propagates through the I/O surface with the standard message.
        with pytest.raises(ConfigValidationError) as exc_info:
            pu.path_write_text("s3a://b/k", "x")
        assert "Unsupported storage scheme" in exc_info.value.message
