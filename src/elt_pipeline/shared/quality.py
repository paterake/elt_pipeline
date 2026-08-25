from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Union

from pydantic import BaseModel, Field, Tag, TypeAdapter, field_validator
from typing_extensions import Annotated

from elt_pipeline.integrations.quality import (
    QualityCheckResult,
    QualityCheckStatus,
    QualityDatasetRef,
)

_QUALITY_BUILTIN_PRODUCER = "elt_pipeline/builtin_quality/1"

_BUILTIN_CHECK_KINDS = (
    "not_null",
    "uniqueness",
    "range",
    "referential_integrity",
    "freshness",
    "regex_format",
)


class NotNullCheck(BaseModel):
    kind: Literal["not_null"] = "not_null"
    check_name: str
    column: str
    dataset_id: str | None = None


class UniquenessCheck(BaseModel):
    kind: Literal["uniqueness"] = "uniqueness"
    check_name: str
    columns: list[str]
    dataset_id: str | None = None

    @field_validator("columns")
    @classmethod
    def _cols_nonempty(cls, v: list[str]) -> list[str]:
        if len([c for c in v if c and c.strip()]) == 0:
            raise ValueError("UniquenessCheck.columns must include at least one column")
        return [c.strip() for c in v if c and c.strip()]


class RangeCheck(BaseModel):
    kind: Literal["range"] = "range"
    check_name: str
    column: str
    min_value: int | float | None = None
    max_value: int | float | None = None
    inclusive_lower: bool = True
    inclusive_upper: bool = True
    dataset_id: str | None = None

    @field_validator("max_value")
    @classmethod
    def _min_before_max(cls, v: int | float | None, info: Any) -> int | float | None:
        min_v = info.data.get("min_value") if isinstance(info.data, dict) else None
        if min_v is not None and v is not None and v < min_v:
            raise ValueError(
                f"RangeCheck.max_value {v} cannot be less than min_value {min_v}"
            )
        return v


class ReferentialIntegrityCheck(BaseModel):
    kind: Literal["referential_integrity"] = "referential_integrity"
    check_name: str
    source_column: str
    target_dataset_id: str
    target_column: str
    dataset_id: str | None = None


class FreshnessCheck(BaseModel):
    kind: Literal["freshness"] = "freshness"
    check_name: str
    timestamp_column: str
    max_age_seconds: float
    dataset_id: str | None = None

    @field_validator("max_age_seconds")
    @classmethod
    def _positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(
                "FreshnessCheck.max_age_seconds must be a positive number"
            )
        return v


class RegexFormatCheck(BaseModel):
    kind: Literal["regex_format"] = "regex_format"
    check_name: str
    column: str
    pattern: str
    dataset_id: str | None = None

    @field_validator("pattern")
    @classmethod
    def _valid_regex(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"RegexFormatCheck.pattern is invalid: {exc}") from exc
        return v


BuiltinQualityCheck = Annotated[
    Union[
        Annotated[NotNullCheck, Tag("not_null")],
        Annotated[UniquenessCheck, Tag("uniqueness")],
        Annotated[RangeCheck, Tag("range")],
        Annotated[ReferentialIntegrityCheck, Tag("referential_integrity")],
        Annotated[FreshnessCheck, Tag("freshness")],
        Annotated[RegexFormatCheck, Tag("regex_format")],
    ],
    Field(discriminator="kind"),
]

BUILTIN_QUALITY_CHECK_ADAPTER: TypeAdapter[BuiltinQualityCheck] = TypeAdapter(BuiltinQualityCheck)


class BuiltinCheckResult(BaseModel):
    check_name: str
    kind: str
    dataset_id: str | None = None
    dataset_name: str | None = None
    status: QualityCheckStatus
    observed_value: int | float | str | None = None
    expected_value: int | float | str | None = None
    message: str | None = None
    violated_records: list[dict[str, Any]] = Field(default_factory=list)


def load_builtin_checks_from_json(path: str) -> list[BuiltinQualityCheck]:
    import json as _json

    from elt_pipeline.shared.path_utils import path_exists, path_read_text

    if not path_exists(path):
        raise FileNotFoundError(f"Builtin checks JSON document not found: {path}")
    raw = _json.loads(path_read_text(path))
    if isinstance(raw, dict):
        raw_list = raw.get("checks") or raw.get("quality_checks") or []
        if not isinstance(raw_list, list):
            key = "'checks' / 'quality_checks'"
            raise ValueError(
                f"Builtin checks JSON at {path} must contain a list under {key}"
            )
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raise ValueError(
            f"Builtin checks JSON at {path} must be a list or an object with a 'checks' array"
        )
    return [BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(item) for item in raw_list]


def load_builtin_checks_from_yaml(path: str) -> list[BuiltinQualityCheck]:
    from elt_pipeline.shared.path_utils import path_exists, path_read_text

    try:
        import yaml  # type: ignore
    except ImportError as exc:  # pragma: no cover - yaml optional at DQ layer
        raise ImportError(
            "PyYAML is required to load builtin checks from YAML documents. "
            "Install with `uv pip install pyyaml`."
        ) from exc
    if not path_exists(path):
        raise FileNotFoundError(f"Builtin checks YAML document not found: {path}")
    raw = yaml.safe_load(path_read_text(path))
    if raw is None:
        return []
    if isinstance(raw, dict):
        raw_list = raw.get("checks") or raw.get("quality_checks") or []
        if not isinstance(raw_list, list):
            key = "'checks' / 'quality_checks'"
            raise ValueError(
                f"Builtin checks YAML at {path} must contain a list under {key}"
            )
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raise ValueError(
            f"Builtin checks YAML at {path} must be a list or an object with a 'checks' array"
        )
    return [BUILTIN_QUALITY_CHECK_ADAPTER.validate_python(item) for item in raw_list]


def _coerce_numeric(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            if "." in s or "e" in s.lower():
                return float(s)
            return int(s)
        except ValueError:
            return None
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (ValueError, OverflowError, OSError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        iso_candidates: list[str] = [s]
        if len(s) == 10 and s[4] == "-":
            iso_candidates.append(f"{s}T00:00:00Z")
        for attempt in iso_candidates:
            try:
                if attempt.endswith("Z"):
                    attempt = f"{attempt[:-1]}+00:00"
                parsed = datetime.fromisoformat(attempt)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except ValueError:
                continue
    return None


def _dataset_matches(spec: Any, dataset_id: str | None, dataset_name: str | None) -> bool:
    sid = getattr(spec, "dataset_id", None)
    if sid is None:
        return True
    return sid == dataset_id or sid == dataset_name


def evaluate_builtin_checks_for_dataset(
    *,
    dataset: QualityDatasetRef,
    checks: list[BuiltinQualityCheck],
    reference_datasets: dict[str, list[dict[str, Any]]] | None = None,
    reference_now: datetime | None = None,
) -> list[BuiltinCheckResult]:
    dataset_id = dataset.dataset_id or None
    dataset_name = dataset.dataset_name or None
    records: list[dict[str, Any]] = (
        dataset.records if isinstance(getattr(dataset, "records", None), list) else []  # type: ignore[attr-defined]
    )

    applicable = [c for c in checks if _dataset_matches(c, dataset_id, dataset_name)]
    if not applicable:
        return []
    if not records:
        return [
            BuiltinCheckResult(
                check_name=spec.check_name,
                kind=spec.kind,
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                status=QualityCheckStatus.skipped,
                message=(
                    f"Builtin quality check '{spec.check_name}' skipped: no records "
                    f"supplied for dataset '{dataset.dataset_name or dataset.dataset_id}'. "
                    "Populate QualityDatasetRef.records with the materialized record list "
                    "to run builtin checks."
                ),
            )
            for spec in applicable
        ]

    results: list[BuiltinCheckResult] = []
    reference_map: dict[str, list[dict[str, Any]]] = reference_datasets or {}
    now = reference_now or datetime.now(tz=UTC)

    for spec in applicable:
        try:
            results.append(
                _evaluate_one_check(
                    spec=spec,
                    records=records,
                    dataset_id=dataset_id,
                    dataset_name=dataset_name,
                    reference_map=reference_map,
                    now=now,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            results.append(
                BuiltinCheckResult(
                    check_name=spec.check_name,
                    kind=spec.kind,
                    dataset_id=dataset_id,
                    dataset_name=dataset_name,
                    status=QualityCheckStatus.fail,
                    message=(
                        f"Error evaluating builtin quality check '{spec.check_name}': {exc}"
                    ),
                )
            )
    return results


def _evaluate_one_check(
    *,
    spec: BuiltinQualityCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
    reference_map: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> BuiltinCheckResult:
    kind = spec.kind
    if kind == "not_null":
        return _check_not_null(spec, records, dataset_id, dataset_name)
    if kind == "uniqueness":
        return _check_uniqueness(spec, records, dataset_id, dataset_name)
    if kind == "range":
        return _check_range(spec, records, dataset_id, dataset_name)
    if kind == "referential_integrity":
        return _check_referential(spec, records, dataset_id, dataset_name, reference_map)
    if kind == "freshness":
        return _check_freshness(spec, records, dataset_id, dataset_name, now)
    if kind == "regex_format":
        return _check_regex(spec, records, dataset_id, dataset_name)
    # pragma: no cover - pydantic discriminator prevents unreachable
    raise ValueError(f"Unknown builtin check kind: {kind!r}")


def _check_not_null(
    spec: NotNullCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
) -> BuiltinCheckResult:
    col = spec.column
    violated: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            violated.append({"record": record})
            continue
        value = record.get(col, "__MISSING__SENTINEL__")
        if value == "__MISSING__SENTINEL__" or value is None:
            violated.append(record)
    status = QualityCheckStatus.pass_ if not violated else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=len(violated),
        expected_value=0,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"NOT NULL check '{spec.check_name}' on column '{col}' failed: "
                f"{len(violated)} of {len(records)} rows are NULL or missing."
            )
        ),
        violated_records=violated,
    )


def _check_uniqueness(
    spec: UniquenessCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
) -> BuiltinCheckResult:
    cols = spec.columns
    seen: dict[tuple[Any, ...], int] = {}
    duplicates: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            duplicates.append({"record": record})
            continue
        key_values: list[Any] = []
        all_missing = True
        for col in cols:
            v = record.get(col)
            key_values.append(v)
            if v is not None:
                all_missing = False
        if all_missing:
            duplicates.append(record)
            continue
        key = tuple(key_values)
        if key in seen:
            duplicates.append(record)
            if seen[key] == 0:
                # first row was index 0; mark it as duplicate on second encounter
                for r in records:
                    if not isinstance(r, dict):
                        continue
                    probe = tuple(r.get(c) for c in cols)
                    if probe == key and r not in duplicates:
                        duplicates.append(r)
                        break
            seen[key] = seen[key] + 1
        else:
            seen[key] = 0

    status = QualityCheckStatus.pass_ if not duplicates else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=len(duplicates),
        expected_value=0,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"Uniqueness check '{spec.check_name}' on columns {cols!r} failed: "
                f"{len(duplicates)} of {len(records)} rows are duplicate or have all-null keys."
            )
        ),
        violated_records=duplicates,
    )


def _check_range(
    spec: RangeCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
) -> BuiltinCheckResult:
    col = spec.column
    min_v = spec.min_value
    max_v = spec.max_value
    if min_v is None and max_v is None:
        return BuiltinCheckResult(
            check_name=spec.check_name,
            kind=spec.kind,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            status=QualityCheckStatus.skipped,
            message=(
                f"Range check '{spec.check_name}' skipped: min_value and max_value are both None."
            ),
        )

    violated: list[dict[str, Any]] = []
    if min_v is not None:
        min_op = ">=" if spec.inclusive_lower else ">"
        min_boundary = f"{min_op}{min_v}"
    else:
        min_boundary = "-inf"
    if max_v is not None:
        max_op = "<=" if spec.inclusive_upper else "<"
        max_boundary = f"{max_op}{max_v}"
    else:
        max_boundary = "+inf"
    for record in records:
        if not isinstance(record, dict):
            violated.append({"record": record})
            continue
        raw = record.get(col)
        numeric = _coerce_numeric(raw)
        if numeric is None and raw is not None and (not isinstance(raw, str) or raw.strip()):
            violated.append(record)
            continue
        if numeric is None:
            continue
        outside = False
        if min_v is not None:
            if spec.inclusive_lower:
                outside = outside or numeric < min_v
            else:
                outside = outside or numeric <= min_v
        if max_v is not None:
            if spec.inclusive_upper:
                outside = outside or numeric > max_v
            else:
                outside = outside or numeric >= max_v
        if outside:
            violated.append(record)

    status = QualityCheckStatus.pass_ if not violated else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=len(violated),
        expected_value=0,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"Range check '{spec.check_name}' on column '{col}' failed: "
                f"{len(violated)} of {len(records)} rows outside [{min_boundary}, {max_boundary}]."
            )
        ),
        violated_records=violated,
    )


def _check_referential(
    spec: ReferentialIntegrityCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
    reference_map: dict[str, list[dict[str, Any]]],
) -> BuiltinCheckResult:
    target_records = reference_map.get(spec.target_dataset_id) or reference_map.get(
        spec.target_dataset_id.split(".")[-1]
    )
    if target_records is None:
        return BuiltinCheckResult(
            check_name=spec.check_name,
            kind=spec.kind,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            status=QualityCheckStatus.skipped,
            message=(
                f"Referential integrity check '{spec.check_name}' skipped: reference dataset "
                f"'{spec.target_dataset_id}' was not supplied. Populate reference_datasets "
                "with the target rows keyed by dataset_id."
            ),
        )
    valid_keys = {
        r[spec.target_column]
        for r in target_records
        if isinstance(r, dict) and spec.target_column in r
    }
    violated: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            violated.append({"record": record})
            continue
        if spec.source_column not in record or record[spec.source_column] is None:
            continue
        if record[spec.source_column] not in valid_keys:
            violated.append(record)

    status = QualityCheckStatus.pass_ if not violated else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=len(violated),
        expected_value=0,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"Referential integrity check '{spec.check_name}' failed: "
                f"{len(violated)} of {len(records)} rows in '{spec.source_column}' do not have "
                f"matching keys in {spec.target_dataset_id}.{spec.target_column}."
            )
        ),
        violated_records=violated,
    )


def _check_freshness(
    spec: FreshnessCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
    now: datetime,
) -> BuiltinCheckResult:
    col = spec.timestamp_column
    stalenesses_seconds: list[float] = []
    violated: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            violated.append({"record": record})
            continue
        if col not in record or record[col] is None:
            continue
        ts = _coerce_datetime(record[col])
        if ts is None:
            violated.append(record)
            continue
        delta = now - ts
        seconds = delta.total_seconds()
        stalenesses_seconds.append(seconds)
        if seconds > spec.max_age_seconds:
            violated.append(record)

    if not stalenesses_seconds and len(records) > 0:
        oldest_seconds: float | None = None
    elif not stalenesses_seconds:
        oldest_seconds = None
    else:
        oldest_seconds = max(stalenesses_seconds)

    status = QualityCheckStatus.pass_ if not violated else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=oldest_seconds,
        expected_value=spec.max_age_seconds,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"Freshness check '{spec.check_name}' failed: "
                f"{len(violated)} of {len(records)} rows are older than "
                f"{timedelta(seconds=spec.max_age_seconds)}; oldest = "
                f"{timedelta(seconds=oldest_seconds or 0.0)}."
            )
        ),
        violated_records=violated,
    )


def _check_regex(
    spec: RegexFormatCheck,
    records: list[dict[str, Any]],
    dataset_id: str | None,
    dataset_name: str | None,
) -> BuiltinCheckResult:
    pattern = re.compile(spec.pattern)
    violated: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            violated.append({"record": record})
            continue
        if spec.column not in record or record[spec.column] is None:
            continue
        if not isinstance(record[spec.column], str):
            violated.append(record)
            continue
        if pattern.search(record[spec.column]) is None:
            violated.append(record)

    status = QualityCheckStatus.pass_ if not violated else QualityCheckStatus.fail
    return BuiltinCheckResult(
        check_name=spec.check_name,
        kind=spec.kind,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        status=status,
        observed_value=len(violated),
        expected_value=0,
        message=(
            None
            if status == QualityCheckStatus.pass_
            else (
                f"Format check '{spec.check_name}' on column '{spec.column}' failed: "
                f"{len(violated)} of {len(records)} rows do not match pattern {spec.pattern!r}."
            )
        ),
        violated_records=violated,
    )


def builtin_check_result_to_adapter(
    result: BuiltinCheckResult,
    *,
    backend_type: str,
    blocking: bool = False,
) -> QualityCheckResult:
    violated = result.violated_records or []
    context_payload: dict[str, Any] | None = None
    if violated:
        context_payload = {
            "builtin_violated_count": len(violated),
            "builtin_violated_records_sample": violated[:10],
            "builtin_producer": _QUALITY_BUILTIN_PRODUCER,
        }
    message = result.message
    return QualityCheckResult(
        backend_type=backend_type,
        check_name=result.check_name,
        status=result.status,
        blocking=blocking,
        dataset_id=result.dataset_id,
        dataset_name=result.dataset_name,
        message=message,
        observed_value=result.observed_value,
        expected_value=result.expected_value,
        # Embed violated records via model extra fields (pydantic v2 accepts extras)
        context=context_payload,
    )


__all__ = [
    "BuiltinCheckResult",
    "BuiltinQualityCheck",
    "FreshnessCheck",
    "NotNullCheck",
    "RangeCheck",
    "ReferentialIntegrityCheck",
    "RegexFormatCheck",
    "UniquenessCheck",
    "builtin_check_result_to_adapter",
    "evaluate_builtin_checks_for_dataset",
    "load_builtin_checks_from_json",
    "load_builtin_checks_from_yaml",
]
