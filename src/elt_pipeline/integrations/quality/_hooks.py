from __future__ import annotations

from typing import Any

from elt_pipeline.integrations.quality._models import (
    BUILTIN_CHECKS_BACKEND_TYPE,
    ROW_COUNT_BACKEND_TYPE,
    QualityCheckResult,
    QualityCheckStatus,
    QualityHookRequest,
    _validate_enabled_stages,
    _validate_row_count_min,
)
from elt_pipeline.shared.errors import ConfigValidationError


class RowCountQualityHook:
    backend_type = ROW_COUNT_BACKEND_TYPE

    def __init__(
        self,
        *,
        row_count_min: int,
        enabled_stages: set[str] | frozenset[str] | None = None,
    ) -> None:
        self._row_count_min = _validate_row_count_min(
            row_count_min=row_count_min,
            backend_type=self.backend_type,
        )
        self._enabled_stages = _validate_enabled_stages(
            enabled_stages or {"normalize", "sql"},
            backend_type=self.backend_type,
        )

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]:
        stage_name = request.stage.strip().lower()
        if stage_name not in self._enabled_stages:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=QualityCheckStatus.skipped,
                    message=f"Stage '{request.stage}' is not enabled for row count checks",
                )
            ]

        if not request.datasets:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=QualityCheckStatus.skipped,
                    message="No datasets were emitted for quality evaluation",
                    expected_value=self._row_count_min,
                )
            ]

        results: list[QualityCheckResult] = []
        for dataset in request.datasets:
            if dataset.row_count is None:
                results.append(
                    QualityCheckResult(
                        backend_type=self.backend_type,
                        check_name="row_count_min",
                        status=QualityCheckStatus.skipped,
                        dataset_id=dataset.dataset_id,
                        dataset_name=dataset.dataset_name,
                        message="Row count metric is unavailable for this dataset",
                        expected_value=self._row_count_min,
                    )
                )
                continue

            passed = dataset.row_count >= self._row_count_min
            results.append(
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="row_count_min",
                    status=(
                        QualityCheckStatus.pass_ if passed else QualityCheckStatus.fail
                    ),
                    dataset_id=dataset.dataset_id,
                    dataset_name=dataset.dataset_name,
                    observed_value=dataset.row_count,
                    expected_value=self._row_count_min,
                    message=(
                        None
                        if passed
                        else (
                            f"Dataset '{dataset.dataset_name}' row count "
                            f"{dataset.row_count} is below minimum {self._row_count_min}"
                        )
                    ),
                )
            )
        return results


class BuiltinQualityHook:
    """Starter built-in check library (BACKLOG item G-8).

    Runs a configured list of `BuiltinQualityCheck` specs against each dataset
    that populates `QualityDatasetRef.records` with materialized rows. For
    each failing check, the resulting `QualityCheckResult.violated_records`
    carries the bad rows so the adapter can persist them into the
    quarantine/DLQ artifact location (see `QualityHookAdapter.evaluate`).
    """

    backend_type = BUILTIN_CHECKS_BACKEND_TYPE

    def __init__(
        self,
        *,
        checks: list[Any],
        enabled_stages: set[str] | frozenset[str] | None = None,
    ) -> None:
        from elt_pipeline.shared.quality import (
            BUILTIN_QUALITY_CHECK_ADAPTER as _BQC_TA,
        )
        from elt_pipeline.shared.quality import (
            FreshnessCheck,
            NotNullCheck,
            RangeCheck,
            ReferentialIntegrityCheck,
            RegexFormatCheck,
            UniquenessCheck,
        )

        _bqc_base = (
            NotNullCheck,
            RangeCheck,
            RegexFormatCheck,
            ReferentialIntegrityCheck,
            FreshnessCheck,
            UniquenessCheck,
        )
        normalized_checks: list[Any] = []
        for idx, check in enumerate(checks or []):
            if isinstance(check, _bqc_base):
                normalized_checks.append(check)
            elif isinstance(check, dict):
                normalized_checks.append(_BQC_TA.validate_python(check))
            else:
                raise ConfigValidationError(
                    message=(
                        "BuiltinQualityHook.checks entries must be "
                        "BuiltinQualityCheck instances or dict specs"
                    ),
                    context={
                        "backend_type": self.backend_type,
                        "invalid_index": idx,
                        "invalid_type": type(check).__name__,
                    },
                )
        self._checks: list[Any] = normalized_checks
        self._enabled_stages = _validate_enabled_stages(
            enabled_stages or {"normalize", "sql"},
            backend_type=self.backend_type,
        )

    @property
    def checks(self) -> list[Any]:
        return list(self._checks)

    def evaluate(self, *, request: QualityHookRequest) -> list[QualityCheckResult]:
        from elt_pipeline.shared.quality import (
            builtin_check_result_to_adapter,
            evaluate_builtin_checks_for_dataset,
        )

        stage_name = request.stage.strip().lower()
        if stage_name not in self._enabled_stages:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="builtin_checks_stage_gate",
                    status=QualityCheckStatus.skipped,
                    message=(
                        f"Stage '{request.stage}' is not enabled for builtin DQ checks"
                    ),
                )
            ]
        if not request.datasets:
            return [
                QualityCheckResult(
                    backend_type=self.backend_type,
                    check_name="builtin_checks_no_datasets",
                    status=QualityCheckStatus.skipped,
                    message=(
                        "No datasets were emitted for builtin quality evaluation"
                    ),
                )
            ]

        results: list[QualityCheckResult] = []
        reference_datasets: dict[str, list[dict[str, Any]]] = dict(
            request.reference_datasets or {}
        )
        # Seed reference_datasets with any dataset that has a dataset_id + records
        # so referential integrity checks can cross-reference within the same run
        # without requiring the caller to pre-populate reference_datasets.
        for ds in request.datasets:
            if ds.dataset_id and ds.dataset_id not in reference_datasets and ds.records:
                reference_datasets[ds.dataset_id] = list(ds.records)
            if (
                ds.dataset_name
                and ds.dataset_name not in reference_datasets
                and ds.records
            ):
                reference_datasets.setdefault(ds.dataset_name, list(ds.records))

        for dataset in request.datasets:
            builtin_results = evaluate_builtin_checks_for_dataset(
                dataset=dataset,
                checks=self._checks,
                reference_datasets=reference_datasets,
            )
            for bres in builtin_results:
                adapted = builtin_check_result_to_adapter(
                    bres,
                    backend_type=self.backend_type,
                )
                if bres.violated_records:
                    adapted.violated_records = [
                        dict(r) if isinstance(r, dict) else {"value": r}
                        for r in bres.violated_records
                    ]
                adapted.check_details = {"kind": bres.kind, **(adapted.check_details or {})}
                results.append(adapted)

            if not builtin_results:
                results.append(
                    QualityCheckResult(
                        backend_type=self.backend_type,
                        check_name="builtin_checks_dataset_no_matching_specs",
                        status=QualityCheckStatus.skipped,
                        dataset_id=dataset.dataset_id,
                        dataset_name=dataset.dataset_name,
                        message=(
                            f"No builtin quality check specs targeted dataset "
                            f"'{dataset.dataset_name or dataset.dataset_id}' "
                            f"(specs count={len(self._checks)})."
                        ),
                    )
                )
        return results
