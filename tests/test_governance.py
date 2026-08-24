from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from elt_pipeline.shared.governance import (
    _COLUMN_CLASSIFICATION_PREFIX,
    _COLUMN_MASKING_PREFIX,
    _TABLE_CLASSIFICATION_KEY,
    DataClassification,
    MaskingStrategy,
    SqlColumnSpec,
    SqlModelGovernance,
    build_erasure_statement,
    build_governance_table_properties,
    build_retention_delete_statement,
    build_row_level_erasure_statement,
    build_trino_masking_view,
    hash_value_for_masking,
)


class TestClassificationAndMaskingEnums:
    def test_data_classification_has_four_tiers(self):
        assert {c.value for c in DataClassification} == {
            "public", "internal", "confidential", "restricted_pii",
        }

    def test_masking_strategy_has_seven_options(self):
        vals = {m.value for m in MaskingStrategy}
        assert "none" in vals
        assert "nullify" in vals
        assert "hash_sha256" in vals
        assert "redact_email" in vals
        assert "redact_ssn" in vals


class TestSqlColumnSpecValidation:
    def test_column_requires_name(self):
        with pytest.raises(ValidationError):
            SqlColumnSpec(name="  ")

    def test_masking_without_classification_rejected(self):
        with pytest.raises(
            ValidationError,
            match="masking=hash_sha256 requires explicit classification",
        ):
            SqlColumnSpec(
                name="x",
                masking=MaskingStrategy.hash_sha256,
            )

    def test_public_cannot_use_hash_sha256(self):
        with pytest.raises(ValidationError):
            SqlColumnSpec(
                name="x",
                classification=DataClassification.public,
                masking=MaskingStrategy.hash_sha256,
            )

    def test_internal_allows_none_and_truncate_end(self):
        SqlColumnSpec(
            name="x",
            classification=DataClassification.internal,
            masking=MaskingStrategy.truncate_end,
        )
        SqlColumnSpec(
            name="y",
            classification=DataClassification.internal,
            masking=MaskingStrategy.none,
        )

    def test_confidential_allows_hash_sha256(self):
        spec = SqlColumnSpec(
            name="x",
            classification=DataClassification.confidential,
            masking=MaskingStrategy.hash_sha256,
        )
        assert spec.masking == MaskingStrategy.hash_sha256

    def test_redact_email_requires_pii_tier(self):
        spec = SqlColumnSpec(
            name="email",
            classification=DataClassification.restricted_pii,
            masking=MaskingStrategy.redact_email,
        )
        assert spec.masking == MaskingStrategy.redact_email
        with pytest.raises(ValidationError):
            SqlColumnSpec(
                name="email",
                classification=DataClassification.confidential,
                masking=MaskingStrategy.redact_email,
            )

    def test_redact_ssn_requires_pii_tier(self):
        with pytest.raises(ValidationError):
            SqlColumnSpec(
                name="ssn",
                classification=DataClassification.confidential,
                masking=MaskingStrategy.redact_ssn,
            )


class TestSqlModelGovernance:
    def test_retention_days_must_be_positive(self):
        with pytest.raises(ValidationError):
            SqlModelGovernance(retention_days=0)

    def test_duplicate_columns_rejected(self):
        with pytest.raises(
            ValidationError, match="duplicate column governance spec for 'x'",
        ):
            SqlModelGovernance(columns=[
                SqlColumnSpec(name="x"),
                SqlColumnSpec(name="x"),
            ])

    def test_retention_column_must_not_be_empty_string(self):
        with pytest.raises(ValidationError):
            SqlModelGovernance(retention_partition_column="   ")

    def test_strictest_classification_table_level_only(self):
        gov = SqlModelGovernance(classification=DataClassification.confidential)
        assert gov.strictest_classification() == DataClassification.confidential

    def test_strictest_classification_column_overrides(self):
        gov = SqlModelGovernance(
            classification=DataClassification.internal,
            columns=[
                SqlColumnSpec(
                    name="email",
                    classification=DataClassification.restricted_pii,
                    masking=MaskingStrategy.redact_email,
                ),
                SqlColumnSpec(name="order_id"),
            ],
        )
        assert gov.strictest_classification() == DataClassification.restricted_pii

    def test_effective_column_masking_pii_defaults_to_nullify(self):
        gov = SqlModelGovernance(classification=DataClassification.restricted_pii)
        assert gov.effective_column_masking("email") == MaskingStrategy.nullify

    def test_effective_column_masking_confidential_defaults_to_truncate_end(self):
        gov = SqlModelGovernance(classification=DataClassification.confidential)
        assert gov.effective_column_masking("x") == MaskingStrategy.truncate_end

    def test_effective_column_masking_public_none(self):
        gov = SqlModelGovernance(classification=DataClassification.public)
        assert gov.effective_column_masking("x") == MaskingStrategy.none

    def test_effective_column_masking_explicit_overrides_table(self):
        gov = SqlModelGovernance(
            classification=DataClassification.restricted_pii,
            columns=[SqlColumnSpec(
                name="email",
                classification=DataClassification.restricted_pii,
                masking=MaskingStrategy.redact_email,
            )],
        )
        assert gov.effective_column_masking("email") == MaskingStrategy.redact_email

    def test_effective_column_classification_uses_column_first(self):
        gov = SqlModelGovernance(
            classification=DataClassification.internal,
            columns=[SqlColumnSpec(
                name="email",
                classification=DataClassification.restricted_pii,
                masking=MaskingStrategy.redact_email,
            )],
        )
        assert gov.effective_column_classification("email") == DataClassification.restricted_pii
        assert gov.effective_column_classification("other") == DataClassification.internal


class TestGovernanceTableProperties:
    def test_empty_governance_returns_basic_only(self):
        props = build_governance_table_properties(governance=None)
        assert props == {}

    def test_domain_owner_and_basic(self):
        props = build_governance_table_properties(
            governance=None,
            domain="sales",
            owner_name="platform",
            owner_email="p@x.com",
        )
        assert props["elt.governance.domain"] == "sales"
        assert props["elt.governance.owner_name"] == "platform"
        assert props["elt.governance.owner_email"] == "p@x.com"

    def test_classification_retention(self):
        gov = SqlModelGovernance(
            classification=DataClassification.confidential,
            retention_days=2555,
            retention_partition_column="dt",
        )
        props = build_governance_table_properties(governance=gov)
        assert props[_TABLE_CLASSIFICATION_KEY] == "confidential"
        assert props["elt.governance.retention_days"] == "2555"
        assert props["elt.governance.retention_partition_column"] == "dt"

    def test_column_level_tags(self):
        gov = SqlModelGovernance(columns=[
            SqlColumnSpec(
                name="email",
                description="User email",
                classification=DataClassification.restricted_pii,
                masking=MaskingStrategy.redact_email,
                custom_tags={"source": "crm"},
            ),
        ])
        props = build_governance_table_properties(governance=gov)
        assert props[f"{_COLUMN_CLASSIFICATION_PREFIX}email"] == "restricted_pii"
        assert props[f"{_COLUMN_MASKING_PREFIX}email"] == "redact_email"
        assert props["elt.governance.column.description.email"] == "User email"
        assert props["elt.governance.custom.column.email.source"] == "crm"

    def test_custom_properties(self):
        gov = SqlModelGovernance(custom_properties={"sla": "gold"})
        props = build_governance_table_properties(governance=gov)
        assert props["elt.governance.custom.sla"] == "gold"

    def test_strictest_wins_over_table(self):
        gov = SqlModelGovernance(
            classification=DataClassification.internal,
            columns=[
                SqlColumnSpec(
                    name="phone",
                    classification=DataClassification.restricted_pii,
                    masking=MaskingStrategy.truncate_middle,
                ),
            ],
        )
        props = build_governance_table_properties(governance=gov)
        assert props[_TABLE_CLASSIFICATION_KEY] == "restricted_pii"


class TestRetentionErasureBuilders:
    def test_retention_builder_rejects_non_positive(self):
        with pytest.raises(Exception, match="retention_days must be positive"):
            build_retention_delete_statement(
                table_fq="t", partition_col="dt", retention_days=0)

    def test_retention_builder_explicit_date(self):
        stmt = build_retention_delete_statement(
            table_fq="catalog.schema.tbl",
            partition_col="dt",
            retention_days=30,
            reference_date=date(2026, 8, 24),
        )
        assert "DELETE FROM catalog.schema.tbl" in stmt
        assert "dt < DATE '2026-07-25'" in stmt

    def test_retention_predicate_only_option(self):
        pred = build_retention_delete_statement(
            table_fq="t",
            partition_col="dt",
            retention_days=1,
            reference_date=date(2026, 1, 2),
            use_delete=False,
        )
        assert pred == "dt < DATE '2026-01-01'"

    def test_erasure_empty_conditions_rejected(self):
        with pytest.raises(ValueError):
            build_erasure_statement(table_fq="t", where_conditions={})

    def test_erasure_escapes_quotes(self):
        stmt = build_erasure_statement(
            table_fq="db.t",
            where_conditions={"name": "O'Neil", "age": 30},
        )
        assert "name = 'O''Neil'" in stmt
        assert "age = 30" in stmt

    def test_row_level_erasure_batched(self):
        stmt = build_row_level_erasure_statement(
            table_fq="t",
            id_column="oid",
            ids_to_erase=["a", "b", "c", "d"],
            batch_size=2,
        )
        assert "DELETE FROM t WHERE oid IN (" in stmt
        # first 2 ids only
        assert "'a'" in stmt
        assert "'b'" in stmt
        assert "'c'" not in stmt
        assert "'d'" not in stmt

    def test_row_level_erasure_full_batch_when_size_none(self):
        stmt = build_row_level_erasure_statement(
            table_fq="t",
            id_column="oid",
            ids_to_erase=["x", "y", "z"],
        )
        for v in ("'x'", "'y'", "'z'"):
            assert v in stmt


class TestTrinoMaskingView:
    def test_build_view_shape(self):
        cols = [
            SqlColumnSpec(name="oid"),
            SqlColumnSpec(
                name="email",
                classification=DataClassification.restricted_pii,
                masking=MaskingStrategy.redact_email,
            ),
        ]
        gov = SqlModelGovernance(classification=DataClassification.confidential, retention_days=365)
        sql = build_trino_masking_view(
            base_table_fq="iceberg.l.t",
            view_fq="masked.v",
            columns=cols,
            governance=gov,
            unmask_role="pii_auditor",
        )
        assert "CREATE OR REPLACE VIEW masked.v" in sql
        assert "SECURITY DEFINER" in sql
        assert "is_role_granted('pii_auditor')" in sql
        # email is masked when no role
        assert "split" in sql or "redact_email" in "redact_email" in sql

    def test_build_view_no_unmask_role(self):
        cols = [SqlColumnSpec(
            name="ssn_col",
            classification=DataClassification.restricted_pii,
            masking=MaskingStrategy.redact_ssn,
        )]
        sql = build_trino_masking_view(
            base_table_fq="b",
            view_fq="v",
            columns=cols,
        )
        assert "***-**-" in sql
        assert "is_role_granted" not in sql

    def test_nullify_produces_cast_null(self):
        cols = [
            SqlColumnSpec(
                name="secret",
                classification=DataClassification.restricted_pii,
                masking=MaskingStrategy.nullify,
            ),
        ]
        gov = SqlModelGovernance(classification=DataClassification.restricted_pii)
        sql = build_trino_masking_view(
            base_table_fq="b",
            view_fq="v",
            columns=cols,
            governance=gov,
        )
        assert "CAST(NULL AS VARCHAR)" in sql


class TestHashValueForMasking:
    def test_hash_none_returns_none(self):
        assert hash_value_for_masking(None) is None

    def test_hash_deterministic(self):
        a = hash_value_for_masking("user@example.com")
        b = hash_value_for_masking("user@example.com")
        assert a == b
        assert len(a) == 64  # sha256 hex

    def test_hash_differs_for_different_inputs(self):
        a = hash_value_for_masking("a@x.com")
        b = hash_value_for_masking("b@x.com")
        assert a != b


class TestManifestYamlRoundTrip:
    def test_sql_model_manifest_accepts_governance_from_dict(self):
        from elt_pipeline.sql.models import (
            SqlModelManifest,
        )

        raw = {
            "manifest_version": "v1",
            "name": "canonical_orders",
            "stage": "level3",
            "domain": "sales",
            "materialization": "table",
            "load_mode": "full_refresh",
            "target": {"table_name": "canonical_orders"},
            "owner": {"name": "platform", "email": "p@x.com"},
            "governance": {
                "classification": "confidential",
                "retention_days": 2555,
                "retention_partition_column": "dt",
                "columns": [
                    {
                        "name": "customer_email",
                        "description": "Customer account email",
                        "classification": "restricted_pii",
                        "masking": "redact_email",
                    },
                    {
                        "name": "order_total_usd",
                        "classification": "internal",
                    },
                ],
                "custom_properties": {"sla": "gold"},
            },
            "quality": {},
        }
        m = SqlModelManifest(**raw)
        assert m.governance.classification == DataClassification.confidential
        assert m.governance.retention_days == 2555
        assert m.governance.retention_partition_column == "dt"
        assert len(m.governance.columns) == 2
        email_col = m.governance.columns[0]
        assert email_col.name == "customer_email"
        assert email_col.classification == DataClassification.restricted_pii
        assert email_col.masking == MaskingStrategy.redact_email
        assert m.governance.custom_properties["sla"] == "gold"
