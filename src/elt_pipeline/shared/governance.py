from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class DataClassification(str, Enum):
    public = "public"
    internal = "internal"
    confidential = "confidential"
    restricted_pii = "restricted_pii"


_CLASSIFICATION_ORDER = {
    DataClassification.public: 0,
    DataClassification.internal: 1,
    DataClassification.confidential: 2,
    DataClassification.restricted_pii: 3,
}


class MaskingStrategy(str, Enum):
    none = "none"
    hash_sha256 = "hash_sha256"
    redact_email = "redact_email"
    redact_ssn = "redact_ssn"
    nullify = "nullify"
    truncate_middle = "truncate_middle"
    truncate_end = "truncate_end"


_PUBLIC_MASKING = frozenset({MaskingStrategy.none})
_INTERNAL_MASKING = frozenset({MaskingStrategy.none, MaskingStrategy.truncate_end})
_CONFIDENTIAL_MASKING = frozenset({
    MaskingStrategy.hash_sha256,
    MaskingStrategy.truncate_middle,
    MaskingStrategy.truncate_end,
    MaskingStrategy.nullify,
})
_PII_MASKING = frozenset({
    MaskingStrategy.hash_sha256,
    MaskingStrategy.redact_email,
    MaskingStrategy.redact_ssn,
    MaskingStrategy.truncate_middle,
    MaskingStrategy.nullify,
})

_CLASSIFICATION_ALLOWED_MASKING: dict[DataClassification, frozenset[MaskingStrategy]] = {
    DataClassification.public: _PUBLIC_MASKING,
    DataClassification.internal: _INTERNAL_MASKING,
    DataClassification.confidential: _CONFIDENTIAL_MASKING,
    DataClassification.restricted_pii: _PII_MASKING,
}

_PII_PATTERN_CLASSIFICATIONS: frozenset[MaskingStrategy] = frozenset({
    MaskingStrategy.redact_email,
    MaskingStrategy.redact_ssn,
})


_TABLE_PROPERTY_PREFIX = "elt.governance."
_TABLE_CLASSIFICATION_KEY = f"{_TABLE_PROPERTY_PREFIX}classification"
_TABLE_RETENTION_DAYS_KEY = f"{_TABLE_PROPERTY_PREFIX}retention_days"
_TABLE_RETENTION_PARTITION_KEY = f"{_TABLE_PROPERTY_PREFIX}retention_partition_column"
_TABLE_OWNER_NAME_KEY = f"{_TABLE_PROPERTY_PREFIX}owner_name"
_TABLE_OWNER_EMAIL_KEY = f"{_TABLE_PROPERTY_PREFIX}owner_email"
_TABLE_DOMAIN_KEY = f"{_TABLE_PROPERTY_PREFIX}domain"
_COLUMN_CLASSIFICATION_PREFIX = f"{_TABLE_PROPERTY_PREFIX}column.classification."
_COLUMN_MASKING_PREFIX = f"{_TABLE_PROPERTY_PREFIX}column.masking."
_COLUMN_DESCRIPTION_PREFIX = f"{_TABLE_PROPERTY_PREFIX}column.description."
_CUSTOM_PROPERTY_PREFIX = f"{_TABLE_PROPERTY_PREFIX}custom."


class SqlColumnSpec(BaseModel):
    name: str
    type: str | None = None
    nullable: bool | None = None
    description: str | None = None
    classification: DataClassification | None = None
    masking: MaskingStrategy | None = None
    custom_tags: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("column name must not be empty")
        return cleaned

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("column type must not be empty if set")
        return cleaned

    @field_validator("masking")
    @classmethod
    def validate_masking_consistency(
        cls, value: MaskingStrategy | None, info: Any
    ) -> MaskingStrategy | None:
        if value is None or value == MaskingStrategy.none:
            return value
        classification = info.data.get("classification")
        if classification is None:
            raise ValueError(
                f"masking={value.value} requires explicit classification on column"
            )
        allowed = _CLASSIFICATION_ALLOWED_MASKING.get(
            DataClassification(classification), frozenset()
        )
        if value not in allowed:
            raise ValueError(
                f"masking={value.value} is not allowed for classification="
                f"{classification}. Allowed: {sorted(m.value for m in allowed)}"
            )
        if value in _PII_PATTERN_CLASSIFICATIONS:
            if DataClassification(classification) != DataClassification.restricted_pii:
                raise ValueError(
                    f"masking={value.value} is only valid for "
                    f"classification=restricted_pii"
                )
        return value


class SqlModelGovernance(BaseModel):
    classification: DataClassification | None = None
    retention_days: int | None = Field(default=None, ge=1)
    retention_partition_column: str | None = None
    columns: list[SqlColumnSpec] = Field(default_factory=list)
    custom_properties: dict[str, str] = Field(default_factory=dict)

    @field_validator("retention_partition_column")
    @classmethod
    def validate_retention_column(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("retention_partition_column must not be empty if set")
        return cleaned

    @field_validator("columns")
    @classmethod
    def validate_unique_columns(cls, values: list[SqlColumnSpec]) -> list[SqlColumnSpec]:
        seen: set[str] = set()
        for spec in values:
            if spec.name in seen:
                raise ValueError(f"duplicate column governance spec for '{spec.name}'")
            seen.add(spec.name)
        return values

    def effective_column_classification(
        self, column_name: str
    ) -> DataClassification | None:
        for spec in self.columns:
            if spec.name == column_name:
                if spec.classification is not None:
                    return spec.classification
        return self.classification

    def effective_column_masking(self, column_name: str) -> MaskingStrategy:
        for spec in self.columns:
            if spec.name == column_name and spec.masking is not None:
                return spec.masking
        eff_class = self.effective_column_classification(column_name)
        if eff_class == DataClassification.restricted_pii:
            return MaskingStrategy.nullify
        if eff_class == DataClassification.confidential:
            return MaskingStrategy.truncate_end
        return MaskingStrategy.none

    def strictest_classification(self) -> DataClassification | None:
        strictest_idx = -1
        strictest: DataClassification | None = None
        if self.classification is not None:
            strictest_idx = _CLASSIFICATION_ORDER[self.classification]
            strictest = self.classification
        for spec in self.columns:
            if spec.classification is None:
                continue
            idx = _CLASSIFICATION_ORDER[spec.classification]
            if idx > strictest_idx:
                strictest_idx = idx
                strictest = spec.classification
        return strictest


def build_governance_table_properties(
    *,
    governance: SqlModelGovernance | None,
    domain: str | None = None,
    owner_name: str | None = None,
    owner_email: str | None = None,
) -> dict[str, str]:
    props: dict[str, str] = {}
    if domain:
        props[_TABLE_DOMAIN_KEY] = domain
    if owner_name:
        props[_TABLE_OWNER_NAME_KEY] = owner_name
    if owner_email:
        props[_TABLE_OWNER_EMAIL_KEY] = owner_email
    if governance is None:
        return props
    strictest = governance.strictest_classification()
    if strictest is not None:
        props[_TABLE_CLASSIFICATION_KEY] = strictest.value
    if governance.retention_days is not None:
        props[_TABLE_RETENTION_DAYS_KEY] = str(governance.retention_days)
    if governance.retention_partition_column is not None:
        props[_TABLE_RETENTION_PARTITION_KEY] = governance.retention_partition_column
    for spec in governance.columns:
        if spec.classification is not None:
            props[f"{_COLUMN_CLASSIFICATION_PREFIX}{spec.name}"] = spec.classification.value
        if spec.masking is not None:
            props[f"{_COLUMN_MASKING_PREFIX}{spec.name}"] = spec.masking.value
        if spec.description:
            props[f"{_COLUMN_DESCRIPTION_PREFIX}{spec.name}"] = spec.description
        for tag_key, tag_val in spec.custom_tags.items():
            props[f"{_CUSTOM_PROPERTY_PREFIX}column.{spec.name}.{tag_key}"] = tag_val
    for custom_key, custom_val in governance.custom_properties.items():
        props[f"{_CUSTOM_PROPERTY_PREFIX}{custom_key}"] = custom_val
    return props


def _coerce_literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, (datetime, date)):
        return f"'{value.isoformat()}'"
    if value is None:
        return "NULL"
    return str(value)


def build_retention_delete_statement(
    *,
    table_fq: str,
    partition_col: str,
    retention_days: int,
    reference_date: date | None = None,
    use_delete: bool = True,
) -> str:
    if retention_days <= 0:
        raise ValueError("retention_days must be positive")
    ref = reference_date if reference_date is not None else datetime.now(timezone.utc).date()
    cutoff = ref.toordinal() - retention_days
    cutoff_date = date.fromordinal(cutoff)
    predicate = f"{partition_col} < DATE '{cutoff_date.isoformat()}'"
    if use_delete:
        return f"DELETE FROM {table_fq} WHERE {predicate}"
    return predicate


def build_erasure_statement(
    *,
    table_fq: str,
    where_conditions: dict[str, Any],
) -> str:
    if not where_conditions:
        raise ValueError("where_conditions must be non-empty for an erasure statement")
    clauses = [f"{col} = {_coerce_literal(val)}" for col, val in where_conditions.items()]
    predicate = " AND ".join(clauses)
    return f"DELETE FROM {table_fq} WHERE {predicate}"


def build_row_level_erasure_statement(
    *,
    table_fq: str,
    id_column: str,
    ids_to_erase: list[Any],
    batch_size: int | None = None,
) -> str:
    if not ids_to_erase:
        raise ValueError("ids_to_erase must be non-empty for an erasure statement")
    if batch_size is not None and batch_size > 0 and len(ids_to_erase) > batch_size:
        batch_ids = ids_to_erase[:batch_size]
    else:
        batch_ids = ids_to_erase
    literals = ", ".join(_coerce_literal(i) for i in batch_ids)
    return f"DELETE FROM {table_fq} WHERE {id_column} IN ({literals})"


def _trino_mask_expression(
    *,
    column: str,
    masking: MaskingStrategy,
    classification: DataClassification | None,
) -> str:
    if masking == MaskingStrategy.none:
        return f'"{column}"'
    if masking == MaskingStrategy.nullify:
        return "CAST(NULL AS VARCHAR)"
    if masking == MaskingStrategy.hash_sha256:
        digest = "to_hex(sha256(to_utf8(CAST("
        return f"IF(\"{column}\" IS NULL, NULL, {digest}\"{column}\" AS VARCHAR)))))"
    if masking == MaskingStrategy.redact_email:
        inner_mid = (
            "regexp_replace(substr(local, 2, length(local) - 2), "
            "'.', '*')"
        )
        local_mask = (
            "IF(length(local) <= 2, regexp_replace(local, '.', '*'), "
            "CONCAT(substr(local, 1, 1), " + inner_mid + ", "
            "substr(local, -1)))"
        )
        return (
            f'IF("{column}" IS NULL, NULL, '
            f"LET(split_email = split(\"{column}\", '@'), "
            f"local = element_at(split_email, 1), "
            f"domain = element_at(split_email, 2), "
            f"CONCAT({local_mask}, '@', domain)))"
        )
    if masking == MaskingStrategy.redact_ssn:
        return (
            f'IF("{column}" IS NULL, NULL, '
            f'CONCAT("***-**-", substr(regexp_replace("{column}", "\\\\D", ""), -4)))'
        )
    if masking == MaskingStrategy.truncate_middle:
        return (
            f'IF("{column}" IS NULL, NULL, '
            f'LET(s = CAST("{column}" AS VARCHAR), n = length(s), '
            f"IF(n <= 4, regexp_replace(s, '.', '*'), "
            "prefix = substr(s, 1, 1), suffix = substr(s, -1), "
            "middle = repeat('*', greatest(n - 2, 2)), "
            "CONCAT(prefix, middle, suffix))))"
        )
    if masking == MaskingStrategy.truncate_end:
        if classification == DataClassification.confidential:
            keep = 4
        else:
            keep = 8
        return (
            f'IF("{column}" IS NULL, NULL, '
            f"LET(s = CAST(\"{column}\" AS VARCHAR), n = length(s), "
            f"IF(n <= {keep}, repeat('*', n), "
            f"CONCAT(substr(s, 1, {keep}), repeat('*', greatest(n - {keep}, 2))))))"
        )
    return f'"{column}"'


def build_trino_masking_view(
    *,
    base_table_fq: str,
    view_fq: str,
    columns: list[SqlColumnSpec],
    governance: SqlModelGovernance | None = None,
    require_role: str | None = None,
    unmask_role: str | None = None,
) -> str:
    projections: list[str] = []
    for spec in columns:
        if unmask_role is not None:
            masking = spec.masking
            if masking is None:
                if governance is not None:
                    masking = governance.effective_column_masking(spec.name)
                else:
                    masking = MaskingStrategy.none
            classification = spec.classification
            if classification is None and governance is not None:
                classification = governance.effective_column_classification(spec.name)
            masked_expr = _trino_mask_expression(
                column=spec.name, masking=masking, classification=classification
            )
            expr = (
                f'IF(is_role_granted(\'{unmask_role}\'), "{spec.name}", {masked_expr})'
            )
            projections.append(f'  {expr} AS "{spec.name}"')
        else:
            masking = spec.masking
            if masking is None:
                if governance is not None:
                    masking = governance.effective_column_masking(spec.name)
                else:
                    masking = MaskingStrategy.none
            classification = spec.classification
            if classification is None and governance is not None:
                classification = governance.effective_column_classification(spec.name)
            expr = _trino_mask_expression(
                column=spec.name, masking=masking, classification=classification
            )
            projections.append(f"  {expr} AS \"{spec.name}\"")
    columns_clause = ",\n".join(projections)
    role_clause = ""
    if require_role is not None:
        role_clause = f"\n-- SECURITY: require role {require_role} to select from view\n"
    strict = governance.strictest_classification() if governance else None
    strict_val = strict.value if strict is not None else "unset"
    retention_val = (
        str(governance.retention_days)
        if governance is not None and governance.retention_days
        else "unset"
    )
    sql = (
        f"-- Auto-generated Trino masking view for {base_table_fq}\n"
        f"-- Classification: {strict_val}\n"
        f"-- Retention: {retention_val} days\n"
        f"{role_clause}"
        f"CREATE OR REPLACE VIEW {view_fq} SECURITY DEFINER AS\n"
        f"SELECT\n{columns_clause}\n"
        f"FROM {base_table_fq}\n"
        f"WITH CHECK OPTION\n"
    )
    return sql


def hash_value_for_masking(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
