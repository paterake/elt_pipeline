from __future__ import annotations

from collections.abc import Iterable, Sequence


def _ident(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Column/identifier name must not be empty")
    if all(c.isalnum() or c == "_" for c in cleaned):
        return cleaned
    escaped = cleaned.replace("`", "``")
    return f"`{escaped}`"


def _col_list(names: Sequence[str], *, alias: str | None = None) -> str:
    if not names:
        raise ValueError("Column list may not be empty")
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"{prefix}{_ident(n)}" for n in names)


def _value_alias_list(names: Sequence[str], *, source_alias: str, target_alias: str) -> str:
    assignments = [
        f"{target_alias}.{_ident(n)} = {source_alias}.{_ident(n)}" for n in names
    ]
    return ", ".join(assignments)


def _merge_key_condition(
    *, key_cols: Sequence[str], target_alias: str, source_alias: str
) -> str:
    if not key_cols:
        raise ValueError("MERGE requires at least one ON key column")
    conjuncts = [
        f"{target_alias}.{_ident(k)} = {source_alias}.{_ident(k)}" for k in key_cols
    ]
    return " AND ".join(conjuncts)


def build_merge_into_sql(
    *,
    target_table: str,
    source_query: str,
    key_columns: Sequence[str],
    value_columns: Sequence[str] | None = None,
    partition_columns: Sequence[str] | None = None,
    target_alias: str = "t",
    source_alias: str = "s",
    when_matched_update: bool = True,
    when_not_matched_insert: bool = True,
    matched_condition: str | None = None,
    not_matched_condition: str | None = None,
) -> str:
    if not target_table:
        raise ValueError("MERGE requires target_table")
    if not source_query:
        raise ValueError("MERGE requires source_query")
    if not when_matched_update and not when_not_matched_insert:
        raise ValueError(
            "MERGE requires at least one of when_matched_update "
            "or when_not_matched_insert"
        )
    key_cols = list(key_columns)
    provided_value_cols = list(value_columns) if value_columns else None
    part_cols = list(partition_columns or [])
    if provided_value_cols is None:
        merged: list[str] = []
        seen: set[str] = set()
        for col in [*part_cols, *key_cols]:
            if col not in seen:
                seen.add(col)
                merged.append(col)
        updateable_value_cols = merged
    else:
        updateable_value_cols = [c for c in provided_value_cols if c not in set(key_cols)]
    update_set_cols = updateable_value_cols
    insert_cols_order: list[str] = []
    insert_seen: set[str] = set()
    for col in [*key_cols, *updateable_value_cols, *part_cols]:
        if col not in insert_seen:
            insert_seen.add(col)
            insert_cols_order.append(col)
    on_clause = _merge_key_condition(
        key_cols=key_cols,
        target_alias=target_alias,
        source_alias=source_alias,
    )
    matched_sql = ""
    if when_matched_update and update_set_cols:
        set_clause = _value_alias_list(
            update_set_cols,
            source_alias=source_alias,
            target_alias=target_alias,
        )
        when = "WHEN MATCHED"
        if matched_condition:
            when = f"{when} AND ({matched_condition})"
        matched_sql = f"  {when} THEN UPDATE SET {set_clause}"
    not_matched_sql = ""
    if when_not_matched_insert and insert_cols_order:
        insert_col_sql = _col_list(insert_cols_order)
        insert_val_sql = _col_list(insert_cols_order, alias=source_alias)
        when = "WHEN NOT MATCHED"
        if not_matched_condition:
            when = f"{when} AND ({not_matched_condition})"
        not_matched_sql = (
            f"  {when} THEN INSERT ({insert_col_sql}) VALUES ({insert_val_sql})"
        )
    clauses = [c for c in (matched_sql, not_matched_sql) if c]
    return (
        f"MERGE INTO {target_table} {target_alias}\n"
        f"USING ({source_query}) {source_alias}\n"
        f"  ON {on_clause}\n"
        + "\n".join(clauses)
        + "\n"
    )


def build_merge_into_from_schema(
    *,
    target_table: str,
    source_query: str,
    schema_columns: Iterable[str],
    key_columns: Sequence[str],
    partition_columns: Sequence[str] | None = None,
    target_alias: str = "t",
    source_alias: str = "s",
) -> str:
    all_cols = [c for c in schema_columns]
    key_set = set(key_columns)
    value_cols = [c for c in all_cols if c not in key_set]
    return build_merge_into_sql(
        target_table=target_table,
        source_query=source_query,
        key_columns=list(key_columns),
        value_columns=value_cols,
        partition_columns=list(partition_columns or []),
        target_alias=target_alias,
        source_alias=source_alias,
    )
