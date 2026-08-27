from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LineageEdge:
    source_dataset: str
    target_dataset: str
    source_column: str | None
    target_column: str | None

    def table_key(self) -> tuple[str, str]:
        return (self.source_dataset, self.target_dataset)


@dataclass
class LineageGraph:
    edges: list[LineageEdge] = field(default_factory=list)
    table_parents: dict[str, list[str]] = field(default_factory=dict)
    table_children: dict[str, list[str]] = field(default_factory=dict)
    column_upstream: dict[str, dict[str, list[tuple[str, str]]]] = field(default_factory=dict)
    column_downstream: dict[str, dict[str, list[tuple[str, str]]]] = field(default_factory=dict)

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)
        parents = self.table_parents.setdefault(edge.target_dataset, [])
        if edge.source_dataset not in parents:
            parents.append(edge.source_dataset)
        children = self.table_children.setdefault(edge.source_dataset, [])
        if edge.target_dataset not in children:
            children.append(edge.target_dataset)
        if edge.target_column and edge.source_column and edge.source_dataset:
            up = self.column_upstream.setdefault(edge.target_dataset, {}).setdefault(
                edge.target_column, []
            )
            pair = (edge.source_dataset, edge.source_column)
            if pair not in up:
                up.append(pair)
            down = self.column_downstream.setdefault(edge.source_dataset, {}).setdefault(
                edge.source_column, []
            )
            pair2 = (edge.target_dataset, edge.target_column)
            if pair2 not in down:
                down.append(pair2)


def _iter_lineage_paths(root_path: str | Path) -> list[Path]:
    root = Path(root_path)
    runs_root = root / "runs"
    if not runs_root.exists():
        return []
    lineage_files: list[Path] = []
    for path in runs_root.rglob("lineage.jsonl"):
        if path.is_file():
            lineage_files.append(path)
    return lineage_files


def _build_dataset_fqn(event: dict[str, Any], ref: dict[str, Any]) -> str:
    job_ns = event.get("job_namespace")
    ref_ns = ref.get("namespace") or job_ns or "elt_pipeline"
    name = ref.get("name")
    if not name:
        return ""
    if ref_ns and ref_ns != "elt_pipeline":
        return f"{ref_ns}:{name}"
    return str(name)


def _extract_column_lineage_from_output_facet(
    *,
    target_dataset_fqn: str,
    output_facet: dict[str, Any],
) -> list[LineageEdge]:
    out: list[LineageEdge] = []
    fields = output_facet.get("fields") or {}
    if not isinstance(fields, dict):
        return out
    for target_col, info in fields.items():
        if not isinstance(info, dict):
            continue
        input_fields = info.get("inputFields") or []
        if not isinstance(input_fields, list):
            continue
        for inp in input_fields:
            if not isinstance(inp, dict):
                continue
            source_ns = inp.get("namespace") or "elt_pipeline"
            source_name = inp.get("name") or ""
            source_col = inp.get("field") or ""
            if not source_name or not source_col:
                continue
            if source_ns and source_ns != "elt_pipeline":
                source_fqn = f"{source_ns}:{source_name}"
            else:
                source_fqn = source_name
            out.append(
                LineageEdge(
                    source_dataset=source_fqn,
                    target_dataset=target_dataset_fqn,
                    source_column=source_col,
                    target_column=target_col,
                )
            )
    return out


def build_lineage_graph(root_path: str | Path) -> LineageGraph:
    graph = LineageGraph()
    for lineage_path in _iter_lineage_paths(root_path):
        try:
            with lineage_path.open("r", encoding="utf-8") as fh:
                for raw in fh:
                    if not raw.strip():
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if event.get("event_type") != "COMPLETE":
                        continue
                    inputs_fqn: list[str] = []
                    for in_ref in event.get("inputs", []) or []:
                        fqn = _build_dataset_fqn(event, in_ref)
                        if fqn:
                            inputs_fqn.append(fqn)
                    for out_ref in event.get("outputs", []) or []:
                        target_fqn = _build_dataset_fqn(event, out_ref)
                        if not target_fqn:
                            continue
                        for source_fqn in inputs_fqn:
                            graph.add_edge(
                                LineageEdge(
                                    source_dataset=source_fqn,
                                    target_dataset=target_fqn,
                                    source_column=None,
                                    target_column=None,
                                )
                            )
                        facets = out_ref.get("facets") or {}
                        if isinstance(facets, dict):
                            col = facets.get("columnLineage")
                            if isinstance(col, dict):
                                for edge in _extract_column_lineage_from_output_facet(
                                    target_dataset_fqn=target_fqn, output_facet=col
                                ):
                                    graph.add_edge(edge)
        except OSError:
            continue
    return graph


def _bfs_table(
    *,
    graph: LineageGraph,
    start_dataset: str,
    direction: str,
    max_depth: int,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    adjacency = graph.table_parents if direction == "upstream" else graph.table_children
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(start_dataset, 0)])
    while queue:
        node, depth = queue.popleft()
        if depth > 0:
            results.append(
                {
                    "dataset": node,
                    "depth": depth,
                    "direction": direction,
                }
            )
        if depth >= max_depth:
            continue
        for neighbor in adjacency.get(node, []):
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, depth + 1))
    return results


def _bfs_columns(
    *,
    graph: LineageGraph,
    start_dataset: str,
    start_column: str,
    direction: str,
    max_depth: int,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    if direction == "upstream":
        adj = graph.column_upstream
    else:
        adj = graph.column_downstream
    visited: set[tuple[str, str]] = set()
    queue: deque[tuple[str, str, int]] = deque([(start_dataset, start_column, 0)])
    while queue:
        dataset, column, depth = queue.popleft()
        if depth > 0:
            results.append(
                {
                    "dataset": dataset,
                    "column": column,
                    "depth": depth,
                    "direction": direction,
                }
            )
        if depth >= max_depth:
            continue
        refs = adj.get(dataset, {}).get(column, [])
        for (next_dataset, next_column) in refs:
            key = (next_dataset, next_column)
            if key in visited:
                continue
            visited.add(key)
            queue.append((next_dataset, next_column, depth + 1))
    return results


def run_lineage_impact_analysis(
    *,
    root_path: str | Path,
    column: str,
    depth: int = 5,
    output_format: str = "table",
) -> dict[str, object]:
    """Return upstream + downstream impact for a fully-qualified column reference.

    The ``column`` argument must be specified in ``<dataset>.<column>`` form,
    where ``<dataset>`` matches the output dataset name as stored in
    ``lineage.jsonl`` (the table name, or the ``namespace:table_name`` fully
    qualified format when the namespace is not ``elt_pipeline``).
    """
    if depth < 1:
        raise ValueError("depth must be >= 1")
    pieces = column.rsplit(".", 1)
    if len(pieces) != 2:
        raise ValueError(
            f"column must be in <dataset>.<column> form; got {column!r}"
        )
    dataset_fqn, column_name = pieces

    graph = build_lineage_graph(root_path)

    upstream_cols = _bfs_columns(
        graph=graph,
        start_dataset=dataset_fqn,
        start_column=column_name,
        direction="upstream",
        max_depth=depth,
    )
    downstream_cols = _bfs_columns(
        graph=graph,
        start_dataset=dataset_fqn,
        start_column=column_name,
        direction="downstream",
        max_depth=depth,
    )

    upstream_tables: set[str] = {
        r["dataset"]
        for r in upstream_cols
        if isinstance(r["dataset"], str)
    }
    downstream_tables: set[str] = {
        r["dataset"]
        for r in downstream_cols
        if isinstance(r["dataset"], str)
    }

    upstream_table_rows = _bfs_table(
        graph=graph, start_dataset=dataset_fqn, direction="upstream", max_depth=depth
    )
    downstream_table_rows = _bfs_table(
        graph=graph,
        start_dataset=dataset_fqn,
        direction="downstream",
        max_depth=depth,
    )
    for row in upstream_table_rows:
        upstream_tables.add(str(row["dataset"]))
    for row in downstream_table_rows:
        downstream_tables.add(str(row["dataset"]))

    def _col_sort(row: dict[str, Any]) -> tuple[int, str, str]:
        return (int(row["depth"]), str(row["dataset"]), str(row["column"]))

    result: dict[str, object] = {
        "query": {
            "column": column,
            "dataset": dataset_fqn,
            "column_name": column_name,
            "depth": depth,
        },
        "upstream": {
            "datasets": sorted(upstream_tables),
            "columns": sorted(upstream_cols, key=_col_sort),
        },
        "downstream": {
            "datasets": sorted(downstream_tables),
            "columns": sorted(downstream_cols, key=_col_sort),
        },
    }

    if output_format == "table":
        result["_display_lines"] = _build_display_lines(result)

    return result


def _build_display_lines(result: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    query = result.get("query", {})
    lines.append(
        "Impact analysis for column: "
        f"{query.get('dataset', '')}.{query.get('column_name', '')}"
        f" (depth={query.get('depth', '')})"
    )
    lines.append("=" * len(lines[0]))
    lines.append("")
    lines.append("UPSTREAM dependencies (ancestors):")
    upstream = result.get("upstream", {}) or {}
    if not upstream.get("datasets"):
        lines.append("  (none found)")
    else:
        lines.append(f"  datasets affected: {', '.join(upstream['datasets'])}")
        if upstream.get("columns"):
            lines.append("  columns:")
            for row in upstream["columns"]:
                lines.append(
                    f"    depth={row['depth']}  "
                    f"{row['dataset']}.{row['column']}"
                )
    lines.append("")
    lines.append("DOWNSTREAM dependencies (descendants):")
    downstream = result.get("downstream", {}) or {}
    if not downstream.get("datasets"):
        lines.append("  (none found)")
    else:
        lines.append(f"  datasets affected: {', '.join(downstream['datasets'])}")
        if downstream.get("columns"):
            lines.append("  columns:")
            for row in downstream["columns"]:
                lines.append(
                    f"    depth={row['depth']}  "
                    f"{row['dataset']}.{row['column']}"
                )
    return lines
