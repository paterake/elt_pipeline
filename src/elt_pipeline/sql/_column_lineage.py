from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _is_java_obj(obj: Any) -> bool:
    try:
        from py4j.java_gateway import JavaObject
    except Exception:  # noqa: BLE001
        return False
    return isinstance(obj, JavaObject)


def _node_simple_name(node: Any) -> str:
    if _is_java_obj(node):
        try:
            return node.getClass().getSimpleName()
        except Exception:  # noqa: BLE001
            return ""
    return type(node).__name__


def _scala_seq_to_list(seq: Any) -> list[Any]:
    if seq is None:
        return []
    if _is_java_obj(seq):
        out: list[Any] = []
        try:
            n = seq.size()
            for i in range(n):
                out.append(seq.apply(i))
        except Exception:  # noqa: BLE001
            pass
        return out
    try:
        return list(seq)
    except Exception:  # noqa: BLE001
        return []


def _java_invoke_or_python_attr(node: Any, name: str) -> Any:
    if _is_java_obj(node):
        try:
            method = getattr(node, name)
            if callable(method):
                return method()
        except Exception:  # noqa: BLE001
            pass
        return None
    return getattr(node, name, None)


def extract_column_lineage_from_dataframe(
    dataframe: "DataFrame",
    *,
    input_datasets_by_alias: dict[str, str],
) -> dict[str, list[tuple[str, str]]]:
    """Return ``{output_col_name: [(input_dataset_fqn, input_col_name), ...]}``.

    The lineage is obtained by walking PySpark's *resolved* query plan
    (``dataframe.queryExecution.analyzed`` on PySpark 3.x, or equivalently
    the Java ``df._jdf.queryExecution().analyzed()`` plan on PySpark 4.x
    where the public Python ``queryExecution`` attribute has been removed).

    No SparkListener, no JVM probes, no reflection against Spark private
    classes: it uses the public PySpark TreeNode walkers only.
    """
    out: dict[str, list[tuple[str, str]]] = {}

    analyzed: Any = None
    try:
        qe = getattr(dataframe, "queryExecution", None)
        if qe is not None:
            analyzed = getattr(qe, "analyzed", None)
    except Exception:  # noqa: BLE001
        analyzed = None

    if analyzed is None:
        try:
            jdf = getattr(dataframe, "_jdf", None)
            if jdf is not None:
                qe_java = jdf.queryExecution()
                analyzed = qe_java.analyzed()
        except Exception:  # noqa: BLE001
            analyzed = None

    if analyzed is None:
        return out

    output_exprs: list[tuple[str, Any]] = _collect_output_named_expressions(analyzed)
    for out_name, expr in output_exprs:
        if not isinstance(out_name, str) or not out_name:
            continue
        refs: list[tuple[str, str]] = []
        for (qualifier, col_name) in _walk_references(expr):
            resolved_alias = None
            if qualifier in input_datasets_by_alias:
                resolved_alias = input_datasets_by_alias[qualifier]
            elif col_name in input_datasets_by_alias:
                resolved_alias = input_datasets_by_alias[col_name]
            else:
                for alias, fqn in input_datasets_by_alias.items():
                    if qualifier.endswith("." + alias) or qualifier == alias:
                        resolved_alias = fqn
                        break
            if resolved_alias is None:
                continue
            pair = (resolved_alias, col_name)
            if pair not in refs:
                refs.append(pair)
        out[out_name] = refs

    return out


def _unwrap_transparent_wrappers(node: Any) -> Any:
    """Unwrap wrapper plan nodes (SubqueryAlias / View) to reach the payload."""
    max_unwrap = 8
    cur = node
    for _ in range(max_unwrap):
        simple = _node_simple_name(cur)
        if simple in ("SubqueryAlias", "View"):
            child = _java_invoke_or_python_attr(cur, "child")
            if child is None:
                children = _scala_seq_to_list(
                    _java_invoke_or_python_attr(cur, "children")
                )
                if len(children) == 1:
                    child = children[0]
            if child is None:
                return cur
            cur = child
        else:
            return cur
    return cur


def _collect_output_named_expressions(
    analyzed: Any,
) -> list[tuple[str, Any]]:
    """Return ``[(output_column_name, expression_tree_node), ...]``.

    For ``Project`` / ``Aggregate`` logical nodes, the source of truth is the
    named-expression list (``projectList`` / ``aggregateExpressions``). The
    ``output`` AttributeReferences are fresh re-numbered attrs whose tree
    contains no references for columns produced via ``Alias``, so walking them
    would miss every computed column (aggregations, concatenations, etc.).
    """
    unwrapped = _unwrap_transparent_wrappers(analyzed)
    simple = _node_simple_name(unwrapped)

    exprs: list[Any] | None = None
    if simple == "Project":
        exprs = _scala_seq_to_list(
            _java_invoke_or_python_attr(unwrapped, "projectList")
        )
    elif simple == "Aggregate":
        exprs = _scala_seq_to_list(
            _java_invoke_or_python_attr(unwrapped, "aggregateExpressions")
        )

    if exprs is not None and len(exprs) > 0:
        out: list[tuple[str, Any]] = []
        output_attrs = _node_output_attrs(analyzed)
        for idx, ne in enumerate(exprs):
            name = _attr_name(ne)
            if (not name or not isinstance(name, str)) and idx < len(output_attrs):
                name = _attr_name(output_attrs[idx])
            if not name or not isinstance(name, str):
                continue
            out.append((name, ne))
        return out

    fallback: list[tuple[str, Any]] = []
    for attr in _node_output_attrs(analyzed):
        name = _attr_name(attr)
        if not name or not isinstance(name, str):
            continue
        fallback.append((name, attr))
    return fallback



def _attr_name(attr: Any) -> str | None:
    val = _java_invoke_or_python_attr(attr, "name")
    if isinstance(val, str):
        return val
    return None


def _attr_qualifier_parts(attr: Any) -> list[str]:
    raw = _java_invoke_or_python_attr(attr, "qualifier")
    if raw is None:
        return []
    parts: list[str] = []
    for part in _scala_seq_to_list(raw):
        if isinstance(part, str):
            parts.append(part)
    return parts


def _node_output_attrs(node: Any) -> list[Any]:
    """Return the list of output Attribute-like objects for a TreeNode."""
    attrs: list[Any] = []
    try:
        raw_output = _java_invoke_or_python_attr(node, "output")
        if raw_output is None:
            raw_output = _java_invoke_or_python_attr(node, "producedAttributes")
        if raw_output is not None:
            attrs.extend(_scala_seq_to_list(raw_output))
    except Exception:  # noqa: BLE001
        return attrs
    return attrs


def _collect_children_via_all_known_seams(node: Any) -> list[Any]:
    children: list[Any] = []

    seams = [
        "children",
        "childrenResolved",
        "exprs",
        "expressions",
        "projectList",
        "aggregateExpressions",
        "groupingExpressions",
        "joinKeys",
        "condition",
        "child",
        "left",
        "right",
    ]
    for seam in seams:
        try:
            raw = _java_invoke_or_python_attr(node, seam)
        except Exception:  # noqa: BLE001
            raw = None
        if raw is None:
            continue
        if seam == "child" or seam == "left" or seam == "right" or seam == "condition":
            if not isinstance(raw, (bool, int, float, str, bytes)):
                children.append(raw)
        else:
            for item in _scala_seq_to_list(raw):
                children.append(item)

    if not _is_java_obj(node):
        try:
            child_items = list(vars(node).values())
        except Exception:  # noqa: BLE001
            child_items = []
        for item in child_items:
            if item is node:
                continue
            if isinstance(item, (list, tuple, set, frozenset)):
                for sub in item:
                    children.append(sub)
            elif isinstance(item, dict):
                for sub in item.values():
                    children.append(sub)
            else:
                children.append(item)
    return children


def _walk_references(node: Any) -> list[tuple[str, str]]:
    """Walk the children/childrenResolved/exprs tree of a Spark TreeNode.

    Returns a de-duplicated list of ``(qualifier, column_name)`` pairs for
    every ``AttributeReference`` node encountered.
    """
    seen: set[tuple[str, str]] = set()
    order: list[tuple[str, str]] = []
    visited_nodes: set[int] = set()
    stack: list[Any] = [node]
    visit_budget = 50_000
    while stack and visit_budget > 0:
        current = stack.pop()
        visit_budget -= 1
        if current is None:
            continue
        if isinstance(current, (bool, int, float, str, bytes)):
            continue
        cid = id(current)
        if cid in visited_nodes:
            continue
        visited_nodes.add(cid)
        cls_name = _node_simple_name(current)
        if cls_name.endswith("AttributeReference") or cls_name == "AttributeReference":
            col_name = _attr_name(current)
            if not isinstance(col_name, str) or not col_name:
                continue
            qualifier_parts = _attr_qualifier_parts(current)
            qualifier = ".".join(qualifier_parts)
            pair = (qualifier, col_name)
            if pair not in seen:
                seen.add(pair)
                order.append(pair)
        else:
            try:
                kids = _collect_children_via_all_known_seams(current)
            except Exception:  # noqa: BLE001
                kids = []
            for child in reversed(kids):
                ccid = id(child)
                if ccid not in visited_nodes:
                    stack.append(child)
    return order
