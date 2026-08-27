from __future__ import annotations

import json
from pathlib import Path

import pytest

from elt_pipeline.config import runtime_context
from elt_pipeline.shared.lineage_impact import (
    build_lineage_graph,
    run_lineage_impact_analysis,
)


@pytest.fixture(autouse=True)
def _reset_runtime_context_before_each() -> None:
    runtime_context._reset_for_tests()
    yield
    runtime_context._reset_for_tests()


def _write_lineage_jsonl(root: Path, event_pairs: list[dict]) -> None:
    """Write minimal COMPLETE lineage events for impact-analysis tests.

    Each ``event_pairs`` entry is a dict::

        {"inputs": [fqn, …], "output": fqn, "column_lineage": {out_col: [(fqn, col), …]}}
    """
    run_dir = (
        root / "runs" / "stage=sql" / "job=test"
        / "run_id=test_run_001"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for entry in event_pairs:
        output_fqn = entry["output"]
        column_lineage = entry.get("column_lineage") or {}
        outputs = [
            {
                "namespace": "elt_pipeline",
                "name": output_fqn,
                "facets": (
                    {
                        "columnLineage": {
                            "fields": {
                                out: {
                                    "inputFields": [
                                        {
                                            "namespace": "elt_pipeline",
                                            "name": fqn,
                                            "field": col,
                                        }
                                        for (fqn, col) in refs
                                    ],
                                }
                                for out, refs in column_lineage.items()
                            }
                        }
                    }
                    if column_lineage
                    else {}
                ),
            }
        ]
        event = {
            "event_type": "COMPLETE",
            "run_id": "test_run_001",
            "job_name": "test",
            "inputs": [
                {"namespace": "elt_pipeline", "name": fqn}
                for fqn in entry["inputs"]
            ],
            "outputs": outputs,
        }
        lines.append(json.dumps(event, sort_keys=True))
    (run_dir / "lineage.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def test_impact_analysis_empty_run_history_returns_no_deps(tmp_path: Path) -> None:
    result = run_lineage_impact_analysis(
        root_path=str(tmp_path),
        column="L4.mart_customer.mrr",
        depth=3,
        output_format="json",
    )
    assert result["query"]["dataset"] == "L4.mart_customer"
    assert result["query"]["column_name"] == "mrr"
    assert result["upstream"]["datasets"] == []
    assert result["upstream"]["columns"] == []
    assert result["downstream"]["datasets"] == []
    assert result["downstream"]["columns"] == []


def test_impact_analysis_rejects_bad_column_form(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"<dataset>\.<column>"):
        run_lineage_impact_analysis(
            root_path=str(tmp_path),
            column="no_dot_here",
            depth=2,
        )


def test_impact_analysis_bfs_walk_bidirectional(tmp_path: Path) -> None:
    _write_lineage_jsonl(
        tmp_path,
        [
            {
                "inputs": ["L2.accounts"],
                "output": "L3.canonical.customer",
                "column_lineage": {
                    "customer_key": [("L2.accounts", "id")],
                    "email_hash": [("L2.accounts", "email_digest")],
                    "ingested_at": [],
                },
            },
            {
                "inputs": ["L3.canonical.customer", "L3.canonical.invoice"],
                "output": "L4.mart_customer",
                "column_lineage": {
                    "customer_key": [("L3.canonical.customer", "customer_key")],
                    "mrr": [("L3.canonical.invoice", "amount_total")],
                },
            },
            {
                "inputs": ["L4.mart_customer"],
                "output": "L5.publish.customer_export",
                "column_lineage": {
                    "exported_mrr": [("L4.mart_customer", "mrr")],
                },
            },
        ],
    )
    result = run_lineage_impact_analysis(
        root_path=str(tmp_path),
        column="L4.mart_customer.mrr",
        depth=5,
        output_format="json",
    )
    upstream_datasets = set(result["upstream"]["datasets"])
    downstream_datasets = set(result["downstream"]["datasets"])
    assert "L3.canonical.invoice" in upstream_datasets
    assert "L2.accounts" in upstream_datasets or "L3.canonical.customer" in upstream_datasets
    assert "L5.publish.customer_export" in downstream_datasets

    upstream_cols = {
        (r["dataset"], r["column"]) for r in result["upstream"]["columns"]
    }
    downstream_cols = {
        (r["dataset"], r["column"]) for r in result["downstream"]["columns"]
    }
    assert ("L3.canonical.invoice", "amount_total") in upstream_cols
    assert ("L5.publish.customer_export", "exported_mrr") in downstream_cols


def test_impact_analysis_depth_limit_bounded(tmp_path: Path) -> None:
    _write_lineage_jsonl(
        tmp_path,
        [
            {"inputs": ["a"], "output": "b", "column_lineage": {"y": [("a", "x")]}},
            {"inputs": ["b"], "output": "c", "column_lineage": {"z": [("b", "y")]}},
            {"inputs": ["c"], "output": "d", "column_lineage": {"w": [("c", "z")]}},
            {"inputs": ["d"], "output": "e", "column_lineage": {"v": [("d", "w")]}},
        ],
    )
    result = run_lineage_impact_analysis(
        root_path=str(tmp_path),
        column="c.z",
        depth=1,
        output_format="json",
    )
    upstream_depths = {row["depth"] for row in result["upstream"]["columns"]}
    downstream_depths = {row["depth"] for row in result["downstream"]["columns"]}
    assert upstream_depths.issubset({1})
    assert downstream_depths.issubset({1})


def test_build_lineage_graph_handles_non_json_lines(tmp_path: Path) -> None:
    run_dir = (
        tmp_path / "runs" / "stage=sql" / "job=j" / "run_id=r1"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    bad_contents = (
        "this is not valid json at all\n"
        + json.dumps({"event_type": "START", "inputs": [], "outputs": []})
        + "\n\n"
        + json.dumps(
            {
                "event_type": "COMPLETE",
                "inputs": [{"namespace": "ns", "name": "in"}],
                "outputs": [{"namespace": "ns", "name": "out", "facets": {}}],
            }
        )
        + "\n"
    )
    (run_dir / "lineage.jsonl").write_text(bad_contents, encoding="utf-8")
    graph = build_lineage_graph(str(tmp_path))
    assert "ns:in" in graph.table_parents.get("ns:out", [])
    assert "ns:out" in graph.table_children.get("ns:in", [])


def test_cli_lineage_impact_integration_table_and_json(tmp_path: Path) -> None:
    _write_lineage_jsonl(
        tmp_path,
        [
            {
                "inputs": ["L2.raw_invoices"],
                "output": "L3.canonical.invoice",
                "column_lineage": {
                    "invoice_id": [("L2.raw_invoices", "invoice_id")],
                    "amount_total": [("L2.raw_invoices", "subtotal"), ("L2.raw_invoices", "tax")],
                },
            },
            {
                "inputs": ["L3.canonical.invoice"],
                "output": "L4.invoice_mart",
                "column_lineage": {
                    "mrr": [("L3.canonical.invoice", "amount_total")],
                },
            },
        ],
    )

    from elt_pipeline._cli_main import main

    # JSON output route
    json_argv = [
        "lineage",
        "impact-analysis",
        "--column", "L4.invoice_mart.mrr",
        "--depth", "3",
        "--format", "json",
        "--root-path", str(tmp_path),
    ]
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc_json = main(json_argv)
    assert rc_json == 0
    parsed = json.loads(buf.getvalue())
    assert parsed["query"]["dataset"] == "L4.invoice_mart"
    assert parsed["query"]["column_name"] == "mrr"
    upstream_datasets = set(parsed["upstream"]["datasets"])
    assert "L2.raw_invoices" in upstream_datasets
    assert "L3.canonical.invoice" in upstream_datasets
    col_pairs = {(r["dataset"], r["column"]) for r in parsed["upstream"]["columns"]}
    assert ("L3.canonical.invoice", "amount_total") in col_pairs
    assert ("L2.raw_invoices", "subtotal") in col_pairs
    assert ("L2.raw_invoices", "tax") in col_pairs

    runtime_context._reset_for_tests()

    # Table output route — just verify no crash + produces display lines
    table_argv = [
        "lineage",
        "impact-analysis",
        "--column", "L4.invoice_mart.mrr",
        "--depth", "1",
        "--format", "table",
        "--root-path", str(tmp_path),
    ]
    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        rc_table = main(table_argv)
    assert rc_table == 0
    printed = buf2.getvalue()
    assert "Impact analysis for column:" in printed
    assert "L4.invoice_mart.mrr" in printed
    assert "UPSTREAM dependencies" in printed
    assert "DOWNSTREAM dependencies" in printed


def test_cli_lineage_invalid_args_returns_exit_2(tmp_path: Path) -> None:
    import io
    from contextlib import redirect_stderr

    from elt_pipeline._cli_main import main

    bad_column_argv = [
        "lineage",
        "impact-analysis",
        "--column", "no_dot_separator",
        "--root-path", str(tmp_path),
    ]
    err_buf = io.StringIO()
    with redirect_stderr(err_buf):
        rc = main(bad_column_argv)
    assert rc == 2
    parsed = json.loads(err_buf.getvalue())
    assert parsed["error"] == "lineage_impact_invalid_args"

    runtime_context._reset_for_tests()

    bad_depth_argv = [
        "lineage",
        "impact-analysis",
        "--column", "dataset.c",
        "--depth", "0",
        "--root-path", str(tmp_path),
    ]
    err_buf2 = io.StringIO()
    with redirect_stderr(err_buf2):
        rc2 = main(bad_depth_argv)
    assert rc2 == 2
    assert json.loads(err_buf2.getvalue())["error"] == "depth must be >= 1"
