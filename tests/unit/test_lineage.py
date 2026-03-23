import datetime
from pathlib import Path
from unittest.mock import patch

from databricks.labs.lakebridge.intermediate.dag import DAG
from databricks.labs.lakebridge.lineage import _generate_dot_file_contents, lineage_generator
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine


def test_generate_dot_empty_dag():
    dag = DAG()
    result = _generate_dot_file_contents(dag)
    assert result == "flowchart TD\n"


def test_generate_dot_single_root_node():
    dag = DAG()
    dag.add_node("orders")
    result = _generate_dot_file_contents(dag)
    assert "Orders" in result
    assert "-->" not in result


def test_generate_dot_with_edges():
    dag = DAG()
    dag.add_edge("source", "target")
    result = _generate_dot_file_contents(dag)
    assert "flowchart TD" in result
    # target has parent "source", so should show Target --> Source
    assert "Target --> Source" in result
    # source is a root node (no parents), should appear standalone
    assert "    Source\n" in result


def test_generate_dot_multiple_parents():
    dag = DAG()
    dag.add_edge("parent1", "child")
    dag.add_edge("parent2", "child")
    result = _generate_dot_file_contents(dag)
    assert "Child --> Parent1" in result
    assert "Child --> Parent2" in result


def test_lineage_generator_creates_file(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    sql_file = input_dir / "test.sql"
    sql_file.write_text("CREATE TABLE t1 AS SELECT * FROM t2;")

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    engine = SqlglotEngine()
    lineage_generator(engine, "snowflake", str(input_dir), str(output_dir))

    date_str = datetime.datetime.now().strftime("%d%m%y")
    expected_file = output_dir / f"lineage_{date_str}.dot"
    assert expected_file.exists()
    content = expected_file.read_text()
    assert "flowchart TD" in content


def test_lineage_generator_single_file(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("CREATE TABLE out AS SELECT a, b FROM src;")

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    engine = SqlglotEngine()
    lineage_generator(engine, "snowflake", str(sql_file), str(output_dir))

    date_str = datetime.datetime.now().strftime("%d%m%y")
    expected_file = output_dir / f"lineage_{date_str}.dot"
    assert expected_file.exists()
