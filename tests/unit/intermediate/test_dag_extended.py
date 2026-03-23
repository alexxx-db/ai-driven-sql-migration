import pytest

from databricks.labs.lakebridge.intermediate.dag import DAG, Node


def test_node_repr():
    node = Node("TestNode")
    assert node.name == "testnode"
    assert repr(node) == "Node(testnode, [])"


def test_node_add_parent_and_child():
    node = Node("center")
    node.add_parent("parent1")
    node.add_child("child1")
    assert node.parents == ["parent1"]
    assert node.children == ["child1"]


def test_dag_add_node_none_skipped():
    dag = DAG()
    dag.add_node("none")
    assert "none" not in dag.nodes


def test_dag_add_node_case_insensitive():
    dag = DAG()
    dag.add_node("MyTable")
    assert "mytable" in dag.nodes
    assert "MyTable" not in dag.nodes


def test_dag_add_duplicate_node():
    dag = DAG()
    dag.add_node("table1")
    dag.add_node("table1")
    assert len([k for k in dag.nodes if k == "table1"]) == 1


def test_dag_add_edge_with_none_child():
    dag = DAG()
    dag.add_edge("parent", None)
    assert "parent" in dag.nodes


def test_dag_identify_parents_nonexistent():
    dag = DAG()
    assert dag.identify_immediate_parents("nonexistent") == []


def test_dag_identify_children_nonexistent():
    dag = DAG()
    assert dag.identify_immediate_children("nonexistent") == []


def test_dag_is_root_node():
    dag = DAG()
    dag.add_edge("root", "child")
    assert dag._is_root_node("root") is True
    assert dag._is_root_node("child") is False


def test_dag_walk_bfs_level_0():
    dag = DAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    root = dag.nodes["a"]
    result = dag.walk_bfs(root, 0)
    assert result == {"a"}


def test_dag_walk_bfs_level_1():
    dag = DAG()
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    root = dag.nodes["a"]
    result = dag.walk_bfs(root, 1)
    assert result == {"b", "c"}


def test_dag_walk_bfs_level_2():
    dag = DAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("b", "d")
    root = dag.nodes["a"]
    result = dag.walk_bfs(root, 2)
    assert result == {"c", "d"}


def test_dag_identify_root_tables_multiple_roots():
    dag = DAG()
    dag.add_edge("root1", "mid")
    dag.add_edge("root2", "mid")
    dag.add_edge("mid", "leaf")
    roots = dag.identify_root_tables(0)
    assert roots == {"root1", "root2"}


def test_dag_repr():
    dag = DAG()
    dag.add_node("t1")
    result = repr(dag)
    assert "t1" in result


def test_dag_complex_graph():
    """Test a diamond-shaped dependency graph: a->b, a->c, b->d, c->d."""
    dag = DAG()
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")

    assert dag.identify_root_tables(0) == {"a"}
    assert dag.identify_root_tables(1) == {"b", "c"}
    assert dag.identify_root_tables(2) == {"d"}
    parents_of_d = set(dag.identify_immediate_parents("d"))
    assert parents_of_d == {"b", "c"}
