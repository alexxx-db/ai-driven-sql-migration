import datetime
import logging
from pathlib import Path

from databricks.labs.lakebridge.intermediate.dag import DAG
from databricks.labs.lakebridge.intermediate.root_tables import RootTableAnalyzer
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine

logger = logging.getLogger(__name__)


def _generate_dot_file_contents(dag: DAG) -> str:
    _lineage_str = "flowchart TD\n"
    for node_name, node in dag.nodes.items():
        if node.parents:
            for parent in node.parents:
                _lineage_str += f"    {node_name.capitalize()} --> {parent.capitalize()}\n"
        else:
            # Include nodes without parents to ensure they appear in the diagram
            _lineage_str += f"    {node_name.capitalize()}\n"
    return _lineage_str


def lineage_generator(engine: SqlglotEngine, source_dialect: str, input_source: str, output_folder: str):
    input_sql_path = Path(input_source)
    output_folder_path = Path(output_folder)

    logger.info(f"Processing for SQLs at this location: {input_sql_path}")
    root_table_analyzer = RootTableAnalyzer(engine, source_dialect, input_sql_path)
    generated_dag = root_table_analyzer.generate_lineage_dag()

    if not generated_dag.nodes:
        logger.warning(f"No tables found in SQL source: {input_sql_path}. Lineage file will be empty.")

    lineage_file_content = _generate_dot_file_contents(generated_dag)

    date_str = datetime.datetime.now().strftime("%d%m%y")
    output_filename = output_folder_path / f"lineage_{date_str}.dot"

    if output_filename.exists():
        logger.warning(f'The output file already exists and will be replaced: {output_filename}')

    logger.info(f"Attempting to write the lineage to {output_filename}")
    try:
        with output_filename.open('w', encoding='utf-8') as f:
            f.write(lineage_file_content)
    except OSError as e:
        raise OSError(f"Failed to write lineage file to {output_filename}: {e}") from e
    logger.info(f"Succeeded to write the lineage to {output_filename}")
