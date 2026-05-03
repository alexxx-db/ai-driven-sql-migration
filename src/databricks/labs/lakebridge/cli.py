import asyncio
import itertools
import json
import logging
import os
import re
import sys
import time
import webbrowser
from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import RootJsonValue, JsonValue
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.lakebridge.app import Lakebridge
from databricks.labs.lakebridge.assessments.configure_assessment import create_assessment_configurator
from databricks.labs.lakebridge.assessments import PROFILER_SOURCE_SYSTEM, PRODUCT_NAME
from databricks.labs.lakebridge.assessments.profiler import Profiler

from databricks.labs.lakebridge.config import TranspileConfig
from databricks.labs.lakebridge.contexts.application import ApplicationContext
from databricks.labs.lakebridge.transpile_config_checker import TranspileConfigChecker, raise_validation_exception
from databricks.labs.lakebridge.connections.credential_manager import cred_file, create_credential_manager
from databricks.labs.lakebridge.connections.database_manager import DatabaseManager
from databricks.labs.lakebridge.connections.env_getter import EnvGetter
from databricks.labs.lakebridge.connections.synapse_connection_helpers import validate_synapse_pools
from databricks.labs.lakebridge.helpers.recon_config_utils import ReconConfigPrompts
from databricks.labs.lakebridge.helpers.telemetry_utils import make_alphanum_or_semver
from databricks.labs.lakebridge.reconcile.runner import ReconcileRunner
from databricks.labs.lakebridge.lineage import lineage_generator
from databricks.labs.lakebridge.reconcile.recon_config import RECONCILE_OPERATION_NAME, AGG_RECONCILE_OPERATION_NAME
from databricks.labs.lakebridge.transpiler.describe import TranspilersDescription
from databricks.labs.lakebridge.transpiler.execute import transpile as do_transpile
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from databricks.labs.lakebridge.transpiler.switch_runner import SwitchRunner
from databricks.labs.lakebridge.transpiler.transpile_engine import TranspileEngine

from databricks.labs.lakebridge.transpiler.transpile_status import ErrorSeverity
from databricks.labs.switch.lsp import get_switch_dialects


lakebridge = Lakebridge(__file__)
logger = get_logger(__file__)


def _create_warehouse(ws: WorkspaceClient) -> str:

    dbsql = ws.warehouses.create_and_wait(
        name=f"lakebridge-warehouse-{time.time_ns()}",
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        cluster_size="Small",  # Adjust size as needed
        auto_stop_mins=30,  # Auto-stop after 30 minutes of inactivity
        enable_serverless_compute=True,
        max_num_clusters=1,
    )

    if dbsql.id is None:
        raise RuntimeError(f"Failed to create warehouse {dbsql.name}")

    logger.info(f"Created warehouse with id: {dbsql.id}")
    return dbsql.id


def _remove_warehouse(ws: WorkspaceClient, warehouse_id: str):
    ws.warehouses.delete(warehouse_id)
    logger.info(f"Removed warehouse post installation with id: {warehouse_id}")


@lakebridge.command
def transpile(  # pylint: disable=too-many-arguments
    *,
    w: WorkspaceClient,
    transpiler_config_path: str | None = None,
    source_dialect: str | None = None,
    overrides_file: str | None = None,
    target_technology: str | None = None,
    input_source: str | None = None,
    output_folder: str | None = None,
    error_file_path: str | None = None,
    skip_validation: str | None = None,
    catalog_name: str | None = None,
    schema_name: str | None = None,
    ctx: ApplicationContext | None = None,
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
):
    """Transpiles source dialect to databricks dialect"""
    if ctx is None:
        ctx = ApplicationContext(w)
    logger.debug(f"Preconfigured transpiler config: {ctx.transpile_config!r}")
    ctx.add_user_agent_extra("cmd", "execute-transpile")
    checker = TranspileConfigChecker(ctx.transpile_config, ctx.prompts, transpiler_repository)
    checker.use_transpiler_config_path(transpiler_config_path)
    checker.use_source_dialect(source_dialect)
    checker.use_overrides_file(overrides_file)
    checker.use_target_technology(target_technology)
    checker.use_input_source(input_source)
    checker.use_output_folder(output_folder)
    checker.use_error_file_path(error_file_path)
    checker.use_skip_validation(skip_validation)
    checker.use_catalog_name(catalog_name)
    checker.use_schema_name(schema_name)
    config, engine = checker.check()
    logger.debug(f"Final configuration for transpilation: {config!r}")
    _add_user_agent_extras_transpile(ctx, config, engine, transpiler_repository)
    result = asyncio.run(_transpile(ctx, config, engine))
    # DO NOT Modify this print statement, it is used by the CLI to display results in GO Table Template
    print(json.dumps(result))


def _add_user_agent_extras_transpile(
    ctx: ApplicationContext,
    config: TranspileConfig,
    engine: TranspileEngine,
    transpiler_repository: TranspilerRepository,
) -> None:
    if config.source_dialect is None:
        raise ValueError("Source dialect has not been set")
    ctx.add_user_agent_extra("transpiler_source_tech", make_alphanum_or_semver(config.source_dialect))

    plugin_name = engine.transpiler_name
    plugin_name = re.sub(r"\s+", "_", plugin_name)
    ctx.add_user_agent_extra("transpiler_plugin_name", plugin_name)

    config_path = config.transpiler_config_path_parsed
    if config_path is None:
        raise ValueError("Transpiler config path has not been set")
    transpiler_version = transpiler_repository.get_installed_version_given_config_path(config_path)
    if transpiler_version:
        ctx.add_user_agent_extra("transpiler_plugin_version", transpiler_version)
    else:
        logger.warning("Cannot determine transpiler plugin version.")

    # Send telemetry
    user = ctx.current_user
    logger.debug(f"User: {user}")


async def _transpile(ctx: ApplicationContext, config: TranspileConfig, engine: TranspileEngine) -> RootJsonValue:
    """Transpiles source dialect to databricks dialect"""
    _override_workspace_client_config(ctx, config.sdk_config)
    status, errors = await do_transpile(ctx.workspace_client, engine, config)

    logger.debug(f"Transpilation completed with status: {status}")

    for path, errors_by_path in itertools.groupby(errors, key=lambda x: x.path):
        errs = list(errors_by_path)
        errors_by_severity = {
            severity.name: len(list(errors)) for severity, errors in itertools.groupby(errs, key=lambda x: x.severity)
        }
        reports = []
        for severity in (ErrorSeverity.ERROR, ErrorSeverity.WARNING):
            if severity.name in errors_by_severity:
                count = errors_by_severity[severity.name]
                label = severity.name.lower() + ("s" if count > 1 else "")
                reports.append(f"{count} {label}")

        msg = ", ".join(reports) + " found"

        if ErrorSeverity.ERROR.name in errors_by_severity:
            logger.error(f"{path}: {msg}")
        elif ErrorSeverity.WARNING.name in errors_by_severity:
            logger.warning(f"{path}: {msg}")

    # Table Template in labs.yml requires the status to be list of dicts Do not change this
    return [status]


def _override_workspace_client_config(ctx: ApplicationContext, overrides: dict[str, str] | None) -> None:
    """
    Override the Workspace client's SDK config with the user provided SDK config.
    Users can provide the cluster_id and warehouse_id during the installation.
    This will update the default config object in-place.
    """
    if not overrides:
        return

    warehouse_id = overrides.get("warehouse_id")
    if warehouse_id:
        ctx.connect_config.warehouse_id = warehouse_id

    cluster_id = overrides.get("cluster_id")
    if cluster_id:
        ctx.connect_config.cluster_id = cluster_id


@lakebridge.command
def reconcile(
    *, w: WorkspaceClient, ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext
) -> None:
    """[EXPERIMENTAL] Reconciles source to Databricks datasets"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "execute-reconcile")
    user = ctx.current_user
    logger.debug(f"User: {user}")
    recon_runner = ReconcileRunner(
        ctx.workspace_client,
        ctx.install_state,
    )

    _, job_run_url = recon_runner.run(operation_name=RECONCILE_OPERATION_NAME)
    if ctx.prompts.confirm(f"Would you like to open the job run URL `{job_run_url}` in the browser?"):
        webbrowser.open(job_run_url)


@lakebridge.command
def aggregates_reconcile(
    *, w: WorkspaceClient, ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext
) -> None:
    """[EXPERIMENTAL] Reconciles Aggregated source to Databricks datasets"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "execute-aggregates-reconcile")
    user = ctx.current_user
    logger.debug(f"User: {user}")
    recon_runner = ReconcileRunner(
        ctx.workspace_client,
        ctx.install_state,
    )

    _, job_run_url = recon_runner.run(operation_name=AGG_RECONCILE_OPERATION_NAME)
    if ctx.prompts.confirm(f"Would you like to open the job run URL `{job_run_url}` in the browser?"):
        webbrowser.open(job_run_url)


@lakebridge.command
def generate_lineage(
    *,
    w: WorkspaceClient,
    source_dialect: str | None = None,
    input_source: str,
    output_folder: str,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """[Experimental] Generates a lineage of source SQL files or folder"""
    ctx = ctx_factory(w)
    logger.debug(f"User: {ctx.current_user}")
    if not os.path.exists(input_source):
        raise_validation_exception(f"Invalid path for '--input-source': Path '{input_source}' does not exist.")
    if not os.path.exists(output_folder):
        raise_validation_exception(f"Invalid path for '--output-folder': Path '{output_folder}' does not exist.")
    if source_dialect is None:
        raise_validation_exception("Value for '--source-dialect' must be provided.")
    engine = SqlglotEngine()
    supported_dialects = engine.supported_dialects
    if source_dialect not in supported_dialects:
        supported_dialects_description = ", ".join(supported_dialects)
        msg = f"Unsupported source dialect provided for '--source-dialect': '{source_dialect}' (supported: {supported_dialects_description})"
        raise_validation_exception(msg)

    lineage_generator(engine, source_dialect, input_source, output_folder)


@lakebridge.command
def configure_secrets(*, w: WorkspaceClient) -> None:
    """Setup reconciliation connection profile details as Secrets on Databricks Workspace"""
    recon_conf = ReconConfigPrompts(w)

    # Prompt for source
    source = recon_conf.prompt_source()

    logger.info(f"Setting up Scope, Secrets for `{source}` reconciliation")
    recon_conf.prompt_and_save_connection_details()


@lakebridge.command
def configure_database_profiler(
    w: WorkspaceClient,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """[Experimental] Installs and runs the Lakebridge Assessment package for database profiling"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "configure-profiler")
    prompts = ctx.prompts
    source_tech_raw = prompts.choice("Select the source technology", PROFILER_SOURCE_SYSTEM)
    if not source_tech_raw:
        raise_validation_exception("Source technology must be selected.")
    source_tech = source_tech_raw.lower()
    ctx.add_user_agent_extra("profiler_source_tech", make_alphanum_or_semver(source_tech))
    user = ctx.current_user
    logger.debug(f"User: {user}")

    # Create appropriate assessment configurator
    assessment = create_assessment_configurator(source_system=source_tech, product_name="lakebridge", prompts=prompts)
    assessment.run()


@lakebridge.command
def install_transpile(
    *,
    w: WorkspaceClient,
    artifact: str | None = None,
    interactive: str | None = None,
    include_llm_transpiler: bool = False,
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """Install or upgrade the Lakebridge transpilers."""
    from databricks.labs.lakebridge.install import installer  # pylint: disable=import-outside-toplevel

    is_interactive = interactive_mode(interactive)
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "install-transpile")
    if artifact:
        ctx.add_user_agent_extra("artifact-overload", Path(artifact).name)
    # Internal: use LAKEBRIDGE_CLUSTER_TYPE=CLASSIC env var to use classic job cluster
    switch_use_serverless = os.environ.get("LAKEBRIDGE_CLUSTER_TYPE", "").upper() != "CLASSIC"
    if include_llm_transpiler:
        ctx.add_user_agent_extra("include-llm-transpiler", "true")
        # Decision was made not to prompt when include_llm_transpiler is set, and we expect users to use llm-transpile
        # and pass all the arguments.
        logger.info("Including LLM transpiler as part of install, interactive mode disabled: will skip questionnaire.")
        is_interactive = False

    user = w.current_user
    logger.debug(f"User: {user}")
    transpile_installer = installer(
        w,
        transpiler_repository,
        is_interactive=is_interactive,
        include_llm=include_llm_transpiler,
        switch_use_serverless=switch_use_serverless,
    )
    transpile_installer.run(module="transpile", artifact=artifact)


def interactive_mode(interactive: str | None, *, default: str = "auto", input_stream: TextIO = sys.stdin) -> bool:
    """Convert the raw '--interactive' argument into a boolean."""
    if interactive is None:
        interactive = default
    match interactive.lower():
        case "true":
            return True
        case "false":
            return False
        # Convention is that if the input_stream is a TTY, user interaction is allowed.
        case "auto":
            return input_stream.isatty()

    msg = f"Invalid value for '--interactive': {interactive!r} must be 'true', 'false' or 'auto'."
    raise_validation_exception(msg)


@lakebridge.command
def describe_transpile(
    *,
    w: WorkspaceClient,
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """Describe the installed Lakebridge transpilers and available options."""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "describe-transpile")
    user = w.current_user.me()
    logger.debug(f"User: {user}")
    transpilers_description = TranspilersDescription(transpiler_repository)
    json_description = transpilers_description.as_json()
    json.dump(json_description, sys.stdout, indent=2)


@lakebridge.command(is_unauthenticated=False)
def configure_reconcile(
    *,
    w: WorkspaceClient,
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """Configure the Lakebridge reconciliation module"""
    from databricks.labs.lakebridge.install import installer  # pylint: disable=import-outside-toplevel

    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "configure-reconcile")
    user = w.current_user
    logger.debug(f"User: {user}")
    if not w.config.warehouse_id:
        dbsql_id = _create_warehouse(w)
        w.config.warehouse_id = dbsql_id
    logger.debug(f"Warehouse ID used for configuring reconcile: {w.config.warehouse_id}.")
    reconcile_installer = installer(w, transpiler_repository, is_interactive=True)
    reconcile_installer.run(module="reconcile")


@lakebridge.command
def analyze(
    *,
    w: WorkspaceClient,
    source_directory: str | None = None,
    report_file: str | None = None,
    source_tech: str | None = None,
    generate_json: bool = False,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
):
    """Run the Analyzer"""
    ctx = ctx_factory(w)
    try:
        result = ctx.analyzer.run_analyzer(source_directory, report_file, source_tech, generate_json)
        ctx.add_user_agent_extra("analyzer_source_tech", result.source_system)
    finally:
        exception_cls, _, _ = sys.exc_info()
        if exception_cls is not None:
            ctx.add_user_agent_extra("analyzer_error", exception_cls.__name__)

        ctx.add_user_agent_extra("cmd", "analyze")
        logger.debug(f"User: {ctx.current_user}")


def _validate_llm_transpile_args(
    input_source: str | None,
    output_ws_folder: str | None,
    source_dialect: str | None,
    prompts: Prompts,
) -> tuple[str, str, str]:

    _switch_dialects = get_switch_dialects()

    # Validate presence after attempting to source from config
    if not input_source:
        input_source = prompts.question("Enter input SQL path")
    if not output_ws_folder:
        output_ws_folder = prompts.question("Enter output workspace folder must start with /Workspace/")
    if not source_dialect:
        source_dialect = prompts.choice("Select the source dialect", sorted(_switch_dialects))

    # Validate input_source path exists (local path)
    if not Path(input_source).exists():
        raise_validation_exception(f"Invalid path for '--input-source': Path '{input_source}' does not exist.")

    # Validate output_ws_folder is a workspace path
    if not str(output_ws_folder).startswith("/Workspace/"):
        raise_validation_exception(
            f"Invalid value for '--output-ws-folder': workspace output path must start with /Workspace/. Got: {output_ws_folder!r}"
        )

    if source_dialect not in _switch_dialects:
        raise_validation_exception(
            f"Invalid value for '--source-dialect': {source_dialect!r} must be one of: {', '.join(sorted(_switch_dialects))}"
        )

    return input_source, output_ws_folder, source_dialect


@lakebridge.command
def llm_transpile(
    *,
    w: WorkspaceClient,
    accept_terms: bool = False,
    input_source: str | None = None,
    output_ws_folder: str | None = None,
    source_dialect: str | None = None,
    catalog_name: str | None = None,
    schema_name: str | None = None,
    volume: str | None = None,
    foundation_model: str | None = None,
    ctx: ApplicationContext | None = None,
) -> None:
    """Transpile source code to Databricks using LLM Transpiler (Switch)"""
    if ctx is None:
        ctx = ApplicationContext(w)
    ctx.add_user_agent_extra("cmd", "llm-transpile")
    user = ctx.current_user
    logger.debug(f"User: {user}")

    if not accept_terms:
        logger.warning(
            """Please read and accept these terms before proceeding:
    This feature leverages a Large Language Model (LLM) to analyse and convert
    your provided content, code and data. You consent to your content being
    transmitted to, processed by, and returned from the foundation models hosted
    by Databricks or external foundation models you have configured in your
    workspace. The outputs of the LLM are generated automatically without human
    review, and may contain inaccuracies or errors. You are responsible for
    reviewing and validating all outputs before relying on them for any critical
    or production use.

    By using this feature you accept these terms, re-run with '--accept-terms=true'.
                """
        )
        raise SystemExit("LLM transpiler terms not accepted, exiting.")

    prompts = ctx.prompts
    resource_configurator = ctx.resource_configurator

    # If CLI args are missing, try to read them from config.yml
    input_source, output_ws_folder, source_dialect = _validate_llm_transpile_args(
        input_source,
        output_ws_folder,
        source_dialect,
        prompts,
    )

    if catalog_name is None:
        catalog_name = resource_configurator.prompt_for_catalog_setup(default_catalog_name="lakebridge")

    if schema_name is None:
        schema_name = resource_configurator.prompt_for_schema_setup(catalog=catalog_name, default_schema_name="switch")

    if volume is None:
        volume = resource_configurator.prompt_for_volume_setup(
            catalog=catalog_name, schema=schema_name, default_volume_name="switch_volume"
        )

    resource_configurator.has_necessary_access(catalog_name, schema_name, volume)

    if foundation_model is None:
        foundation_model = resource_configurator.prompt_for_foundation_model_choice()

    job_list = ctx.install_state.jobs
    if "Switch" not in job_list:
        logger.debug(f"Missing Switch from installed state jobs: {job_list!r}")
        raise RuntimeError(
            "Switch Job not found. "
            "Please run 'databricks labs lakebridge install-transpile --include-llm-transpiler true' first."
        )
    job_id = int(job_list["Switch"])
    logger.debug(f"Switch job ID found: {job_id}")

    ctx.add_user_agent_extra("transpiler_source_dialect", source_dialect)
    job_runner = SwitchRunner(ctx.workspace_client)
    volume_input_path = job_runner.upload_to_volume(
        local_path=Path(input_source),
        catalog=catalog_name,
        schema=schema_name,
        volume=volume,
    )

    job_runner.run(
        volume_input_path=volume_input_path,
        output_ws_folder=output_ws_folder,
        source_tech=source_dialect,
        catalog=catalog_name,
        schema=schema_name,
        foundation_model=foundation_model,
        job_id=job_id,
    )


@lakebridge.command()
def execute_database_profiler(
    w: WorkspaceClient,
    source_tech: str | None = None,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """Execute the Profiler Extraction for the given source technology"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "execute-profiler")
    prompts = ctx.prompts
    if source_tech is None:
        source_tech = prompts.choice("Select the source technology", PROFILER_SOURCE_SYSTEM)
    source_tech = source_tech.lower()

    if source_tech not in PROFILER_SOURCE_SYSTEM:
        logger.error(f"Only the following source systems are supported: {PROFILER_SOURCE_SYSTEM}")
        raise_validation_exception(f"Invalid source technology {source_tech}")

    ctx.add_user_agent_extra("profiler_source_tech", make_alphanum_or_semver(source_tech))
    user = ctx.current_user
    logger.debug(f"User: {user}")
    # check if cred_file is present which has the connection details before running the profiler
    file = cred_file(PRODUCT_NAME)
    if not file.exists():
        raise_validation_exception(
            f"Connection details not found. Please run `databricks labs lakebridge configure-database-profiler` "
            f"to set up connection details for {source_tech}."
        )
    profiler = Profiler.create(source_tech)

    # TODO: Add extractor logic to ApplicationContext instead of creating inside the Profiler class
    profiler.profile()


@lakebridge.command()
def visualize_profiler_results(
    *,
    w: WorkspaceClient,
<<<<<<< HEAD
<<<<<<< HEAD
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
) -> None:
    """Deploys a profiler summary as a Databricks dashboard"""
    from databricks.labs.lakebridge.install import installer  # pylint: disable=cyclic-import, import-outside-toplevel

    ctx = ApplicationContext(w)
    ctx.add_user_agent_extra("cmd", "visualize-profiler-results")

    # Deploy the profiler dashboard and ingestion job
    if not w.config.warehouse_id:
        dbsql_id = _create_warehouse(w)
        w.config.warehouse_id = dbsql_id
    logger.debug(f"Warehouse ID used for running the profiler dashboard: {w.config.warehouse_id}.")
    profiler_dashboard_installer = installer(w, transpiler_repository, is_interactive=True)
    profiler_dashboard_installer.run(module="profiler_dashboard")
=======
=======
>>>>>>> d948aec4 (Improve Create Profiler Dashboard CLI Usage (#2319))
    extract_file: str,
    source_tech: str,
    volume_path: str,
    catalog_name: str,
    schema_name: str,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """Deploys a profiler summary as a Databricks dashboard"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "create-profiler-dashboard")
    ctx.dashboard_manager.upload_duckdb_to_uc_volume(extract_file, volume_path)
    ctx.dashboard_manager.create_profiler_summary_dashboard(source_tech, catalog_name, schema_name)
<<<<<<< HEAD
>>>>>>> 6c59ab85 (Refactoring)
=======
=======
    transpiler_repository: TranspilerRepository = TranspilerRepository.user_home(),
) -> None:
    """Deploys a profiler summary as a Databricks dashboard"""
    from databricks.labs.lakebridge.install import installer  # pylint: disable=cyclic-import, import-outside-toplevel

    ctx = ApplicationContext(w)
    ctx.add_user_agent_extra("cmd", "visualize-profiler-results")

    # Deploy the profiler dashboard and ingestion job
    if not w.config.warehouse_id:
        dbsql_id = _create_warehouse(w)
        w.config.warehouse_id = dbsql_id
    logger.debug(f"Warehouse ID used for running the profiler dashboard: {w.config.warehouse_id}.")
    profiler_dashboard_installer = installer(w, transpiler_repository, is_interactive=True)
    profiler_dashboard_installer.run(module="profiler_dashboard")
>>>>>>> c3d21feb (Improve Create Profiler Dashboard CLI Usage (#2319))
>>>>>>> d948aec4 (Improve Create Profiler Dashboard CLI Usage (#2319))


def _test_database_connection(source_tech: str, raw_config: dict) -> None:
    """Test connection to the source database with appropriate error handling."""
    # Handle synapse-specific validation using dedicated helper
    if source_tech == "synapse":
        validate_synapse_pools(raw_config)
        logger.info("Connection to the source system successful")
        return

    # For other source technologies, use DatabaseManager directly
    with DatabaseManager(source_tech, raw_config) as db_manager:
        response = db_manager.check_connection()
    logger.debug(f"Connection response: {response}")
    logger.info("Connection to the source system successful")


@lakebridge.command()
def test_profiler_connection(
    *,
    w: WorkspaceClient,
    source_tech: str | None = None,
    cred_file_path: str | None = None,
    ctx_factory: Callable[[WorkspaceClient], ApplicationContext] = ApplicationContext,
) -> None:
    """[Internal] Test the connection to the source database for profiling"""
    ctx = ctx_factory(w)
    ctx.add_user_agent_extra("cmd", "test-profiler-connection")
    prompts = ctx.prompts

    source_tech = (
        source_tech.lower()
        if source_tech
        else prompts.choice("Select the source technology", PROFILER_SOURCE_SYSTEM).lower()
    )

    if source_tech not in PROFILER_SOURCE_SYSTEM:
        logger.error(f"Only the following source systems are supported: {PROFILER_SOURCE_SYSTEM}")
        raise_validation_exception(f"Invalid source technology {source_tech}")

    ctx.add_user_agent_extra("profiler_source_tech", make_alphanum_or_semver(source_tech))
    logger.debug(f"User: {ctx.current_user}")

    # Use provided credential file path or fall back to default
    credential_file = Path(cred_file_path) if cred_file_path else cred_file(PRODUCT_NAME)

    # Check if credential file exists
    if not credential_file.exists():
        raise_validation_exception(
            f"Connection details not found. Please run `databricks labs lakebridge configure-database-profiler` "
            f"to set up connection details for {source_tech}."
        )

    logger.info(f"Testing connection for source technology: {source_tech}")

    cred_manager = create_credential_manager(PRODUCT_NAME, EnvGetter(), creds_path=credential_file)

    try:
        raw_config = cred_manager.get_credentials(source_tech)
    except KeyError as e:
        logger.error(f"Credential configuration error: {e}")
        logger.fatal(
            f"Invalid credentials for {source_tech}. Please run `databricks labs lakebridge configure-database-profiler`."
        )
        return

    try:
        _test_database_connection(source_tech, raw_config)
    except ConnectionError as e:
        logger.error(f"Failed to connect to the source system: {e}")
        error_msg = str(e).lower()
        if any(pattern in error_msg for pattern in ("im002", "odbc driver not found", "can't open lib")):
            logger.fatal("Missing ODBC driver, Please install pre-req. Exiting...")
        else:
            logger.fatal("Connection validation failed. Exiting...")
    except Exception as e:  # pylint: disable=broad-exception-caught
        # Catch all exceptions to provide user-friendly error messages for CLI
        logger.error(f"Unexpected error during connection test: {e}")
        logger.fatal("Connection test failed. Exiting...")


if __name__ == "__main__":
    lakebridge()
