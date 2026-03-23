import logging
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.lakebridge.analyzer.lakebridge_analyzer import LakebridgeAnalyzer, AnalyzerPrompts, AnalyzerRunner
from databricks.labs.lakebridge.assessments.dashboards.dashboard_manager import DashboardManager

from databricks.labs.lakebridge.config import TranspileConfig, ReconcileConfig, LakebridgeConfiguration
from databricks.labs.lakebridge.deployment.configurator import ResourceConfigurator
from databricks.labs.lakebridge.deployment.dashboard import DashboardDeployment
from databricks.labs.lakebridge.deployment.installation import WorkspaceInstallation
from databricks.labs.lakebridge.deployment.recon import TableDeployment, JobDeployment, ReconDeployment
from databricks.labs.lakebridge.deployment.switch import SwitchDeployment
from databricks.labs.lakebridge.helpers.metastore import CatalogOperations

logger = logging.getLogger(__name__)


class _CoreContext:
    """Core workspace properties: client, user, product info, installation, config."""

    _ws: WorkspaceClient

    @property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @cached_property
    def current_user(self) -> User:
        return self.workspace_client.current_user.me()

    @cached_property
    def product_info(self) -> ProductInfo:
        return ProductInfo.from_class(LakebridgeConfiguration)

    @cached_property
    def installation(self) -> Installation:
        return Installation.assume_user_home(self.workspace_client, self.product_info.product_name())

    @cached_property
    def install_state(self) -> InstallState:
        return InstallState.from_installation(self.installation)

    @property
    def connect_config(self) -> Config:
        return self.workspace_client.config

    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.connect_config.warehouse_id)

    @cached_property
    def catalog_operations(self) -> CatalogOperations:
        return CatalogOperations(self.workspace_client)

    @cached_property
    def upgrades(self):
        return Upgrades(self.product_info, self.installation)


class _TranspileContext:
    """Transpile-specific properties."""

    # These are accessed via self.X which resolves through MRO to _CoreContext
    installation: Installation
    product_info: ProductInfo
    prompts: Prompts

    @cached_property
    def transpile_config(self) -> TranspileConfig | None:
        try:
            return self.installation.load(TranspileConfig)  # type: ignore[attr-defined]
        except NotFound as err:
            logger.debug(f"Couldn't find existing `transpile` installation: {err}")
            return None

    @cached_property
    def analyzer(self):
        is_debug = logger.getEffectiveLevel() == logging.DEBUG
        prompts = AnalyzerPrompts(self.prompts)  # type: ignore[attr-defined]
        runner = AnalyzerRunner.create(is_debug)
        return LakebridgeAnalyzer(prompts, runner)


class _ReconcileContext:
    """Reconcile-specific properties."""

    @cached_property
    def recon_config(self) -> ReconcileConfig | None:
        try:
            return self.installation.load(ReconcileConfig)  # type: ignore[attr-defined]
        except NotFound as err:
            logger.debug(f"Couldn't find existing `reconcile` installation: {err}")
            return None


class _DeploymentContext:
    """Deployment-specific properties (jobs, dashboards, tables, switch)."""

    @cached_property
    def resource_configurator(self) -> ResourceConfigurator:
        return ResourceConfigurator(self.workspace_client, self.prompts, self.catalog_operations)  # type: ignore[attr-defined]

    @cached_property
    def table_deployment(self) -> TableDeployment:
        return TableDeployment(self.sql_backend)  # type: ignore[attr-defined]

    @cached_property
    def job_deployment(self) -> JobDeployment:
        return JobDeployment(self.workspace_client, self.installation, self.install_state, self.product_info)  # type: ignore[attr-defined]

    @cached_property
    def dashboard_deployment(self) -> DashboardDeployment:
        return DashboardDeployment(self.workspace_client, self.installation, self.install_state)  # type: ignore[attr-defined]

    @cached_property
    def dashboard_manager(self) -> DashboardManager:
        is_debug = logger.getEffectiveLevel() == logging.DEBUG
        return DashboardManager(self.workspace_client, self.installation, self.install_state, is_debug)  # type: ignore[attr-defined]

    @cached_property
    def recon_deployment(self) -> ReconDeployment:
        return ReconDeployment(
            self.workspace_client,  # type: ignore[attr-defined]
            self.installation,  # type: ignore[attr-defined]
            self.install_state,  # type: ignore[attr-defined]
            self.product_info,  # type: ignore[attr-defined]
            self.table_deployment,
            self.job_deployment,
            self.dashboard_deployment,
        )

    @cached_property
    def switch_deployment(self) -> SwitchDeployment:
        return SwitchDeployment(
            self.workspace_client,  # type: ignore[attr-defined]
            self.installation,  # type: ignore[attr-defined]
            self.install_state,  # type: ignore[attr-defined]
        )

    @cached_property
    def workspace_installation(self) -> WorkspaceInstallation:
        return WorkspaceInstallation(
            self.workspace_client,  # type: ignore[attr-defined]
            self.installation,  # type: ignore[attr-defined]
            self.recon_deployment,
            self.switch_deployment,
            self.product_info,  # type: ignore[attr-defined]
            self.upgrades,  # type: ignore[attr-defined]
        )


class ApplicationContext(_CoreContext, _TranspileContext, _ReconcileContext, _DeploymentContext):
    """Main application context composing all sub-contexts.

    Properties are organized into focused groups:
    - _CoreContext: workspace client, user, product info, installation, SQL backend
    - _TranspileContext: transpile config, analyzer
    - _ReconcileContext: reconcile config
    - _DeploymentContext: jobs, dashboards, tables, switch, workspace installation
    """

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def replace(self, **kwargs):
        """Replace cached properties for unit testing purposes."""
        for key, value in kwargs.items():
            self.__dict__[key] = value
        return self

    @cached_property
    def remorph_config(self) -> LakebridgeConfiguration:
        return LakebridgeConfiguration(transpile=self.transpile_config, reconcile=self.recon_config)

    def add_user_agent_extra(self, key: str, value: str) -> None:
        new_config = self._ws.config.copy().with_user_agent_extra(key, value)
        logger.debug(f"Added User-Agent extra {key}={value}")

        # Recreate the WorkspaceClient from the same type to preserve type information
        self._ws = type(self._ws)(config=new_config)
