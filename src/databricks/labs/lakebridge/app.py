import logging

from databricks.labs.blueprint.cli import App
from databricks.sdk import WorkspaceClient


class Lakebridge(App):
    """Subclass to allow controlled access to protected methods."""

    def create_workspace_client(self) -> WorkspaceClient:
        """Create a workspace client, with the appropriate product and version information.

        This is intended only for use by the install/uninstall hooks.
        """
        self._patch_databricks_host()
        return self._workspace_client()

    def _log_level(self, raw: str) -> int:
        """Convert the log-level provided by the Databricks CLI into a logging level supported by Python."""
        log_level = super()._log_level(raw)
        # Due to an issue in the handoff of the intended logging level from the Databricks CLI to our
        # application, we can't currently distinguish between --log-level=WARN and nothing at all, where we
        # prefer (and the application logging expects) INFO.
        #
        # Rather than default to only have WARNING logs show, it's preferable to default to INFO and have
        # --log-level=WARN not work for now.
        #
        # See: https://github.com/databrickslabs/lakebridge/issues/2167
        # TODO: Remove this once #2167 has been resolved.
        if log_level == logging.WARNING:
            log_level = logging.INFO
        return log_level
