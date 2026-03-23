"""Validation and consolidation of transpile configuration.

Extracted from cli.py to reduce module size and improve testability.
"""

import dataclasses
import logging
from collections.abc import Callable
from pathlib import Path

from databricks.labs.blueprint.installation import JsonValue
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.lakebridge.config import TranspileConfig, LSPConfigOptionV1
from databricks.labs.lakebridge.transpiler.lsp.lsp_engine import LSPEngine
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.labs.lakebridge.transpiler.transpile_engine import TranspileEngine

logger = logging.getLogger(__name__)


def raise_validation_exception(msg: str):
    raise ValueError(msg)


class TranspileConfigChecker:
    """Helper class for the 'transpile' command to check and consolidate the configuration.

    Configuration parameters can come from 3 sources:
     - Command-line arguments (e.g., --input-source, --output-folder, etc.)
     - The configuration file, stored in the user's workspace home directory.
     - User prompts.

    Conventions:
     - Command-line arguments take precedence over the configuration file.
     - Prompting is a last resort, only used when a required value has not been provided.
     - An invalid value results in a halt, not a fallback to another source.
    """

    _config: TranspileConfig
    _prompts: Prompts
    _source_dialect_override: str | None = None
    _transpiler_repository: TranspilerRepository

    def __init__(
        self,
        config: TranspileConfig | None,
        prompts: Prompts,
        transpiler_repository: TranspilerRepository,
    ) -> None:
        if config is None:
            logger.debug("No workspace transpile configuration, starting from defaults.")
            config = TranspileConfig()
        self._config = config
        self._prompts = prompts
        self._transpiler_repository = transpiler_repository
        self._source_dialect_override = None

    # --- Shared helpers for path-based config fields ---

    def _use_path_field(
        self, value: str | None, field_name: str, flag_name: str, validator: Callable[[str, str], None],
    ) -> None:
        if value is not None:
            logger.debug(f"Setting {field_name} to: {value!r}")
            validator(value, f"Invalid path for '--{flag_name}', does not exist: {value}")
            self._config = dataclasses.replace(self._config, **{field_name: value})

    def _check_path_field(
        self, field_name: str, prompt_question: str, validator: Callable[[str, str], None],
        config_error_template: str,
    ) -> None:
        current_value = getattr(self._config, field_name)
        if current_value is None:
            prompted_value = self._prompts.question(prompt_question).strip()
            if not prompted_value:
                raise_validation_exception(f"A value is required for {field_name}.")
            logger.debug(f"Setting {field_name} to: {prompted_value!r}")
            validator(prompted_value, f"Invalid {field_name}, path does not exist: {prompted_value}")
            self._config = dataclasses.replace(self._config, **{field_name: prompted_value})
        else:
            validator(current_value, config_error_template.format(current_value))

    # --- Individual field setters (from CLI args) ---

    @staticmethod
    def _validate_transpiler_config_path(transpiler_config_path: str, msg: str) -> None:
        if not Path(transpiler_config_path).exists():
            raise_validation_exception(msg)

    def use_transpiler_config_path(self, transpiler_config_path: str | None) -> None:
        self._use_path_field(
            transpiler_config_path, "transpiler_config_path", "transpiler-config-path",
            self._validate_transpiler_config_path,
        )

    def use_source_dialect(self, source_dialect: str | None) -> None:
        if source_dialect is not None:
            logger.debug(f"Pending source_dialect override: {source_dialect!r}")
            self._source_dialect_override = source_dialect

    @staticmethod
    def _validate_overrides_file(overrides_file: str, msg: str) -> None:
        if not Path(overrides_file).exists():
            raise_validation_exception(msg)

    def use_overrides_file(self, overrides_file: str | None) -> None:
        if overrides_file is not None:
            logger.debug(f"Setting overrides_file to: {overrides_file!r}")
            msg = f"Invalid path for '--overrides-file', does not exist: {overrides_file}"
            self._validate_overrides_file(overrides_file, msg)
            self._set_config_transpiler_option("overrides-file", overrides_file)

    def use_target_technology(self, target_technology: str | None) -> None:
        if target_technology is not None:
            logger.debug(f"Setting target_technology to: {target_technology!r}")
            self._set_config_transpiler_option("target-tech", target_technology)

    @staticmethod
    def _validate_input_source(input_source: str, msg: str) -> None:
        if not Path(input_source).exists():
            raise_validation_exception(msg)

    def use_input_source(self, input_source: str | None) -> None:
        self._use_path_field(input_source, "input_source", "input-source", self._validate_input_source)

    @staticmethod
    def _validate_output_folder(output_folder: str, msg: str) -> None:
        if not Path(output_folder).parent.exists():
            raise_validation_exception(msg)

    def use_output_folder(self, output_folder: str | None) -> None:
        self._use_path_field(output_folder, "output_folder", "output-folder", self._validate_output_folder)

    @staticmethod
    def _validate_error_file_path(error_file_path: str | None, msg: str) -> None:
        if error_file_path is not None and not Path(error_file_path).parent.exists():
            raise_validation_exception(msg)

    def use_error_file_path(self, error_file_path: str | None) -> None:
        self._use_path_field(error_file_path, "error_file_path", "error-file-path", self._validate_error_file_path)

    def use_skip_validation(self, skip_validation: str | None) -> None:
        if skip_validation is not None:
            skip_validation_lower = skip_validation.lower()
            if skip_validation_lower not in {"true", "false"}:
                msg = f"Invalid value for '--skip-validation': {skip_validation!r} must be 'true' or 'false'."
                raise_validation_exception(msg)
            new_skip_validation = skip_validation_lower == "true"
            logger.debug(f"Setting skip_validation to: {new_skip_validation!r}")
            self._config = dataclasses.replace(self._config, skip_validation=new_skip_validation)

    def use_catalog_name(self, catalog_name: str | None) -> None:
        if catalog_name:
            logger.debug(f"Setting catalog_name to: {catalog_name!r}")
            self._config = dataclasses.replace(self._config, catalog_name=catalog_name)

    def use_schema_name(self, schema_name: str | None) -> None:
        if schema_name:
            logger.debug(f"Setting schema_name to: {schema_name!r}")
            self._config = dataclasses.replace(self._config, schema_name=schema_name)

    # --- Internal helpers ---

    def _set_config_transpiler_option(self, flag: str, value: str) -> None:
        existing = self._config.transpiler_options
        transpiler_options = {flag: value} if existing is None else {**existing, flag: value}
        self._config = dataclasses.replace(self._config, transpiler_options=transpiler_options)

    def _configure_transpiler_config_path(self, source_dialect: str) -> TranspileEngine | None:
        compatible_transpilers = self._transpiler_repository.transpilers_with_dialect(source_dialect)
        match len(compatible_transpilers):
            case 0:
                return None
            case 1:
                transpiler_name = next(iter(compatible_transpilers))
                logger.debug(f"Using only transpiler available for dialect {source_dialect!r}: {transpiler_name!r}")
            case _:
                logger.debug(
                    f"Multiple transpilers available for dialect {source_dialect!r}: {compatible_transpilers!r}"
                )
                transpiler_name = self._prompts.choice("Select the transpiler:", list(compatible_transpilers))
        transpiler_config_path = self._transpiler_repository.transpiler_config_path(transpiler_name)
        logger.info(f"Lakebridge will use the {transpiler_name} transpiler.")
        self._config = dataclasses.replace(self._config, transpiler_config_path=str(transpiler_config_path))
        return LSPEngine.from_config_path(transpiler_config_path)

    def _configure_source_dialect(
        self, source_dialect: str, engine: TranspileEngine | None, msg_prefix: str
    ) -> TranspileEngine:
        if engine is None:
            engine = self._configure_transpiler_config_path(source_dialect)
            if engine is None:
                supported_dialects = ", ".join(self._transpiler_repository.all_dialects())
                raise_validation_exception(f"{msg_prefix}: {source_dialect!r} (supported dialects: {supported_dialects})")
            else:
                self._config = dataclasses.replace(self._config, source_dialect=source_dialect)
        else:
            if source_dialect not in engine.supported_dialects:
                supported = ", ".join(engine.supported_dialects)
                raise_validation_exception(
                    f"Invalid value for '--source-dialect': {source_dialect!r} must be one of: {supported}"
                )
            self._config = dataclasses.replace(self._config, source_dialect=source_dialect)
        return engine

    def _prompt_source_dialect(self) -> TranspileEngine:
        supported_dialects = self._transpiler_repository.all_dialects()
        match len(supported_dialects):
            case 0:
                raise_validation_exception(
                    "No transpilers are available, install using 'install-transpile' or use --transpiler-conf-path'."
                )
            case 1:
                source_dialect = next(iter(supported_dialects))
                logger.debug(f"Using only source dialect available: {source_dialect!r}")
            case _:
                logger.debug(f"Multiple source dialects available, choice required: {supported_dialects!r}")
                source_dialect = self._prompts.choice("Select the source dialect:", list(supported_dialects))
        engine = self._configure_transpiler_config_path(source_dialect)
        if engine is None:
            raise_validation_exception(
                "No transpiler engine available for a supported dialect; configuration is invalid."
            )
        self._config = dataclasses.replace(self._config, source_dialect=source_dialect)
        return engine

    def _check_lsp_engine(self) -> TranspileEngine:
        engine: TranspileEngine | None
        transpiler_config_path = self._config.transpiler_config_path
        if transpiler_config_path is not None:
            self._validate_transpiler_config_path(
                transpiler_config_path,
                f"Error: Invalid value for '--transpiler-config-path': '{transpiler_config_path}', file does not exist.",
            )
            engine = LSPEngine.from_config_path(Path(transpiler_config_path))
        else:
            engine = None

        source_dialect = self._source_dialect_override
        if source_dialect is not None:
            logger.debug(f"Setting source_dialect override: {source_dialect!r}")
            engine = self._configure_source_dialect(source_dialect, engine, "Invalid value for '--source-dialect'")
        else:
            source_dialect = self._config.source_dialect
            if source_dialect is not None:
                logger.debug(f"Using configured source_dialect: {source_dialect!r}")
                engine = self._configure_source_dialect(source_dialect, engine, "Invalid configured source dialect")
            else:
                logger.debug("No source_dialect available, prompting.")
                engine = self._prompt_source_dialect()
        return engine

    def _check_transpiler_options(self, engine: TranspileEngine) -> None:
        if not isinstance(engine, LSPEngine):
            return
        if self._config.source_dialect is None:
            raise ValueError("Source dialect must be set before checking transpiler options.")
        options_for_dialect = engine.options_for_dialect(self._config.source_dialect)
        transpiler_options = self._config.transpiler_options or {}
        checked_options = {
            option.flag: (
                transpiler_options[option.flag]
                if option.flag in transpiler_options
                else self._handle_missing_transpiler_option(option)
            )
            for option in options_for_dialect
        }
        self._config = dataclasses.replace(self._config, transpiler_options=checked_options)

    def _handle_missing_transpiler_option(self, option: LSPConfigOptionV1) -> JsonValue:
        if option.is_optional():
            return None
        return option.prompt_for_value(self._prompts)

    # --- Main entry point ---

    def check(self) -> tuple[TranspileConfig, TranspileEngine]:
        """Checks that all configuration parameters are present and valid."""
        logger.debug(f"Checking config: {self._config!r}")
        self._check_input_source()
        self._check_output_folder()
        self._check_error_file_path()
        engine = self._check_lsp_engine()
        self._check_transpiler_options(engine)
        config = self._config
        logger.debug(f"Validated config: {config!r}")
        return config, engine
