from pathlib import Path

import pytest
from databricks.labs.bladespector.analyzer import Analyzer
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.lakebridge.analyzer.lakebridge_analyzer import (
    AnalyzerPrompts,
    AnalyzerResult,
    AnalyzerRunner,
    LakebridgeAnalyzer,
)


def _mock_analyze(
    _directory: Path, result: Path, _platform: str, _is_debug: bool = False, _json_result: Path | None = None
) -> None:
    result.touch()


def test_analyzer_runner_invalid_source_dir(tmp_path: Path):
    runner = AnalyzerRunner(runnable=_mock_analyze, is_debug=False)
    # Path that doesn't exist and isn't writable
    bad_path = Path("/nonexistent/path/that/does/not/exist")
    with pytest.raises(ValueError, match="Invalid source directory"):
        runner.run(bad_path, tmp_path / "report.xlsx", "Synapse")


def test_analyzer_runner_resolves_relative_paths(tmp_path: Path):
    runner = AnalyzerRunner(runnable=_mock_analyze, is_debug=True)
    # Use relative paths that resolve to tmp_path
    source = tmp_path / "src"
    source.mkdir()
    report = tmp_path / "report.xlsx"

    result = runner.run(source, report, "Synapse")
    assert result.source_directory.is_absolute()
    assert result.report_path.is_absolute()


def test_analyzer_runner_non_xlsx_extension(tmp_path: Path):
    """Test that non-.xlsx extension triggers the staged report workaround."""
    runner = AnalyzerRunner(runnable=_mock_analyze, is_debug=False)
    source = tmp_path / "src"
    source.mkdir()
    report = tmp_path / "report-no-ext"

    result = runner.run(source, report, "Synapse")
    assert result.report_path == report


def test_analyzer_runner_json_output(tmp_path: Path):
    """Test generate_json flag passes through correctly."""
    called_with_json = []

    def _capture_analyze(directory, result, platform, is_debug=False, json_result=None):
        called_with_json.append(json_result)
        result.touch()

    runner = AnalyzerRunner(runnable=_capture_analyze, is_debug=False)
    source = tmp_path / "src"
    source.mkdir()
    report = tmp_path / "report.xlsx"

    runner.run(source, report, "Synapse", generate_json=True)
    assert called_with_json[0] is not None
    assert called_with_json[0].suffix == ".json"


def test_analyzer_prompts_invalid_platform(tmp_path: Path):
    """Test that an invalid platform triggers the choice prompt."""
    first_tech = next(iter(sorted(Analyzer.supported_source_technologies(), key=str.casefold)))
    mock_prompts = MockPrompts({"Select the source technology": "0"})
    prompts = AnalyzerPrompts(mock_prompts)
    result = prompts.get_source_system("InvalidPlatform")
    assert result == first_tech


def test_analyzer_prompts_valid_platform():
    """Test that a valid platform passes through without prompting."""
    first_tech = next(iter(sorted(Analyzer.supported_source_technologies(), key=str.casefold)))
    mock_prompts = MockPrompts({})
    prompts = AnalyzerPrompts(mock_prompts)
    result = prompts.get_source_system(first_tech)
    assert result == first_tech


def test_analyzer_prompts_none_platform():
    """Test that None platform triggers the choice prompt."""
    first_tech = next(iter(sorted(Analyzer.supported_source_technologies(), key=str.casefold)))
    mock_prompts = MockPrompts({"Select the source technology": "0"})
    prompts = AnalyzerPrompts(mock_prompts)
    result = prompts.get_source_system(None)
    assert result == first_tech


def test_analyzer_result_dataclass():
    result = AnalyzerResult(
        source_directory=Path("/some/dir"),
        report_path=Path("/some/report.xlsx"),
        source_system="Synapse",
    )
    assert result.source_directory == Path("/some/dir")
    assert result.source_system == "Synapse"


def test_analyzer_runner_create():
    runner = AnalyzerRunner.create(is_debug=True)
    assert runner._is_debug is True
    assert runner._runnable == Analyzer.analyze
