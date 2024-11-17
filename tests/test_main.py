import json

from unittest.mock import patch

import pytest

from typer.testing import CliRunner

from sqlecto.main import app
from sqlecto.main import parse_table_mapping


runner = CliRunner()


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory with some test files."""
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM table1")
    py_file = tmp_path / "test.py"
    py_file.write_text('query = "SELECT * FROM table1"')
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
        {
            "source_dialect": "postgres",
            "target_dialect": "mysql",
            "table_mappings": [
                {"src_table": "old_table", "dst_table": "new_table"}
            ]
        }
        """
    )
    mappings_file = tmp_path / "mappings.json"
    mappings_file.write_text(
        """
        {
            "table_mappings": [
                {"src_table": "table1", "dst_table": "table2"}
            ]
        }
        """
    )
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()  # Ensure empty directory exists
    return tmp_path


def test_parse_table_mapping():
    input_mappings = ["table1:table2", "old_table:new_table"]
    expected = [
        {"src_table": "table1", "dst_table": "table2"},
        {"src_table": "old_table", "dst_table": "new_table"},
    ]
    assert parse_table_mapping(input_mappings) == expected


@patch("sqlecto.main.process_file")
def test_basic_file_processing(mock_process, temp_dir):
    mock_process.return_value = str(temp_dir / "output.sql")
    result = runner.invoke(
        app,
        [
            "--source-files",
            str(temp_dir / "test.sql"),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code == 0
    mock_process.assert_called_once()


@patch("sqlecto.main.process_file")
def test_directory_processing(mock_process, temp_dir):
    mock_process.return_value = str(temp_dir / "output.sql")
    result = runner.invoke(
        app,
        [
            "--source-dir",
            str(temp_dir),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code == 0
    assert mock_process.call_count == 2


def test_missing_required_params(temp_dir):
    result = runner.invoke(app, ["--source-files", str(temp_dir / "test.sql")])
    assert result.exit_code != 0
    assert "Missing required parameter: --source-dialect" in result.stdout


def test_invalid_dialect(temp_dir):
    result = runner.invoke(
        app,
        [
            "--source-files",
            str(temp_dir / "test.sql"),
            "--source-dialect",
            "invalid_dialect",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code != 0
    assert "Unsupported source dialect" in result.stdout


@patch("sqlecto.main.load_config")
@patch("sqlecto.main.process_file")
def test_config_file_processing(mock_process, mock_load_config, temp_dir):
    # Mock the return value of load_config to prevent loading extra files or directories
    mock_load_config.return_value = {
        "source_dialect": "postgres",
        "target_dialect": "mysql",
        "table_mappings": [{"src_table": "old_table", "dst_table": "new_table"}],
    }

    # Run the command with controlled config
    result = runner.invoke(
        app,
        [
            "--config-file",
            str(temp_dir / "config.json"),
            "--source-files",
            str(temp_dir / "test.sql"),
        ],
    )
    assert result.exit_code == 0
    mock_process.assert_called_once()


@patch("sqlecto.main.process_file")
def test_table_mappings(mock_process, temp_dir):
    mock_process.return_value = str(temp_dir / "output.sql")
    result = runner.invoke(
        app,
        [
            "--source-files",
            str(temp_dir / "test.sql"),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
            "--table-mappings",
            "table1:table2",
            "--table-mappings-file",
            str(temp_dir / "mappings.json"),
        ],
    )
    assert result.exit_code == 0
    mock_process.assert_called_once()


def test_no_files_found(temp_dir):
    empty_dir = temp_dir / "empty_dir"
    result = runner.invoke(
        app,
        [
            "--source-dir",
            str(empty_dir),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code != 0
    assert "No files found to process." in result.stdout


@patch("sqlecto.main.process_file")
def test_process_file_error_handling(mock_process, temp_dir, caplog):
    # Cause `process_file` to raise an exception
    mock_process.side_effect = Exception("Processing error")

    # Invoke the command
    result = runner.invoke(
        app,
        [
            "--source-files",
            str(temp_dir / "test.sql"),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )

    # Check that the exit code is 0 (indicating CLI handled the error gracefully)
    assert result.exit_code == 0

    # Use caplog.records to check for the expected log message
    assert any("Error processing file" in record.message for record in caplog.records)
    assert any("Processing error" in record.message for record in caplog.records)


@pytest.mark.parametrize(
    "source_dialect,target_dialect",
    [
        ("postgres", "mysql"),
        ("mysql", "postgres"),
        ("oracle", "postgres"),
        ("postgres", "oracle"),
    ],
)
@patch("sqlecto.main.process_file")
def test_different_dialect_combinations(
    mock_process, temp_dir, source_dialect, target_dialect
):
    mock_process.return_value = str(temp_dir / "output.sql")
    result = runner.invoke(
        app,
        [
            "--source-files",
            str(temp_dir / "test.sql"),
            "--source-dialect",
            source_dialect,
            "--target-dialect",
            target_dialect,
        ],
    )
    assert result.exit_code == 0
    mock_process.assert_called_once()


@pytest.fixture
def sql_file(tmp_path):
    """Create a single SQL test file."""
    file = tmp_path / "test.sql"
    file.write_text("SELECT * FROM table1")
    return file


@pytest.fixture
def py_file(tmp_path):
    """Create a single Python test file."""
    file = tmp_path / "test.py"
    file.write_text('query = "SELECT * FROM table1"')
    return file


@pytest.fixture
def config_file(tmp_path):
    """Create a test config file."""
    file = tmp_path / "config.json"
    config = {
        "source_dialect": "postgres",
        "target_dialect": "mysql",
        "table_mappings": [{"src_table": "old_table", "dst_table": "new_table"}],
    }
    file.write_text(json.dumps(config, indent=2))
    return file


@patch("sqlecto.main.process_file")
def test_directory_processing_improved(mock_process, tmp_path, sql_file, py_file):
    mock_process.return_value = str(tmp_path / "output.sql")
    result = runner.invoke(
        app,
        [
            "--source-dir",
            str(tmp_path),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code == 0
    assert mock_process.call_count == 2
    # Verify correct files were processed
    calls = mock_process.call_args_list
    processed_files = [call.args[0] for call in calls]
    assert str(sql_file) in processed_files
    assert str(py_file) in processed_files


def test_invalid_config_file(tmp_path):
    """Test behavior with invalid config file."""
    invalid_config = tmp_path / "invalid.json"
    invalid_config.write_text("{invalid json}")
    result = runner.invoke(
        app,
        [
            "--config-file",
            str(invalid_config),
            "--source-dialect",
            "postgres",
            "--target-dialect",
            "mysql",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid config file" in result.stdout
