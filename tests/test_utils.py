import json

from unittest.mock import mock_open
from unittest.mock import patch

import pytest
import yaml

from sqlecto.utils import load_config
from sqlecto.utils import read_file
from sqlecto.utils import validate_dialect


class TestReadFile:
    def test_read_file_success(self):
        """Test successful file reading"""
        test_content = "Hello, World!"
        mock = mock_open(read_data=test_content)

        with patch("builtins.open", mock):
            result = read_file("dummy.txt")

        assert result == test_content
        mock.assert_called_once_with("dummy.txt", encoding="utf-8")

    def test_read_file_file_not_found(self):
        """Test behavior when file is not found"""
        with pytest.raises(FileNotFoundError):
            read_file("nonexistent.txt")


class TestLoadConfig:
    @pytest.fixture
    def json_config(self):
        return {"name": "test", "value": 123}

    @pytest.fixture
    def yaml_config(self):
        return {"name": "test", "value": 123}

    def test_load_json_config(self, json_config):
        """Test loading JSON configuration"""
        mock = mock_open(read_data=json.dumps(json_config))

        with patch("builtins.open", mock):
            result = load_config("config.json")

        assert result == json_config
        mock.assert_called_once_with("config.json", encoding="utf-8")

    def test_load_yaml_config(self, yaml_config):
        """Test loading YAML configuration"""
        yaml_content = yaml.dump(yaml_config)
        mock = mock_open(read_data=yaml_content)

        with patch("builtins.open", mock):
            result = load_config("config.yaml")

        assert result == yaml_config
        mock.assert_called_once_with("config.yaml", encoding="utf-8")

    def test_load_yml_config(self, yaml_config):
        """Test loading .yml configuration"""
        yaml_content = yaml.dump(yaml_config)
        mock = mock_open(read_data=yaml_content)

        with patch("builtins.open", mock):
            result = load_config("config.yml")

        assert result == yaml_config
        mock.assert_called_once_with("config.yml", encoding="utf-8")

    def test_load_config_unsupported_extension(self):
        """Test loading config with unsupported file extension"""
        mock = mock_open(read_data="some content")

        with patch("builtins.open", mock), pytest.raises(ValueError) as exc_info:
            load_config("config.txt")

        assert "Unsupported file type" in str(exc_info.value)

    def test_load_config_file_not_found(self):
        """Test behavior when config file is not found"""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.json")

    def test_load_config_invalid_json(self):
        """Test behavior with invalid JSON content"""
        invalid_json = "{ invalid json }"
        mock = mock_open(read_data=invalid_json)

        with patch("builtins.open", mock), pytest.raises(json.JSONDecodeError):
            load_config("config.json")

    def test_load_config_invalid_yaml(self):
        """Test behavior with invalid YAML content"""
        invalid_yaml = ": invalid: yaml: content"
        mock = mock_open(read_data=invalid_yaml)

        with patch("builtins.open", mock), pytest.raises(yaml.YAMLError):
            load_config("config.yaml")


class TestValidateDialect:
    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", True),
            ("mysql", True),
            ("sqlite", True),
            ("invalid_dialect", False),
            ("", True),
            ("POSTGRES", True),  # Test case insensitivity
        ],
    )
    def test_validate_dialect(self, dialect, expected):
        """Test dialect validation with various inputs"""
        assert validate_dialect(dialect) == expected

    def test_validate_dialect_none_input(self):
        """Test dialect validation with None input"""
        with pytest.raises(AttributeError):
            validate_dialect(None)


def test_integration_json_workflow():
    """Integration test for reading and loading JSON config"""
    config_content = '{"name": "test", "value": 123}'
    mock = mock_open(read_data=config_content)

    with patch("builtins.open", mock):
        # Test both reading and loading
        raw_content = read_file("config.json")
        config = load_config("config.json")

    assert raw_content == config_content
    assert config == {"name": "test", "value": 123}


def test_integration_yaml_workflow():
    """Integration test for reading and loading YAML config"""
    config_content = "name: test\nvalue: 123"
    mock = mock_open(read_data=config_content)

    with patch("builtins.open", mock):
        # Test both reading and loading
        raw_content = read_file("config.yaml")
        config = load_config("config.yaml")

    assert raw_content == config_content
    assert config == {"name": "test", "value": 123}
