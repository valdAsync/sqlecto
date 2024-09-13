from __future__ import annotations

import json
import os

import sqlglot
import yaml


def read_file(file_path: str) -> str:
    """
    Reads the contents of a file and returns it as a string.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The contents of the file as a string.
    """
    with open(file_path, encoding="utf-8") as file:
        return file.read()


def load_config(config_path: str) -> dict:
    """
    Load the configuration from a JSON or YAML file.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        dict: The configuration dictionary.
    """
    _, ext = os.path.splitext(config_path)
    with open(config_path, encoding="utf-8") as file:
        if ext.lower() in [".yml", ".yaml"]:
            return yaml.safe_load(file)
        elif ext.lower() == ".json":
            return json.load(file)
        else:
            raise ValueError(
                "Unsupported file type. Only .json, .yml, and .yaml files are supported."
            )


def validate_dialect(dialect_name: str) -> bool:
    """
    Validate if the given dialect is supported by sqlglot.

    Args:
        dialect_name (str): The name of the SQL dialect.

    Returns:
        bool: True if the dialect is supported, False otherwise.
    """
    return dialect_name.lower() in sqlglot.dialects.DIALECTS
