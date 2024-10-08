from __future__ import annotations

import logging
import os

import click

from converter import process_file
from utils import load_config
from utils import validate_dialect


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--source-files",
    "source_files",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the SQL or Python file(s) to process.",
)
@click.option(
    "--source-dir",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the directory containing SQL or Python files to process.",
)
@click.option(
    "--source-dialect",
    help="Source SQL dialect.",
)
@click.option(
    "--target-dialect",
    help="Target SQL dialect.",
)
@click.option(
    "--table-mappings",
    multiple=True,
    help="Table name mappings in the format: source_table:target_table.",
)
@click.option(
    "--table-mappings-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a file containing table mappings (JSON or YAML).",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the configuration file (JSON or YAML).",
)
@click.option(
    "--output-dir",
    default="transpiled_queries",
    type=click.Path(file_okay=False),
    show_default=True,
    help="Directory to save the transpiled queries.",
)
def main(
    source_files,
    source_dir,
    source_dialect,
    target_dialect,
    table_mappings,
    table_mappings_file,
    config_file,
    output_dir,
):
    if config_file:
        config = load_config(config_file)

        source_files = config.get("source_files", [])
        source_files = (
            source_files if isinstance(source_files, list) else [source_files]
        )

        source_dir = source_dir or config.get("source_dir")
        source_dialect = source_dialect or config.get("source_dialect")
        target_dialect = target_dialect or config.get("target_dialect")
        output_dir = output_dir or config.get("output_dir")
        if "table_mappings" in config:
            table_mappings = config["table_mappings"]
    else:
        config = {}

    if table_mappings and isinstance(table_mappings[0], str):
        table_mappings = [
            {"src_table": mapping.split(":")[0], "dst_table": mapping.split(":")[1]}
            for mapping in table_mappings
        ]

    if not source_files and not source_dir:
        source_dir = "."
        logger.info(
            "No source file or directory specified. Defaulting to current directory."
        )

    if not source_dialect:
        raise click.UsageError("Missing required parameter: --source-dialect")

    if not target_dialect:
        raise click.UsageError("Missing required parameter: --target-dialect")

    if not validate_dialect(source_dialect):
        raise ValueError(f"Unsupported source dialect: {source_dialect}")

    if not validate_dialect(target_dialect):
        raise ValueError(f"Unsupported target dialect: {target_dialect}")

    files_to_process = []

    if source_files:
        files_to_process.extend(source_files)

    if source_dir:
        for root, _, files in os.walk(source_dir):
            for file in files:
                if file.endswith((".py", ".sql")):
                    files_to_process.append(os.path.join(root, file))

    if not files_to_process:
        raise click.UsageError("No files found to process.")

    for file_path in files_to_process:
        logger.info(f"Processing file: {file_path}")
        try:
            process_file(file_path, source_dialect, target_dialect, table_mappings)
        except Exception as e:
            logger.error(f"Error processing file: {file_path}\nError: {e}")


if __name__ == "__main__":
    main()
