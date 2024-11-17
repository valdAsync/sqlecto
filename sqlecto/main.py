import json
import logging
import os

from pathlib import Path
from typing import Annotated
from typing import Optional

import typer

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import track

from sqlecto.converter import process_file
from sqlecto.utils import load_config
from sqlecto.utils import validate_dialect


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="SQL Dialect Converter CLI")

console = Console()


def parse_table_mapping(value: list[str]) -> list[dict]:
    """
    Parses a list of table mapping strings into a list of dictionaries.

    Each string in the input list should be in the format "src_table:dst_table".
    The function splits each string by the colon character and creates a dictionary
    with keys "src_table" and "dst_table" corresponding to the source and destination
    table names, respectively.

    Args:
        value (list[str]): A list of table mapping strings.

    Returns:
        list[dict]: A list of dictionaries with keys "src_table" and "dst_table".
    """
    return [
        {"src_table": mapping.split(":")[0], "dst_table": mapping.split(":")[1]}
        for mapping in value
    ]


@app.command()
def main(
    source_files: Annotated[
        Optional[list[Path]],
        typer.Option(
            "--source-files",
            help="Path to the SQL or Python file(s) to process",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    source_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the directory containing SQL or Python files to process",
            exists=True,
            file_okay=False,
        ),
    ] = Path("."),
    source_dialect: Annotated[
        Optional[str],
        typer.Option(
            "--source-dialect",
            help="Source SQL dialect",
        ),
    ] = None,
    target_dialect: Annotated[
        Optional[str],
        typer.Option(
            "--target-dialect",
            help="Target SQL dialect",
        ),
    ] = None,
    table_mappings: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Table name mappings in the format: source_table:target_table"
        ),
    ] = None,
    table_mappings_file: Annotated[
        Optional[Path],
        typer.Option(
            help="Path to a file containing table mappings (JSON or YAML)",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    config_file: Annotated[
        Optional[Path],
        typer.Option(
            help="Path to the configuration file (JSON or YAML)",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option(help="Directory to save the transpiled queries")
    ] = Path("transpiled_queries"),
) -> None:
    if config_file:
        try:
            config = load_config(config_file)
            if source_files is None:
                source_files = [Path(f) for f in config.get("source_files", [])]
        except (json.JSONDecodeError, Exception) as e:
            raise typer.BadParameter(f"Invalid config file: {e}")

        if source_dir == Path("."):
            source_dir = Path(config.get("source_dir", source_dir))
        source_dialect = source_dialect or config.get("source_dialect")
        target_dialect = target_dialect or config.get("target_dialect")
        output_dir = Path(config.get("output_dir", output_dir))
        config_table_mappings = config.get("table_mappings", [])
    else:
        config_table_mappings = []

    if not source_dialect:
        raise typer.BadParameter("Missing required parameter: --source-dialect")

    if not target_dialect:
        raise typer.BadParameter("Missing required parameter: --target-dialect")

    if not validate_dialect(source_dialect):
        raise typer.BadParameter(f"Unsupported source dialect: {source_dialect}")

    if not validate_dialect(target_dialect):
        raise typer.BadParameter(f"Unsupported target dialect: {target_dialect}")

    final_table_mappings = []

    if table_mappings_file:
        table_mappings_file_config = load_config(table_mappings_file)
        if "table_mappings" in table_mappings_file_config:
            final_table_mappings.extend(table_mappings_file_config["table_mappings"])
        else:
            logger.warning(f"No 'table_mappings' key found in {table_mappings_file}.")

    if table_mappings:
        final_table_mappings.extend(parse_table_mapping(table_mappings))

    if config_table_mappings:
        final_table_mappings.extend(config_table_mappings)

    files_to_process = []

    if source_files:
        files_to_process.extend(source_files)
    elif source_dir != Path(".") or not source_files:
        for root, _, files in os.walk(source_dir):
            for file in files:
                if file.endswith((".py", ".sql")):
                    files_to_process.append(os.path.join(root, file))
    else:
        logger.info(
            "No source file or directory specified. Defaulting to current directory."
        )
        for root, _, files in os.walk("."):
            for file in files:
                if file.endswith((".py", ".sql")):
                    files_to_process.append(os.path.join(root, file))

    if not files_to_process:
        raise typer.BadParameter("No files found to process.")

    processed_files = []

    for file_path in track(files_to_process, description="Processing files..."):
        try:
            output_file_path = process_file(
                file_path,
                source_dialect,
                target_dialect,
                final_table_mappings,
                output_dir,
            )
            processed_files.append(output_file_path)
        except Exception as e:
            logger.error(f"Error processing file: {file_path}\nError: {e}")

    console.print("\n[bold green]Processed files:[/bold green]")

    for processed_file in processed_files:
        console.print(f":white_check_mark: {processed_file}")


if __name__ == "__main__":
    app()
