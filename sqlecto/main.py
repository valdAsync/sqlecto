from __future__ import annotations

import logging
import os

from converter import process_file
from utils import load_config
from utils import validate_dialect


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    config_path = "config.yaml"
    config = load_config(config_path)

    source_files = config.get("source_files", [])
    source_files = source_files if isinstance(source_files, list) else [source_files]
    source_dir = config.get("source_dir")
    src_dialect = config.get("source_dialect")
    tgt_dialect = config.get("target_dialect")
    table_mappings = config.get("table_mappings")

    # If neither source_file nor source_dir is provided, default to current directory
    if not source_files and not source_dir:
        source_dir = "."
        logger.info(
            "No source file or directory specified. Defaulting to current directory."
        )

    # Validate dialects
    if not validate_dialect(src_dialect):
        raise ValueError(f"Unsupported source dialect: {src_dialect}")

    if not validate_dialect(tgt_dialect):
        raise ValueError(f"Unsupported target dialect: {tgt_dialect}")

    # Build a list of files to process
    files_to_process = []

    if source_files:
        files_to_process.extend(source_files)

    if source_dir:
        for root, _, files in os.walk(source_dir):
            for file in files:
                if file.endswith((".py", ".sql")):
                    files_to_process.append(os.path.join(root, file))

    if not files_to_process:
        raise ValueError("No files found to process.")

    for file_path in files_to_process:
        logger.info(f"Processing file: {file_path}")
        try:
            process_file(file_path, src_dialect, tgt_dialect, table_mappings)
        except Exception as e:
            logger.error(f"Error processing file: {file_path}\nError: {e}")


if __name__ == "__main__":
    main()
