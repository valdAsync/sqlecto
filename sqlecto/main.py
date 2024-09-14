from __future__ import annotations

import logging

from converter import process_file
from utils import load_config
from utils import validate_dialect


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    config_path = "config.json"
    config = load_config(config_path)

    file_path = config["source_file"]
    src_dialect = config["source_dialect"]
    tgt_dialect = config["target_dialect"]
    table_mappings = config["table_mappings"]

    # Validate dialects
    if not validate_dialect(src_dialect):
        raise ValueError(f"Unsupported source dialect: {src_dialect}")

    if not validate_dialect(tgt_dialect):
        raise ValueError(f"Unsupported target dialect: {tgt_dialect}")

    process_file(file_path, src_dialect, tgt_dialect, table_mappings)


if __name__ == "__main__":
    main()
