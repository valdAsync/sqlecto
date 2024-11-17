# SQLecto

SQLecto is a command-line tool for converting SQL queries across different SQL dialects, providing flexibility for users working with SQL transformations. This tool leverages the powerful [SQLGlot](https://github.com/tobymao/sqlglot) library for parsing and transpiling SQL queries.

## Features

- **Dialect Conversion**: Transpile SQL queries from one SQL dialect to another (e.g., Spark to Snowflake)
- **File Support**: Process individual SQL/Python files or entire directories containing SQL code
- **Configurable Mappings**: Define custom table mappings and configurations via JSON or YAML files

## Requirements

- Python 3.8+

## Quck Start

### Installation

You can install SQLecto via `pipx` (recommended) or `pip`.

#### Using `pipx`

```bash
pipx install sqlecto
```

#### Using `pip`

```bash
pip install sqlecto
```

### Basic Usage

Convert a single SQL file:

```bash
sqlecto --source-dialect spark --target-dialect snowflake --source-files ./my_query.sql
```

Convert all SQL files in a directory

```bash
sqlecto --source-dialect spark --target-dialect snowflake --source-dir ./sql_scripts
```

## Command Options

| Option                  | Required | Description                           | Default              |
| ----------------------- | -------- | ------------------------------------- | -------------------- |
| `--source-files`        | No       | SQL/Python files to process           | None                 |
| `--source-dir`          | No       | Directory containing SQL/Python files | Current directory    |
| `--source-dialect`      | Yes      | Source SQL dialect                    | None                 |
| `--target-dialect`      | Yes      | Target SQL dialect                    | None                 |
| `--table-mappings`      | No       | Table name mappings                   | None                 |
| `--table-mappings-file` | No       | File containing table mappings        | None                 |
| `--config-file`         | No       | Configuration file path               | None                 |
| `--output-dir`          | No       | Output directory for converted files  | ./transpiled_queries |

## Example Command

```bash
sqlecto --source-dialect spark --target-dialect snowflake \
    --source-dir ./sql_scripts --output-dir ./transpiled_queries
```

## Configuration

### Configuration File Example

Configuration can be managed via `JSON` or `YAML` files, allowing reusable settings:

```json
{
  "source_files": ["./path/to/file.sql", "./path/to/file2.py"],
  "source_dialect": "spark",
  "target_dialect": "snowflake",
  "output_dir": "./transpiled_queries",
  "table_mappings": [{ "src_table": "old_table", "dst_table": "new_table" }]
}
```

### Table Mappings

To replace table names within SQL queries, use `--table-mappings` or specify mappings in a configuration file.
Mappings follow the format:

```bash
--table-mappings source_table:target_table
```

Or in `JSON/YAML`:

```json
"table_mappings": [
    {"src_table": "old_table", "dst_table": "new_table"}
]
```

```yaml
# mappings.yaml
table_mappings:
  - src_table: old_table1
    dst_table: new_table1
  - src_table: old_table2
    dst_table: new_table2
```

## Examples

### Basic Conversion

```bash
sqlecto --source-dialect spark --target-dialect snowflake \
    --source-files ./query.sql --output-dir ./converted
```

### Directory Processing with Table Mappings

```bash
sqlecto --source-dialect spark --target-dialect snowflake \
    --source-dir ./sql_scripts \
    --table-mappings old_db.table1:new_db.table1 \
    --output-dir ./converted
```

### Using Configuration File

```bash
sqlecto --config-file ./config.yaml
```

## Credits

This tool heavily relies on the [SQLGlot](https://github.com/tobymao/sqlglot) package by Toby Mao, providing robust and reliable SQL dialect transformations.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request for any improvements.

## License

MIT License
