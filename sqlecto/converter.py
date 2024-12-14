import logging
import os
import re

from pathlib import Path

import sqlglot

from sqlecto.utils import read_file


logger = logging.getLogger(__name__)


def filter_create_table_queries(queries: list[str]) -> list[str]:
    """
    Filters out SQL queries that start with 'CREATE TABLE'.

    Args:
        queries (list[str]): A list of SQL queries.

    Returns:
        list[str]: A list of SQL queries that do not start with 'CREATE TABLE'.
    """
    filtered_queries = [
        query
        for query in queries
        if not query.strip().upper().startswith("CREATE TABLE")
    ]
    return filtered_queries


def extract_spark_queries(code: str) -> list[str]:
    """
    Extracts SQL queries from the given Python code.

    Args:
        python_code (str): The Python code containing SQL queries.

    Returns:
        list[str]: A list of SQL queries extracted from the Python code.
    """
    pattern = re.compile(r'spark\.sql\(\s*f?[\'"]{3}.*?[\'"]{3}\s*\)', re.DOTALL)
    matches = pattern.findall(code)
    queries = []

    for match in matches:
        m = re.search(r'[\'"]{3}.*?[\'"]{3}', match, re.DOTALL)
        if m:
            queries.append(m.group(0)[3:-3].strip())
        else:
            logger.warning(f"Could not extract SQL query from: {match}")
    return queries


def extract_sql_queries(content: str) -> list[str]:
    """
    Extracts SQL queries from the given SQL file content.

    Args:
        content (str): The content of the SQL file.

    Returns:
        list[str]: A list of SQL queries extracted from the file content.
    """
    queries = content.split(";")
    return [query.strip() for query in queries if query.strip()]


def replace_table_names(queries: list[str], mappings: list[dict]) -> list[str]:
    """
    Replaces table names in SQL queries based on the provided mappings.

    Args:
        queries (list[str]): A list of SQL queries.
        mappings (list[dict]): A list of mappings from source to target tables.

    Returns:
        list[str]: A list of SQL queries with replaced table names.
    """
    for mapping in mappings:
        src_table = mapping["src_table"]
        dst_table = mapping["dst_table"]
        queries = [query.replace(src_table, dst_table) for query in queries]
    return queries


def transpile_sql_queries(queries: list[str], src_dialect: str, dst_dialect) -> list:
    """
    Transpiles a list of SQL queries from Spark.

    Args:
        queries (list[str]): A list of SQL queries to be transpiled.
        src_dialect (str): A dialect of SQL source query.
    Returns:
        list: A list of transpiled SQL queries.
    """
    transpiled_queries = []
    for query in queries:
        try:
            transpiled_query = sqlglot.transpile(
                query, read=f"{src_dialect}", write=dst_dialect, pretty=True
            )[0]
            transpiled_queries.append(transpiled_query)
        except Exception as e:
            logger.error(f"Error transpiling query: {query}\nError: {e}")
            transpiled_queries.append(f"-- Error transpiling query:\n-- {e}\n{query}")
    return transpiled_queries


def process_file(
    file_path: Path,
    src_dialect: str,
    tgt_dialect: str,
    table_mappings: list[dict[str, str]],
    output_dir: Path = Path("transpiled_queries"),
) -> str:
    """
    Process a file containing code and extract and transpile SQL queries.

    Args:
        file_path (str): The path to the file.
        src_dialect (str): The source SQL dialect.
        tgt_dialect (str): The target SQL dialect.
        table_mappings (list[dict]): A list of table name mappings.

    Returns:
        Output file paths.
    """
    content = read_file(file_path)

    if str(file_path).endswith(".py"):
        all_queries = extract_spark_queries(content)
    elif str(file_path).endswith(".sql"):
        all_queries = extract_sql_queries(content)
    else:
        raise ValueError(
            "Unsupported file type. Only .py and .sql files are supported."
        )

    queries = filter_create_table_queries(all_queries)

    queries = replace_table_names(queries, table_mappings)

    transpiled_queries = transpile_sql_queries(queries, src_dialect, tgt_dialect)

    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.basename(file_path)
    name_without_ext = os.path.splitext(base_name)[0]
    output_file_name = f"converted_{name_without_ext}.sql"
    output_file_path = os.path.join(output_dir, output_file_name)

    with open(output_file_path, "w", encoding="utf-8") as output_file:
        for query in transpiled_queries:
            output_file.write(query + ";\n\n")
            output_file.write("\n" + "-" * 80 + "\n\n")

    return output_file_path
