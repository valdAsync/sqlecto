import tempfile

from pathlib import Path

import pytest

from sqlecto.converter import extract_spark_queries
from sqlecto.converter import extract_sql_queries
from sqlecto.converter import filter_create_table_queries
from sqlecto.converter import process_file
from sqlecto.converter import replace_table_names
from sqlecto.converter import transpile_sql_queries


# Import the functions to test


# Test data
SAMPLE_SPARK_CODE = '''
def some_function():
    spark.sql("""
        SELECT *
        FROM table1
        WHERE id > 10
    """)

    spark.sql(f"""
        SELECT name, count(*)
        FROM table2
        GROUP BY name
    """)
'''

SAMPLE_SQL_FILE = """
SELECT * FROM table1;
CREATE TABLE temp AS SELECT * FROM table2;
SELECT count(*) FROM table3;
"""


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


def test_filter_create_table_queries():
    queries = [
        "SELECT * FROM table1",
        "CREATE TABLE temp AS SELECT * FROM table2",
        "create table another AS SELECT 1",
        "SELECT count(*) FROM table3",
    ]

    filtered = filter_create_table_queries(queries)
    assert len(filtered) == 2
    assert "CREATE TABLE" not in filtered[0].upper()
    assert "CREATE TABLE" not in filtered[1].upper()


def test_extract_spark_queries():
    queries = extract_spark_queries(SAMPLE_SPARK_CODE)
    assert len(queries) == 2
    assert "SELECT *" in queries[0]
    assert "GROUP BY name" in queries[1]


def test_extract_spark_queries_empty():
    queries = extract_spark_queries("def empty_function(): pass")
    assert len(queries) == 0


def test_extract_sql_queries():
    queries = extract_sql_queries(SAMPLE_SQL_FILE)
    assert len(queries) == 3
    assert queries[0].strip() == "SELECT * FROM table1"


def test_extract_sql_queries_empty():
    queries = extract_sql_queries("")
    assert len(queries) == 0


def test_replace_table_names():
    queries = ["SELECT * FROM old_table", "SELECT count(*) FROM another_old_table"]
    mappings = [
        {"src_table": "old_table", "dst_table": "new_table"},
        {"src_table": "another_old_table", "dst_table": "another_new_table"},
    ]

    replaced = replace_table_names(queries, mappings)
    assert "new_table" in replaced[0]
    assert "another_new_table" in replaced[1]


def test_transpile_sql_queries():
    queries = ["SELECT CURRENT_TIMESTAMP()"]  # Spark syntax
    transpiled = transpile_sql_queries(queries, "spark", "snowflake")
    assert "CURRENT_TIMESTAMP" in transpiled[0]  # Snowflake syntax


def test_transpile_sql_queries_with_error():
    queries = ["INVALID SQL QUERY"]
    transpiled = transpile_sql_queries(queries, "spark", "snowflake")
    assert transpiled[0].startswith("-- Error transpiling query")


@pytest.mark.parametrize(
    "file_extension,content", [(".py", SAMPLE_SPARK_CODE), (".sql", SAMPLE_SQL_FILE)]
)
def test_process_file(temp_dir, file_extension, content):
    # Create temporary test file
    test_file = Path(temp_dir) / f"test_file{file_extension}"
    test_file.write_text(content)

    # Test mappings
    mappings = [{"src_table": "table1", "dst_table": "new_table1"}]

    # Process the file
    process_file(str(test_file), "spark", "snowflake", mappings, output_dir=temp_dir)

    # Check if output file was created
    output_file = Path(temp_dir) / "converted_test_file.sql"
    assert output_file.exists()
    content = output_file.read_text()
    assert content  # File should not be empty


def test_process_file_invalid_extension(temp_dir):
    invalid_file = Path(temp_dir) / "test.txt"
    invalid_file.write_text("some content")

    with pytest.raises(ValueError, match="Unsupported file type"):
        process_file(str(invalid_file), "spark", "snowflake", [], output_dir=temp_dir)
