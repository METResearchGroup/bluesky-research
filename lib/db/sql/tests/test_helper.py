"""Tests for SQL helper functions."""

from lib.db.sql.helper import normalize_sql


class TestNormalizeSQL:
    """Tests for normalize_sql function."""

    def test_basic_whitespace(self):
        """Test basic whitespace normalization."""
        input_sql = """
            SELECT *
            FROM   table
            WHERE  column = 'value'
        """
        expected = "SELECT * FROM table WHERE column='value'"
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_multiple_spaces(self):
        """Test handling of multiple spaces."""
        input_sql = "SELECT   col1,    col2   FROM   table"
        expected = "SELECT col1,col2 FROM table"
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_newlines_and_tabs(self):
        """Test handling of newlines and tabs."""
        input_sql = "SELECT\ncol1,\n\tcol2\nFROM\ttable"
        expected = "SELECT col1,col2 FROM table"
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_operators(self):
        """Test handling of operators."""
        input_sql = "WHERE col1 = 'value' , col2= 'value2' ,col3 ='value3'"
        expected = "WHERE col1='value',col2='value2',col3='value3'"
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_complex_query(self):
        """Test complex query with multiple clauses."""
        input_sql = """
            SELECT col1, col2, col3
            FROM   table1
            JOIN   table2 ON table1.id = table2.id
            WHERE  col1 = 'value'
            AND    col2 = 'value2'
            ORDER  BY col1 DESC
        """
        expected = (
            "SELECT col1,col2,col3 FROM table1 JOIN table2 ON table1.id=table2.id "
            "WHERE col1='value' AND col2='value2' ORDER BY col1 DESC"
        )
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_multiline_indentation(self):
        """Test handling of multiline queries with different indentation."""
        input_sql = """
            SELECT col1,
                col2,
                    col3
            FROM table
        """
        expected = "SELECT col1,col2,col3 FROM table"
        assert normalize_sql(input_sql) == normalize_sql(expected)

    def test_empty_lines(self):
        """Test handling of empty lines in queries."""
        input_sql = """

            SELECT col1, col2

            FROM table

            WHERE col1 = 'value'

        """
        expected = "SELECT col1,col2 FROM table WHERE col1='value'"
        assert normalize_sql(input_sql) == normalize_sql(expected)
