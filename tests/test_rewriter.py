# pylint: disable=no-member
import unittest

from sqlglot import parse_one
from sqlglot.rewriter import Rewriter


class TestRewriter(unittest.TestCase):
    def test_ctas(self):
        expression = parse_one("SELECT * FROM y")

        self.assertEqual(
            Rewriter(expression).ctas("x").expression.sql(),
            "CREATE TABLE x AS SELECT * FROM y",
        )

        self.assertEqual(
            Rewriter(expression)
            .ctas("x", db="foo", format="parquet", y="2")
            .expression.sql("hive"),
            "CREATE TABLE foo.x STORED AS PARQUET TBLPROPERTIES ('y' = '2') AS SELECT * FROM y",
        )

        self.assertEqual(expression.sql(), "SELECT * FROM y")

        rewriter = Rewriter(expression).ctas("x")
        self.assertEqual(rewriter.expression.sql(), "CREATE TABLE x AS SELECT * FROM y")

        with self.assertRaises(ValueError):
            rewriter.ctas("y").expression.sql()

    def test_add_selects(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x) y")

        self.assertEqual(
            Rewriter(expression)
            .add_selects(
                "a",
                "sum(b) as c",
            )
            .expression.sql("hive"),
            "SELECT *, a, SUM(b) AS c FROM (SELECT * FROM x) AS y",
        )


    def test_add_where(self):
        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'col2 IN (1, 2, 3)',
            )
                .expression.sql("hive"),
            "SELECT * FROM x WHERE col2 IN (1, 2, 3) AND col1 > 5",
        )

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'OR',
                'col2 IN (1, 2, 3)',
            )
                .expression.sql("hive"),
            "SELECT * FROM x WHERE col2 IN (1, 2, 3) OR col1 > 5",
        )

        expression = parse_one("SELECT * FROM x")

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                '',
                "(col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
            )
                .expression.sql("hive"),
            "SELECT * FROM x WHERE (col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
        )


    def test_add_join(self):
        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'JOIN y ON x.col1 = y.col1 AND x.col2 = y.col2',
            )
                .expression.sql("hive"),
            "SELECT * FROM x JOIN y ON x.col1 = y.col1 AND x.col2 = y.col2 WHERE col1 > 5",
        )

        expression = parse_one("SELECT * FROM x JOIN z ON x.col1 = z.col1 WHERE col1 > 5")

        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'JOIN y ON objects00(x.col1) = objects00(y.col1) JOIN q ON color(x.col2) = color(q.col2)',
            )
                .expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 JOIN y ON objects00(x.col1) = objects00(y.col1) "
            "JOIN q ON color(x.col2) = color(q.col2) WHERE col1 > 5",
        )
