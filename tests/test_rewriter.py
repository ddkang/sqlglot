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
        expression = parse_one("SELECT * FROM x")

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                '',
                "(col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE (col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
        )

        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        # test multi conditions
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                '((col1 = 1 AND col2 = 2) OR col3 = 3)',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE ((col1 = 1 AND col2 = 2) OR col3 = 3) AND col1 > 5",
        )

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'OR',
                '((col1 = 1 AND col2 = 2) OR col3 = 3)',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE ((col1 = 1 AND col2 = 2) OR col3 = 3) OR col1 > 5",
        )

        # test IN operator
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'col2 IN (1, 2, 3)',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE col2 IN (1, 2, 3) AND col1 > 5",
        )

        # test subquery
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'a > (SELECT 1 FROM x GROUP BY y)',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE a > (SELECT 1 FROM x GROUP BY y) AND col1 > 5",
        )

        # test CASE expression
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'OR',
                'CASE WHEN col2 = 1 THEN col1 > 100 ELSE col1 > 50 END;',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE CASE WHEN col2 = 1 THEN col1 > 100 ELSE col1 > 50 END OR col1 > 5",
        )

        # test NOT operator
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'NOT col1 = 10',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE NOT col1 = 10 AND col1 > 5",
        )

        # test mathematical expression
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                '(col1 * col2) > 100',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE (col1 * col2) > 100 AND col1 > 5",
        )

        # test IS NULL
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'col1 IS NULL',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE col1 IS NULL AND col1 > 5",
        )

        # test BETWEEN operator
        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                'col1 BETWEEN 10 AND 100',
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE col1 BETWEEN 10 AND 100 AND col1 > 5",
        )

        self.assertEqual(
            Rewriter(expression)
                .add_where(
                'AND',
                "col1 RLIKE '^J'",
            ).expression.sql("hive"),
            "SELECT * FROM x WHERE col1 RLIKE '^J' AND col1 > 5",
        )


    def test_add_join(self):
        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'JOIN y ON x.col1 = y.col1 AND x.col2 = y.col2',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN y ON x.col1 = y.col1 AND x.col2 = y.col2 WHERE col1 > 5",
        )

        expression = parse_one("SELECT * FROM x JOIN z ON x.col1 = z.col1 WHERE col1 > 5")

        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'JOIN y ON objects00(x.col1) = objects00(y.col1) JOIN q ON color(x.col2) = color(q.col2)',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 JOIN y ON objects00(x.col1) = objects00(y.col1) "
            "JOIN q ON color(x.col2) = color(q.col2) WHERE col1 > 5",
        )

        # test LEFT JOIN and RIGHT JOIN
        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'LEFT JOIN y ON objects00(x.col1) = objects00(y.col1) RIGHT JOIN q ON color(x.col2) = color(q.col2)',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 LEFT JOIN y ON objects00(x.col1) = objects00(y.col1) "
            "RIGHT JOIN q ON color(x.col2) = color(q.col2) WHERE col1 > 5",
        )

        # test INNER JOIN
        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'INNER JOIN y ON x.col1 = y.col1',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 INNER JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )

        # test FULL OUTER JOIN
        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'FULL OUTER JOIN y ON x.col1 = y.col1',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 FULL OUTER JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )

        # test CROSS JOIN
        self.assertEqual(
            Rewriter(expression)
                .add_join(
                'CROSS JOIN y ON x.col1 = y.col1',
            ).expression.sql("hive"),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 CROSS JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )
