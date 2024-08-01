# pylint: disable=no-member
import unittest

from sqlglot import parse_one


class TestRewriter(unittest.TestCase):
    def test_add_selects(self):
        expression = parse_one("SELECT * FROM (SELECT * FROM x) y")
        self.assertEqual(
            expression.select(
                "a",
                "sum(b) as c",
            ).sql(),
            "SELECT *, a, SUM(b) AS c FROM (SELECT * FROM x) AS y",
        )

    def test_where(self):
        expression = parse_one("SELECT * FROM x")

        self.assertEqual(
            expression.where(
                "(col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
            ).sql(),
            "SELECT * FROM x WHERE (col1 IN (1, 2, 3) AND col2 = 3) OR col3 LIKE 'cat'",
        )

        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        # test multi conditions
        self.assertEqual(
            expression.where(
                "((col1 = 1 AND col2 = 2) OR col3 = 3)",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND ((col1 = 1 AND col2 = 2) OR col3 = 3)",
        )

        self.assertEqual(
            expression.where()
            .or_(
                "(col1 = 1 AND col2 = 2) OR col3 = 3",
            )
            .sql(),
            "SELECT * FROM x WHERE col1 > 5 OR ((col1 = 1 AND col2 = 2) OR col3 = 3)",
        )

        # test IN operator
        self.assertEqual(
            expression.where(
                "col2 IN (1, 2, 3)",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND col2 IN (1, 2, 3)",
        )

        # test subquery
        self.assertEqual(
            expression.where(
                "a > (SELECT 1 FROM x GROUP BY y)",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND a > (SELECT 1 FROM x GROUP BY y)",
        )

        # test CASE expression
        self.assertEqual(
            expression.where()
            .or_(
                "CASE WHEN col2 = 1 THEN col1 > 100 ELSE col1 > 50 END;",
            )
            .sql(),
            "SELECT * FROM x WHERE col1 > 5 OR CASE WHEN col2 = 1 THEN col1 > 100 ELSE col1 > 50 END",
        )

        # test NOT operator
        self.assertEqual(
            expression.where(
                "NOT col1 = 10",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND NOT col1 = 10",
        )

        # test mathematical expression
        self.assertEqual(
            expression.where(
                "(col1 * col2) > 100",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND (col1 * col2) > 100",
        )

        # test IS NULL
        self.assertEqual(
            expression.where(
                "col1 IS NULL",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND col1 IS NULL",
        )

        # test BETWEEN operator
        self.assertEqual(
            expression.where(
                "col1 BETWEEN 10 AND 100",
            ).sql(),
            "SELECT * FROM x WHERE col1 > 5 AND col1 BETWEEN 10 AND 100",
        )

        self.assertEqual(
            expression.where(
                "col1 RLIKE '^J'",
            ).sql("hive"),
            "SELECT * FROM x WHERE col1 > 5 AND col1 RLIKE '^J'",
        )

    def test_join(self):
        expression = parse_one("SELECT * FROM x WHERE col1 > 5")

        self.assertEqual(
            expression.join(
                "y ON x.col1 = y.col1 AND x.col2 = y.col2",
            ).sql(),
            "SELECT * FROM x JOIN y ON x.col1 = y.col1 AND x.col2 = y.col2 WHERE col1 > 5",
        )

        expression = parse_one("SELECT * FROM x JOIN z ON x.col1 = z.col1 WHERE col1 > 5")

        self.assertEqual(
            expression.join("y ON OBJECTS00(x.col1) = OBJECTS00(y.col1)")
            .join("q ON COLOR(x.col2) = COLOR(q.col2)")
            .sql(),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 JOIN y ON OBJECTS00(x.col1) = OBJECTS00(y.col1) "
            "JOIN q ON COLOR(x.col2) = COLOR(q.col2) WHERE col1 > 5",
        )

        # test LEFT JOIN and RIGHT JOIN
        self.assertEqual(
            expression.join("LEFT JOIN y ON OBJECTS00(x.col1) = OBJECTS00(y.col1)")
            .join(
                "RIGHT JOIN q ON COLOR(x.col2) = COLOR(q.col2)",
            )
            .sql(),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 LEFT JOIN y ON OBJECTS00(x.col1) = OBJECTS00(y.col1) "
            "RIGHT JOIN q ON COLOR(x.col2) = COLOR(q.col2) WHERE col1 > 5",
        )

        # test INNER JOIN
        self.assertEqual(
            expression.join(
                "INNER JOIN y ON x.col1 = y.col1",
            ).sql(),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 INNER JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )

        # test FULL OUTER JOIN
        self.assertEqual(
            expression.join(
                "FULL OUTER JOIN y ON x.col1 = y.col1",
            ).sql(),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 FULL OUTER JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )

        # test CROSS JOIN
        self.assertEqual(
            expression.join(
                "CROSS JOIN y ON x.col1 = y.col1",
            ).sql(),
            "SELECT * FROM x JOIN z ON x.col1 = z.col1 CROSS JOIN y ON x.col1 = y.col1 WHERE col1 > 5",
        )
