import unittest

from sqlglot import ParseError, parse_one


class TestParser(unittest.TestCase):
    def test_error_target_positive(self):
        expression = parse_one("SELECT a FROM b ERROR_TARGET 5.8%")
        assert expression.args["error_target"].args["this"].args["this"] == "5.8"
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b ERROR_TARGET 5.8%",
        )
        self.assertEqual(expression, parse_one(expression.sql()))

    def test_error_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b ERROR_TARGET 5.8")

    def test_confidence_positive(self):
        expression = parse_one("SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95%")
        assert expression.args["confidence"].args["this"].args["this"] == "95"
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95%",
        )
        self.assertEqual(expression, parse_one(expression.sql()))

    def test_confidence_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95")

    def test_recall_target_positive(self):
        expression = parse_one("SELECT a FROM b RECALL_TARGET 5.8%")
        assert expression.args["recall_target"].args["this"].args["this"] == "5.8"
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b RECALL_TARGET 5.8%",
        )
        self.assertEqual(expression, parse_one(expression.sql()))

    def test_recall_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b RECALL_TARGET 5.8")

    def test_precision_target_positive(self):
        expression = parse_one("SELECT a FROM b PRECISION_TARGET 5.8%")
        assert expression.args["precision_target"].args["this"].args["this"] == "5.8"
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b PRECISION_TARGET 5.8%",
        )
        self.assertEqual(expression, parse_one(expression.sql()))

    def test_precision_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b PRECISION_TARGET 5.8")

    def test_query_in(self):
        self.assertEqual(
            parse_one("SELECT b FROM test WHERE z IN (1, 2)", "").sql(),
            "SELECT b FROM test WHERE z IN (1, 2)",
        )

        self.assertEqual(
            parse_one("SELECT (a) FROM test WHERE (x, y) IN ((1, 2), (3, 4))", "").sql(),
            "SELECT (a) FROM test WHERE (x, y) IN ((1, 2), (3, 4))",
        )

        self.assertEqual(
            parse_one("SELECT (a) FROM test WHERE x IN (1)", "").sql(),
            "SELECT (a) FROM test WHERE x IN (1)",
        )

        self.assertEqual(
            parse_one(
                "SELECT a AS aaa FROM test WHERE (x, y) IN (SELECT b AS bb, c FROM test2)", ""
            ).sql(),
            "SELECT a AS aaa FROM test WHERE (x, y) IN (SELECT b AS bb, c FROM test2)",
        )

    def test_user_function(self):
        self.assertEqual(
            parse_one(
                "SELECT a, COLORS02(col1, col2, col3) FROM test WHERE a > 2 "
                "AND COLORS02(col1, col2, col3) > (SELECT 1 FROM x GROUP BY y)",
                "",
            ).sql(),
            "SELECT a, COLORS02(col1, col2, col3) FROM test WHERE a > 2 "
            "AND COLORS02(col1, col2, col3) > (SELECT 1 FROM x GROUP BY y)",
        )

        self.assertEqual(
            parse_one(
                """SELECT a, OBJECTS00(frame) AS (result1, result2) FROM test WHERE result1 > 1000""",
                "",
            ).sql(),
            """SELECT a, OBJECTS00(frame) AS (result1, result2) FROM test WHERE result1 > 1000""",
        )

        self.assertEqual(
            parse_one("""SELECT a, OBJECTS00() FROM test""", "").sql(),
            """SELECT a, OBJECTS00() FROM test""",
        )

        self.assertEqual(
            parse_one(
                "SELECT * FROM test JOIN test2 ON OBJECTS00(test.frame) = COLORS02(test2.id)", ""
            ).sql(),
            "SELECT * FROM test JOIN test2 ON OBJECTS00(test.frame) = COLORS02(test2.id)",
        )

        self.assertEqual(
            parse_one(
                "SELECT * FROM test JOIN test2 ON OBJECTS00(test.frame, test.id) = COLORS02(test2.id, test2.name) "
                "WHERE test.frame > 10000",
                "",
            ).sql(),
            "SELECT * FROM test JOIN test2 ON OBJECTS00(test.frame, test.id) = COLORS02(test2.id, test2.name) "
            "WHERE test.frame > 10000",
        )
