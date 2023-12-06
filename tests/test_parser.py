import unittest

import sqlglot.expressions as exp
from sqlglot import ErrorLevel, Parser, ParseError, parse, parse_one


class TestParser(unittest.TestCase):
    def test_column(self):
        columns = parse_one("select a, ARRAY[1] b, case when 1 then 1 end").find_all(
            exp.Column
        )
        assert len(list(columns)) == 1

    def test_command(self):
        expressions = parse("SET x = 1; ADD JAR s3://a; SELECT 1")
        self.assertEqual(len(expressions), 3)
        self.assertEqual(expressions[0].sql(), "SET x = 1")
        self.assertEqual(expressions[1].sql(), "ADD JAR s3://a")
        self.assertEqual(expressions[2].sql(), "SELECT 1")

    def test_identify(self):
        expression = parse_one(
            """
            SELECT a, "b", c AS c, d AS "D", e AS "y|z'"
            FROM y."z"
        """
        )

        assert expression.args["expressions"][0].text("this") == "a"
        assert expression.args["expressions"][1].text("this") == "b"
        assert expression.args["expressions"][2].text("alias") == "c"
        assert expression.args["expressions"][3].text("alias") == "D"
        assert expression.args["expressions"][4].text("alias") == "y|z'"
        table = expression.args["from"].args["expressions"][0]
        assert table.args["this"].args["this"] == "z"
        assert table.args["db"].args["this"] == "y"

    def test_multi(self):
        expressions = parse(
            """
            SELECT * FROM a; SELECT * FROM b;
        """
        )

        assert len(expressions) == 2
        assert (
            expressions[0].args["from"].args["expressions"][0].args["this"].args["this"]
            == "a"
        )
        assert (
            expressions[1].args["from"].args["expressions"][0].args["this"].args["this"]
            == "b"
        )

    def test_expression(self):
        ignore = Parser(error_level=ErrorLevel.IGNORE)
        self.assertIsInstance(ignore.expression(exp.Hint, expressions=[""]), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint, y=""), exp.Hint)
        self.assertIsInstance(ignore.expression(exp.Hint), exp.Hint)

        default = Parser()
        self.assertIsInstance(default.expression(exp.Hint, expressions=[""]), exp.Hint)
        default.expression(exp.Hint, y="")
        default.expression(exp.Hint)
        self.assertEqual(len(default.errors), 3)

        warn = Parser(error_level=ErrorLevel.WARN)
        warn.expression(exp.Hint, y="")
        assert isinstance(warn.errors[0], ParseError)

    def test_function_arguments_validation(self):
        with self.assertRaises(ParseError):
            parse_one("IF(a > 0, a, b, c)")

        with self.assertRaises(ParseError):
            parse_one("IF(a > 0)")

    def test_space(self):
        self.assertEqual(
            parse_one(
                "SELECT ROW() OVER(PARTITION  BY x) FROM x GROUP  BY y", ""
            ).sql(),
            "SELECT ROW() OVER(PARTITION BY x) FROM x GROUP BY y",
        )

    def test_missing_by(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT FROM x GROUP BY")

    def test_error_target_positive(self):
        expression = parse_one("SELECT a FROM b ERROR_TARGET 5.8%")
        assert expression.args["error_target"].args["this"].args["this"] == '5.8'
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b ERROR_TARGET 5.8%",
        )
        self.assertEqual(
            expression,
            parse_one(expression.sql())
        )

    def test_error_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b ERROR_TARGET 5.8")

    def test_confidence_positive(self):
        expression = parse_one("SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95%")
        assert expression.args["confidence"].args["this"].args["this"] == '95'
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95%",
        )
        self.assertEqual(
            expression,
            parse_one(expression.sql())
        )

    def test_confidence_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b ERROR_TARGET 5.8% CONFIDENCE 95")

    def test_recall_target_positive(self):
        expression = parse_one("SELECT a FROM b RECALL_TARGET 5.8%")
        assert expression.args["recall_target"].args["this"].args["this"] == '5.8'
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b RECALL_TARGET 5.8%",
        )
        self.assertEqual(
            expression,
            parse_one(expression.sql())
        )

    def test_recall_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b RECALL_TARGET 5.8")

    def test_precision_target_positive(self):
        expression = parse_one("SELECT a FROM b PRECISION_TARGET 5.8%")
        assert expression.args["precision_target"].args["this"].args["this"] == '5.8'
        self.assertEqual(
            expression.sql(),
            "SELECT a FROM b PRECISION_TARGET 5.8%",
        )
        self.assertEqual(
            expression,
            parse_one(expression.sql())
        )

    def test_precision_target_negative(self):
        with self.assertRaises(ParseError):
            parse_one("SELECT a FROM b PRECISION_TARGET 5.8")

    def test_annotations(self):
        expression = parse_one(
            """
            SELECT
                a #annotation1,
                b as B #annotation2:testing ,
                "test#annotation",c#annotation3, d #annotation4,
                e #
            FROM foo
        """
        )

        assert expression.args["expressions"][0].text("this") == "annotation1"
        assert expression.args["expressions"][1].text("this") == "annotation2:testing"
        assert expression.args["expressions"][2].text("this") == "test#annotation"
        assert expression.args["expressions"][3].text("this") == "c#annotation3"
        assert expression.args["expressions"][4].text("this") == "annotation4"
        assert expression.args["expressions"][5].text("this") == ""

    def test_query_in(self):
        self.assertEqual(
            parse_one(
                "SELECT b FROM test WHERE z IN (1, 2)", ""
            ).sql(),
            "SELECT b FROM test WHERE z IN (1, 2)",
        )

        self.assertEqual(
            parse_one(
                "SELECT (a) FROM test WHERE (x, y) IN ((1, 2), (3, 4))", ""
            ).sql(),
            "SELECT (a) FROM test WHERE (x, y) IN ((1, 2), (3, 4))",
        )

        self.assertEqual(
            parse_one(
                "SELECT (a) FROM test WHERE x IN (1)", ""
            ).sql(),
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
                "SELECT a, colors02(col1, col2, col3) FROM test WHERE a > 2 "
                    "AND colors02(col1, col2, col3) > (SELECT 1 FROM x GROUP BY y)", ""
            ).sql(),
            "SELECT a, colors02(col1, col2, col3) FROM test WHERE a > 2 "
                "AND colors02(col1, col2, col3) > (SELECT 1 FROM x GROUP BY y)",
        )


        self.assertEqual(
          parse_one(
            '''SELECT a, objects00(frame) AS (result1, result2) FROM test WHERE result1 > 1000''', ""
          ).sql(),
          '''SELECT a, objects00(frame) AS (result1, result2) FROM test WHERE result1 > 1000''',
        )

        self.assertEqual(
          parse_one('''SELECT a, objects00() FROM test''', ""
          ).sql(),
          '''SELECT a, objects00() FROM test''',
        )

        self.assertEqual(
          parse_one(
            "SELECT * FROM test JOIN test2 ON objects00(test.frame) = colors02(test2.id)", ""
          ).sql(),
            "SELECT * FROM test JOIN test2 ON objects00(test.frame) = colors02(test2.id)",
        )


        self.assertEqual(
          parse_one(
            "SELECT * FROM test JOIN test2 ON objects00(test.frame, test.id) = colors02(test2.id, test2.name) "
            "WHERE test.frame > 10000", ""
          ).sql(),
          "SELECT * FROM test JOIN test2 ON objects00(test.frame, test.id) = colors02(test2.id, test2.name) "
            "WHERE test.frame > 10000",
        )
        