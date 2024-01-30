import tests.context  # noqa: F401
import unittest
import pandas as pd
from tests.utils import NanbiTest

import nanbi.connectors.pandas as nb
from nanbi.connectors.common import col, lit
from nanbi.operations.auxiliary import Window


class TestPandasEvaluator(unittest.TestCase):
    def setUp(self):
        # TODO: Organized the house a bit around here.
        # Use better variable names: df2??? really???

        self.precision = 0.001

        pandas_df = pd.DataFrame([
            [10, 41],
            [50, 51],
            [20, 21],
            [50, 31],
            [20, 11],
        ],
        columns=["num_a", "num_b"])

        pandas_df2 = pd.DataFrame([
            ["a", 1, 1.1, "apple"],
            ["a", 2, 2.1, "pineapple"],
            ["b", 1, 1.1, "orange"],
            ["a", 4, 4.1, "apple"],
            ["c", 5, 5.1, "orange"],
            ["d", 6, 6.1, "orange"],
            ["a", 7, 7.1, "apricot"],
            ["b", 8, 8.1, "grape"],
        ],
        columns=["farmer", "weight", "price", "fruit"])

        self.df = nb.from_dataframe(pandas_df)
        self.df2 = nb.from_dataframe(pandas_df2)

    def test_eval_add(self):
        result = self.df.with_column("result", col("num_a") + col("num_b")).evaluate()

        expected = pd.DataFrame([
            [10, 41, 51],
            [50, 51, 101],
            [20, 21, 41],
            [50, 31, 81],
            [20, 11, 31],
        ],
        columns=["num_a", "num_b", "result"])

        NanbiTest.assertEquals(result, expected)

    def test_eval_sub(self):
        result = self.df.with_column("result", col("num_a") - col("num_b")).evaluate()

        expected = pd.DataFrame([
            [10, 41, -31],
            [50, 51, -1],
            [20, 21, -1],
            [50, 31, 19],
            [20, 11, 9],
        ],
        columns=["num_a", "num_b", "result"])

        NanbiTest.assertEquals(result, expected)

    def test_eval_order_by(self):
        result1 = self.df.order_by(col("num_a"), col("num_b")).evaluate()
        result2 = self.df.order_by(col("num_a").desc(), col("num_b")).evaluate()
        result3 = self.df.order_by(col("num_a").desc(), col("num_b").desc()).evaluate()

        expected1 = pd.DataFrame([
            [10, 41],
            [20, 11],
            [20, 21],
            [50, 31],
            [50, 51],
        ],
        columns=["num_a", "num_b"])

        expected2 = pd.DataFrame([
            [50, 31],
            [50, 51],
            [20, 11],
            [20, 21],
            [10, 41],
        ],
        columns=["num_a", "num_b"])

        expected3 = pd.DataFrame([
            [50, 51],
            [50, 31],
            [20, 21],
            [20, 11],
            [10, 41],
        ],
        columns=["num_a", "num_b"])

        NanbiTest.assertEquals(result1, expected1)
        NanbiTest.assertEquals(result2, expected2)
        NanbiTest.assertEquals(result3, expected3)

    def test_eval_where(self):
        result1 = self.df.where(col("num_a") == lit(10)).evaluate()
        result2 = self.df.where(col("num_a") > lit(20)).evaluate()
        result3 = self.df.where((col("num_a") == lit(20)) & (col("num_b") == lit(21))).evaluate()
        result4 = self.df.where((col("num_a") == lit(20)) | (col("num_b") == lit(31))).evaluate()
        result5 = self.df.where((col("num_a") == lit(20)) & (col("num_b") == lit(31))).evaluate()
        result6 = self.df2.where(
            (col("farmer") == lit("a"))
            & (col("weight") >= lit(1.5))
            & (col("weight") <= lit(2.5))
        ).evaluate()

        # TODO: Implement "and" and "or" instead of "&" and "|". If users doesn't
        # clearly specify the operations order with "()", the behaviour will be very
        # confusing and misleading

        expected1 = pd.DataFrame([
            [10, 41],
        ],
        columns=["num_a", "num_b"])

        expected2 = pd.DataFrame([
            [50, 51],
            [50, 31],
        ],
        columns=["num_a", "num_b"])

        expected3 = pd.DataFrame([
            [20, 21],
        ],
        columns=["num_a", "num_b"])

        expected4 = pd.DataFrame([
            [20, 21],
            [50, 31],
            [20, 11],
        ],
        columns=["num_a", "num_b"])

        expected5 = pd.DataFrame({"num_a": [], "num_b": []}, dtype="int64")

        expected6 = pd.DataFrame([
            ["a", 2, 2.1, "pineapple"],
        ],
        columns=["farmer", "weight", "price", "fruit"])

        NanbiTest.assertEquals(result1, expected1)
        NanbiTest.assertEquals(result2, expected2)
        NanbiTest.assertEquals(result3, expected3)
        NanbiTest.assertEquals(result4, expected4)
        NanbiTest.assertEquals(result5, expected5)
        NanbiTest.assertEquals(result6, expected6)

    def test_eval_group_by(self):

        # TODO: Change grouping columns from List[str] to List[col]
        # TODO: Implement group by all cols "*"

        result1 = self.df2.group_by(
            "fruit", [col("weight").mean().r("mean_weight"), col("price").mean().r("mean_price")]
        ).evaluate()
        result2 = self.df2.group_by(
            ["farmer", "fruit"],
            [col("weight").mean().r("mean_weight"), col("price").mean().r("mean_price")],
        ).evaluate()

        expected1 = pd.DataFrame([
            ["apple", 2.5, 2.6],
            ["apricot", 7.0, 7.1],
            ["grape", 8.0, 8.1],
            ["orange", 4.0, 4.1],
            ["pineapple", 2.0, 2.1],
        ],
        columns=["fruit", "mean_weight", "mean_price"])

        expected2 = pd.DataFrame([
            ["a", "apple", 2.5, 2.6],
            ["a", "apricot", 7.0, 7.1],
            ["a", "pineapple", 2.0, 2.1],
            ["b", "grape", 8.0, 8.1],
            ["b", "orange", 1.0, 1.1],
            ["c", "orange", 5.0, 5.1],
            ["d", "orange", 6.0, 6.1],
        ],
        columns=["farmer", "fruit", "mean_weight", "mean_price"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        NanbiTest.assertEquals(result2, expected2, check_exact=False, atol=self.precision)

    def test_eval_window(self):
        result1 = self.df2.with_column(
            "acc_mean_price",
            col("price").mean().over(Window.partition_by(col("farmer")).order_by(col("price"))),
        ).with_column(
            "acc_max_price",
            col("price").max(),
        ).with_column(
            "acc_min_price",
            col("price").min(),
        ).with_column(
            "acc_sum_price",
            col("price").sum().over(Window.order_by(col("farmer"))),
        ).evaluate()

        expected1 = pd.DataFrame([
            ["a", 1, 1.1, "apple", 1.1, 8.1, 1.1, 1.1],
            ["a", 2, 2.1, "pineapple", 1.6, 8.1, 1.1, 3.2],
            ["b", 1, 1.1, "orange", 1.1, 8.1, 1.1, 15.5],
            ["a", 4, 4.1, "apple", 2.433333, 8.1, 1.1, 7.3],
            ["c", 5, 5.1, "orange", 5.1, 8.1, 1.1, 28.7],
            ["d", 6, 6.1, "orange", 6.1, 8.1, 1.1, 34.8],
            ["a", 7, 7.1, "apricot", 3.6, 8.1, 1.1, 14.4],
            ["b", 8, 8.1, "grape", 4.6, 8.1, 1.1, 23.6],
        ],
        columns=["farmer", "weight", "price", "fruit", "acc_mean_price", "acc_max_price", "acc_min_price", "acc_sum_price"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)

        # TODO: Add more test cases
        # 1. [DONE] Set one partition by and one order by ASC
        # 2. Set multiple partition by and multiple order by ASC
        # 3. Set one partition by and one order by DESC
        # 4. Set only partition by
        # 5. Set only order by

    def test_eval_substring(self):
        result1 = self.df2.with_column(
            "substr_fruit",
            col("fruit").substring(1, 2),
        ).evaluate()

        expected1 = pd.DataFrame([
            ["a", 1, 1.1, "apple", "pp"],
            ["a", 2, 2.1, "pineapple", "in"],
            ["b", 1, 1.1, "orange", "ra"],
            ["a", 4, 4.1, "apple", "pp"],
            ["c", 5, 5.1, "orange", "ra"],
            ["d", 6, 6.1, "orange", "ra"],
            ["a", 7, 7.1, "apricot", "pr"],
            ["b", 8, 8.1, "grape", "ra"],
        ],
        columns=["farmer", "weight", "price", "fruit", "substr_fruit"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        # TODO: Add more test cases

    def test_eval_slice(self):
        result1 = self.df2.with_column(
            "substr_fruit",
            col("fruit").slice(1, 3),
        ).evaluate()

        expected1 = pd.DataFrame([
            ["a", 1, 1.1, "apple", "pp"],
            ["a", 2, 2.1, "pineapple", "in"],
            ["b", 1, 1.1, "orange", "ra"],
            ["a", 4, 4.1, "apple", "pp"],
            ["c", 5, 5.1, "orange", "ra"],
            ["d", 6, 6.1, "orange", "ra"],
            ["a", 7, 7.1, "apricot", "pr"],
            ["b", 8, 8.1, "grape", "ra"],
        ],
        columns=["farmer", "weight", "price", "fruit", "substr_fruit"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        # TODO: Add more test cases

    def test_eval_concat(self):
        result1 = self.df2.with_column(
            "farmer_and_fruit",
            col("farmer").concat(col("fruit")),
        ).evaluate()

        expected1 = pd.DataFrame([
            ["a", 1, 1.1, "apple", "aapple"],
            ["a", 2, 2.1, "pineapple", "apineapple"],
            ["b", 1, 1.1, "orange", "borange"],
            ["a", 4, 4.1, "apple", "aapple"],
            ["c", 5, 5.1, "orange", "corange"],
            ["d", 6, 6.1, "orange", "dorange"],
            ["a", 7, 7.1, "apricot", "aapricot"],
            ["b", 8, 8.1, "grape", "bgrape"],
        ],
        columns=["farmer", "weight", "price", "fruit", "farmer_and_fruit"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        # TODO: Add more test cases

    def test_eval_cast(self):
        result1 = self.df2.with_column(
            "weight",
            col("weight").cast("string"),
        ).evaluate()

        expected1 = pd.DataFrame([
            ["a", "1", 1.1, "apple"],
            ["a", "2", 2.1, "pineapple"],
            ["b", "1", 1.1, "orange"],
            ["a", "4", 4.1, "apple"],
            ["c", "5", 5.1, "orange"],
            ["d", "6", 6.1, "orange"],
            ["a", "7", 7.1, "apricot"],
            ["b", "8", 8.1, "grape"],
        ],
        columns=["farmer", "weight", "price", "fruit"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        # TODO: Add more test cases

    def test_eval_union_by_name(self):
        result1 = (
            self.df2
            .union_by_name(self.df2)
            .evaluate()
        )

        expected1 = pd.DataFrame([
            ["a", 1, 1.1, "apple"],
            ["a", 2, 2.1, "pineapple"],
            ["b", 1, 1.1, "orange"],
            ["a", 4, 4.1, "apple"],
            ["c", 5, 5.1, "orange"],
            ["d", 6, 6.1, "orange"],
            ["a", 7, 7.1, "apricot"],
            ["b", 8, 8.1, "grape"],
            ["a", 1, 1.1, "apple"],
            ["a", 2, 2.1, "pineapple"],
            ["b", 1, 1.1, "orange"],
            ["a", 4, 4.1, "apple"],
            ["c", 5, 5.1, "orange"],
            ["d", 6, 6.1, "orange"],
            ["a", 7, 7.1, "apricot"],
            ["b", 8, 8.1, "grape"],
        ],
        columns=["farmer", "weight", "price", "fruit"])

        NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
        # TODO: Add more test cases


if __name__ == "__main__":
    unittest.main()
