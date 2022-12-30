import tests.context
import unittest
from tests.utils import NanbiTest

import nanbi.connectors.pandas as nb
from nanbi.connectors.common import col, lit
from nanbi.operations.auxiliary import Window
import pandas as pd

class TestPandasEvaluator(unittest.TestCase):

  def setUp(self):
    # TODO: Organized the house a bit around here.
    # Use better variable names: df2??? really???

    self.precision = 0.001

    pandas_df = pd.DataFrame({"num_a": [10, 50, 20, 50, 20],
                              "num_b": [41, 51, 21, 31, 11]})

    self.df = nb.from_dataframe(pandas_df)

    pandas_df2 = pd.DataFrame({"fruit": ["apple", "pineapple", "orange", "apple", "orange", "orange", "apricot", "grape"],
                               "farmer": ["a", "a", "b", "a", "c", "d", "a", "b"],
                               "weight": [1, 2, 1, 4, 5, 6, 7, 8],
                               "price": [1.1, 2.1, 1.1, 4.1, 5.1, 6.1, 7.1, 8.1]})
    
    self.df2 = nb.from_dataframe(pandas_df2)

  def test_eval_add(self):
    result = self.df.with_column("result", col("num_a") + col("num_b")).evaluate()

    expected = pd.DataFrame({"num_a": [10, 50, 20, 50, 20],
                             "num_b": [41, 51, 21, 31, 11],
                             "result": [51, 101, 41, 81, 31]})
    
    NanbiTest.assertEquals(result, expected)
  
  def test_eval_sub(self):
    result = self.df.with_column("result", col("num_a") - col("num_b")).evaluate()

    expected = pd.DataFrame({"num_a": [10, 50, 20, 50, 20],
                             "num_b": [41, 51, 21, 31, 11],
                             "result": [-31, -1, -1, 19, 9]})
    
    NanbiTest.assertEquals(result, expected)
  
  def test_eval_order_by(self):
    result1 = self.df.order_by(col("num_a"), col("num_b")).evaluate()
    result2 = self.df.order_by(col("num_a").desc(), col("num_b")).evaluate()
    result3 = self.df.order_by(col("num_a").desc(), col("num_b").desc()).evaluate()

    expected1 = pd.DataFrame({"num_a": [10, 20, 20, 50, 50],
                              "num_b": [41, 11, 21, 31, 51]})
    
    expected2 = pd.DataFrame({"num_a": [50, 50, 20, 20, 10],
                              "num_b": [31, 51, 11, 21, 41]})
    
    expected3 = pd.DataFrame({"num_a": [50, 50, 20, 20, 10],
                              "num_b": [51, 31, 21, 11, 41]})
    
    NanbiTest.assertEquals(result1, expected1)
    NanbiTest.assertEquals(result2, expected2)
    NanbiTest.assertEquals(result3, expected3)
  
  def test_eval_where(self):
    result1 = self.df.where(col("num_a") == lit(10)).evaluate()
    result2 = self.df.where(col("num_a") > lit(20)).evaluate()
    result3 = self.df.where((col("num_a") == lit(20)) & (col("num_b") == lit(21))).evaluate()
    result4 = self.df.where((col("num_a") == lit(20)) | (col("num_b") == lit(31))).evaluate()
    result5 = self.df.where((col("num_a") == lit(20)) & (col("num_b") == lit(31))).evaluate()

    # TODO: Implement "and" and "or" instead of "&" and "|". If users doesn't
    # clearly specify the operations order with "()", the behaviour will be very
    # confusing and misleading

    expected1 = pd.DataFrame({"num_a": [10],
                              "num_b": [41]})
    
    expected2 = pd.DataFrame({"num_a": [50, 50],
                              "num_b": [51, 31]})
    
    expected3 = pd.DataFrame({"num_a": [20],
                              "num_b": [21]})
    
    expected4 = pd.DataFrame({"num_a": [20, 50, 20],
                              "num_b": [21, 31, 11]})
    
    expected5 = pd.DataFrame({"num_a": [],
                              "num_b": []},
                             dtype="int64")
    
    NanbiTest.assertEquals(result1, expected1)
    NanbiTest.assertEquals(result2, expected2)
    NanbiTest.assertEquals(result3, expected3)
    NanbiTest.assertEquals(result4, expected4)
    NanbiTest.assertEquals(result5, expected5)
  
  def test_eval_group_by(self):

    # TODO: Change grouping columns from List[str] to List[col]
    # TODO: Implement group by all cols "*"

    result1 = self.df2.group_by("fruit",
                                [col("weight").mean().r("mean_weight"),
                                 col("price").mean().r("mean_price")
                                 ]).evaluate()
    result2 = self.df2.group_by(["farmer", "fruit"],
                                [col("weight").mean().r("mean_weight"),
                                 col("price").mean().r("mean_price")
                                 ]).evaluate()
    
    expected1 = pd.DataFrame({"fruit": ["apple", "apricot", "grape", "orange", "pineapple"],
                              "mean_weight": [2.5, 7.0, 8.0, 4.0, 2.0],
                              "mean_price": [2.6, 7.1, 8.1, 4.1, 2.1]})
    
    expected2 = pd.DataFrame({"farmer": ["a", "a", "a", "b", "b", "c", "d"],
                              "fruit": ["apple", "apricot", "pineapple", "grape", "orange", "orange", "orange"],
                              "mean_weight": [2.5, 7.0, 2.0, 8.0, 1.0, 5.0, 6.0],
                              "mean_price": [2.6, 7.1, 2.1, 8.1, 1.1, 5.1, 6.1]})
    
    NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)
    NanbiTest.assertEquals(result2, expected2, check_exact=False, atol=self.precision)
  
  def test_eval_window(self):
    result1 = self.df2.with_column("acc_mean_price", col("price").mean().over(Window.partition_by(col("farmer")).order_by(col("price")))).evaluate()

    expected1 = pd.DataFrame({"fruit": ["apple", "pineapple", "orange", "apple", "orange", "orange", "apricot", "grape"],
                              "farmer": ["a", "a", "b", "a", "c", "d", "a", "b"],
                              "weight": [1, 2, 1, 4, 5, 6, 7, 8],
                              "price": [1.1, 2.1, 1.1, 4.1, 5.1, 6.1, 7.1, 8.1],
                     "acc_mean_price": [1.1, 1.6, 1.1, 2.433333, 5.1, 6.1, 3.6, 4.6]})

    NanbiTest.assertEquals(result1, expected1, check_exact=False, atol=self.precision)

    # TODO: Add more test cases
    # 1. [DONE] Set one partition by and one order by ASC
    # 2. Set multiple partition by and multiple order by ASC
    # 3. Set one partition by and one order by DESC
    # 4. Set only partition by
    # 5. Set only order by

if __name__ == '__main__':
    unittest.main()
