import tests.context  # noqa: F401
import unittest
from tests.utils import NanbiTest

from nanbi.adapters.dataframe import DataFrame
from nanbi.operations.leaf import DataFrameReference
from nanbi.evaluators.pandas import PandasEvaluator
import nanbi.connectors.pandas as nb

import pandas as pd


class TestPandasConnector(unittest.TestCase):
    def test_from_dataframe(self):
        pandas_df = pd.DataFrame(
            {
                "letters": ["a", "b", "c", "d", "e", "f"],
                "numbers": [1, 2, 3, 4, 5, 6],
                "other_numbers": [10, 20, 30, 40, 50, 60],
                "fruits": ["apple", "orange", "appricot", "pineapple", "strawberry", "grape"],
            }
        )

        df = nb.from_dataframe(pandas_df)

        self.assertTrue(isinstance(df, DataFrame))
        self.assertTrue(isinstance(df.op, DataFrameReference))
        self.assertTrue(isinstance(df.eval, PandasEvaluator))
        NanbiTest.assertEquals(df.evaluate(), pandas_df)


if __name__ == "__main__":
    unittest.main()
