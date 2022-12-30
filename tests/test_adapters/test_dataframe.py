import tests.context
import unittest
from tests.utils import NanbiTest

from nanbi.adapters.dataframe import DataFrame
from nanbi.adapters.column import Column
from nanbi.operations.leaf import DataFrameReference, ColumnReference
from nanbi.operations.node import OperationWithColumn
from nanbi.connectors.common import col


class TestDataFrame(unittest.TestCase):

  def test_with_column(self):
    df = DataFrame(DataFrameReference("test", {}))
    result = df.with_column("test_column", col("a"))
    expected = DataFrame(OperationWithColumn(DataFrameReference("test", {}), "test_column", Column(ColumnReference("a"))))
    NanbiTest.assertEquals(result, expected)

    # TODO: Good luck covering 100%...

if __name__ == '__main__':
    unittest.main()
