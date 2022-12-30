import tests.context
import unittest
from tests.utils import NanbiTest

from nanbi.adapters.column import Column
from nanbi.connectors.common import col
from nanbi.operations.node import OperationSub, OperationAdd, OperationDiv, OperationMul, OperationRename
from nanbi.operations.leaf import ColumnReference

class TestColumn(unittest.TestCase):

  def test_add_between_two_cols(self):
    result = col("a") + col("b")
    expected = Column(OperationAdd(ColumnReference("a"), ColumnReference("b")))
    NanbiTest.assertEquals(result, expected)
  
  def test_add_between_cols_and_lit(self):
    pass
  
  def test_add_between_two_lits(self):
    pass
  
  def test_sub_between_two_cols(self):
    result = col("a") - col("b")
    expected = Column(OperationSub(ColumnReference("a"), ColumnReference("b")))
    NanbiTest.assertEquals(result, expected)
  
  def test_div_between_two_cols(self):
    result = col("a") / col("b")
    expected = Column(OperationDiv(ColumnReference("a"), ColumnReference("b")))
    NanbiTest.assertEquals(result, expected)
  
  def test_mul_between_two_cols(self):
    result = col("a") * col("b")
    expected = Column(OperationMul(ColumnReference("a"), ColumnReference("b")))
    NanbiTest.assertEquals(result, expected)
  
  def test_rename(self):
    result = col("a").rename("b")
    expected = Column(OperationRename(ColumnReference("a"), "b"))
    NanbiTest.assertEquals(result, expected)
  
  # TODO: Good luck covering 100%...

if __name__ == '__main__':
    unittest.main()
