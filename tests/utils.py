import tests.context
from nanbi.operations.base import Operation
from nanbi.adapters.column import Column
from nanbi.adapters.dataframe import DataFrame

import pandas as pd
import pandas.testing as PandasTest

class NanbiTest():
  # TODO: Remove this from the class, importing the lib
  # should be enough

  @staticmethod
  def _node_as_dir(obj):
    d = {}
    for k, v in vars(obj).items():
      is_nanbi_obj = isinstance(v, Operation) or isinstance(v, Column) or isinstance(v, DataFrame)

      if is_nanbi_obj:
        d[k] = NanbiTest._node_as_dir(v)
      else:
        d[k] = v
    
    return d
  
  @staticmethod
  def _assert_pandas_df_equals(result, expected, check_exact, atol):
    # TODO: Implement a dict comparison tool so the user can see what part of
    # the DF the comparison fails (instead of a simple True/False).
    # Or at least add a str/dict representation of the "result" and "expected"
    # in the error message.
    try:
      PandasTest.assert_frame_equal(result, expected, check_exact, atol)
    except Exception as e:
      raise AssertionError(str(e)
      + "\nThe result and expected pandas dataframes do not match:"
      + f"\nResult:\n{str(result)}"
      + f"\nExpected:\n{str(expected)}")
    
    return True

  @staticmethod
  def assertEquals(result, expected, check_exact=True, atol=0.001):
    # TODO: Double check if there isn't a backed in way to compare objects in
    # Python by only checking their attributes (note that you've already
    # overidden the `==` method).

    # TODO: Why have you applied camel case here?!?!?! Switch to snake case

    if type(result) != type(expected):
      raise AssertionError("Types are mismatching when they should be the same"
      + f"The result object is of type {type(result)}, while the expected"
      + f"object is of type {type(expected)}")
    
    # TODO(?): Maybe remove pandas comparision, probably it is better to just
    # use pandas native testing.assert.
    if type(result) == pd.DataFrame:
      return NanbiTest._assert_pandas_df_equals(result, expected, check_exact, atol)
    
    result_dir = NanbiTest._node_as_dir(result)
    expected_dir = NanbiTest._node_as_dir(expected)

    # TODO: Implement a dict comparison tool so the user can see what part of
    # the dict/obj comparison fails (instead of a simple True/False)
    if result_dir != expected_dir:
      raise AssertionError("The result and expected values doesn't match: "
      + f" Result: {result_dir}"
      + f" Expected: {expected_dir}")
    
    return True
