import pandas as pd
from random import choices
from string import ascii_letters

from nanbi.operations.leaf import DataFrameReference
from nanbi.adapters.dataframe import DataFrame
from nanbi.evaluators.pandas import PandasEvaluator


def _get_id(num_digits=6):
    alphabet_list = list(ascii_letters)
    return "".join(choices(alphabet_list, k=num_digits))


def from_dataframe(pandas_df):
    df_id = _get_id(6)
    df_ref = DataFrameReference(df_name=df_id, data_ref=pandas_df)
    evaluator = PandasEvaluator()
    return DataFrame(df_ref, evaluator)


def from_dictionary(d):
    pandas_df = pd.DataFrame(d)
    return from_dataframe(pandas_df)


def from_csv(file_path):
    # TODO: Takes a csv file_path and build
    # a pandas df
    pass
