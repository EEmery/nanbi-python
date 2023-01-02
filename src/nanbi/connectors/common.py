from nanbi.adapters.column import Column
from nanbi.operations.leaf import ColumnReference, ColumnLiteral


def col(col_name):
    return Column(ColumnReference(col_name))


def lit(value):
    return Column(ColumnLiteral(value))


def dataframe(df_name, cols):
    # TODO: Make a connector that returns a generic
    # dataframe object with no evaluator
    pass
