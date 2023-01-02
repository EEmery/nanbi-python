from nanbi.operations.base import OperationLeaf


class ColumnReference(OperationLeaf):
    def __init__(self, col_name):
        self.col_name = col_name

    def __str__(self):
        return f"col({self.col_name})"

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return self.__super__() + ["col_name"]


class ColumnLiteral(OperationLeaf):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"lit({self.value})"

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return self.__super__() + ["value"]


class DataFrameReference(OperationLeaf):
    def __init__(self, df_name, data_ref):
        # TODO(Maybe): Leave the arguments open for the evaluator
        # creator to decide. Example: def __init__(self, *args)
        # then the evaluator creator can use this as an open
        # bag to store everything they'll need when evaluating
        # Question: should this be done to the column too?
        # Might not be the same case as the evaluator creator
        # will not properly reach the column as they will
        # reach the DataFrame
        self.data_ref = data_ref
        self.df_name = df_name

    def __str__(self):
        return f"DataFrame({self.df_name})"

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return self.__super__() + ["data_ref", "df_name"]
