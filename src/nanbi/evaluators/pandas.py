import pandas as pd

from nanbi.evaluators.base import Evaluator
from nanbi.operations.leaf import ColumnReference, ColumnLiteral, DataFrameReference
import nanbi.operations.node as opn


class PandasEvaluator(Evaluator):
    def __init__(self):
        self.handlers = {
            # Math Column Operators
            opn.OperationAdd: self.add_handler,
            opn.OperationSub: self.sub_handler,
            opn.OperationMul: self.mul_handler,
            opn.OperationDiv: self.div_handler,
            # Logical Column Operators
            opn.OperationLT: self.lt_handler,
            opn.OperationLE: self.le_handler,
            opn.OperationEQ: self.eq_handler,
            opn.OperationNE: self.ne_handler,
            opn.OperationGT: self.gt_handler,
            opn.OperationGE: self.ge_handler,
            opn.OperationAnd: self.and_handler,
            opn.OperationXOr: self.xor_handler,
            opn.OperationOr: self.or_handler,
            opn.OperationInvert: self.invert_handler,
            # Grouping Column Operators
            opn.OperationMean: self.mean_handler,
            # Misc Column Operators
            opn.OperationRename: self.rename_handler,
            opn.OperationWindow: self.window_handler,
            # DataFrame Transformation Operators
            opn.OperationSelect: self.select_handler,
            opn.OperationWithColumn: self.with_column_handler,
            opn.OperationWhere: self.where_handler,
            opn.OperationJoin: self.join_handler,
            opn.OperationGroupBy: self.group_by_handler,
            opn.OperationOrderBy: self.order_by_handler,
            # Leaf Operators
            ColumnReference: self.column_reference_handler,
            ColumnLiteral: self.column_literal_handler,
            DataFrameReference: self.dataframe_refence_handler,
        }

    def eval(self, df):
        op = df.op
        return self._eval(op)

    def _eval(self, op, pandas_df=None):
        op_type = type(op)

        # TODO: Handle non implemented handlers
        # if op_type not in self.handlers.keys():
        # pass

        if pandas_df is None:
            return self.handlers[op_type](op)
        else:
            return self.handlers[op_type](op, pandas_df)

    def add_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left + right

    def sub_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left - right

    def mul_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left * right

    def div_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left / right

    def lt_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left < right

    def le_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left <= right

    def eq_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left == right

    def ne_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left != right

    def gt_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left > right

    def ge_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left >= right

    def and_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left & right

    def xor_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return (left | right) & (~(left & right))

    def or_handler(self, op, pandas_df):
        left = self._eval(op.left, pandas_df)
        right = self._eval(op.right, pandas_df)
        return left | right

    def invert_handler(self, op, pandas_df):
        next = self._eval(op.next, pandas_df)
        return ~next

    def mean_handler(self, op, pandas_df):
        return self._eval(op.next, pandas_df).mean()

    def window_handler(self, op, pandas_df):
        has_partitions = (op.partition_by is not None) or len(op.partition_by) == 0
        has_ordered_by = (op.order_by is not None) or len(op.order_by) == 0

        if has_partitions:
            partition_by_names = [c.op.col_name for c in op.partition_by]

        if has_ordered_by:
            order_by_names = partition_by_names + [c.op.col_name for c in op.order_by]
            col_is_asc = [True] * len(partition_by_names) + [c.is_ascending for c in op.order_by]
            pandas_df = pandas_df.sort_values(
                by=order_by_names, ascending=col_is_asc, inplace=False
            )
            original_index_order = pandas_df.index

        partitions = (
            pandas_df.groupby(by=partition_by_names) if has_partitions else [None, pandas_df]
        )

        eval_partitions = []
        for _, partition in partitions:
            if has_ordered_by:
                # rolling window
                windows = [partition.iloc[: i + 1] for i in range(len(partition))]
            else:
                # non-rolling window
                windows = [partition.iloc[: i + 1] for i in range(len(partition))]

            if type(op) == opn.OperationRename:
                eval_windows = [
                    pd.Series(self._eval(op.next, window)).rename(op.new_name) for window in windows
                ]
            else:
                # TODO: Make sure the operation after is either a window function
                # or a rename followed by a window function
                eval_windows = [pd.Series(self._eval(op.next, window)) for window in windows]

            eval_partition = pd.concat(eval_windows, axis=1)
            eval_partitions.append(eval_partition)

        unordered_final_series = pd.concat(eval_partitions, axis=1).squeeze().reset_index(drop=True)
        unordered_final_series.index = original_index_order
        return unordered_final_series.sort_index()

    def rename_handler(self, op, pandas_df):
        return self._eval(op.next, pandas_df).rename(op.new_name)

    def select_handler(self, op):
        pandas_df = self._eval(op.next)
        evaluated_cols = [self._eval(col.op, pandas_df) for col in op.cols]

        return pd.concat(evaluated_cols, join="inner", axis=1)

    def with_column_handler(self, op):
        pandas_df = self._eval(op.next)
        tmp_pandas_df = pandas_df.copy(deep=True)
        evaluated_col = self._eval(op.col.op, tmp_pandas_df)

        tmp_pandas_df[op.col_name] = evaluated_col
        return tmp_pandas_df

    def where_handler(self, op):
        pandas_df = self._eval(op.next)
        evaluated_col = self._eval(op.col.op, pandas_df)
        return pandas_df.loc[evaluated_col].reset_index(drop=True)

    def join_handler(self, op):
        left_df = self._eval(op.left)
        right_df = self._eval(op.right)
        # TODO: Throw error when "on" argument is not defined
        return left_df.merge(right_df, on=op.on, how=op.join_type)

    def group_by_handler(self, op):
        # TODO: group_keys is being treated as a List[str]
        # should this be th case of change to List[col]?
        pandas_df = self._eval(op.next)

        is_full_group = op.group_keys is None
        grouped = not is_full_group and pandas_df.groupby(by=op.group_keys)

        pandas_series = []
        for col in op.group_cols:
            if type(col.op) == opn.OperationRename and is_full_group:
                group = pd.Series(self._eval(col.op.next, pandas_df)).rename(col.op.new_name)

            elif type(col.op) == opn.OperationRename and not is_full_group:
                group = grouped.apply(lambda df: self._eval(col.op.next, df)).rename(
                    col.op.new_name
                )

            elif type(col.op) != opn.OperationRename and is_full_group:
                # TODO: Handle case where operation is not
                # an aggregation operation
                group = pd.Series(self._eval(col.op, pandas_df)).rename(str(col.op))

            elif type(col.op) != opn.OperationRename and not is_full_group:
                group = grouped.apply(lambda df: self._eval(col.op, df)).rename(str(col.op))
            pandas_series.append(group)

        grouped_df = pd.concat(pandas_series, axis=1)
        return grouped_df if is_full_group else grouped_df.reset_index()

    def order_by_handler(self, op):
        # TODO: Handle case where the column is not a ColRef
        # It should be possible to passe a col expression and
        # order by it.
        pandas_df = self._eval(op.next)
        col_names = [c.op.col_name for c in op.order_by_cols]
        col_is_asc = [c.is_ascending for c in op.order_by_cols]
        return pandas_df.sort_values(by=col_names, ascending=col_is_asc, inplace=False).reset_index(
            drop=True
        )

    def column_reference_handler(self, op, pandas_df):
        return pandas_df[op.col_name]

    def column_literal_handler(self, op, _):
        return op.value

    def dataframe_refence_handler(self, op):
        return op.data_ref
