from collections.abc import Iterable
import nanbi.operations.node as op


class DataFrame:
    def __init__(self, op, evaluator=None):
        self.op = op
        self.eval = evaluator

    def select(self, *cols):
        df = self.copy()
        df.op = op.OperationSelect(self.op, cols)
        return df

    def with_column(self, col_name, col):
        df = self.copy()
        df.op = op.OperationWithColumn(self.op, col_name, col)
        return df

    def where(self, col):
        df = self.copy()
        df.op = op.OperationWhere(self.op, col)
        return df

    def join(self, other, cond=None, on=None, join_type="inner"):
        # TODO: Throw error when "cond" and "on" argument are both defined
        # TODO: Throw error when join type does not exist
        df = self.copy()
        df.op = op.OperationJoin(self.op, other.op, cond, on, join_type)
        return df

    def group_by(self, group_keys=None, group_cols=None):
        # TODO: throw error whe group_cols is None. Only group_keys
        # can be None. You also shouldn't switch the order of the arguments
        # TODO(?): Add support for "*" instead of None
        df = self.copy()
        group_keys = (
            group_keys if isinstance(group_keys, Iterable) or group_keys is None else [group_keys]
        )
        group_cols = group_cols if isinstance(group_cols, Iterable) else [group_cols]
        df.op = op.OperationGroupBy(self.op, group_keys, group_cols)
        return df

    def order_by(self, *order_by_cols):
        df = self.copy()
        df.op = op.OperationOrderBy(self.op, order_by_cols)
        return df

    def transform(self, f, *args, **kargs):
        return f(self, *args, **kargs)

    def copy(self):
        return DataFrame(op=self.op, evaluator=self.eval)

    def evaluate(self):
        # TODO: Handle undefined evaluator
        return self.eval.eval(self)

    def display(self):
        return self.evaluate()

    def d(self):
        return self.d(self)

    def __str__(self):
        return str(self.op)

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return ["op", "eval"]
