import nanbi.operations.node as op


class Column:
    def __init__(self, op, is_ascending=True):
        self.op = op
        self.is_ascending = is_ascending

    def name(self):
        # TODO: Test
        if isinstance(self.op, op.OperationRename):
            return self.op.new_name
        elif isinstance(self.op, op.ColumnReference):
            return self.op.col_name
        else:
            raise NameError(
                "This columns is anonymous, i.e., it has no defined names. Use .rename() to give"
                + " this column a name."
            )

    def asc(self):
        return Column(self.op, is_ascending=True)

    def desc(self):
        return Column(self.op, is_ascending=False)

    def rename(self, new_name):
        return Column(op.OperationRename(self.op, new_name))

    def r(self, new_name):
        return self.rename(new_name)

    def over(self, window_spec, partition_by=None, order_by=None):
        # TODO: Throw error when both window spec and other args are set
        if partition_by is None and order_by is None:
            partition_by = window_spec.partition_by_cols
            order_by = window_spec.order_by_cols

        return Column(op.OperationWindow(self.op, partition_by, order_by))

    def __add__(self, other):
        return Column(op.OperationAdd(self.op, other.op))

    def __sub__(self, other):
        return Column(op.OperationSub(self.op, other.op))

    def __mul__(self, other):
        return Column(op.OperationMul(self.op, other.op))

    def __truediv__(self, other):
        return Column(op.OperationDiv(self.op, other.op))

    def __iadd__(self, other):
        return self.__add__(other)

    def __isub__(self, other):
        return self.__sub__(other)

    def __imul__(self, other):
        return self.__mul__(other)

    def __itruediv__(self, other):
        return self.__div__(other)

    def __radd__(self, other):
        return self.__add__(other)

    def __rsub__(self, other):
        return other.__sub__(self)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __rtruediv__(self, other):
        return other.__div__(self)

    def __lt__(self, other):
        return Column(op.OperationLT(self.op, other.op))

    def __le__(self, other):
        return Column(op.OperationLE(self.op, other.op))

    def __eq__(self, other):
        return Column(op.OperationEQ(self.op, other.op))

    def __ne__(self, other):
        return Column(op.OperationNE(self.op, other.op))

    def __gt__(self, other):
        return Column(op.OperationGT(self.op, other.op))

    def __ge__(self, other):
        return Column(op.OperationGE(self.op, other.op))

    def __and__(self, other):
        return Column(op.OperationAnd(self.op, other.op))

    def __xor__(self, other):
        return Column(op.OperationXOr(self.op, other.op))

    def __or__(self, other):
        return Column(op.OperationOr(self.op, other.op))

    def __invert__(self):
        return Column(op.OperationInvert(self.op))

    def mean(self):
        return Column(op.OperationMean(self.op))

    def __str__(self):
        return str(self.op)

    def __repr__(self):
        return self.__str__()

    def eval(self, df):
        return self.op.eval(df)

    def __dir__(self):
        return ["op", "is_ascending"]
