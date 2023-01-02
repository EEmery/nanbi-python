from nanbi.operations.base import OperationBinaryNode, OperationUnaryNode


class OperationAdd(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("+", left, right)


class OperationSub(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("-", left, right)


class OperationMul(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("*", left, right)


class OperationDiv(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("/", left, right)


class OperationLT(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("<", left, right)


class OperationLE(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("<=", left, right)


class OperationEQ(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("==", left, right)


class OperationNE(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("!=", left, right)


class OperationGT(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__(">", left, right)


class OperationGE(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__(">=", left, right)


class OperationAnd(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("and", left, right)


class OperationXOr(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("xor", left, right)


class OperationOr(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("or", left, right)


class OperationInvert(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("not", next)


class OperationMean(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("mean", next)


class OperationRename(OperationUnaryNode):
    def __init__(self, next, new_name):
        super().__init__("as", next)
        self.new_name = new_name

    def __dir__(self):
        return self.__super__() + ["new_name"]


class OperationSelect(OperationUnaryNode):
    def __init__(self, next, cols):
        super().__init__(".select", next)
        self.cols = cols

    def __dir__(self):
        return self.__super__() + ["cols"]


class OperationWithColumn(OperationUnaryNode):
    def __init__(self, next, col_name, col):
        super().__init__(".with_column", next)
        self.col_name = col_name
        self.col = col

    def __dir__(self):
        return self.__super__() + ["col_name", "col"]


class OperationWhere(OperationUnaryNode):
    def __init__(self, next, col):
        super().__init__(".where", next)
        self.col = col

    def __dir__(self):
        return self.__super__() + ["col"]


class OperationJoin(OperationBinaryNode):
    def __init__(self, left, right, cond, on, join_type):
        super().__init__("join", left, right)
        self.cond = cond
        self.on = on
        self.join_type = join_type

    def __dir__(self):
        return self.__super__() + ["cond", "on", "join_type"]


class OperationGroupBy(OperationUnaryNode):
    def __init__(self, next, group_keys, group_cols):
        super().__init__("group_by", next)
        self.group_keys = group_keys
        self.group_cols = group_cols

    def __dir__(self):
        return self.__super__() + ["group_keys", "group_cols"]


class OperationOrderBy(OperationUnaryNode):
    def __init__(self, next, order_by_cols):
        super().__init__("order_by", next)
        self.order_by_cols = order_by_cols

    def __dir__(self):
        return self.__super__() + ["order_by_cols"]


class OperationWindow(OperationUnaryNode):
    def __init__(self, next, partition_by, order_by):
        super().__init__("over", next)
        self.partition_by = partition_by
        self.order_by = order_by

    def __dir__(self):
        return self.__super__() + ["partition_by", "order_by"]
