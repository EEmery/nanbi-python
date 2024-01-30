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


class OperationMean(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("mean", next)


class OperationMax(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("max", next)


class OperationMin(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("min", next)


class OperationSum(OperationUnaryNode):
    def __init__(self, next):
        super().__init__("sum", next)


class OperationSubstring(OperationUnaryNode):
    def __init__(self, next, position, length):
        super().__init__("substring", next)
        self.position = position
        self.length = length


class OperationSlice(OperationUnaryNode):
    def __init__(self, next, start, end, step):
        super().__init__("slice", next)
        self.start = start
        self.end = end
        self.step = step


class OperationConcat(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("+", left, right)


class OperationRename(OperationUnaryNode):
    def __init__(self, next, new_name):
        super().__init__("as", next)
        self.new_name = new_name

    def __dir__(self):
        return self.__super__() + ["new_name"]


class OperationCast(OperationUnaryNode):
    def __init__(self, next, new_type):
        super().__init__("cast", next)
        self.new_type = new_type

    def __dir__(self):
        return self.__super__() + ["new_type"]


class OperationWhen(OperationUnaryNode):
    def __init__(self, next, value, else_value=None):
        super().__init__("when", next)
        self.value = value
        self.else_value = else_value

    def __dir__(self):
        return self.__super__() + ["value", "else_value"]


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


class OperationUnionByName(OperationBinaryNode):
    def __init__(self, left, right):
        super().__init__("union_by_name", left, right)


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
