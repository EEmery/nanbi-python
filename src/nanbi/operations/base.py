class Operation:
    def __str__(self):
        pass

    def __repr_(self):
        pass

    def eval(self):
        pass

    def __dir__(self):
        pass


class OperationLeaf(Operation):
    def __str__(self):
        pass

    def __repr_(self):
        pass

    def eval(self):
        pass

    def __dir__(self):
        pass


class OperationBinaryNode(Operation):
    def __init__(self, symbol, left, right):
        self.symbol = symbol
        self.left = left
        self.right = right

    def __str__(self):
        return f"({self.left} {self.symbol} {self.right})"

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return ["symbol", "left", "right"]


class OperationUnaryNode(Operation):
    def __init__(self, symbol, next):
        self.symbol = symbol
        self.next = next

    def __str__(self):
        return f"{self.symbol}({self.next})"

    def __repr__(self):
        return self.__str__()

    def __dir__(self):
        return ["symbol", "next"]
