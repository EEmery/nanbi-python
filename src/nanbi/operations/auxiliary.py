class Window:
    def __init__(self, partition_by=None, order_by=None):
        self.partition_by_cols = partition_by
        self.order_by_cols = order_by
        self.partition_by = self._partition_by
        self.order_by = self._order_by

    def _partition_by(self, *partition_by):
        self.partition_by_cols = partition_by
        return self

    def _order_by(self, *order_by):
        self.order_by_cols = order_by
        return self

    @staticmethod
    def partition_by(*partition_by):
        return Window(partition_by=partition_by)

    @staticmethod
    def order_by(*order_by):
        return Window(order_by=order_by)
