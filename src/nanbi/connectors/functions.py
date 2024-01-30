from nanbi.operations.auxiliary import Window
from nanbi.adapters.column import Column
import nanbi.operations.node as op


window = Window

def when(condition, value, else_value=None):
    if else_value is not None:
        return Column(op.OperationWhen(condition.op, value.op, else_value.op))

    return Column(op.OperationWhen(condition.op, value.op))
