class Op:
    """
    Operation class
    """
    def __init__(self, op_type, var, transaction, value=None):
        self.op_type = op_type
        self.var = var
        self.value = value
        self.transaction = transaction
    