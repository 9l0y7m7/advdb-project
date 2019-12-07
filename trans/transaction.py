from trans.op import Op
class Transaction:
    """
    Transaction class
    Author: Yiming Li & Xinsen Lu
    """
    def __init__(self, ID, trans_type, time):
        self.transid = ID       #T1, T2, ...
        self.type = trans_type  #RW, RO
        self.status = "RUNNING"
        self.ifabort = False
        self.time = time
        self.endtime = None
        self.op = None
        self.allstatus = ["RUNNING","BLOCKED","COMMITTED"]


    def update_op(self, op):
        self.op = op

    def get_status(self):
        """
        get transaction status
        Author: Yiming Li
        input: None
        output: transaction status
        side effect: None
        """
        return self.status

    def set_status(self, newstatus):
        """
        set transaction status
        Author: Yiming Li
        input: new status to update(string)
        output: None
        side effect: transaction status changed
        """
        if newstatus in self.allstatus:
            self.status = newstatus
