class Site:
    """
    Site class
    Author: Yiming Li
    """
    def __init__(self, ID):
        """
        initialize variable values in the site, even index variables in all sites,
        odd index variables in site (1+index mod 10)
        Author: Yiming Li
        input: None
        output: None
        side effect: the variables stored in the site are initialized and put into pre_version
        """
        self.siteid = ID    #site 1, 2, 3, ...
        self.status = "ON" #ON, OFF
        self.version = 0    #commit version????
        self.variable = [None]*20 #variable values
        self.locktable = dict() #format {<int:variable>:['read/write',[<str:trans id>]]}
        self.buffer = dict() #store the values changed for each transaction before commit
                            #format{<str:trans id>:[{<int:variable>:<int:value>}]}
        self.pre_version = dict() #store variable values from previous versions
                                #format {<int:commit_timestamp>:variable list}
        for i in range(2,21,2):
            self.variable[i-1] = 10*i
        if self.siteid % 2 == 0:
            self.variable[self.siteid - 2] = (self.siteid - 1) * 10
            self.variable[self.siteid - 2 + 10] = (self.siteid + 10 - 1) * 10
        self.pre_version[0] = self.variable

    def get_variable(self):
        """
        get all variables stored in the site
        Author: Yiming Li
        input: None
        output: site variables
        side effect: None
        """
        return self.variable

    """
    read & write
    """
    def lock(self, op, trans, var, wait_list, block_list):
        """
        check whether a transaction can access a lock
        the transaction type is RW not RO
        input: operation, transaction, variable, wait_list, block_list
        output: True/False
        side effect: if lock cannot be accessed, add transaction to wait_list and block_list
        """
        if trans.type == "RW":
            return True
        if op.type == "R":
            # read operation
            
        else: #write operation


    def read(self, trans, var, wait_list, block_list):
        """
        read variable from site
        Author: Yiming Li
        input: transaction ID(string), variable(int), wait_bool(boolean)
        output: operation and variable value
            operation:
                1: read - successfully read from site, add to locktable if needed
                2: wait - cannot access read lock, add to wait list
        side effect: None
        """
        #read-only transaction reads from previous commit
        if trans.type == "RO":
            #read from last commit, no lock required
            key_list = list(self.pre_version.keys())
            for i in reversed(key_list):
                if trans.time > i:
                    version = i
            return 'read', self.pre_version[version][var-1]
        else: #read-write transactions
            if var not in self.locktable:
                # no lock on the variable
                self.locktable[var] = ['read',[trans]]
                return 'read', self.variable[var-1]
            else: # has a lock on the variable
                if self.locktable[var][0] == 'write':
                    #a write lock
                    if trans in self.locktable[var][1]:
                        #transaction already has a write lock on this variable
                        return 'read', self.buffer[trans][var]
                    else: #other transaction has the write, wait
                        if var in wait_list:

                        return 'wait', 0
                else: #a read lock
                    if wait_bool:
                        #exist transaction waiting in wait list

                #already has a write lock on this variable, read directly
                return 'read', self.buffer[trans][var]
            elif trans not in self.locktable[var][1] and self.locktable[var][0] == 'write':
                #variable write lock hold by others, need to add to wait table
                return 'wait', 0
            elif self.locktable[var][0] == 'read':
                #share read lock with others
                self.locktable[var][1].append(trans)
                return 'read', self.variable[var-1]

    def write(self, trans, var, val):
        """
        write to a variable in buffer in a site
        input: transaction ID(string), variable(int), value(int)
        output:
            write: successfully write to buffer
            wait: cannot access write lock, add to wait list
            read: variable has a read lock, check wait list in TM
        side effect: buffer changes according to write values
        """
        add_buf = dict()
        add_buf[var] = val
        if self.locktable[var][0] == 'write' and trans in self.locktable[var][1]:
            #already has a write lock, write to buffer
            if trans in self.buffer:
                self.buffer[trans].append(add_buf)
            else:
                self.buffer[trans] = [add_buf]
            return 'write'
        elif self.locktable[var][0] == 'write' and trans not in self.locktable[var][1]:
            #write lock hold by other transaction, wait
            return 'wait'
        elif var not in self.locktable:
            self.locktable[var] = ['write',[trans]]
            if trans in self.buffer:
                self.buffer[trans].append(add_buf)
            else:
                self.buffer[trans] = [add_buf]
            return 'write'
        elif self.locktable[var][0] == 'read' and (trans not in self.locktable[var][1]
                                                   or (trans in self.locktable[var][1] and len(self.locktable[var][1] != 1))):
            #read lock hold by other transaction, wait
            return 'wait'
        elif self.locktable[var][0] == 'read' and trans in self.locktable[var][1] and len(self.locktable[var][1] == 1):
            #transaction has read lock, check wait table to see if it can get write lock
            return 'read'


    def dump(self):
        """
        output the value of all variables in a site
        Author: Yiming Li
        input: None
        output: value of all 20 variables
        side effect: None
        """
        print("site {} -".format(self.siteid), end = " ")
        for i in range(20):
            if self.variable[i] is None:
                continue
            if i != 19:
                print("x{}: {},".format(i+1, self.variable[i]), end = " ")
            else:
                print("x{}: {}".format(i+1, self.variable[i]))

    def failed(self):
        """
        fail a site and clear the lock table
        Author: Yiming Li
        input: None
        output: return a list of transactions needed to abort when they commit
        side effect: site status changes to OFF, and lock table is cleared
        """
        self.status = "OFF"
        trans_to_abort = []
        for k,v in self.locktable.items():
            for tran in v[1]:
                if tran not in trans_to_abort:
                    trans_to_abort.append(tran)
        #search for the lock table
        #find out which variable is locked by which transaction
        #and mark this trans to abort when commit
        #then clear the lock table.
        self.locktable.clear()
        return trans_to_abort

    def recovered(self):
        """
        recover a site
        Author: Yiming Li
        input: None
        output: None
        side effect: site status changes to ON
        """
        self.status = "ON"
