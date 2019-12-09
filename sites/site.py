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
        self.locktable = dict() #format {<int:variable>:['R/W',set(<str:trans id>)]}
        self.buffer = dict() #store the values changed for each transaction before commit
                            #format{<str:trans id>:{<int:variable>:<int:value>...}}
        self.pre_version = dict() #store variable values from previous versions
                                #format {<int:commit_timestamp>:variable list}
        self.read_available = [True]*10
        self.recovered_map = {} #store variable values with list of transaction
        for i in range(2,21,2):
            self.variable[i-1] = 10*i
        if self.siteid % 2 == 0:
            self.variable[self.siteid - 2] = (self.siteid - 1) * 10
            self.variable[self.siteid - 2 + 10] = (self.siteid + 10 - 1) * 10
        self.pre_version[0] = [self.variable[i] for i in range(len(self.variable))]

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
    def check_lock(self, trans, wait_list):
        """
        check whether the transaction can get the lock
        input: transaction, variable, wait_list
        output: True/False
        side effect: None
        """
        var = trans.op.var
        if var not in self.locktable:
            #the variable is not locked
            # print(wait_list)
            if var in wait_list and len(wait_list[var]) != 0 and trans not in wait_list[var]:
                return False
            return True
        else:
            #variable has a lock
            if self.locktable[var][0] == "R":
                #has a read lock
                if trans.op.op_type == "R":
                    #new op is read
                    if var in wait_list and len(wait_list[var]) != 0:
                        return False
                    return True
                else: # new op is write
                    if len(self.locktable[var][1])==1 and trans.transid in self.locktable[var][1]:
                        if var not in wait_list or len(wait_list[var]) == 0 or (wait_list[var][0]==trans):
                            return True
                    return False
            else: # has a write lock
                if trans in self.locktable[var][1] and trans.op.op_type == "W":
                    #already has a write lock
                    return True
                return False

    def lock(self, trans, wait_list, block_list):
        """
        access a lock or add to wait_list and block_list
        input: operation, transaction, variable, wait_list, block_list
        output: True/False
        side effect: if lock cannot be accessed, add transaction to wait_list and block_list
            otherwise access the lock
        """
        var = trans.op.var
        if self.check_lock(trans, wait_list):
            #add lock to locktable
            if var not in self.locktable:
                self.locktable[var] = [trans.op.op_type,set([trans.transid])]
            else:
                if trans.transid not in self.locktable[var][1]:
                    self.locktable[var][1].add(trans.transid)
            if var in wait_list:
                if trans in wait_list[var]:
                    wait_list[var].pop(0)
            return True
        else:
            if var not in wait_list:
                wait_list[var] = []
            if trans.transid not in block_list:
                block_list[trans.transid] = set()
            if len(wait_list[var]) == 0:
                for i in self.locktable[var][1]:
                    if i != trans.transid:
                        block_list[trans.transid].add(i)
            else:
                sublist = wait_list[var]
                idx = len(sublist)-1
                item = sublist[idx]
                if item.op.op_type == "W":
                    if item != trans:
                        block_list[trans.transid].add(item.transid)
                else:
                    if trans.op.op_type == "R":
                        block_list[trans.transid] = [ele for ele in block_list[item.transid]]
                    else:
                        while(idx >= 0 and sublist[idx].op.op_type=="R"):
                            if trans != sublist[idx]:
                                block_list[trans.transid].add(item.transid)
                            idx-=1
            if trans not in wait_list[var]:
                wait_list[var].append(trans)
                
            return False

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



    def unlock(self, transaction):
        """
        unlock all the lock made by the target transaction
        Author: Xinsen Lu
        input: transaction
        output: return_list
        side effect: None
        """
        #clean lock
        id = transaction.transid
        keys = []
        return_list = None
        for key, value in self.locktable.items():
            if id in value[1]:
                value[1].remove(id)
                if len(value[1])==0:
                    keys.append(key)
        for key in keys:
            _ =  self.locktable.pop(key, None)
        #clean buffer
        if transaction.ifabort == False:
            return_list = self.commit_trans(id, transaction.endtime)
        _ = self.buffer.pop(id, None)
        return return_list

    def commit_trans(self, trans_id, time):
        """
        commit all the update made by the target transaction
        Author: Xinsen Lu
        input: trans_id
        output: return_list
        side effect: None
        """

        return_list = set()
        #variable update
        if trans_id in self.buffer:
            for key, value in self.buffer[trans_id].items():
                # minus one because variable ranges from 0 to 19 in self.variable
                self.variable[key-1] = value
                if key%2==0:
                    self.read_available[(key-1)//2]=True
                    if key in self.recovered_map:
                        return_list.update(self.recovered_map[key])

        #pre-version update
        self.pre_version[time] = []
        for i in range(len(self.variable)):
            self.pre_version[time].append(self.variable[i])
    
        return return_list