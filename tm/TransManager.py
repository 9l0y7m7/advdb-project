from site.site import Site
from trans.transaction import Transaction
class TransactionManager:
    """
    Transaction Manager
    Author: Yiming Li & Xinsen Lu
    """
    def __init__(self):
        """
        create new sites and initialize all variables in each site
        append sites to site_list
        input: None
        output: None
        side effect: site_list in TM will have all info of 10 sites
        """
        self.trans_list = dict()    #{<str:transid>: Transaction}}
        self.site_list = [Site(0)]     #site list
        for i in range(10):
            new_site = Site(i+1)
            self.site_list.append(new_site)
        self.wait_list = dict() #wait table{<int:variable>:[transaction id]}
        self.block_list = dict() #{<str:transid>:set(transid)}

    def dump(self, var = -1):
        """
        output committed values of all copies of all variables at all sites
        or output values of a variable from all sites(easy to debug)
        this function will call dump() in class Site
        Author: Yiming Li
        input: variable(optional):int
        output: committed variable values of all sites, or value of one variable from all sites
        side effect: None
        """
        if var != -1:
            #output based on variable
            print("x{} - ".format(var), end = " ")
            for i in range(1,11):
                if self.site_list[i].variable[var-1] is not None:
                    print("site{}: {};".format(i, self.site_list[i].variable[var-1]), end = " ")
            print()
        else:
            for i in range(1,11):
                self.site_list[i].dump()

    def read(self, transid):
        """
        read a value from available sites
        input: trans id
        output: the value of the variable
        side effect: access lock, or add to wait_list and block_list
        """
        var = self.trans_list[transid].op.var
        if self.trans_list[transid].type == "RO":
            #read only transaction
            roval = -1
            for i in range(1,11):
                if self.site_list[i].status == "ON":
                    if self.site_list[i].variable[var-1] == None:
                        continue
                    else:
                        keys = list(self.site_list[var%10+1].pre_version.keys())
                        for j in reversed(keys):
                            if j < self.trans_list[transid].time:
                                key = j
                                break
                        roval = self.site_list[var%10+1].pre_version[key][var-1]
                        break
            if roval == -1:
                self.trans_list[transid].ifabort = True
            else:
                print("x{}:{}".format(var,roval))
        else: #read write transaction
            if var % 2 != 0: #odd variable
                if self.site_list[var%10+1].status == "ON":
                    if self.site_list[var%10+1].lock(self.trans_list[transid], self.wait_list, self.block_list):
                        val = self.site_list[var%10+1].variable[var-1]
                    print("x{}:{}".format(var,val))
                else:
                    self.trans_list[transid].ifabort = True
            else: # even variable
                flag = False
                for i in range(1,11):
                    if self.site_list[i].status == "ON" and self.site_list[var%10+1].lock(self.trans_list[transid], self.wait_list, self.block_list):
                        val = self.site_list[i].variable[var-1]
                        print("x{}:{}".format(var,val))
                        flag = True
                        break
                if not flag:
                    self.trans_list[transid].ifabort = True

    def write(self, transid, val):
        """
        write a value to a variable, or cannot access lock
        input: transaction ID(string), value(int)
        output: None
        side effect: value written to buffer, or add to wait_list and block_list
        """
        var = self.trans_list[transid].op.var
        val = self.trans_list[transid].op.value
        add_buf = {}
        add_buf[var] = val
        if var % 2 != 0: #odd variable
            if self.site_list[var%10+1].status == "ON":
                if self.site_list[var%10+1].lock(self.trans_list[transid], self.wait_list, self.block_list):
                    if transid not in self.site_list[var%10+1].buffer:#???
                        self.site_list[var%10+1].buffer[transid] = {}
                        self.site_list[var%10+1].buffer[transid] = add_buf
                    else:
                        self.site_list[var%10+1].buffer[transid].add(var, val)
            else:
                self.trans_list[transid].ifabort = True
        else: #even variable
            flag = True
            index = 0
            for i in range(1,11):
                if self.site_list[i].status == "OFF":
                    continue
                else:
                    if not self.site_list[i].check_lock(self.trans_list[transid], self.wait_list):
                        flag = False
                        index = i
                        break
            if not flag:
                #add to wait list and block list
                if var in self.wait_list:
                    self.wait_list[var].append(transid)
                else:
                    self.wait_list[var] = [transid]
                if transid in self.block_list:
                    for i in self.site_list[index].locktable[var][1]:
                        self.block_list[transid].add(i)
                else:
                    self.block_list[transid] = {}
                    for i in self.site_list[index].locktable[var][1]:
                        self.block_list[transid].add(i)
            else:
                for i in range(1,11):
                    if self.site_list[i].status == "OFF":
                        continue
                    else:
                        if self.site_list[i].lock(self.trans_list[transid], self.wait_list, self.block_list):
                            if transid in self.site_list[i].buffer:
                                self.site_list[i].buffer[transid][var] = val
                            else:
                                self.site_list[i].buffer[transid] = {}
                                self.site_list[i].buffer[transid][var] = val

    def fail(self, site_id):
        """
        fail a site according to the site id
        call failed() in class Site and set all related transactions to "ABORTED"
        Author: Yiming Li
        input: site id(int)
        output: site:<site_id> fails
        side effect: status of transactions from list(trans_to_abort) are set to "ABORTED"
        """
        trans_to_abort = self.site_list[site_id - 1].failed()
        print("site {} fails".format(site_id))
        for trans in trans_to_abort:
            t = self.trans_list[trans]
            t.ifabort = True

    def recover(self, site_id):
        """
        recover a site according to the site id
        call recovered() in class Site
        Author: Yiming Li
        input: site id(int)
        output: site <site_id> recovers
        side effect: non-replicated variables remain unchanged, replicated variables
                    are read from other up sites
        """
        self.site_list[site_id - 1].recovered()
        print("site {} recovers".format(site_id))
        #use option 1 in transproc slide(page 46) to recover the site
        #for i in range(1,11):
        #    if i == site_id:
        #        continue
        #    if self.site_list[i].status == "ON":
        #        s = self.site_list[i]
        #        break
        #for v in range(2,21,2):
        #    self.site_list[site_id-1].variable[v-1] = s.variable[v-1]

    def begin(self, trans_id, trans_type, time):
        """
        initialize a target transaction
        Author: Xinsen Lu
        input: trans_id, trans_type, time
        output: True or False
        side effect: None
        """
        newTrans = Transaction(trans_id, trans_type, time)
        if trans_id in self.trans_list:
            return False
        else:
            self.trans_list[trans_id] = newTrans
            return True
    def resume(self, trans_id):
        """
        resume a target transaction
        Author: Xinsen Lu
        input: trans_id
        output: None
        side effect: None
        """
        trans = self.trans_list[trans_id]
        op = trans.op
        if op != None:
            if op.op_type == "R":
                self.read(trans_id)
            else:
                self.write(trans_id)
        return


    def end(self, trans_id, time):
        """
        end a target transaction
        Author: Xinsen Lu
        input: trans_id
        output: transaction commit(True) or abort(Fail)
        side effect: None
        """
        trans = self.trans_list[trans_id]
        trans.endtime = time
        #unlock
        for i in range(1, 11):
            self.site_list[i].unlock(trans)

        #clear block_list
        resume_list = []
        for key, value in self.block_list.items():
            if trans_id in value:
                self.block_list[key].remove(trans_id)
                if len(self.block_list[key])==0:
                    resume_list.append(key)

        #resume
        for item in resume_list:
            for key, value in self.wait_list.items():
                if len(value)>0 and value[0] == item:
                    self.resume(item)
        return not trans.ifabort

    def detect_deadlock(self):
        """
        detect a deadlock
        Author: Xinsen Lu
        input: None
        output: trans_id or ""
        side effect: None
        """
        outdeg = {}
        edges = {}
        for key in self.trans_list:
            outdeg[key] = 0
        for key, item in self.block_list.items():
            outdeg[key] = len(item)
            for ele in item:
                if ele not in edges:
                    edges[ele] = []
                edges[ele].append(key)
        q = []
        for key, item in outdeg.items():
            if item == 0:
                q.append(key)
        while len(q) >0:
            tmp = q.pop(0)
            for item in edges[tmp]:
                outdeg[item]-=1
                if outdeg[item]==0:
                    q.append(item)
        res = ""
        for key, item in outdeg.items():
            if item != 0:
                if len(res) == 0:
                    res = key
                else:
                    if self.trans_list[key].time > self.trans_list[res].time:
                        res = key
        return res
