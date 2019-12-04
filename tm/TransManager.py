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
        self.wait_list = dict() #wait table{<int:variable>:[<str:transid>]}
        self.block_list = dict()

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

    def read(self, trans, var):
        status = "failed"
        if var % 2 != 0: #odd variable, read from only one site
            if self.site_list[var % 10].status == "ON":
                status, val = self.site_list[var % 10].read(trans, var)
        else: #even variable, try any up site
            for i in range(10):
                if self.site_list[i].status == "ON":
                    status, val = self.site_list[i].read(trans, var)
                    if status == 'read':
                        break
        if status == "fail":
            #fail to read, abort
            trans.set_status("ABORTED")
        elif status == "read":
            #successfully read from site
            print("x{}: {}".format(var,val))
        elif status == "wait":
            #no access to variable, add to waitlist
            self.wait_list.append(trans)

    def write(self, trans, var, val):
        """
        check write operation from site.write() and finish writing procedure
        input: transaction ID(string), variable(int), value(int)
        output: None
        side effect: value written to site buffer, update wait list
        """


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
        for i in range(1,11):
            if i == site_id:
                continue
            if self.site_list[i].status == "ON":
                s = self.site_list[i]
                break
        for v in range(2,21,2):
            self.site_list[site_id-1].variable[v-1] = s.variable[v-1]

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
