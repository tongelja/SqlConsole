import os

class HereHost:
    def __init__(self):

        self.host_file        = os.environ['HOME'] + '/work/oracle_host'
        self.dbs         = {}


        if os.path.isfile(self.host_file):
            hosts = open(self.host_file, 'r').readlines()
        else:
            hosts = [] 
       
        for i in hosts:
            i = i.split()
            db = i[0].strip()
            host = i[1].strip()
            user = i[2].replace('\n', '').strip()

            self.dbs[db] = {'db': db, 'host': host, 'user': user} 

    def getDb(self, db):
        return self.dbs[db]['db']
 
    def getHost(self, db):
        return self.dbs[db]['host']

    def getUser(self, db):
        return self.dbs[db]['user']

    def addDb(self, db, host, user):
        f = open(self.host_file, 'a')
        line = f.write( db + ' ' + host + ' ' + user + '\n')
        f.close()

   
