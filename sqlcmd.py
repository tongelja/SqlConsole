import SqlConsole, getpass
o = SqlConsole.SqlConsole()
host       = input('Host: ')
db         = input('Service: ')
user       = input('User: ')
password   = getpass.getpass('Password: ')
o.oracleOpen(host, db, user, password)
o.cmd()
