

import sys, getpass, re
try:
    import pypyodbc
except:
    print('Unable to import python modules')
    sys.exit(2)


class mssqlConsole:
    def __init__(self):
        self.type      = 'SQLServer'
        self.conn = None
        self.curs = None
        self.results = []

    def open(self, server=None, user=None, password=None, port=1433):
        if server is None:
            server = input('Server ==> ')

        if user is None:
            user = input('User ==>')

        if password is None:
            password = getpass.getpass('Password ==>')

        print('Connecting to ' + user + '/*********' + '@' + server + ':' + str(port)  )
        conn_string = 'driver=/usr/local/lib/libtdsodbc.so;server=' + server + ';port=' + str(port) + ';uid=' + user + ';pwd=' + password
        conn = pypyodbc.connect(conn_string)
        self.conn = conn 

    def __sql_prompt__(self):
        sql_stmt = input('\nTSQL> ')
        return sql_stmt

    def __write_table__(self):
        row_length = len(self.results[0])
        max_length = [1] * row_length

        for i in self.results:
            for j in range(len(i)):
                if max_length[j] < len(str(i[j])):
                    max_length[j] = len(str(i[j]))

        for i in self.results:
            print('\n', end='')
            for j in range(len(i)):
                length = max_length[j]
                print('{0:<{width}}'.format(str(i[j]), width=length), end='  ')


    def __parse_sql_query__(self, sql_stmt):
        self.curs = self.conn.cursor()
        output = self.curs.execute(sql_stmt)
        self.results = [ r for r in output ]
        self.__write_table__()


    def __parse_sql__(self, sql_stmt):
        if self.conn == 'None':
                print('\nERROR: No SQL Connection')
                sys.exit(2)

        if re.search('^\s*SAVE', sql_stmt.upper(), re.I):
            action = 'SAVE'
        elif re.search('^\s*DESC', sql_stmt.upper(), re.I):
            action = 'DESC'
        else:
            action = 'QUERY'

        if action.upper() == 'DESC':
            pass
        else:
            self.__parse_sql_query__(sql_stmt)

    def cmd(self):
        if self.conn is None:
            print('No Connection')
            sys.exit(2)

        while True:
            sql_stmt = self.__sql_prompt__()
            if sql_stmt.upper() == 'EXIT':
                break
            elif sql_stmt.upper() == '':
                pass
            else:
                self.__parse_sql__(sql_stmt)

