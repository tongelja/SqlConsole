

import sys, getpass, re
try:
    import pypyodbc, cx_Oracle
except:
    print('Unable to import python modules')
    sys.exit(2)


class SqlConsole:
    def __init__(self):
        self.connection_info = {}
        self.conn = None
        self.curs = None
        self.results = []

    def mssqlOpen(self, server=None, user=None, password=None, port=1433):
        self.connection_info['type'] = 'MSSql'
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
        results = conn.cursor().execute('select @@SERVERNAME').fetchone()
        self.connection_info['host'] = results[0]
        results = conn.cursor().execute('select db_name()').fetchone()
        self.connection_info['database'] = results[0]


    def oracleOpen(self, server=None, database=None, user=None, password=None):
        self.connection_info['type'] = 'Oracle'
        if server is None:
            server = input('Server ==> ')

        if database is None:
            database = input('Database ==> ')

        if user is None:
            user = input('User ==>')

        if password is None:
            password = getpass.getpass('Password ==>')

        if user.upper() == 'SYS':
            mode =  cx_Oracle.SYSDBA
        else:
            mode = ''
        dsn = server + ':1521/' + database
        print('Connecting to ' +dsn)
        conn = cx_Oracle.connect(user, password, dsn, mode)
        self.conn = conn
        curs = conn.cursor()
        results = curs.execute('select host_name from v$instance').fetchone()
        self.connection_info['host'] = results[0]
        curs = conn.cursor()
        results = curs.execute('select name from v$database').fetchone()
        self.connection_info['database'] = results[0]


    def __sql_prompt__(self):
        sql_stmt = ''
        text = ''
        print('\n')
        prompt = self.connection_info['host'].upper() + '/' + self.connection_info['database'].lower() 
        print('-'* (len(prompt) + 5))
        print('| ' + prompt)
        print('-'* (len(prompt) + 5))

        text = input('\nSQL> ').upper()

        while True:
            if text.strip() == '/':
                break
            elif re.search('.*;$', text):
                sql_stmt += ' ' + str(text)
                break
            elif text.strip() == 'EXIT':
                return 'EXIT'
            else:
                sql_stmt += ' ' + str(text)
            text = input('      ')

        sql_stmt = re.sub('(\n|;$)', ' ', sql_stmt).strip()
        return sql_stmt

    def __write_table__(self):
        row_length = len(self.results[0])
        max_length = [1] * row_length

        i = self.results_description
        for j in range(len(self.results_description)):
            if max_length[j] < len(str(i[0])):
                max_length[j] = len(str(i[0]))

        for i in self.results:
            if max_length[j] < len(str(i[0])):
                max_length[j] = len(str(i[0]))


        print('\n', end='')
        for j in range(len(i)):
            length = max_length[j]
            print('{0:<{width}}'.format(str(i[0]), width=length), end='  ')

        print('\n', end='')
        for j in range(len(i)):
            length = max_length[j]
            print('{0:<{width}}'.format('-'*length, width=length), end='  ')
            
        for i in self.results:
            print('\n', end='')
            for j in range(len(i)):
                length = max_length[j]
                print('{0:<{width}}'.format(str(i[j]), width=length), end='  ')


    def __parse_mssql_sql_query__(self, sql_stmt):
        self.curs = self.conn.cursor()

        output      = self.curs.execute(sql_stmt)
        description = output.description

        self.results             = [ r for r in output ]
        self.results_description = description

        self.__write_table__()


    def __parse_oracle_sql_query__(self, sql_stmt):
        self.curs = self.conn.cursor()

        output = self.curs.execute(sql_stmt)
        description = output.description

        self.results             = [ r for r in output ]
        self.results_description = description

        self.__write_table__()


    def __parse_sql__(self, sql_stmt):
        if self.conn == 'None':
                print('\nERROR: No SQL Connection')
                sys.exit(2)

        exec_query = {'MSSql'  : 'self.__parse_mssql_sql_query__(sql_stmt)',
                      'Oracle' : 'self.__parse_oracle_sql_query__(sql_stmt)' }

        if re.search('^\s*SAVE', sql_stmt.upper(), re.I):
            action = 'SAVE'
        else:
            action = 'QUERY'

        if action.upper() == 'SAVE':
            pass
        else:
            eval( exec_query[ self.connection_info['type'] ]  )

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

