#!/bin/python3

import sys, getpass, re, traceback, os, traceback, subprocess

try:
    import pypyodbc, cx_Oracle
except:
    print('Unable to import python modules')
    sys.exit(2)


class SqlDb:
    def __init__(self):
        self.connection_info = {}
        self.conn = None
        self.curs = None
        self.results01 = {}
        self.saved_results = {}
        self.results = []

        self.connected_user = ''

        self.keyword_values = [ 'EXIT', 'SAVE' , 'DESC', 'DESCRIBE', '@', 'RUN' ]

        self.path = ['/Users/jtongeli/work/scripts/instantclient_12_1_6/', '/usr/local/bin/']

        self.user = None
        self.password = None
        self.database    = None
        self.server  = None
        

    def add_path(self, directory):
        self.path = self.path + ':' + directory


    def cmd(self, sql_stmt=None):
        if self.conn is None:
            print('No Connection')
            sys.exit(2)

        if sql_stmt != None:
            self.parse_sql(sql_stmt)
        else:
            while True:
                sql_stmt = self.sql_prompt()
                if sql_stmt.upper() == 'EXIT':
                    break
                elif sql_stmt.upper() == '':
                    pass
                else:
                    try:
                        self.parse_sql(sql_stmt)
                    except Exception as err:
                        print('\nERROR parsing SQL\n')
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=4, file=sys.stdout)


    def parse_sql(self, sql_stmt):
        if self.conn == 'None':
                print('\nERROR: No SQL Connection')
                sys.exit(2)

        match =  re.search('^\s*(.+?)(\s|$)+', sql_stmt, re.I)
        keyword_input = match.group(1).upper()
        match = re.search('(.)', keyword_input)
        keyword_first_char = match.group(1)

        if keyword_input == 'SAVE':
            self.save_results(sql_stmt)

        elif keyword_input == 'DESCRIBE' or keyword_input == 'DESC':
            self.desc(sql_stmt)

        elif keyword_input == 'RUN' or keyword_first_char == '@':
            self.script(sql_stmt)

        else:
            self.query(sql_stmt)



    def write_table(self):
        if len(self.results01['data']) == 0:
            print('No data found')
            return

        row_length = len(self.results01['data'][0])
        max_length = [1] * row_length

        i = self.results01['desc']
        for j in range(len(i)):
            if max_length[j] < len(str(i[j][0])):
                max_length[j] = len(str(i[j][0]))

        for i in self.results01['data']:
            for j in range(len(i)):
                if max_length[j] < len(str(i[j])):
                    max_length[j] = len(str(i[j]))


        i = self.results01['desc']
        print('\n', end='')
        for j in range(len(i)):
            length = max_length[j]
            print('{:<{width}}'.format(str(i[j][0]).upper(), width=length), end='  ')

        print('\n', end='')
        for j in range(len(i)):
            length = max_length[j]
            print('{:<{width}}'.format('-'*length, width=length), end='  ')

        for i in self.results01['data']:
            print('\n', end='')
            for j in range(len(i)):
                length = max_length[j]
                print('{:<{width}}'.format(str(i[j]), width=length), end='  ')
        print('\n', end='')


    def save_results(self, sql_stmt):

        match = re.search('save (\w+)', sql_stmt, re.I)
        if match:
           result_name = match.group(1).lower()
        else:
            result_name = 'default_save'

        self.saved_results[result_name] = self.results01['data']


    def sql_prompt(self):
        sql_stmt = ''
        text = ''
        print('\n')
        prompt = self.connection_info['host'].split('.')[0].upper() + '/' + self.connection_info['database'].lower()
        print('-'* (len(prompt) + 5))
        print('| ' + prompt)
        print('-'* (len(prompt) + 5))

        text = input('\nSQL> ')

        while True:

            ##
            ## Keyword matching in this section to allow keywords to end with no ';'
            ## This functionality replicates other SQL consoles
            ##

            match =  re.search('^\s*(\w+)(\s|$)+', text, re.I)
            if match:
                keyword_input = match.group(1).upper()
            else:
                keyword_input = ''

            match =  re.search('^\s*(.)', text, re.I)
            if match:
                keyword_first_char_input = match.group(1).upper()
            else:
                keyword_first_char_input = ''

            if keyword_input in self.keyword_values or keyword_first_char_input in self.keyword_values:
                sql_stmt = text
                break
            elif text.strip() == '':
                sql_stmt = text
                break
            elif text.strip() == '/' or text.strip == ';':
                break
            elif re.search('.*;$', text):
                sql_stmt += ' ' + str(text)
                break
            else:
                sql_stmt += ' ' + str(text)
            text = input('      ')

        sql_stmt = re.sub('(\n|;$|\/$)', ' ', sql_stmt).strip()
        return sql_stmt



class Mssql(SqlDb):

    def open(self, server=None, user=None, password=None, port=1433):
        self.connection_info['type'] = 'MSSql'
        if server is None:
            server = input('Server ==> ')

        if user is None:
            user = input('User ==>')

        if password is None:
            password = getpass.getpass('Password ==>')

        self.user = user
        self.server = server
        self.password = password

        print('Connecting to ' + user + '/*********' + '@' + server + ':' + str(port)  )
        conn_string = 'driver=/usr/local/lib/libtdsodbc.so;server=' + server + ';port=' + str(port) + ';uid=' + user + ';pwd=' + password
        conn = pypyodbc.connect(conn_string)
        self.conn = conn 
        results = conn.cursor().execute('select @@SERVERNAME').fetchone()
        self.connection_info['host'] = results[0]
        results = conn.cursor().execute('select db_name()').fetchone()
        self.connection_info['database'] = results[0]

    def query(self, sql_stmt):
        self.curs = self.conn.cursor()

        output      = self.curs.execute(sql_stmt)

        if hasattr(output, 'description'):
            description = output.description
        else:
            description = 'None'

        description = output.description

        self.results01['data'] = [ r for r in output ]
        self.results01['desc'] = description
        self.results = self.results01['data']

        self.write_table()

    def script(self, sql_stmt):

        match = re.search('(@|run\s+)?(.*)', sql_stmt, re.I)

        if match:
            script_name = match.group(2)
            script = open(script_name).read().strip()
        script = re.sub('\n', ' ', script)
        sql_stmt = re.search('(select .*)(\;$|\/$)', script, re.I).group(1)
        #sql_stmt = re.sub('(;|\/)', ' ', sql_stmt)

        match = re.search('(&\w+)', sql_stmt, re.I)
        if match:
            input_variables=match.groups()
        else:
            input_variables=[]

        var_translation = {}
        for i in range(len(input_variables)):
            var_substitute = input('Value for ' + input_variables[i] + ':  ')
            var_translation[input_variables[i]] = var_substitute
            sql_stmt = re.sub(input_variables[i], var_substitute, sql_stmt)


        print('File: ' + script_name)
        print('SQL: ' + sql_stmt)

        self.query(sql_stmt)


    def desc(self, sql_stmt):

        match = re.search('desc(ribe)? (.*)', sql_stmt, re.I)
        if match:
            fullpath_object = match.group(2)

        if len(fullpath_object.split('.')) == 2:
            db_owner  = fullpath_object.split('.')[0].upper()
            db_object = fullpath_object.split('.')[1].upper()
        elif len(object.split('.')) == 1:
            db_object = fullpath_object
        else:
            db_owner=''
            db_object = ''

        sql_stmt = 'exec sp_columns ' + db_object
        self.query(sql_stmt)

    def sqlcmd(self, directory=None):
        for i in self.path:
            filename = i + '/sqlcmd'
            if os.path.isfile(filename):
                cmd = filename + ' -U ' + self.user + ' -P ' + self.password  + ' -S ' + self.server 
                print('Running ' + cmd)
                subprocess.call(cmd, shell=True)



class Oracle(SqlDb):

    def open(self, server=None, database=None, user=None, password=None):
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

        self.user = user
        self.database = database
        self.server = server
        self.password = password

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
 
        self.connected_user = user.upper()


    def execproc(self, procedure_name, input_variables):
       
        self.curs = self.conn.cursor()

        try:
            output = self.curs.callproc(procedure_name, input_variables)

        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nERROR parsing SQL Procedure\n')
            print(str(err))



    def query(self, sql_stmt):
        self.curs = self.conn.cursor()
        self.results01 = {}

        try:
            output = self.curs.execute(sql_stmt)

            if hasattr(output, 'description'):
                description = output.description
            else:
                description = 'None'

            if output is None:
                self.results01['data'] = [['Complete']]
                self.results01['desc'] = [['Call Results']]
            else:
                self.results01['data'] = [ r for r in output ]
                self.results01['desc'] = description
                self.results = [ r for r in self.results01['data'] ]

            self.write_table()

        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nERROR parsing SQL: ' + sql_stmt +'\n')


    def script(self, sql_stmt):

        match = re.search('(@|run\s+)?(.*)', sql_stmt, re.I)

        if match:
            script_name = match.group(2)
            script = open(script_name).read().strip()
        script = re.sub('\n', ' ', script)
        sql_stmt = re.search('(select .*)(\;$|\/$)', script, re.I).group(1)
        #sql_stmt = re.sub('(;|\/)', ' ', sql_stmt)

        match = re.search('(&\w+)', sql_stmt, re.I)
        if match:
            input_variables=match.groups()
        else:
            input_variables=[]

        var_translation = {}
        for i in range(len(input_variables)):
            var_substitute = input('Value for ' + input_variables[i] + ':  ')
            var_translation[input_variables[i]] = var_substitute
            sql_stmt = re.sub(input_variables[i], var_substitute, sql_stmt) 
        
        
        print('File: ' + script_name)
        print('SQL: ' + sql_stmt)

        self.query(sql_stmt)

    def desc(self, sql_stmt):

        match = re.search('desc(ribe)? (.*)', sql_stmt, re.I)
        if match:
            fullpath_object = match.group(2)

        if len(fullpath_object.split('.')) == 2:
            db_owner  = fullpath_object.split('.')[0].upper()
            db_object = fullpath_object.split('.')[1].upper().replace('V$','V_$')
            
        elif len(fullpath_object.split('.')) == 1:
            db_object = fullpath_object.upper().replace('V$','V_$')
            db_owner='SYS'
        else:
            db_owner=''
            db_object = ''

        sql_stmt = "select rpad(column_name, 40, ' ' ) ||  data_type || '(' ||  data_length || ')'  as " + db_object + \
                   " from dba_tab_columns where owner = '" + db_owner + "' and table_name = '" + db_object + "'"

        self.query(sql_stmt)

    def sqlplus(self, directory=None):
        for i in self.path:
            filename = i + '/sqlplus'
            if os.path.isfile(filename):
                if self.user.lower() == 'sys': 
                    as_sysdba = ' as sysdba'
                else:
                    as_sysdba = ' '
                cmd = filename + ' ' + self.user + '/' + self.password  + '@' + self.server + ':1521/' + self.database + as_sysdba

                print('Running ' + cmd)
                subprocess.call(cmd, shell=True)        
 




