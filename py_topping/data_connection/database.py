import pandas as pd
from sqlalchemy import create_engine

class da_tran_SQL :
    """interact with SQL"""
    def __init__(self, sql_type, host_name, database_name, user = '', password = '' , credentials_path = ''
                , chunksize = 150, partition_size = 5000, parallel_dump = False, max_parallel = 2, **kwargs):
        """Create connection to SQL Server"""
        sql_type = sql_type.upper()
        self.sql_type = sql_type
        self.chunksize = int(chunksize)
        if sql_type == 'BIGQUERY' : self.method = None
        else : self.method = 'multi'
        self.partition_size = int(partition_size)
        self.parallel_dump = parallel_dump
        self.max_parallel = int(max_parallel)
        type_dic = {
                    'MSSQL' : ['mssql', 'pymssql', '1433','[',']','EXEC'],
                    'MYSQL' : ['mysql', 'pymysql','3306','`','`','CALL'],
                    'POSTGRESQL' : ['postgresql', 'psycopg2','5432','"','"','SELECT * FROM '],
                    'SQLITE' : ['sqlite', 'sqlite', '','[',']','EXEC'],
                    'BIGQUERY' : ['bigquery', 'bigquery','','`','`','CALL'],
                    }
        
        if (sql_type == 'SQLITE') & (chunksize == 150) :
            self.chunksize = 50

        if type_dic.get(sql_type,'Error') == 'Error' :
            raise Exception("Please Insert Type of your Database with these range \n{}".format(list(type_dic.keys())))
        
        if (kwargs.get('driver',None) == 'pyodbc') & (kwargs.get('parameter', None) == None) :
            raise Exception("Please Insert ODBC's Driver in parameter agument\nfor example, for MSSQL : parameter = 'driver=SQL+Server'")
        
        if kwargs.get('parameter', None) == None : additional_param = ''
        else : additional_param = '?' + kwargs['parameter']

        self.begin_name, self.end_name, self.call_SP = type_dic[sql_type][3], type_dic[sql_type][4], type_dic[sql_type][5]
        
        if (sql_type == 'SQLITE') & (kwargs.get('driver',None) == None) :
            connection_str = """{}:///{}{}""".format(type_dic[sql_type][0] , host_name , additional_param)
        elif sql_type == 'BIGQUERY' :
            connection_str = "{}://{}/{}".format(type_dic[sql_type][0],host_name,database_name)
        else :
            connection_str = """{}+{}://{}:{}@{}:{}/{}{}""".format(type_dic[sql_type][0],kwargs.get('driver',type_dic[sql_type][1])
                                                            ,user,password,host_name,kwargs.get('port',type_dic[sql_type][2])
                                                            ,database_name , additional_param)
        
        if (credentials_path != '') & (sql_type == 'BIGQUERY') :
            self.engine = create_engine(connection_str, credentials_path = credentials_path)
        else :
            self.engine = create_engine(connection_str)
        print(pd.read_sql_query("""SELECT 'Connection OK'""", con = self.engine).iloc[0,0]) 

    def sub_dump(self, df_in,table_name_in,mode_in) :
        """normal to_sql for dask's delayed in dump_main"""
        df_in.to_sql(table_name_in, con = self.engine, index = False,if_exists = mode_in,chunksize = self.chunksize, method = self.method)
        return len(df_in)

    def dump_main(self, df_in, table_name_in ,mode_in) :
        """Divide and Dump Dataframe"""
        if len(df_in) <= self.partition_size : sum_len = self.sub_dump(df_in,table_name_in,mode_in) 
        else :
            if self.parallel_dump : 
                from dask import delayed
                dask_dump = delayed(self.sub_dump)
            i, j, sum_len, df_length = 0, 1, 0, len(df_in)
            while i < df_length :
                i_next = i + self.partition_size
                if self.parallel_dump :
                    sum_len += dask_dump(df_in.iloc[i:i_next,:],table_name_in,mode_in) 
                    if (j == self.max_parallel) | (i_next >= df_length) : 
                        sum_len, j = sum_len.compute(), 1
                        if sum_len < i_next - 1  : 
                            raise Exception("Parallel Dump Error Around row {} to row {}".format(int(i_next - self.partition_size*j),i_next))
                else : sum_len += self.sub_dump(df_in.iloc[i:i_next,:],table_name_in,mode_in) 
                j += 1
                i += self.partition_size
        return sum_len


    def read(self, table_name_in, condition_in = '', SP = False, param = ''):
        """Read Table or View or Store Procedure"""
        if SP :
            if self.sql_type == 'POSTGRESQL' : return 'SP/Function None Avaliable for PostgreSQL, right now'
            sql_q = self.call_SP + ' ' + table_name_in + ' '
            if param == '' : raise Exception("Please insert SP's parameter")
            elif type(param) != dict : raise Exception("param must be dict")
            else :
                n = 0
                for i in param.keys() :
                    if type(param[i]) == str : param[i] = """'{}'""".format(param[i])
                    if self.sql_type in ['MYSQL','POSTGRESQL'] : 
                        if n == 0 : sql_q += ' (' 
                        else : sql_q += ' , '
                        sql_q += """ {}""".format(param[i])
                        if n + 1 == len(param.keys()) : sql_q += ') ' 
                    elif self.sql_type in ['MSSQL'] : 
                        if n > 0 : sql_q += ' , '
                        sql_q += """ {} = {}""".format(i, param[i])
                    else :
                        if n > 0 : sql_q += ' , '
                        sql_q += """ {} = {}""".format(i, param[i])
                    n += 1
        else :
            sql_q = """SELECT * FROM {}{}{}""".format(self.begin_name,table_name_in,self.end_name)
            if not condition_in == '' :
                sql_q += """ WHERE """ + condition_in
        return pd.read_sql_query(sql_q , con = self.engine)

    def write_logic_sql(self, key , value_in):
        """Write SQL Condition Query that include >, <, ="""
        #logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' '
        value_in['type'] = value_in.get('type','')
        if value_in.get('value','there is no value') != 'there is no value' :
            if ('date' in value_in['type']) | ('time' in value_in['type']):  # Datetime will need '' in SQL Statement
                logic_query = 'CAST(' + self.begin_name+ str(key) + self.end_name + ' AS {}) '.format(value_in['type']) + value_in['logic'] + ' '
                logic_query += """'""" + str(value_in['value']) + """'"""
            else : 
                logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' ' + str(value_in['value'])
#                logic_query += 
        else : raise Exception("Please insert value data")  # will get error if not specific value
        return logic_query

    def dump_whole(self, df_in, table_name_in , fix_table = False, debug = False) :
        """Delete exists table and replace with new df"""
        try :
            if fix_table : 
                print('Delete Existing data from Table at ',pd.Timestamp.now())
                self.engine.execute("""DELETE FROM {}{}{}""".format(self.begin_name,table_name_in,self.end_name))
            else :
                print('Drop Existing Table at ',pd.Timestamp.now())
                self.engine.execute("""DROP TABLE {}{}{}""".format(self.begin_name,table_name_in,self.end_name))
        except Exception as e:
            print('Delete error or Do not have table to delete at',pd.Timestamp.now())
            if debug : print(e)
        #Dump df_in to database
        self.dump_main( df_in, table_name_in ,mode_in = 'append') 
        print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def write_in_sql(self,df_in , key) :
        """Write SQL Condition Query 'in (x,x,x)'"""
        filter_filter = tuple(df_in[key].fillna('Will BE rEpLaCe wItH NULL').astype('str').unique())
        if len(filter_filter) == 1 : 
            if 'Will BE rEpLaCe wItH NULL' in filter_filter :
                logic_query = '({}{}{} IS NULL'.format(self.begin_name , key, self.end_name) + ')'
            else :
                filter_filter = str(filter_filter).replace(',)',')')  # tuple with 1 value will be ( x , ) => need to convert
                if self.sql_type != 'BIGQUERY':
                    logic_query = self.begin_name + key  + self.end_name + ' in ' + filter_filter
                else :
                    logic_query = 'CAST(' + self.begin_name + key  + self.end_name + ' AS STRING) in ' + filter_filter
        elif len(filter_filter) > 1 :
            filter_filter = str(filter_filter) # tuple with > 1 values will be ( x, y, z) which can be use in SQL
            if self.sql_type != 'BIGQUERY':
                logic_query = self.begin_name + key  + self.end_name + ' in ' + filter_filter
            else :
                logic_query = 'CAST(' + self.begin_name + key  + self.end_name + ' AS STRING) in ' + filter_filter
        else : logic_query = '' # Return Nothing
        #print(logic_query)
        if "'Will BE rEpLaCe wItH NULL'" in logic_query :
            #print(logic_query)
            logic_query = logic_query.replace(", 'Will BE rEpLaCe wItH NULL'",'')
            #print(logic_query)
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL',",'')
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL'",'')
            logic_query = '(' + logic_query + ' OR {}{}{} IS NULL'.format(self.begin_name , key, self.end_name) + ')'

        return logic_query

    def dump_replace(self, df_in, table_name_in, list_key, math_logic = '',debug = False):
        """Delete exists row of table in database with same key(s) as df and dump df append to table"""
        #Create SQL Query for Delete
        sql_q = """DELETE FROM {}{}{} where """.format(self.begin_name,table_name_in,self.end_name)

        if math_logic != '' : logic_list = list(math_logic.keys())
        else : logic_list = ['There_is_no_logic_math_need_to_write'] # column in logic_list will never be true

        #for single key
        if 'str' in str(type(list_key))  :
            if not list_key in df_in.columns :
                raise Exception("{} is not in df's columns".format(list_key))
            if list_key in logic_list :
                sql_q += self.write_logic_sql(list_key, math_logic[list_key])
            else :  
                sql_q += self.write_in_sql(df_in,list_key)

        #for multi keys    
        elif 'list' in str(type(list_key)) :
            if not min(x in df_in.columns for x in list_key) :
                raise Exception("Some keys are not in df's columns")
            i = 0
            while i < len(list_key) :
                filter_name = list_key[i]
                if filter_name in logic_list :
                    sql_q += self.write_logic_sql(filter_name, math_logic[filter_name])
                else :
                    sql_q += self.write_in_sql(df_in,filter_name)
                i += 1
                if i < len(list_key) : sql_q += ' and '

        #if key is not either string or list
        else :
            raise Exception("List of Key must be string or list")
        
        #Delete exiting row from table
        try :
            print('Start delete old data at',pd.Timestamp.now())
            self.engine.execute(sql_q)
            print('Delete Last '+str(list_key)+' at',pd.Timestamp.now())
        except Exception as e:
            print('Delete error or Do not have table to delete at',pd.Timestamp.now())
            if debug : print(e)
            pass
            
        if debug : print(sql_q)
        #Dump df_in append to database
        #df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = self.chunksize, method = 'multi')
        self.dump_main(df_in, table_name_in ,mode_in = 'append')
        print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def dump_new(self, df_in, table_name_in, list_key , debug = False) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""

        # Check if Table existing or not
        if table_name_in in list(self.engine.table_names()) :
            print('Start Filter Existing data from df at ',pd.Timestamp.now())
            #for single key
            if 'str' in str(type(list_key))  :
                if not list_key in df_in.columns :
                    raise Exception("{} is not in df's columns".format(list_key))       
                sql_q = 'select distinct '+ self.begin_name +  list_key + self.end_name +' from ' + table_name_in
                filter_filter = pd.read_sql_query(sql_q, con = self.engine).iloc[:,0]
                logic_filter = ~df_in[list_key].isin(filter_filter)

            #for multi keys    
            elif 'list' in str(type(list_key)) :
                if not min(x in df_in.columns for x in list_key) :
                    raise Exception("Some keys are not in df's columns")
                i = 0
                df_in['key_sql_filter'] = ''
                sql_q = 'SELECT DISTINCT '
                while i < len(list_key) :
                    filter_name = list_key[i]
                    if i != 0 : sql_q += ' , '
                    df_in['key_sql_filter'] = df_in['key_sql_filter'].astype('str') + df_in[filter_name].astype('str')
                    sql_q += self.begin_name +  filter_name + self.end_name + ' ' 
                    i += 1
                sql_q += ' FROM ' + table_name_in
                if debug : print(sql_q)
                filter_filter = pd.read_sql_query(sql_q, con = self.engine)
                filter_filter['key_sql_filter'] = ''
                for i in filter_filter.columns :
                    if  i == 'key_sql_filter' : pass
                    else : filter_filter['key_sql_filter'] = filter_filter['key_sql_filter'].astype('str') + filter_filter[i].astype('str')
                if debug : print(filter_filter.head())
                filter_filter = filter_filter['key_sql_filter'].unique()
                if debug : print('======filter======\n',filter_filter,'\n======in======\n',df_in.head())
                logic_filter = ~df_in['key_sql_filter'].isin(filter_filter)
                df_in = df_in.drop('key_sql_filter', axis = 1)

            #if key is not either string or list
            else :
                raise Exception("List of Key must be string or list")

            df_in = df_in[logic_filter]
            
        else : print('Table not existing at ',pd.Timestamp.now())

        #Dump df_in append to database
        #df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = self.chunksize, method = 'multi')
        self.dump_main( df_in, table_name_in ,mode_in = 'append') 
        print('Dump data to ',table_name_in,' End ', pd.Timestamp.now())

    def dump_new_old(self, df_in, table_name_in, list_key) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""
        print('Start Filter Existing data from df at ',pd.Timestamp.now())

        #for single key
        if 'str' in str(type(list_key))  :
            if not list_key in df_in.columns :
                raise Exception("{} is not in df's columns".format(list_key))       
            sql_q = 'select distinct '+ self.begin_name +  list_key + self.end_name +' from ' + table_name_in
            filter_filter = pd.read_sql_query(sql_q, con = self.engine).iloc[:,0]
            logic_filter = ~df_in[list_key].isin(filter_filter)

        #for multi keys    
        elif 'list' in str(type(list_key)) :
            if not min(x in df_in.columns for x in list_key) :
                raise Exception("Some keys are not in df's columns")
            i = 0
            while i < len(list_key) :
                filter_name = list_key[i]
                sql_q = 'select distinct '+ self.begin_name +  filter_name + self.end_name +' from ' + table_name_in
                filter_filter = pd.read_sql_query(sql_q, con = self.engine).iloc[:,0]
                if i == 0 : logic_filter = df_in[filter_name].isin(filter_filter)
                else : logic_filter = (logic_filter) & (df_in[filter_name].isin(filter_filter))
                i += 1

            logic_filter = ~logic_filter

        #if key is not either string or list
        else :
            raise Exception("List of Key must be string or list")

        df_in = df_in[logic_filter]

        #Dump df_in append to database
        #df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = self.chunksize, method = 'multi')
        self.dump_main(df_in, table_name_in ,mode_in = 'append')
        print('Dump data to ',table_name_in,' End ', pd.Timestamp.now())