import pandas as pd
from sqlalchemy import create_engine , inspect, text
import os
from urllib.parse import quote_plus
from datetime import datetime


class lazy_SQL :
    """interact with SQL : Update 2022-06-03

    :sql_type = string - type of database, support MSSQL, MYSQL, POSTGRESQL, SQLITE, BIGQUERY

    :host_name = string 
       - for MSSQL, MYSQL, POSTGRESQL : database's hostname or ip address
       - for SQLITE : location of db file
       - for BIGQUERY : project id on GCP

    : database_name = string
       - for MSSQL, MYSQL, POSTGRESQL : database's name
       - for SQLITE : Leave as ''
       - for BIGQUERY : dataset's name

    : user = string - User for MSSQL, MYSQL, POSTGRESQL

    : password = string - password for MSSQL, MYSQL, POSTGRESQL

    : credentials_path = string - path to GCP credentials file (json)

    more sample of this class at https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb
    """
    def __init__(self, sql_type, host_name, database_name, user = '', password = '' , credentials_path = None
                , chunksize = 150, partition_size = 5000, parallel_dump = False, max_parallel = 2, mute = False, **kwargs):
        """Create connection to SQL"""
        sql_type = sql_type.upper()
        self.sql_type = sql_type
        self.chunksize = int(chunksize)
        self.mute = mute
        password = quote_plus(str(password))
        if sql_type == 'BIGQUERY' : self.method = None
        else : self.method = 'multi'
        self.partition_size = int(partition_size)
        self.parallel_dump = parallel_dump
        if parallel_dump : print('Parallel dump has been remove since 0.3.18, this parameter will remove in 0.3.19')
        self.max_parallel = int(max_parallel)
        type_dic = {
                    'MSSQL' : ['mssql', 'pytds', '1433','[',']','EXEC'],
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
            from google.oauth2 import service_account
            self.project_id = host_name
            # self.dataset = self.begin_name + database_name + self.end_name + '.'
            self.dataset = database_name + '.'
            if credentials_path == None : 
                self.credentials, self.credentials_path = None, None
                self.engine = create_engine(connection_str)
            else : 
                self.engine = create_engine(connection_str, credentials_path = credentials_path)
                self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.credentials_path = credentials_path
            if not self.mute : print(pd.read_gbq("""SELECT 'Connection OK'""",project_id = self.project_id,credentials = self.credentials).iloc[0,0]) 
        else :
            self.credentials = ''
            self.credentials_path  = None
            self.dataset = ''
            self.engine = create_engine(connection_str)
            if not self.mute : 
                with self.engine.connect() as conn :
                    print(pd.read_sql_query(text("""SELECT 'Connection OK'"""), con = conn).iloc[0,0]) 

    def read(self, table_name_in, condition_in = '', SP = False,raw = False, param = '', columns_list = []):
        """Read Table or View or Store Procedure"""
        columns_read = ''
        if len(columns_list) == 0 :columns_read = '*'
        else : 
            for i in columns_list :
                if columns_read != '' : columns_read += ', '
                columns_read += '{}{}{}.{}{}{}'.format(self.begin_name,table_name_in,self.end_name  ,  self.begin_name,i,self.end_name)
        if SP :
            if self.sql_type == 'POSTGRESQL' : return 'SP/Function None Avaliable for PostgreSQL, right now'
            sql_q = self.call_SP + ' ' + self.dataset + table_name_in + ' '
            if param == '' : raise Exception("Please insert SP's parameter")
            elif type(param) != dict : raise Exception("param must be dict")
            else :
                n = 0
                for i in param.keys() :
                    if type(param[i]) == str : param[i] = """'{}'""".format(param[i])
                    if self.sql_type in ['MYSQL','POSTGRESQL','BIGQUERY'] : 
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
        elif raw : sql_q = str(table_name_in)
        else :
            sql_q = """SELECT {} FROM {}{}{}{}""".format(columns_read, self.dataset,self.begin_name,table_name_in,self.end_name)
            if not condition_in == '' :
                sql_q += """ WHERE """ + condition_in
        if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
            return pd.read_gbq(sql_q, project_id = self.project_id,credentials = self.credentials)
        else :
            with self.engine.connect() as conn :
                out_ = pd.read_sql_query(text(sql_q) , con = conn)
            return out_

    # def sub_dump(self, df_in,table_name_in,mode_in) :
    #     """normal to_sql for dask's delayed in dump_main"""
    #     if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
    #         # print('{}{}'.format(self.dataset , table_name_in)) # For Debug
    #         df_in.to_gbq('{}{}'.format(self.dataset , table_name_in),project_id = self.project_id
    #                                     ,credentials = self.credentials , if_exists = mode_in) #, reauth = True
    #     else :
    #         with self.engine.connect() as conn :
    #             df_in.to_sql(table_name_in, con = conn, index = False,if_exists = mode_in,chunksize = self.chunksize, method = self.method)
    #     return len(df_in)

    def dump_main(self, df_in, table_name_in ,mode_in, partition_size = '', con = '') :
        """Divide and Dump Dataframe Into Database"""
        con = self.engine if con == '' else con
        partition_size = self.partition_size if partition_size == '' else partition_size
        i, j, sum_len, df_length = 0, 1, 0, len(df_in)
        while i < df_length :
            i_next = i + partition_size
            # sum_len += self.sub_dump(df_in.iloc[i:i_next,:],table_name_in,mode_in) 
            df_dump = df_in.iloc[i:i_next,:]
            sum_len += len(df_dump)
            if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
                # print('{}{}'.format(self.dataset , table_name_in)) # For Debug
                df_dump.to_gbq('{}{}'.format(self.dataset , table_name_in),project_id = self.project_id
                                            ,credentials = self.credentials , if_exists = mode_in) #, reauth = True
            else :
                # with self.engine.connect() as con :
                df_dump.to_sql(table_name_in, con = con, index = False,if_exists = mode_in,chunksize = self.chunksize, method = self.method)
            j += 1
            i += partition_size
        return sum_len

    def write_logic_sql(self, key , value_in):
        """Write SQL Condition Query that include >, <, ="""
        #logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' ' # For Debug
        value_in['type'] = value_in.get('type','')
        if value_in.get('value','there is no value') != 'there is no value' :
            if ('date' in value_in['type']) | ('time' in value_in['type']):  # Datetime will need '' in SQL Statement
                logic_query = 'CAST(' + self.begin_name+ str(key) + self.end_name + ' AS {}) '.format(value_in['type']) + value_in['logic'] + ' '
                logic_query += """'""" + str(value_in['value']) + """'"""
            else : 
                logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' ' + str(value_in['value'])
        else : raise Exception("Please insert value data")  # will get error if not specific value
        return logic_query

    def dump_whole(self, df_in, table_name_in , fix_table = False, debug = False) :
        """Delete exists table and replace with new df"""
        result = {}
        with self.engine.connect() as con : 
            trans = con.begin()
            try :
                if inspect(self.engine).has_table(table_name = table_name_in) :
                    if fix_table : 
                        if not self.mute : print('Delete Existing data from Table at ',pd.Timestamp.now())
                        if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
                        # with self.engine.connect() as con :
                        con.execute(text("""DELETE FROM {}{}{}""".format(self.begin_name,table_name_in,self.end_name)))
                    else :
                        if not self.mute : print('Drop Existing Table at ',pd.Timestamp.now())
                        if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
                        # with self.engine.connect() as con :
                        con.execute(text("""DROP TABLE {}{}{}""".format(self.begin_name,table_name_in,self.end_name)))
                else :
                    if not self.mute : print('Do not have table to delete at',pd.Timestamp.now())
                #Dump df_in to database
                if not self.mute : print('Dump data to ',table_name_in,' Begin ',pd.Timestamp.now())
                self.dump_main( df_in, table_name_in ,mode_in = 'append', con = con) 
                trans.commit()
                if not self.mute : print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())
                result['result'] = True
            except Exception as e :
                print(e)
                if not self.mute : print('Error During Dump to ',table_name_in,' Begin Rollback ',pd.Timestamp.now())
                trans.rollback()
                if not self.mute : print('Rollback ',table_name_in,' End ',pd.Timestamp.now())
                result['result'], result['error'] = False, e
            return result


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
            #print(logic_query) # For Debug
            logic_query = logic_query.replace(", 'Will BE rEpLaCe wItH NULL'",'')
            #print(logic_query) # For Debug
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL',",'')
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL'",'')
            logic_query = '(' + logic_query + ' OR {}{}{} IS NULL'.format(self.begin_name , key, self.end_name) + ')'
        return logic_query

    def delete_old_data(self, df_in, table_name_in, list_key, math_logic, debug, con = '') :
        """Sub Function in dump_replace to delete row in target table"""
        #Create SQL Query for Delete
        sql_q = """DELETE FROM {}{}{} where """.format(self.begin_name,table_name_in,self.end_name)

        if math_logic != '' : logic_list = list(math_logic.keys())
        else : logic_list = ['There_is_no_logic_math_need_to_write'] # column in logic_list will never be true

        #for single key
        if 'str' in str(type(list_key))  :  list_key = [list_key]
         
        if 'list' in str(type(list_key)) :
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
        if inspect(self.engine).has_table(table_name = table_name_in) :
            if not self.mute : print('Start delete old data at',pd.Timestamp.now())
            if debug : print(sql_q)
            if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
            # with self.engine.connect() as con :
            con.execute(text(sql_q))
            if not self.mute : print('Delete Last '+str(list_key)+' at',pd.Timestamp.now())
        else :
            if not self.mute : print('Do not have table to delete at',pd.Timestamp.now())


    def create_merge_statement(self, sql_type, DATASET , DESTINATION_TABLE ,TEMP_TABLE ,SCHEMA, list_key, list_column, list_value) :
        key_columns_string = ' , '.join([i for i in list_key])
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        create_index, drop_index = [], []
        for i in [DESTINATION_TABLE , TEMP_TABLE] :
            create_index.append(f"CREATE UNIQUE INDEX topping{i}{now} ON {i} ({key_columns_string})")
            if sql_type == 'postgresql' : drop_index.append(f"DROP INDEX topping{i}{now};")
            else : drop_index.append(f"DROP INDEX topping{i}{now} ON {i} ;")

        if len(list_value) == 0 : update_value_str = " , ".join([f"""{i} = SOURCE.{i}""" for i in list_key])
        else : update_value_str = " , ".join([f"""{i} = SOURCE.{i}""" for i in list_value])

        if sql_type == 'MYSQL' : 
            columns_list_str = " , ".join([i for i in list_column])
            columns_list_str2 = " , ".join([f'SOURCE.{i}' for i in list_column])
            sql_statement = f' INSERT INTO {DESTINATION_TABLE} ({columns_list_str})'
            sql_statement += f' SELECT {columns_list_str2} FROM {TEMP_TABLE}  AS SOURCE'
            sql_statement += f' ON DUPLICATE KEY UPDATE {update_value_str}'
            return sql_statement, create_index, drop_index

        if sql_type == 'MSSQL' : DATASET = f'{DATASET}.{SCHEMA}'
        
        if sql_type == 'POSTGRESQL' : sql_statement = f"""MERGE INTO {DESTINATION_TABLE} TARGET USING {TEMP_TABLE} SOURCE ON """
        else : sql_statement = f"""MERGE {DATASET}.{DESTINATION_TABLE} TARGET USING {DATASET}.{TEMP_TABLE} SOURCE ON """

        sql_statement += " AND ".join([f"""TARGET.{i} = SOURCE.{i}""" for i in list_key])
        sql_statement += f""" WHEN MATCHED THEN UPDATE SET """
        sql_statement += update_value_str

        if sql_type == 'POSTGRESQL' : sql_statement += f""" WHEN NOT MATCHED THEN INSERT ("""
        else : sql_statement += f""" WHEN NOT MATCHED BY TARGET THEN INSERT ("""

        sql_statement += " , ".join([i for i in list_column])
        sql_statement += f""") VALUES ("""      
        sql_statement += " , ".join([i for i in list_column])
        sql_statement += f""") ;"""

        return sql_statement, create_index, drop_index

    def upsert(self, df_in, table_name_in, list_key, debug = False, indexed = False, keep_temp = False) :
        list_column = df_in.columns
        list_value = [i for i in list_column if i not in list_key]
        result = {}
        if self.sql_type == 'MYSQL' : indexed = True
        TEMP_TABLE = f"{table_name_in}topping{datetime.now().strftime('%Y%m%d%H%M%S')}" 
        sql_statement, create_index, drop_index = self.create_merge_statement(
                                      sql_type = self.sql_type, DATASET = self.database_name
                                    , DESTINATION_TABLE = table_name_in
                                    , TEMP_TABLE = TEMP_TABLE
                                    , SCHEMA = 'dbo', list_key = list_key, list_column = list_column, list_value = list_value)
        with self.engine.connect() as con :
            try : 
                trans = con.begin()
                self.dump_whole( df_in , TEMP_TABLE)
                if indexed : 
                    for i in create_index : 
                        if debug : print(i)
                        con.execute(text(i))
                if debug : print(sql_statement)
                con.execute(text(sql_statement))
                if indexed : 
                    for i in drop_index : 
                        if debug : print(i)
                        con.execute(text(i))
                if not keep_temp : con.execute(text(f'DROP TABLE {TEMP_TABLE}'))
                else : result['temp'] = TEMP_TABLE
                trans.commit()
                result['result'] = True
            except Exception as e :
                if debug : print(e)
                trans.rollback()
                result['result'], result['error'] = False, e
        return result

    def dump_replace(self, df_in, table_name_in, list_key, math_logic = '', partition_delete = 50000, debug = False):
        """Delete exists row of table in database with same key(s) as df and dump df append to table"""
        result = {}
        with self.engine.connect() as con : 
            trans = con.begin()
            try :
                if len(df_in) == 0 : 
                    if not self.mute : print('No Data to dump into',table_name_in,' End ',pd.Timestamp.now())
                else :
                    if len(df_in) <= partition_delete : 
                        self.delete_old_data(df_in, table_name_in, list_key, math_logic, debug, con = con)
                    else :
                        i, df_length = 0, len(df_in)
                        while i < df_length :
                            i_next = i + partition_delete
                            if not self.mute : print("Processing {}-{} row from {} rows".format(i + 1, i_next, df_length))
                            self.delete_old_data(df_in.iloc[i:i_next,:],table_name_in, list_key, math_logic, debug, con = con)
                            i += partition_delete
                    #Dump df_in append to database
                    self.dump_main(df_in, table_name_in ,mode_in = 'append', con = con)
                    trans.commit()
                    if not self.mute : print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())
                    result['result'] = True
            except Exception as e :
                print(e)
                if not self.mute : print('Error During Dump to ',table_name_in,' Begin Rollback ',pd.Timestamp.now())
                trans.rollback()
                if not self.mute : print('Rollback ',table_name_in,' End ',pd.Timestamp.now())
                result['result'], result['error'] = False, e
            return result

    def dump_new(self, df_in, table_name_in, list_key , debug = False) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""
        result = {}
        with self.engine.connect() as con : 
            trans = con.begin()
            try :
                # Check if Table existing or not
                if inspect(self.engine).has_table(table_name = table_name_in) :
                    if not self.mute : print('Start Filter Existing data from df at ',pd.Timestamp.now())
                    df_out = df_in.copy()
                    #for single key
                    if 'str' in str(type(list_key))  :  list_key = [list_key]
                    if 'list' in str(type(list_key)) :
                        if not min(x in df_out.columns for x in list_key) :
                            raise Exception("Some keys are not in df's columns")
                        i = 0
                        df_out['key_sql_filter'] = ''
                        sql_q = 'SELECT DISTINCT '
                        while i < len(list_key) :
                            filter_name = list_key[i]
                            if i != 0 : sql_q += ' , '
                            df_out['key_sql_filter'] = df_out['key_sql_filter'].astype('str') + df_out[filter_name].astype('str')
                            sql_q += self.begin_name +  filter_name + self.end_name + ' ' 
                            i += 1
                        sql_q += ' FROM ' + self.dataset + table_name_in
                        if debug : print(sql_q)
                        if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
                            filter_filter = pd.read_gbq(sql_q ,project_id = self.project_id,credentials = self.credentials)
                        else :
                            # with self.engine.connect() as con :
                            filter_filter = pd.read_sql_query(text(sql_q), con = con)
                        # filter_filter = pd.read_sql_query(sql_q, con = self.engine)
                        filter_filter['key_sql_filter'] = ''
                        for i in filter_filter.columns :
                            if  i == 'key_sql_filter' : pass
                            else : filter_filter['key_sql_filter'] = filter_filter['key_sql_filter'].astype('str') + filter_filter[i].astype('str')
                        if debug : print(filter_filter.head())
                        filter_filter = filter_filter['key_sql_filter'].unique()
                        if debug : print('======filter======\n',filter_filter,'\n======in======\n',df_out.head())
                        logic_filter = ~df_out['key_sql_filter'].isin(filter_filter)
                        df_out = df_out.drop('key_sql_filter', axis = 1)
                    #if key is not either string or list
                    else :
                        raise Exception("List of Key must be string or list")
                    df_out = df_out[logic_filter]
                else : 
                    if not self.mute : print('Table not existing at ',pd.Timestamp.now())
                #Dump df_in append to database
                if not self.mute : print('Dump data to ',table_name_in,' Begin ',pd.Timestamp.now())
                self.dump_main( df_out, table_name_in ,mode_in = 'append', con = con) 
                trans.commit()
                if not self.mute : print('Dump data to ',table_name_in,' End ', pd.Timestamp.now())
                result['result'] = True
            except Exception as e :
                print(e)
                if not self.mute : print('Error During Dump to ',table_name_in,' Begin Rollback ',pd.Timestamp.now())
                trans.rollback()
                if not self.mute : print('Rollback ',table_name_in,' End ',pd.Timestamp.now())
                result['result'], result['error'] = False, e
            return result

class da_tran_SQL :
    """interact with SQL : Update 2021-11-25

    :sql_type = string - type of database, support MSSQL, MYSQL, POSTGRESQL, SQLITE, BIGQUERY

    :host_name = string 
       - for MSSQL, MYSQL, POSTGRESQL : database's hostname or ip address
       - for SQLITE : location of db file
       - for BIGQUERY : project id on GCP

    : database_name = string
       - for MSSQL, MYSQL, POSTGRESQL : database's name
       - for SQLITE : Leave as ''
       - for BIGQUERY : dataset's name

    : user = string - User for MSSQL, MYSQL, POSTGRESQL

    : password = string - password for MSSQL, MYSQL, POSTGRESQL

    : credentials_path = string - path to GCP credentials file (json)

    more sample of this class at https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb
    """
    def __init__(self, sql_type, host_name, database_name, user = '', password = '' , credentials_path = None
                , chunksize = 150, partition_size = 5000, parallel_dump = False, max_parallel = 2, **kwargs):
        """Create connection to SQL"""
        sql_type = sql_type.upper()
        self.sql_type = sql_type
        self.chunksize = int(chunksize)
        password = quote_plus(str(password))
        if sql_type == 'BIGQUERY' : self.method = None
        else : self.method = 'multi'
        self.partition_size = int(partition_size)
        self.parallel_dump = parallel_dump
        if parallel_dump : print('Set to use parallel dump, there is a risk to use this method, use it with caution')
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
            from google.oauth2 import service_account
            self.project_id = host_name
            # self.dataset = self.begin_name + database_name + self.end_name + '.'
            self.dataset = database_name + '.'
            if credentials_path == None : 
                self.credentials, self.credentials_path = None, None
                self.engine = create_engine(connection_str)
            else : 
                self.engine = create_engine(connection_str, credentials_path = credentials_path)
                self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.credentials_path = credentials_path
            print(pd.read_gbq("""SELECT 'Connection OK'""",project_id = self.project_id,credentials = self.credentials).iloc[0,0]) 
        else :
            self.credentials = ''
            self.credentials_path  = None
            self.dataset = ''
            self.engine = create_engine(connection_str)
            print(pd.read_sql_query("""SELECT 'Connection OK'""", con = self.engine).iloc[0,0]) 

    def sub_dump(self, df_in,table_name_in,mode_in) :
        """normal to_sql for dask's delayed in dump_main"""
        if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
            # print('{}{}'.format(self.dataset , table_name_in)) # For Debug
            df_in.to_gbq('{}{}'.format(self.dataset , table_name_in),project_id = self.project_id
                                        ,credentials = self.credentials , if_exists = mode_in) #, reauth = True
        else :
            df_in.to_sql(table_name_in, con = self.engine, index = False,if_exists = mode_in,chunksize = self.chunksize, method = self.method)
        return len(df_in)

    def dump_main(self, df_in, table_name_in ,mode_in) :
        """Divide and Dump Dataframe Into Database"""
        if len(df_in) <= self.partition_size : sum_len = self.sub_dump(df_in,table_name_in,mode_in) 
        else :
            if self.parallel_dump : 
                print('About to use parallel dump, there is a risk to use this method, use it with caution')
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


    def read(self, table_name_in, condition_in = '', SP = False,raw = False, param = '', columns_list = []):
        """Read Table or View or Store Procedure"""
        columns_read = ''
        if len(columns_list) == 0 :columns_read = '*'
        else : 
            for i in columns_list :
                if columns_read != '' : columns_read += ', '
                columns_read += '{}{}{}.{}{}{}'.format(self.begin_name,table_name_in,self.end_name  ,  self.begin_name,i,self.end_name)
        if SP :
            if self.sql_type == 'POSTGRESQL' : return 'SP/Function None Avaliable for PostgreSQL, right now'
            sql_q = self.call_SP + ' ' + self.dataset + table_name_in + ' '
            if param == '' : raise Exception("Please insert SP's parameter")
            elif type(param) != dict : raise Exception("param must be dict")
            else :
                n = 0
                for i in param.keys() :
                    if type(param[i]) == str : param[i] = """'{}'""".format(param[i])
                    if self.sql_type in ['MYSQL','POSTGRESQL','BIGQUERY'] : 
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
        elif raw : sql_q = str(table_name_in)
        else :
            sql_q = """SELECT {} FROM {}{}{}{}""".format(columns_read, self.dataset,self.begin_name,table_name_in,self.end_name)
            if not condition_in == '' :
                sql_q += """ WHERE """ + condition_in
        if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
            return pd.read_gbq(sql_q,project_id = self.project_id,credentials = self.credentials)
        else :
            return pd.read_sql_query(sql_q , con = self.engine)

    def write_logic_sql(self, key , value_in):
        """Write SQL Condition Query that include >, <, ="""
        #logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' ' # For Debug
        value_in['type'] = value_in.get('type','')
        if value_in.get('value','there is no value') != 'there is no value' :
            if ('date' in value_in['type']) | ('time' in value_in['type']):  # Datetime will need '' in SQL Statement
                logic_query = 'CAST(' + self.begin_name+ str(key) + self.end_name + ' AS {}) '.format(value_in['type']) + value_in['logic'] + ' '
                logic_query += """'""" + str(value_in['value']) + """'"""
            else : 
                logic_query = self.begin_name+ str(key) + self.end_name + ' ' + value_in['logic'] + ' ' + str(value_in['value'])
        else : raise Exception("Please insert value data")  # will get error if not specific value
        return logic_query

    def dump_whole(self, df_in, table_name_in , fix_table = False, debug = False) :
        """Delete exists table and replace with new df"""
        if table_name_in in list(self.engine.table_names()) :
            if fix_table : 
                print('Delete Existing data from Table at ',pd.Timestamp.now())
                if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
                self.engine.execute("""DELETE FROM {}{}{}""".format(self.begin_name,table_name_in,self.end_name))
            else :
                print('Drop Existing Table at ',pd.Timestamp.now())
                if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
                self.engine.execute("""DROP TABLE {}{}{}""".format(self.begin_name,table_name_in,self.end_name))
        else :
            print('Do not have table to delete at',pd.Timestamp.now())
        #Dump df_in to database
        print('Dump data to ',table_name_in,' Begin ',pd.Timestamp.now())
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
            #print(logic_query) # For Debug
            logic_query = logic_query.replace(", 'Will BE rEpLaCe wItH NULL'",'')
            #print(logic_query) # For Debug
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL',",'')
            logic_query = logic_query.replace("'Will BE rEpLaCe wItH NULL'",'')
            logic_query = '(' + logic_query + ' OR {}{}{} IS NULL'.format(self.begin_name , key, self.end_name) + ')'
        return logic_query

    def delete_old_data(self, df_in, table_name_in, list_key, math_logic, debug) :
        """Sub Function in dump_replace to delete row in target table"""
        #Create SQL Query for Delete
        sql_q = """DELETE FROM {}{}{} where """.format(self.begin_name,table_name_in,self.end_name)

        if math_logic != '' : logic_list = list(math_logic.keys())
        else : logic_list = ['There_is_no_logic_math_need_to_write'] # column in logic_list will never be true

        #for single key
        if 'str' in str(type(list_key))  :  list_key = [list_key]
         
        if 'list' in str(type(list_key)) :
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
        if table_name_in in list(self.engine.table_names()) :
            print('Start delete old data at',pd.Timestamp.now())
            if debug : print(sql_q)
            if self.credentials_path != None : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials_path
            self.engine.execute(sql_q)
            print('Delete Last '+str(list_key)+' at',pd.Timestamp.now())
        else :
            print('Do not have table to delete at',pd.Timestamp.now())

    def dump_replace(self, df_in, table_name_in, list_key, math_logic = '', partition_delete = 50000, debug = False):
        """Delete exists row of table in database with same key(s) as df and dump df append to table"""
        if len(df_in) == 0 : 
            print('No Data to dump into',table_name_in,' End ',pd.Timestamp.now())
        else :
            if len(df_in) <= partition_delete : 
                self.delete_old_data(df_in, table_name_in, list_key, math_logic, debug)
            else :
                i, df_length = 0, len(df_in)
                while i < df_length :
                    i_next = i + partition_delete
                    print("Processing {}-{} row from {} rows".format(i + 1, i_next, df_length))
                    self.delete_old_data(df_in.iloc[i:i_next,:],table_name_in, list_key, math_logic, debug)
                    i += partition_delete
            #Dump df_in append to database
            self.dump_main(df_in, table_name_in ,mode_in = 'append')
            print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def dump_new(self, df_in, table_name_in, list_key , debug = False) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""

        # Check if Table existing or not
        if table_name_in in list(self.engine.table_names()) :
            print('Start Filter Existing data from df at ',pd.Timestamp.now())
            
            df_out = df_in.copy()
            #for single key
            if 'str' in str(type(list_key))  :  list_key = [list_key]

            if 'list' in str(type(list_key)) :
                if not min(x in df_out.columns for x in list_key) :
                    raise Exception("Some keys are not in df's columns")
                i = 0
                df_out['key_sql_filter'] = ''
                sql_q = 'SELECT DISTINCT '
                while i < len(list_key) :
                    filter_name = list_key[i]
                    if i != 0 : sql_q += ' , '
                    df_out['key_sql_filter'] = df_out['key_sql_filter'].astype('str') + df_out[filter_name].astype('str')
                    sql_q += self.begin_name +  filter_name + self.end_name + ' ' 
                    i += 1
                sql_q += ' FROM ' + self.dataset + table_name_in
                if debug : print(sql_q)
                if (self.credentials != '') & (self.sql_type == 'BIGQUERY') :
                    filter_filter = pd.read_gbq(sql_q,project_id = self.project_id,credentials = self.credentials)
                else :
                    filter_filter = pd.read_sql_query(sql_q, con = self.engine)
                # filter_filter = pd.read_sql_query(sql_q, con = self.engine)
                filter_filter['key_sql_filter'] = ''
                for i in filter_filter.columns :
                    if  i == 'key_sql_filter' : pass
                    else : filter_filter['key_sql_filter'] = filter_filter['key_sql_filter'].astype('str') + filter_filter[i].astype('str')
                if debug : print(filter_filter.head())
                filter_filter = filter_filter['key_sql_filter'].unique()
                if debug : print('======filter======\n',filter_filter,'\n======in======\n',df_out.head())
                logic_filter = ~df_out['key_sql_filter'].isin(filter_filter)
                df_out = df_out.drop('key_sql_filter', axis = 1)

            #if key is not either string or list
            else :
                raise Exception("List of Key must be string or list")

            df_out = df_out[logic_filter]
            
        else : print('Table not existing at ',pd.Timestamp.now())

        #Dump df_in append to database
        print('Dump data to ',table_name_in,' Begin ',pd.Timestamp.now())
        self.dump_main( df_out, table_name_in ,mode_in = 'append') 
        print('Dump data to ',table_name_in,' End ', pd.Timestamp.now())