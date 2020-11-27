import pandas as pd
from sqlalchemy import create_engine

class da_tran_SQL :
    def __init__(self, sql_type, host_name, database_name, user, password , **kwargs):
        """Create connection to SQL Server"""
        
        type_dic = {'MSSQL' : ['mssql', 'pymssql', '1433']}
        
        if type_dic.get(sql_type,'Error') == 'Error' :
            raise Exception("Please Insert Type of your Database with these range \n{}".format(list(type_dic.keys())))
        
        connection_str = str(type_dic[sql_type][0] + '+' + kwargs.get('driver',type_dic[sql_type][1])
                           + '://' + user + ':' + password + '@' + host_name
                           + ':' + kwargs.get('port',type_dic[sql_type][2]) + '/' + database_name)
        self.engine = create_engine(connection_str)
        print(pd.read_sql_query("""SELECT 'Connection to {} in {} : OK'""".format(database_name,host_name), con = self.engine).iloc[0,0]) 

    def read(self, table_name_in, condition_in = ''):
        """Read Table or View"""
        sql_q = """SELECT * FROM [{}]""".format(table_name_in)
        if not condition_in == '' :
            sql_q += """ WHERE """ + condition_in
        return pd.read_sql_query(sql_q , con = self.engine)


    def dump_replace(self, df_in, table_name_in, list_key):
        """Delete exists row of table in database with same key(s) as df and dump df append to table"""
        #Create SQL Query for Delete
        sql_q = 'delete from ' + table_name_in + ' where '

        #for single key
        if 'str' in str(type(list_key))  :
            if not list_key in df_in.columns :
                raise Exception("{} is not in df's columns".format(list_key))       
            filter_filter = tuple(df_in[list_key].astype('str').unique())
            filter_filter = str(filter_filter)
            sql_q += '[' + list_key  + ']' + ' in ' + filter_filter

        #for multi keys    
        elif 'list' in str(type(list_key)) :
            if not min(x in df_in.columns for x in list_key) :
                raise Exception("Some keys are not in df's columns")
            i = 0
            while i < len(list_key) :
                filter_name = list_key[i]
                filter_filter = tuple(df_in[filter_name].astype('str').unique())
                filter_filter = str(filter_filter)
                
                if i > 0 : sql_q += ' and '
                
                sql_q += '[' + filter_name  + ']' + ' in ' + filter_filter
                i += 1

        #if key is not either string or list
        else :
            raise Exception("List of Key must be string or list")

        #Delete exiting row from table
        try :
            print('Start delete old data at',pd.Timestamp.now())
            print('\n',sql_q,'\n')
            self.engine.execute(sql_q)
            print('Delete Last '+str(list_key)+' at',pd.Timestamp.now())
        except :
            print('Delete error or Do not have table to delete at',pd.Timestamp.now())
            pass

        #Dump df_in append to database
        df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = 150, method = 'multi')
        print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def dump_new(self, df_in, table_name_in, list_key) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""
        print(table_name_in,' Start ',pd.Timestamp.now())
        #for single key
        if 'str' in str(type(list_key))  :
            if not list_key in df_in.columns :
                raise Exception("{} is not in df's columns".format(list_key))       
            sql_q = 'select distinct '+ '[' +  list_key + ']' +' from ' + table_name_in
            filter_filter = pd.read_sql_query(sql_q, con = self.engine).iloc[:,0]
            logic_filter = ~df_in[list_key].isin(filter_filter)

        #for multi keys    
        elif 'list' in str(type(list_key)) :
            if not min(x in df_in.columns for x in list_key) :
                raise Exception("Some keys are not in df's columns")
            i = 0
            while i < len(list_key) :
                filter_name = list_key[i]
                sql_q = 'select distinct '+ '[' +  filter_name + ']' +' from ' + table_name_in
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
        df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = 150, method = 'multi')
        print('Dump data to ',table_name_in,' End ', pd.Timestamp.now())