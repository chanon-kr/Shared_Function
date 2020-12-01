import pandas as pd
from sqlalchemy import create_engine

class da_tran_SQL :
    """interact with SQL"""
    def __init__(self, sql_type, host_name, database_name, user, password , **kwargs):
        """Create connection to SQL Server"""
        
        type_dic = {'MSSQL' : ['mssql', 'pymssql', '1433']}
        
        if type_dic.get(sql_type,'Error') == 'Error' :
            raise Exception("Please Insert Type of your Database with these range \n{}".format(list(type_dic.keys())))
        
        connection_str = str(type_dic[sql_type][0] + '+' + kwargs.get('driver',type_dic[sql_type][1])
                           + '://' + user + ':' + password + '@' + host_name
                           + ':' + kwargs.get('port',type_dic[sql_type][2]) + '/' + database_name)
        self.engine = create_engine(connection_str)
        print(pd.read_sql_query("""SELECT 'Connection OK'""", con = self.engine).iloc[0,0]) 

    def read(self, table_name_in, condition_in = '', SP = False, param = ''):
        """Read Table or View or Store Procedure"""
        if SP :
            sql_q = """EXEC """ + table_name_in + ' '
            if param == '' : raise Exception("Please insert SP's parameter")
            elif type(param) != dict : raise Exception("param must be dict")
            else :
                n = 0
                for i in param.keys() :
                    if type(param[i]) == str : param[i] = """'{}'""".format(param[i])
                    if n > 0 : sql_q += ' , '
                    sql_q += """ {} = {}""".format(i, param[i])
                    n += 1
        else :
            sql_q = """SELECT * FROM [{}]""".format(table_name_in)
            if not condition_in == '' :
                sql_q += """ WHERE """ + condition_in
        return pd.read_sql_query(sql_q , con = self.engine)

    def write_logic_sql(self, key , value_in):
        """Write SQL Condition Query that include >, <, ="""
        logic_query = '['+ str(key) + '] ' + value_in['logic'] + ' '
        value_in['type'] = value_in.get('type','')
        if value_in.get('value','there is no value') != 'there is no value' :
            if ('date' in value_in['type']) | ('time' in value_in['type']):  # Datetime will need '' in SQL Statement
                logic_query += """'""" + str(value_in['value']) + """'"""
            else : logic_query += str(value_in['value'])
        else : raise Exception("Please insert value data")  # will get error if not specific value
        return logic_query

    def dump_whole(self, df_in, table_name_in) :
        """Delete exists table and replace with new df"""
        print('Start Filter Existing data from df at ',pd.Timestamp.now())
        #Dump df_in to database
        df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'replace',chunksize = 150, method = 'multi')
        print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def write_in_sql(self,df_in , key) :
        """Write SQL Condition Query 'in (x,x,x)'"""
        filter_filter = tuple(df_in[key].astype('str').unique())
        if len(filter_filter) == 1 : 
            filter_filter = str('(' + str(filter_filter[0]) + ')') # tuple with 1 value will be ( x , ) => need to convert
            logic_query = '[' + key  + ']' + ' in ' + filter_filter
        elif len(filter_filter) > 1 :
            filter_filter = str(filter_filter) # tuple with > 1 values will be ( x, y, z) which can be use in SQL
            logic_query = '[' + key  + ']' + ' in ' + filter_filter
        else : logic_query = '' # Return Nothing
        return logic_query

    def dump_replace(self, df_in, table_name_in, list_key, math_logic = ''):
        """Delete exists row of table in database with same key(s) as df and dump df append to table"""
        #Create SQL Query for Delete
        sql_q = 'delete from ' + table_name_in + ' where '

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
        except :
            print('Delete error or Do not have table to delete at',pd.Timestamp.now())
            pass

        #Dump df_in append to database
        df_in.to_sql(table_name_in,con = self.engine,index = False,if_exists = 'append',chunksize = 150, method = 'multi')
        print('Dump data to ',table_name_in,' End ',pd.Timestamp.now())

    def dump_new(self, df_in, table_name_in, list_key) :
        """Delete exists row of df that has same key(s) as table and dump df_in append to table"""
        print('Start Filter Existing data from df at ',pd.Timestamp.now())

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