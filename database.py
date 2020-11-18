import os 
import pandas as pd
import numpy as np
from datetime import datetime
import pymssql
from sqlalchemy import create_engine

def dump_replace_existsame(df_in, con_dict, table_name_in,filter_in) :
    """Create connection to database, delete exists row of table in database with key filter_in and dump df_in append to table"""
    print(table_name_in,' Start ',datetime.now())
    #Create connection to database
    connection_str = str(con_dict['type'] + '+' + con_dict['driver'] 
                    + '://' + con_dict['user'] + ':' + con_dict['pass'] + '@' + con_dict['host'] 
                    + ':' + con_dict['port'] + '/' + con_dict['database'])

    engine = create_engine(connection_str)
    sql_q = 'delete from ' + table_name_in + ' where '

    #Create SQL Query for Delete
    i = 0
    while i < len(filter_in) :
        filter_name = filter_in[i]
        filter_filter = tuple(df_in[filter_name].astype('str').unique())
        #prevent error for 1 filter row by add ()
        if len(filter_filter) == 1 :
            filter_filter = "('" + filter_filter[0] + "')"
        else :
            filter_filter = str(filter_filter)

        #Add "and" for second columns 
        if i > 0 :
            sql_q = sql_q + ' and '

        sql_q = sql_q + '[' + filter_name  + ']' + ' in ' + filter_filter
        i += 1

    #Delete exiting row from table
    try :
        print('start delete')
        engine.execute(sql_q)
        print('Delete Last '+str(filter_in)+' at',datetime.now())
    except :
        print('Delete error or Do not have table to delete at',datetime.now())
        pass

    #Dump df_in append to database
    df_in.to_sql(table_name_in,con = engine,index = False,if_exists = 'append',chunksize = 150, method = 'multi')
    print(table_name_in,' End ',datetime.now())
    return True

def dump_only_new(df_in, con_dict, table_name_in,filter_in) :
    """Create connection to database, delete exists row of df that has key filter_in and dump df_in append to table --not test yet"""
    print(table_name_in,' Start ',datetime.now())
    #Create connection to database
    connection_str = str(con_dict['type'] + '+' + con_dict['driver'] 
                    + '://' + con_dict['user'] + ':' + con_dict['pass'] + '@' + con_dict['host'] 
                    + ':' + con_dict['port'] + '/' + con_dict['database'])

    engine = create_engine(connection_str)

    #Loop delete --may get error , not test yet
    for i_in in filter_in :
        #get key values from database
        filter_name = filter_in
        sql_q = 'select distinct '+ '[' +  filter_name + ']' +' from ' + table_name_in
        filter_filter = pd.read_sql_query(sql_q, con = engine111)

        #delete existing key from df_in
        filter_filter = ~df_in[filter_name].astype('str').isin(filter_filter[filter_name].astype('str'))
        df_in = df_in[filter_filter]

    #Dump df_in append to database
    df_in.to_sql(table_name_in,con = engine111,index = False,if_exists = 'append',chunksize = 150, method = 'multi')
    print(table_name_in,' End ',datetime.now())
    return True

# # Sample 
"""
config_db = {
            'type' :'BAHBAHBAH',  # type of database you use
            'driver':'BAHBAHBAH',  # type of driver you use
            'user' : 'BAHBAHBAH', # user to connect database
            'pass' : 'BAHBAHBAH', # password to connect database
            'host' :'BAHBAHBAH' , # server's name or server's ip
            'port' : 'BAHBAHBAH', # port number
            'database' : 'BAHBAHBAH' # database's name
            }

df = pd.DataFrame({'BAH':[1,2,3,4],'BAHBAH':[1,2,3,4],'BAHBAHBAH':[1,2,3,4]}) # Pandas dataframe
filter_in = ['BAH' , 'BAHBAH' , 'BAHBAHBAH'] # List of Key column
table = 'BAHBAHBAH' # Table's name

dump_replace_existsame(df, config_db, table ,filter_in)
dump_only_new(df, config_db, table ,filter_in)
"""