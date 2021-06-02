### Not Test
# def lazy_SQL(sql_type, host_name, database_name, user = '', password = '' , credentials_path = ''
#                , chunksize = 150, partition_size = 5000, parallel_dump = False, max_parallel = 2) :
#     from .data_connection.database import da_tran_SQL
#     return da_tran_SQL(sql_type, host_name, database_name, user , password , credentials_path 
#                 , chunksize , partition_size , parallel_dump , max_parallel )
# from .data_connection.sharepoint import da_tran_SP365 as lazy_SP365
# from .data_connection.sharepoint import da_tran_SP_PRIM as lazy_SP_PRIM
# from .run_pipeline import run_pipeline