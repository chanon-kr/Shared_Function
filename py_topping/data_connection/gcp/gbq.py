from google.oauth2 import service_account
from google.cloud import bigquery
import string, random
from google.cloud.exceptions import NotFound
from pandas_gbq import to_gbq
from pandas import DataFrame
import json

class lazy_GBQ :
    def __init__(self, 
                 project_id : str, 
                 dataset_id= None, 
                 credentials_path = None, 
                 mute= False) :
        """
        Create a Google BigQuery connection,
        Args :
         - project_id : The default project id for connection
        Optional Args :
         - dataset_id : The default dataset id, only used for get_draft_schema method, default None
         - credentials_path : Path to your credentials file, default None
         - mute : If True, the connection will not print debuging message, default False
        """
        # Set up
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.mute = mute
        # Set up service account if needed
        if credentials_path != None : 
            self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        else : self.credentials = None
        # Create connection
        self.client= bigquery.Client(project=self.project_id, credentials=self.credentials)
        # Print connection result if not mute
        if not self.mute : 
            r= self.client.query("SELECT 'CONNECTION OK'").result().to_arrow()
            print(r[0][0])

    def insert_replace(self, 
                       dataframe : DataFrame, 
                       table_name : str, 
                       schema_path : str) :
        """
        Core insert method, using `WRITE_TRUNCATE`
        Args :
         - dataframe : A Pandas DataFrame to insert into GBQ
         - table_name : A full GBQ's table name
         - schema_path : A path to table's schema file
        """
        job_config = bigquery.LoadJobConfig(
                                        schema= self.client.schema_from_json(schema_path),
                                        write_disposition="WRITE_TRUNCATE",
                                        )
        job = self.client.load_table_from_dataframe(dataframe, table_name, job_config=job_config) 
        return job.result()

    def create_table(self,
                     table_name : str, 
                     schema_path : str, 
                     table_parameter : dict) :
        """
        A function to create GBQ Table,
        Args :
         - table_name : A full GBQ's table name
         - schema_path : A path to schema file
         - table_parameter : A dict of GBQ parameter which contains
           - clustering_fields : A list of clustering fields
           - description : A description of the table
           - labels : A dict of table's labels
           - partition_column : A Partition Column's name
           - require_partition_filter : A boolean to require partition filter or not
           - partition_type : Type of table's partition, in range, int, day, month, year
           - range_partition_start : For range/int partition_type, specific where partition start
           - range_partition_end : For range/int partition_type, specific where partition end
           - range_partition_interval : For range/int partition_type, specific partition interval
           example of table_parameter :
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'day', 
                }
                or
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'range',
                    'range_partition_start' : 1000,
                    'range_partition_end' : 3000,
                    'range_partition_interval' : 10,
                     
                }
        """
        # Set up
        table= bigquery.Table(table_name, self.client.schema_from_json(schema_path))
        # If no table_parameter -> Fail
        if table_parameter == None : raise Exception ("Please provide `table_parameter` parameter`")
        # Add parameters
        table.clustering_fields= table_parameter.get('clustering_fields',None)
        table.description= table_parameter.get('description',None)
        table.labels= table_parameter.get('labels',{})
        # Add partition parameters
        if table_parameter.get('partition_column',None) != None :
            partition_type= table_parameter['partition_type'].lower()
            if partition_type in ['range','int'] :
                # Create Range Partition
                table.range_partitioning = bigquery.RangePartitioning(
                    field= table_parameter['partition_column'],
                    range_=bigquery.PartitionRange(start=table_parameter['range_partition_start'], 
                                                   end=table_parameter['range_partition_end'], 
                                                   interval=table_parameter['range_partition_interval']),
                                                                      )
            else :
                # Create partition helper
                time_partition_dict= { 'day' : bigquery.TimePartitioningType.DAY,
                                       'month' : bigquery.TimePartitioningType.MONTH,
                                       'year' : bigquery.TimePartitioningType.YEAR  }
                # Create Time Partition
                table.time_partitioning= bigquery.TimePartitioning(
                    type_= time_partition_dict[partition_type],
                    field= table_parameter['partition_column'], 
                                                                   )
            table.require_partition_filter= table_parameter.get('require_partition_filter',None)
        # Create and return
        return self.client.create_table(table)
        
    def get_draft_schema(self, 
                         dataframe : DataFrame, 
                         schema_path= 'schema.json') :
        """
        A Function to get schema from the dataframe
        Args :
         - dataframe : A Pandas DataFrame to insert into GBQ
        Optional Args :
         - schema_path : An output path for schema, default 'schema.json'
        """
        assert self.dataset_id != None, "`dataset_id` must be setted to get draft schema"
        # Create Random String as Temp Table Name
        temp_table_name= ''.join(random.choice(string.ascii_lowercase) for _ in range(20))
        full_temp_name= f'{self.project_id}.{self.dataset_id}.{temp_table_name}'
        # Insert blank dataframe into BQ
        to_gbq(dataframe.head(0), full_temp_name, 
               project_id = self.project_id, credentials = self.credentials, 
               progress_bar= ~self.mute, if_exists = 'fail', )
        # Get Table and Schema into a file
        temp_table= self.client.get_table(full_temp_name)
        self.client.schema_to_json(temp_table.schema, schema_path)
        # Delete Table
        self.client.delete_table(full_temp_name, not_found_ok=True)
        return schema_path
    
    def table_exists_or_create(self, 
                               table_name : str, 
                               schema_path : str, 
                               table_parameter : dict, 
                               ignore_create_parameter : bool) :
        """
        A function to check if table exist,
        If not exist, will create a table as configured
        Args :
         - table_name : A target table to check or create
         - schema_path : A path to schema file
         - ignore_create_parameter : Will return False when the table doesn't exist and config files not input
         - table_parameter : A dict of GBQ parameter which contains
           - clustering_fields : A list of clustering fields
           - description : A description of the table
           - labels : A dict of table's labels
           - partition_column : A Partition Column's name
           - require_partition_filter : A boolean to require partition filter or not
           - partition_type : Type of table's partition, in range, int, day, month, year
           - range_partition_start : For range/int partition_type, specific where partition start
           - range_partition_end : For range/int partition_type, specific where partition end
           - range_partition_interval : For range/int partition_type, specific partition interval
           example of table_parameter :
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'day', 
                }
                or
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'range',
                    'range_partition_start' : 1000,
                    'range_partition_end' : 3000,
                    'range_partition_interval' : 10,
                     
                }
        """
        try:
            return self.client.get_table(table_name)
        except NotFound:
            if schema_path != None :
                return self.create_table(table_name, schema_path, table_parameter) 
            elif ignore_create_parameter :
                return False
            else : 
                raise Exception ("""Table not exists, please input `schema_path`, or set `ignore_create_parameter = True`""")

    def get_partition_dict(self, table) :
        # Time Partition
        if table.time_partitioning != None :
            partition_type= 'time'
            format_dict= {'DAY' : '%Y%m%d', 'MONTH' : '%Y%m', 'YEAR' : '%Y'}
            partition_col= table.time_partitioning.field
            partition_format= format_dict[table.time_partitioning.type_]
        # Range Partition
        elif table.range_partitioning != None :                
            partition_type= 'range'
            partition_col= table.range_partitioning.field
            partition_format= table.range_partitioning.range_.interval
        # No Partition
        else : return {'partition_type' : None}
        # Return Partition Type
        return {
                'partition_type' : partition_type,
                'partition_col' : partition_col,
                'partition_format' : partition_format
                }

    def get_partition_id_series(self, dataframe, partition_dict) :
        if partition_dict['partition_type'] == 'time' :
            return dataframe[partition_dict['partition_col']].dt.strftime(partition_dict['partition_format'])
        else :
            return dataframe[partition_dict['partition_col']].apply(lambda x : int(x/partition_dict['partition_format']))
        
    def partition_insert(self, 
                         dataframe : DataFrame, 
                         table_name : str, 
                         schema_path : str, 
                         table_parameter= None, 
                         ignore_create_parameter= False,) :
        """
        A function to insert a Pandas DataFrame into partition(s) of BigQuery's Table
        Args : 
         - dataframe : A Pandas DataFrame to insert into GBQ
         - table_name : A full GBQ's table name
         - schema_path : A path to table's schema file
         - ignore_create_parameter : Will return False when the table doesn't exist and config files not input, default False
         - table_parameter : A dict of GBQ parameter which contains
           - clustering_fields : A list of clustering fields
           - description : A description of the table
           - labels : A dict of table's labels
           - partition_column : A Partition Column's name
           - require_partition_filter : A boolean to require partition filter or not
           - partition_type : Type of table's partition, in range, int, day, month, year
           - range_partition_start : For range/int partition_type, specific where partition start
           - range_partition_end : For range/int partition_type, specific where partition end
           - range_partition_interval : For range/int partition_type, specific partition interval
           example of table_parameter :
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'day', 
                }
                or
                table_parameter = { 
                    'clustering_fields' : ['col1','col2'],
                    'description' : 'description',
                    'labels' : {'key1' : 'value1',},
                    'partition_column' : 'partition_col',
                    'require_partition_filter' : True,
                    'partition_type' : 'range',
                    'range_partition_start' : 1000,
                    'range_partition_end' : 3000,
                    'range_partition_interval' : 10,
                     
                }
        """
        # Check Table Exists
        table= self.table_exists_or_create(table_name, schema_path, table_parameter, ignore_create_parameter)
        # Get Partition Info
        if table != False : partition_dict= self.get_partition_dict(table)
        else : partition_dict = {'partition_type' : None}
        # If without Partition
        if partition_dict['partition_type'] == None :
            return self.insert_replace(dataframe= dataframe, table_name= table_name, schema_path= schema_path)
        # For Partition Table
        partition_id_series= self.get_partition_id_series(dataframe= dataframe, partition_dict= partition_dict)
        unique_id= list(partition_id_series.unique())
        for run_id in unique_id :
            table_id = f"{table_name}${run_id}"
            if not self.mute : print(table_id)
            self.insert_replace(dataframe[(partition_id_series == run_id)], 
                                table_name = table_id, 
                                schema_path= schema_path)
