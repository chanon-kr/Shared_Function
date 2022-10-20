# API as a framework
from py_topping.run_pipeline.api import lazy_API
# To Connect Database
from py_topping.data_connection.database import lazy_SQL
# To send LINE message when fail
from py_topping.general_use import lazy_LINE


## Create Custom Callback when fail
def callback(x) :
    # Send LINE when fail
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_LINE.ipynb
    callback_line = lazy_LINE(token = '{your token}')
    callback_line.send(str(x))

## Create FastAPI
# Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/api.ipynb
app = lazy_API(title = 'DB to DB' 
             , version = 1
             , description= 'Database to Database'
             , username = 'user'
             , password = 'password'
             , api_weak_authen= True
             , callback= callback
             )

## Create Transform Data Function
def transform_data(df) :
    return df

## Create Function
def db_to_db(x) :
    # Database connection
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb
    ## Soruce
    db_source = lazy_SQL( sql_type = '{Your SQL Type}'
                        , host_name = '{YOUR HOST NAME or ID Address}'
                        , database_name = '{Your Database Name}'
                        , user = '{Your Username}'
                        , password = '{Your Password}')
    df = db_source.read('{Source Table Name}')
    ## Data Transform (if needed)
    df = transform_data(df)
    ## Destination
    db_destination = lazy_SQL( sql_type = '{Your SQL Type}'
                             , host_name = '{YOUR HOST NAME or ID Address}'
                             , database_name = '{Your Database Name}'
                             , user = '{Your Username}'
                             , password = '{Your Password}')
    db_destination.dump_replace(  df_in = df
                                , table_name_in= '{Destination Table Name}'
                                , list_key = ['key_column'])

## Insert Function into the App
app.create_post(function = db_to_db
              , name = 'DB to DB'
              , tag = ['db','db']
              , example = {}
              , callback = 'default')

## Run App
if __name__ == '__main__' :
    app.run( port = 8080
           , host = '0.0.0.0')