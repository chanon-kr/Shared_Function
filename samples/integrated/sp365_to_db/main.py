# API as a framework
from py_topping.run_pipeline.api import lazy_API
# To Connect Database
from py_topping.data_connection.database import lazy_SQL
# To Connect Sharepoint 365
from py_topping.data_connection.sharepoint import lazy_SP365
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
app = lazy_API(title = 'SP365 to DB' 
             , version = 1
             , description= 'Sharepoint 365 to Database'
             , username = 'user'
             , password = 'password'
             , api_weak_authen= False 
             , callback= callback
             )

## Create Transform Data Function
def transform_data(df) :
    return df

## Create Function
def sp365_to_db(x) :
    # Sharepoint 365 connection
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/sharepoint.ipynb
    sp = lazy_SP365(site_url = '{YOUR Site UURL}'
                  , client_id = '{Your Client ID}'
                  , client_secret = '{Your Client Secret}')
    df = sp.download(sharepoint_location = '{File Location on Sharepoint}'
                   , as_dataframe = True)
    ## Data Transform (if needed)
    df = transform_data(df)
    # Database connection
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb
    db = lazy_SQL( sql_type = '{Your SQL Type}'
                             , host_name = '{YOUR HOST NAME or ID Address}'
                             , database_name = '{Your Database Name}'
                             , user = '{Your Username}'
                             , password = '{Your Password}')
    db.dump_replace(df_in = df
                  , table_name_in= '{Destination Table Name}'
                  , list_key = ['key_column'])

## Insert Function into the App
app.create_post(function = sp365_to_db
              , name = 'SP365 to DB'
              , tag = ['sp365','db']
              , example = {}
              , callback = 'default')

## Run App
if __name__ == '__main__' :
    app.run( port = 8080
           , host = '0.0.0.0')