# API as a framework
from py_topping.run_pipeline.api import lazy_API
# To Connect Database
from py_topping.data_connection.database import lazy_SQL
# To connect Google Cloud Storage
from py_topping.data_connection.gcp import lazy_GCS
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
app = lazy_API(title = 'DB to GCS' 
             , version = 1
             , description= 'Database to Google Cloud Storage'
             , username = 'user'
             , password = 'password'
             , api_weak_authen= True
             , callback= callback
             )

## Create Function
def db_to_gcs(x) :
    # Database Part
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb
    db = lazy_SQL( sql_type = '{Your SQL Type}'
                 , host_name = '{YOUR HOST NAME or ID Address}'
                 , database_name = '{Your Database Name}'
                 , user = '{Your Username}'
                 , password = '{Your Password}')
    df = db.read('{Table Name}')
    local_path = '{File Part}'
    df.to_csv(local_path, index = False)
    # Google Cloud Storage Part 
    # Read more in this link : https://github.com/chanon-kr/Shared_Function/blob/main/samples/gcp.ipynb
    gcs = lazy_GCS( project_id = '{Your Project ID}'
                  , bucket_name = '{Your Bucket Name}'
                  , credential = '{Path to Your Credential Json File (Optional)}')
    bucket_path = '{}'
    gcs.upload( bucket_file = bucket_path
              , local_file= local_path
              , remove_file = True)
    return bucket_path

## Insert Function into the App
app.create_post(function = db_to_gcs
                , name = 'DB to GCS'
                , tag = ['db','gcs']
                , example = {}
                , callback = 'default')

## Run App
if __name__ == '__main__' :
    app.run( port = 8080
           , host = '0.0.0.0')