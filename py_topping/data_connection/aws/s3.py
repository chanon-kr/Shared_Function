import os
from boto3.session import Session
from glob import glob
from datetime import timedelta

class lazy_S3 :
    def __init__(self, 
                 region_name : str, 
                 bucket_name : str,
                 aws_access_key_id : str,
                 aws_secret_access_key : str
                 ) :
        session= Session(aws_access_key_id= aws_access_key_id, 
                         aws_secret_access_key= aws_secret_access_key)
        self.client= session.client('s3')
        self.bucket_name = bucket_name

    def list_folder(self, bucket_folder ,as_blob = False, include_self= False
                    , get_file = True, get_folder = False, all_file = False , debug = False) :
        r= self.client.list_objects_v2(Bucket= self.bucket_name).get('Contents',[])
        if as_blob : return [blob for blob in r if blob['Key'].startswith(bucket_folder)]
        else : return [blob['Key'] for blob in r if blob['Key'].startswith(bucket_folder)]
        

    def download(self, bucket_file, local_file):
        self.client.download_file(Bucket= self.bucket_name,
                                 Filename= local_file,
                                 Key= bucket_file)

    def download_folder(self, bucket_folder, local_folder, create_folder = False):
        return None

    def upload(self, bucket_file, local_file , remove_file = False, generate_signed_url = False, url_expiration = 60 ):
        self.client.upload_file(Bucket= self.bucket_name,
                                Filename= local_file,
                                Key= bucket_file)
        if remove_file : os.remove(local_file)


    def upload_folder(self, bucket_folder, local_folder , remove_file = False, generate_signed_url = False, url_expiration = 60 ):
        return None
    
    def delete(self, bucket_file):
        self.client.delete_object(Bucket= self.bucket_name,
                                  Key= bucket_file)
            
    def delete_folder(self, bucket_folder , delete_folder = False, deep_delete = False, debug = False):
        return None

    def copy(self, source_bucket_file, destination_bucket_name, destination_bucket_file = ''):
        return None

    def copy_folder(self, source_bucket_folder, destination_bucket_name, destination_bucket_folder = ''):
        return None

