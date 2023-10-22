import os
from google.cloud import storage
from glob import glob
from datetime import timedelta

class lazy_GCS :
    def __init__(self, project_id, bucket_name,credential = '') :
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials = credential

    def list_folder(self, bucket_folder ,as_blob = False, include_self= False
                    , get_file = True, get_folder = False, all_file = False , debug = False) :
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # list all objects in the directory
        blobs = bucket.list_blobs(prefix=bucket_folder)
        files, folders = [], []
        for blob in blobs :
            if debug : print(f"{blob} -> {blob.name}")
            if (blob.name.endswith("/")) : 
                if (blob.name == f'{bucket_folder}/') : source = [blob]
                else : folders.append(blob) # Folder
            else : files.append(blob) # File
        if not all_file :
            for folder in folders : files = [file for file in files if not file.name.startswith(folder.name)]
        out_put = [file for file in files if get_file] + [folder for folder in folders if get_folder]
        if include_self : out_put += source
        if as_blob : return [blob for blob in out_put]
        else : return [blob.name for blob in out_put]
        
    def generate_signed_url(self, bucket_name, object_name, blob = None, expiration = 60):
        """
        Reference from https://cloud.google.com/storage/docs/access-control/signing-urls-with-helpers#code-samples
        """
        if blob == None :
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
        url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration),
                method="GET")
        return url

    def download(self, bucket_file, local_file):
        """Need 2 permissions Storage Legacy Object Reader and Storage Legacy Bucket Writer"""
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(bucket_file)
        # Download the file to a destination
        blob.download_to_filename(local_file)

    def download_folder(self, bucket_folder, local_folder, create_folder = False):
        """Need 2 permissions Storage Legacy Object Reader and Storage Legacy Bucket Writer"""
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        if not os.path.isdir(local_folder) :
            if create_folder :
                os.mkdir(local_folder)
            else : print('Local Folder Not Found, Create Folder Manual or add create_folder = True')
        # Initialise a client
        client = storage.Client(self.project_id)
        bucket = client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=bucket_folder)  # Get list of files
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            filename = blob.name.split('/')[-1]
            blob.download_to_filename(os.path.join(local_folder,filename))  # Download

    def upload(self, bucket_file, local_file , remove_file = False, generate_signed_url = False, url_expiration = 60 ):
        """Need a permission Storage Legacy Bucket Writer"""
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(bucket_file)
        # Download the file to a destination
        blob.upload_from_filename(local_file)
        if remove_file : os.remove(local_file)
        if generate_signed_url :
            return self.generate_signed_url(  bucket_name = self.bucket_name
                                            , object_name = bucket_file
                                            , blob = blob
                                            , expiration = url_expiration)


    def upload_folder(self, bucket_folder, local_folder , remove_file = False, generate_signed_url = False, url_expiration = 60 ):
        """Need a permission Storage Legacy Bucket Writer"""
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create List of signed_url if used
        if generate_signed_url : link_list = []
        # Upload File from List
        for i in glob(local_folder + '/*') :
            # Create File Name
            bucket_file = i.replace(local_folder,bucket_folder).replace('\\','/')
            # Create a blob object from the filepath
            blob = bucket.blob(bucket_file)
            # Download the file to a destination
            blob.upload_from_filename(i)
            if remove_file : os.remove(i)
            if generate_signed_url :
                link_list.append(self.generate_signed_url(  bucket_name = self.bucket_name
                                            , object_name = bucket_file
                                            , blob = blob
                                            , expiration = url_expiration))
        if generate_signed_url : return link_list

    def delete(self, bucket_file):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        client = storage.Client(self.project_id)
        bucket = client.get_bucket(self.bucket_name)
        bucket.delete_blob(bucket_file)
        # blob = bucket.blob(bucket_file)
        # blob.delete()
            
    def delete_folder(self, bucket_folder , delete_folder = False, deep_delete = False, debug = False):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Get blobs
        blobs = self.list_folder(bucket_folder = bucket_folder 
                                , as_blob = True, get_file = True, get_folder = deep_delete
                                , all_file = deep_delete, include_self = False, debug= debug)
        if debug : 
            for i in blobs : print(blob)
        for blob in blobs : blob.delete()
        if delete_folder : self.delete(bucket_file= bucket_folder + '' if bucket_folder.endswith('/') else '/')

    def copy(self, source_bucket_file, destination_bucket_name, destination_bucket_file = ''):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(source_bucket_file)
        # Download the file to a destination
        destination_bucket = client.get_bucket(destination_bucket_name) # Destination Bucket
        if destination_bucket_file == '' : destination_file_name = source_bucket_file
        else : destination_file_name = destination_bucket_file
        bucket.copy_blob(
              blob= blob
            , destination_bucket = destination_bucket
            , new_name = destination_file_name,
        )

    def copy_folder(self, source_bucket_folder, destination_bucket_name, destination_bucket_folder = ''):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Initialise a client
        client = storage.Client(self.project_id)
        bucket = client.get_bucket(self.bucket_name) # Source Bucket
        destination_bucket = client.get_bucket(destination_bucket_name) # Destination Bucket
        blobs = bucket.list_blobs(prefix=source_bucket_folder)  # Get list of files
        for blob in blobs:
            if blob.name.endswith("/"): continue
            source_filename = blob.name
            if destination_bucket_folder == '' : destination_file_name = source_filename
            else : destination_file_name = source_filename.replace(source_bucket_folder, destination_bucket_folder)
            bucket.copy_blob(
                  blob= blob
                , destination_bucket = destination_bucket
                , new_name= destination_file_name,
            )


class da_tran_bucket:
    def __init__(self, project_id, bucket_name,credential = '') :
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials = credential

    def download(self, bucket_file, local_file):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(bucket_file)
        # Download the file to a destination
        blob.download_to_filename(local_file)

    def download_folder(self, bucket_folder, local_folder, create_folder = False):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        if not os.path.isdir(local_folder) :
            if create_folder :
                os.mkdir(local_folder)
            else : print('Local Folder Not Found, Create Folder Manual or add create_folder = True')
        # Initialise a client
        client = storage.Client(self.project_id)
        bucket = client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=bucket_folder)  # Get list of files
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            filename = blob.name.split('/')[-1]
            blob.download_to_filename(os.path.join(local_folder,filename))  # Download

    def upload(self, bucket_file, local_file , remove_file = False):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(bucket_file)
        # Download the file to a destination
        blob.upload_from_filename(local_file)
        if remove_file : os.remove(local_file)

    def upload_folder(self, bucket_folder, local_folder , remove_file = False):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # Upload File from List
        for i in glob(local_folder + '/*') :
            # Create File Name
            bucket_file = i.replace(local_folder,bucket_folder).replace('\\','/')
            # Create a blob object from the filepath
            blob = bucket.blob(bucket_file)
            # Download the file to a destination
            blob.upload_from_filename(i)
            if remove_file : os.remove(i)
