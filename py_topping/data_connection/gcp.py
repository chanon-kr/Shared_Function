import os
from google.cloud import storage
from glob import glob
from datetime import timedelta

class lazy_GCS :
    def __init__(self, project_id, bucket_name,credential = '') :
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials = credential

    def list_folder(self, bucket_folder ,as_blob = False):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Initialise a client
        client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = client.get_bucket(self.bucket_name)
        # list all objects in the directory
        blobs = bucket.list_blobs(prefix=bucket_folder)
        if as_blob : return [blob for blob in blobs]
        else : return [blob.name for blob in blobs]
        
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
        blob = bucket.blob(bucket_file)
        blob.delete()
            
    def delete_folder(self, bucket_folder):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials
        # Get blobs
        blobs = self.list_folder(bucket_folder = bucket_folder ,as_blob = True)
        for blob in blobs : blob.delete()

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
