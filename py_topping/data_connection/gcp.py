import os
from google.cloud import storage
from glob import glob

class lazy_gcs :
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