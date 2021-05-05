import os
from google.cloud import storage


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

    def download_folder(self, bucket_folder, local_folder):
        if self.credentials == '' : pass
        else : os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.credentials

        # Initialise a client
        client = storage.Client(self.project_id)
        bucket = client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=bucket_folder)  # Get list of files
        for blob in blobs:
            filename = blob.name.split('/')[-1]
            blob.download_to_filename(local_folder + '/'+ filename)  # Download

    def upload(self, bucket_file, local_file):
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