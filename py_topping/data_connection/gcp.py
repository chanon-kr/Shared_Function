import os
from google.cloud import storage
from glob import glob
import binascii
import collections
import datetime
import hashlib
import sys

# pip install google-auth
from google.oauth2 import service_account
# pip install six
import six
from six.moves.urllib.parse import quote

class lazy_GCS :
    def __init__(self, project_id, bucket_name,credential = '') :
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials = credential
        
    def generate_signed_url(self, service_account_file, bucket_name, object_name,
                        subresource=None, expiration=604800, http_method='GET',
                        query_parameters=None, headers=None):
        """Reference from https://github.com/GoogleCloudPlatform/python-docs-samples/tree/e7379a8b33ab29b354292d370c3655f481bf3be5/storage/signed_urls"""
        if expiration > 604800:
            raise ValueError('Expiration Time can\'t be longer than 604800 seconds (7 days).')
        escaped_object_name = quote(six.ensure_binary(object_name), safe=b'/~')
        canonical_uri = f'/{escaped_object_name}'
        
        datetime_now = datetime.datetime.now(tz=datetime.timezone.utc)
        request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
        datestamp = datetime_now.strftime('%Y%m%d')

        google_credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client_email = google_credentials.service_account_email
        credential_scope = f'{datestamp}/auto/storage/goog4_request'
        credential = f'{client_email}/{credential_scope}'

        if headers is None: headers = dict()
        host = f'{bucket_name}.storage.googleapis.com'
        headers['host'] = host

        canonical_headers = ''
        ordered_headers = collections.OrderedDict(sorted(headers.items()))
        for k, v in ordered_headers.items():
            lower_k = str(k).lower()
            strip_v = str(v).lower()
            canonical_headers += f'{lower_k}:{strip_v}\n'

        signed_headers = ''
        for k, _ in ordered_headers.items():
            lower_k = str(k).lower()
            signed_headers += f'{lower_k};'
        signed_headers = signed_headers[:-1]  # remove trailing ';'

        if query_parameters is None: query_parameters = dict()
        query_parameters['X-Goog-Algorithm'] = 'GOOG4-RSA-SHA256'
        query_parameters['X-Goog-Credential'] = credential
        query_parameters['X-Goog-Date'] = request_timestamp
        query_parameters['X-Goog-Expires'] = expiration
        query_parameters['X-Goog-SignedHeaders'] = signed_headers
        if subresource:
            query_parameters[subresource] = ''

        canonical_query_string = ''
        ordered_query_parameters = collections.OrderedDict(
            sorted(query_parameters.items()))
        for k, v in ordered_query_parameters.items():
            encoded_k , encoded_v = quote(str(k), safe='') , quote(str(v), safe='') # C Modify
            canonical_query_string += f'{encoded_k}={encoded_v}&'
        canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

        canonical_request = '\n'.join([http_method,
                                       canonical_uri,
                                       canonical_query_string,
                                       canonical_headers,
                                       signed_headers,
                                       'UNSIGNED-PAYLOAD'])

        canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

        string_to_sign = '\n'.join(['GOOG4-RSA-SHA256',
                                    request_timestamp,
                                    credential_scope,
                                    canonical_request_hash])

        # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
        signature = binascii.hexlify(
            google_credentials.signer.sign(string_to_sign)
        ).decode()

        scheme_and_host = f'https://{host}'
        signed_url = f'{scheme_and_host}{canonical_uri}?{canonical_query_string}&x-goog-signature={signature}'

        return signed_url

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
            return self.generate_signed_url(  service_account_file = self.credentials
                                            , bucket_name = self.bucket_name
                                            , object_name = bucket_file
                                            , subresource=None
                                            , expiration=url_expiration
                                            , http_method='GET'
                                            , query_parameters=None
                                            , headers=None)

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
                link_list.append(self.generate_signed_url(  service_account_file = self.credentials
                                                , bucket_name = self.bucket_name
                                                , object_name = bucket_file
                                                , subresource=None
                                                , expiration=url_expiration
                                                , http_method='GET'
                                                , query_parameters=None
                                                , headers=None))
        if generate_signed_url : return link_list

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
