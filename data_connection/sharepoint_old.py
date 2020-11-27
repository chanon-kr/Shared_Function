from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import os
import csv
from io import BytesIO
import pandas as pd

class da_tran_SP365:
    def __init__(self, site_url, client_id, client_secret):
        """Create connection to Sharepoint Site"""
        self.site_url = site_url
        self.client_id = client_id
        self.client_secret = client_secret
        ctx_auth = AuthenticationContext( self.site_url)
        ctx_auth.acquire_token_for_app( self.client_id , self.client_secret )
        self.ctx = ClientContext(self.site_url, ctx_auth)
        web = self.ctx.web
        self.ctx.load(web)
        self.ctx.execute_query()
        print('Authen to {} OK'.format(web.properties['Title']))

    def download(self, sharepoint_location, local_location = '', as_dataframe = False, sheet_name = None) :
        """Download file from sharepoint or Read Excel/csv from sharepoint as pd dataframe"""
        response = File.open_binary(self.ctx, sharepoint_location ) # save file from sharepoint as binary
        if str(response.status_code) == '200' :
            if as_dataframe :
                toread = BytesIO()
                toread.write(response.content)  # pass your `decrypted` string as the argument here
                toread.seek(0)  # reset the pointer
                if '.csv' in sharepoint_location : return pd.read_csv(toread)
                else : return pd.read_excel(toread, sheet_name = sheet_name)
            else :
                with open(local_location, "wb") as local_file: 
                    local_file.write(response.content) # write in your pc
                print('Download OK File')
        else :
            raise Exception('Cannot Download File')

    def upload(self, sharepoint_location, local_location):
        """Upload file to sharepoint"""
        with open(local_location, 'rb') as content_file: 
            file_content = content_file.read() # read file from your pc
        dir_, name = os.path.split(sharepoint_location)
        self.ctx.web.get_folder_by_server_relative_url(dir_).upload_file(name, file_content).execute_query() # upload file to sharepoint

    def read_list(self, list_title, local_location = '', as_dataframe = False):
        """Read list from Sharepoint and Download as csv"""
        list_to_export = self.ctx.web.lists.get_by_title(list_title)
        list_items = list_to_export.items.get().execute_query()
        self.list_items = list_items
        if len(list_items) == 0:
            print("No data found")
        else :
            if as_dataframe :
                pass
                #return
            else :
                with open(local_location, 'w',newline='') as fh:
                    fields = list_items[0].properties.keys()
                    w = csv.DictWriter(fh, fields)
                    w.writeheader()
                    for item in list_items:
                        w.writerow(item.properties)
                print('Download List OK')


# # Download File
def Download_SP365(dict_in) :
    """Download file from sharepoint as described in dict_in"""
    file_part_sp = dict_in['folder_sp'] + dict_in['file_name_sp'] # create path to sharepoint
    file_part_local = dict_in['folder_local'] + dict_in['file_name_local'] # create path to your pc
    url = dict_in['site_url'] # get site url
    ctx_auth = AuthenticationContext(url)
    ctx_auth.acquire_token_for_app(dict_in['cli_id']
                                   , dict_in['cli_secret'])
    ctx = ClientContext(url, ctx_auth)
    response = File.open_binary(ctx, file_part_sp ) # save file from sharepoint as binary
    with open(file_part_local, "wb") as local_file: 
        local_file.write(response.content) # write in your pc
    return True

# # Sample of Download_SP365
"""
import_sp_in = {
                'file_name_sp' : 'BAHBAHBAH' , # your file name on sharepoint
                'file_name_local' : 'BAHBAHBAH' , # the file name you want to keep on your pc
                'folder_sp' : '/sites/BAHBAHBAH' , # the subfolder name on sharepoint
                'folder_local' : 'BAHBAHBAH' , # the folder name you want to keep file on your pc
                'site_url' : 'BAHBAHBAH/sites/BAHBAHBAH' , # site's URL
                'cli_id' : 'BAHBAHBAH' , # Client ID Generated from Sharepoint
                'cli_secret' : 'BAHBAHBAH' # Client Secret Generated from Sharepoint
                }

Download_SP365(import_sp_in)
"""

# # Upload File
def Upload_SP365(dict_in):
    """Upload file to sharepoint as described in dict_in"""
    url = dict_in['site_url'] # get site url
    localpath = dict_in['folder_local'] + dict_in['file_name_local']  # create path to your pc
    remotepath = dict_in['folder_sp'] + dict_in['file_name_sp']  # create path to sharepoint
    ctx_auth = AuthenticationContext(url)
    ctx_auth.acquire_token_for_app(dict_in['cli_id']
                                       , dict_in['cli_secret'])
    ctx = ClientContext(url, ctx_auth) 
    with open(localpath, 'rb') as content_file: 
        file_content = content_file.read() # read file from your pc
    dir_, name = os.path.split(remotepath)
    file = ctx.web.get_folder_by_server_relative_url(dir_).upload_file(name, file_content).execute_query() # upload file to sharepoint
    return True

# # Sample of Upload_SP365
"""
export_sp_out = {
                'file_name_sp' : 'BAHBAHBAH' , # your file name on sharepoint
                'file_name_local' : 'BAHBAHBAH' , # the file name you want to keep on your pc
                'folder_sp' : '/sites/BAHBAHBAH' , # the subfolder name on sharepoint
                'folder_local' : 'BAHBAHBAH' , # the folder name you want to keep file on your pc
                'site_url' : 'BAHBAHBAH/sites/BAHBAHBAH' , # site's URL
                'cli_id' : 'BAHBAHBAH' , # Client ID Generated from Sharepoint
                'cli_secret' : 'BAHBAHBAH' # Client Secret Generated from Sharepoint
                }

Upload_SP365(export_sp_out)
"""