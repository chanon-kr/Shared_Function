from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import os

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