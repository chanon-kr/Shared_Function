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
        print('Authen OK')

    def create_link(self, file_link):
        file_name = file_link.split('/')[-1]
        link = '/sites' + file_link.replace(file_name,'').split('sites')[-1]
        link += file_name.split('?')[0]
        return link

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
        """Read list from Sharepoint and Download as csv or pandas dataframe"""
        list_to_export = self.ctx.web.lists.get_by_title(list_title)
        list_items = list_to_export.items.get().execute_query()
        self.list_items = list_items
        if len(list_items) == 0: print("No data found")
        else :
            if as_dataframe :
                df_in = pd.DataFrame()
                for i in list_items : df_in = df_in.append(pd.DataFrame(data = [list(i.properties.values())], columns = i.properties.keys()))
                return df_in
            elif local_location == '' : raise Exception("Please input your savefile's name")
            else :
                with open(local_location, 'w',newline='') as fh:
                    fields = list_items[0].properties.keys()
                    w = csv.DictWriter(fh, fields)
                    w.writeheader()
                    for item in list_items: w.writerow(item.properties)
                print('Download List OK')