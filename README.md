# Version Control for Shared Functions used within my team

## data_connection
### sharepoint
* da_tran_SP365, a class that use to interact with SharePoint365
  - download file from SP365
  - read csv/excel from SP365 as pandas dataframe
  - download List as csv file from SP365
  - read List from SP365 as pandas dataframe (Coming Soon)
  - upload file to SP365


### database
* da_tran_SQL, a class that use to interact with SQL database
  - read view or table as pandas dataframe
  - insert pandas dataframe into SQL with option for existing row :
    - Replace same key(s) with new from dataframe
    - Only add non-existing key(s)
    - ">" or "<" condition (Coming Soon)