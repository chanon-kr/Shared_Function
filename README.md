# py_topping, a topping on some already great libraries
## I create this library to make my team's projects easier and have VCS

PyPi Project Page : (https://pypi.org/project/py-topping)
<br><br>To Install a Stable Version<br>
```python
pip install py-topping
```
<br>To Install a Newest Version<br>
```python
pip install git+https://github.com/chanon-kr/Shared_Function.git
```
<br>
You could see samples of how to use this library inside the samples folder in github<br>
(https://github.com/chanon-kr/Shared_Function)
<br><br>

***
## data_connection
***
### sharepoint
  - to download file from SP365 or SP on prim
  - to read csv/excel from SP365 as pandas dataframe
  - to download List as csv or pandas dataframe from SP365
  - upload file to SP365 or SP on prim

***
### database
  - Work with SQL Server, MySQL and PostGreSQL
  - To read view, table or store procedure as pandas dataframe 
  - To insert pandas dataframe into SQL with option for existing row :
    - Replace same key(s) with new from dataframe with option to use ">" or "<" condition
    - Add Only row with non-existing key(s)
    - Replace whole table
  - Can parallel insert by using dask's delayed
  - Can't read Store Procedure in PostGreSQL, will solve this in later version
  - Will working with Oracle Database in later version

***
## general_use
***
### general_use
  - To send email with python
  - To logging in csv file

***
## run_pipeline
***
### run_pipeline
  - to run your python or notebook script 
    - with logging as a csv
    - with logging into sql table
    - with emailing the log when have/don't have errors