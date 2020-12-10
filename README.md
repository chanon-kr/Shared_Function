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
By the way, I'm not provide required libraries for you because of the topping concept,<br> 
I don't think it's a good idea to install every libraries for everyone.

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
  - to read view, table or store procedure as pandas dataframe 
  - to insert pandas dataframe into SQL with option for existing row :
    - Replace same key(s) with new from dataframe with option to use ">" or "<" condition
    - Add Only row with non-existing key(s)
    - Replace whole table
  - Can't read Store Procedure in PostGreSQL, will solve this in later version
  - Will working with Oracle Database in later version

***
## general_use
***
### general_use
  - to send email with python
  - to logging in csv file